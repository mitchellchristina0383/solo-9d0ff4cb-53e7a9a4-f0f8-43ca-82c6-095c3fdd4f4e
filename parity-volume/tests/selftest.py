"""End-to-end self tests for the parity file volume.

Run with:  PV_EXTRA_SYS_PATH=/tmp/pylibs python -I tests/selftest.py

A real uvicorn server is started in a background thread (same process, so the
Volume object is reachable in-process for fault injection); all traffic goes
over real HTTP against temporary on-disk directories.
"""

from __future__ import annotations

import concurrent.futures
import io
import os
import shutil
import sys
import tempfile
import threading
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
_extra = os.environ.get("PV_EXTRA_SYS_PATH")
if _extra:
    for p in reversed(_extra.split(os.pathsep)):
        sys.path.insert(0, p)

_TMP = Path(tempfile.mkdtemp(prefix="pv-selftest-"))
os.environ["PV_DATA_DIR"] = str(_TMP / "data")
os.environ["PV_DB_PATH"] = str(_TMP / "data" / "meta" / "volume.db")
os.environ["PV_CHUNK_SIZE"] = str(64 * 1024)  # 64 KiB: many stripes quickly

import httpx  # noqa: E402
import uvicorn  # noqa: E402

import app.main as webapp  # noqa: E402
from app import storage  # noqa: E402
from app.volume import Volume  # noqa: E402

PASS = 0
FAIL = 0
PORT = int(os.environ.get("PV_TEST_PORT", "8765"))
BASE = f"http://127.0.0.1:{PORT}"
DATA_DIR = _TMP / "data"
DB_PATH = _TMP / "data" / "meta" / "volume.db"
CHUNK = 64 * 1024


def check(name: str, condition: bool, detail: str = "") -> None:
    global PASS, FAIL
    if condition:
        PASS += 1
        print(f"  [PASS] {name}")
    else:
        FAIL += 1
        print(f"  [FAIL] {name} {detail}")


def section(title: str) -> None:
    print(f"\n=== {title} ===")


def client() -> httpx.Client:
    return httpx.Client(base_url=BASE, timeout=30)


def vol() -> Volume:
    return webapp.volume


def upload(cli: httpx.Client, data: bytes, name: str) -> str:
    r = cli.post("/api/files",
                 files={"file": (name, io.BytesIO(data), "application/octet-stream")})
    r.raise_for_status()
    return r.json()["file"]["id"]


def download(cli: httpx.Client, file_id: str) -> bytes:
    r = cli.get(f"/api/files/{file_id}")
    if r.status_code != 200:
        raise AssertionError(f"download {file_id} -> {r.status_code} {r.text}")
    return r.content


def block_sizes(file_id: str, stripe_index: int) -> list[int]:
    """Sorted on-disk sizes of the three block files of a stripe."""
    return sorted(
        os.path.getsize(storage.block_path(vol().roots[b["disk_index"]],
                                           b["relpath"]))
        for b in vol().db.get_blocks(file_id, stripe_index)
    )


def wait_rebuild(timeout: float = 60.0) -> dict:
    """Wait for the most recently created rebuild job to reach a terminal state."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        job = vol().db.last_rebuild_job()
        if job is not None and job["state"] != "running":
            return job
        time.sleep(0.05)
    raise TimeoutError("rebuild did not finish")


def restart_service() -> None:
    """Simulate a full service restart in-process."""
    old = webapp.volume
    old.shutdown()
    webapp.volume = Volume(DATA_DIR, DB_PATH, chunk_size=CHUNK)
    webapp.volume.start()


def main() -> int:
    config = uvicorn.Config("app.main:app", host="127.0.0.1", port=PORT,
                            log_level="warning")
    server = uvicorn.Server(config)
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    # Wait for the HTTP listener.
    for _ in range(100):
        try:
            httpx.get(f"{BASE}/api/status", timeout=1)
            break
        except httpx.TransportError:
            time.sleep(0.1)

    try:
        with client() as cli:
            # ---- startup ----------------------------------------------
            section("启动与空状态")
            st = cli.get("/api/status").json()
            check("三块盘初始在线",
                  [d["state"] for d in st["disks"]] == ["online"] * 3)
            check("初始无文件", cli.get("/api/files").json()["files"] == [])

            # ---- uploads ----------------------------------------------
            section("上传与完整性")
            payload_a = os.urandom(300_000)      # 5 stripes, last partial
            payload_b = os.urandom(CHUNK * 3 + 7)
            id_a = upload(cli, payload_a, "alpha.bin")
            id_b = upload(cli, payload_b, "beta.bin")
            id_e = upload(cli, b"", "empty.bin")
            check("下载 alpha 内容一致", download(cli, id_a) == payload_a)
            check("下载 beta 内容一致", download(cli, id_b) == payload_b)
            check("空文件可下载", download(cli, id_e) == b"")
            check("文件列表只含已确认文件",
                  {f["name"] for f in cli.get("/api/files").json()["files"]}
                  == {"alpha.bin", "beta.bin", "empty.bin"})
            check("sha256 已记录",
                  all(f["sha256"] for f in cli.get("/api/files").json()["files"]))

            block_dirs = list(_TMP.glob("data/disk*/blocks"))
            counts = [sum(1 for _ in d.rglob("*.block")) for d in block_dirs]
            check("三块盘上都有块文件", all(c > 0 for c in counts), str(counts))

            # ---- single disk failure for each disk --------------------
            section("单盘下线后读取（逐盘演练）")
            for dead in range(3):
                r = cli.post(f"/api/disks/{dead}/offline")
                check(f"disk{dead} 下线成功", r.status_code == 200)
                check(f"disk{dead} 下线后 alpha 可恢复",
                      download(cli, id_a) == payload_a)
                check(f"disk{dead} 下线后 beta 可恢复",
                      download(cli, id_b) == payload_b)

                # ---- two-disk failure is explicitly unrecoverable -----
                other = (dead + 1) % 3
                vol().db.set_disk_state(other, "offline")  # fault injection
                r = cli.get(f"/api/files/{id_a}")
                check("两盘失效明确报不可恢复(503 unrecoverable)",
                      r.status_code == 503
                      and r.json()["error"] == "unrecoverable", r.text)
                vol().db.set_disk_state(other, "online")

                # ---- replace + rebuild --------------------------------
                section(f"disk{dead} 换空盘与重建")
                r = cli.post(f"/api/disks/{dead}/replace")
                check(f"disk{dead} 换盘并启动重建", r.status_code == 200, r.text)
                job = wait_rebuild()
                check("重建任务成功", job.get("state") == "done", str(job))
                check("重建后磁盘回到 online",
                      vol().db.disk_state(dead) == "online")
                check("重建后 alpha 内容一致", download(cli, id_a) == payload_a)
                check("重建后 beta 内容一致", download(cli, id_b) == payload_b)

                all_three = True
                for f in (id_a, id_b):
                    for s in vol().db.get_stripes(f):
                        for b in vol().db.get_blocks(f, s["stripe_index"]):
                            if not storage.block_exists(
                                    vol().roots[b["disk_index"]], b["relpath"]):
                                all_three = False
                check("重建后每条条纹三盘齐备", all_three)

            # ---- rebuild interleaved with upload ----------------------
            section("重建与上传交错：新条带不遗漏")
            cli.post("/api/disks/1/offline").raise_for_status()
            r = cli.post("/api/disks/1/replace")
            check("重建已启动", r.status_code == 200, r.text)
            new_ids, new_payloads = [], {}
            for k in range(4):
                p = os.urandom(150_000 + k * 1234)
                fid = upload(cli, p, f"concurrent-{k}.bin")
                new_ids.append(fid)
                new_payloads[fid] = p
                check(f"并发上传 {k} 立即可下载", download(cli, fid) == p)
            job = wait_rebuild()
            check("交错场景重建成功", job.get("state") == "done", str(job))
            check("重建收尾扫描确认无缺失条带",
                  vol()._stripes_missing_on_disk(1) == [])
            for fid in new_ids:
                check(f"新文件 {fid[:8]} 重建后可读且容错",
                      download(cli, fid) == new_payloads[fid])

            cli.post("/api/disks/2/offline").raise_for_status()
            check("再坏一盘，新文件仍可下载",
                  all(download(cli, fid) == new_payloads[fid] for fid in new_ids))
            check("旧文件在再坏一盘时仍可下载",
                  download(cli, id_a) == payload_a
                  and download(cli, id_b) == payload_b)
            cli.post("/api/disks/2/replace").raise_for_status()
            check("第二次重建成功", wait_rebuild().get("state") == "done")

            # ---- interrupted upload: unconfirmed file invisible --------
            section("上传中断 / 崩溃恢复")
            interrupted = os.urandom(200_000)
            fake_id = "crashed00000000000000000000000000"
            vol().db.create_file({"id": fake_id, "name": "half.bin", "size": 0,
                                  "chunk_size": CHUNK, "stripe_count": 0})
            vol()._write_stripe(fake_id, 0, interrupted[:CHUNK])
            vol()._write_stripe(fake_id, 1, interrupted[CHUNK:2 * CHUNK])
            check("中断文件未确认、不在列表",
                  all(f["id"] != fake_id
                      for f in cli.get("/api/files").json()["files"]))
            check("中断文件残留块存在于盘上（待恢复清理）",
                  sum(1 for _ in _TMP.rglob(f"{fake_id}*")) > 0)

        # ---- service restart ------------------------------------------
        section("服务重启恢复")
        restart_service()
        with client() as cli:
            names = {f["name"] for f in cli.get("/api/files").json()["files"]}
            check("重启后未确认文件不可见", "half.bin" not in names)
            check("重启后已确认文件仍可见",
                  {"alpha.bin", "beta.bin", "empty.bin"}.issubset(names))
            check("重启清掉了未确认文件的残留块",
                  sum(1 for _ in _TMP.rglob(f"{fake_id}*")) == 0)
            check("已确认文件重启后仍可读",
                  download(cli, id_a) == payload_a
                  and download(cli, id_b) == payload_b)
            check("重启后无悬挂重建任务", vol().status()["rebuild"] is None)

        # ---- rebuild interrupted by restart, then resumed --------------
        section("重建中断后自动续建")
        import app.volume as volmod
        volmod._REBUILD_STRIPE_DELAY = 0.03  # make the rebuild interruptible
        vol().take_disk_offline(0)
        vol().replace_disk(0)
        time.sleep(0.1)  # a few stripes rebuilt, then "process" restarts
        restart_service()
        with client() as cli:
            st = vol().status()
            check("重启后重建任务自动续跑",
                  st["rebuild"] is not None
                  and st["rebuild"]["state"] == "running")
            job = wait_rebuild()
            check("续建最终成功", job.get("state") == "done", str(job))
            check("续建后磁盘 online", vol().db.disk_state(0) == "online")
            check("续建后全部文件可读",
                  download(cli, id_a) == payload_a
                  and download(cli, id_b) == payload_b)
            check("续建后无缺失条带", vol()._stripes_missing_on_disk(0) == [])
        volmod._REBUILD_STRIPE_DELAY = 0

        # ---- upload refused when a disk is offline --------------------
        section("降级状态下拒绝产生残缺条带")
        vol().take_disk_offline(2)
        with client() as cli:
            r = cli.post("/api/files",
                         files={"file": ("x.bin", io.BytesIO(b"hi"), "text/plain")})
            check("盘离线时上传被拒(503)", r.status_code == 503, r.text)
            check("失败上传不留可见文件",
                  all(f["name"] != "x.bin"
                      for f in cli.get("/api/files").json()["files"]))
            vol().replace_disk(2)
            check("恢复重建成功", wait_rebuild().get("state") == "done")
            fid = upload(cli, b"final-check", "final.bin")
            check("恢复冗余后上传正常", download(cli, fid) == b"final-check")

        # ---- physical disk loss: directory vanishes --------------------
        section("物理盘目录消失（重启后识别为下线）")
        payload_c = os.urandom(120_000)
        id_c = upload(client(), payload_c, "gamma.bin")
        vol().shutdown()
        lost = DATA_DIR / "disk1"
        shutil.move(str(lost), str(_TMP / "disk1-ripped-out"))
        webapp.volume = Volume(DATA_DIR, DB_PATH, chunk_size=CHUNK)
        webapp.volume.start()
        with client() as cli:
            st = cli.get("/api/status").json()
            check("缺失目录的盘被标记 offline",
                  next(d for d in st["disks"] if d["disk_index"] == 1)["state"]
                  == "offline")
            check("目录缺失时文件仍可下载（其余两块盘重建）",
                  download(cli, id_c) == payload_c)
            # operator replaces the disk; service provisions a fresh dir
            r = cli.post("/api/disks/1/replace")
            check("换盘接口自动重建空目录并启动重建",
                  r.status_code == 200, r.text)
            check("物理换盘后重建成功",
                  wait_rebuild().get("state") == "done")
            check("物理换盘后文件可读", download(cli, id_c) == payload_c)

        # ---- interleaved upload + rebuild interrupted by restart --------
        section("重建中上传，随后重启：新条带不遗漏")
        import app.volume as volmod2
        volmod2._REBUILD_STRIPE_DELAY = 0.02
        vol().take_disk_offline(0)
        vol().replace_disk(0)
        time.sleep(0.05)
        with client() as cli:
            mid_payload = os.urandom(140_000)
            mid_id = upload(cli, mid_payload, "mid-rebuild.bin")
            check("重建中途上传成功且可读",
                  download(cli, mid_id) == mid_payload)
        restart_service()
        with client() as cli:
            check("重启后续建任务在跑",
                  vol().status()["rebuild"] is not None
                  and vol().status()["rebuild"]["state"] == "running")
            check("续建成功", wait_rebuild().get("state") == "done")
            check("中途上传的文件续建后可读",
                  download(cli, mid_id) == mid_payload)
            check("中途文件在新盘上无缺失条带",
                  vol()._stripes_missing_on_disk(0) == [])
            # another failure must still be survivable for the new file
            vol().take_disk_offline(2)
            check("再坏一盘，中途文件仍可下载",
                  download(cli, mid_id) == mid_payload)
            vol().replace_disk(2)
            check("最终重建成功", wait_rebuild().get("state") == "done")
        volmod2._REBUILD_STRIPE_DELAY = 0

        # ---- compaction: basic flow ----------------------------------
        section("末条带压实：基础流程与合计节省字节数")
        last_len = 100
        payload_c1 = os.urandom(CHUNK * 2 + last_len)  # 3 stripes, last partial
        with client() as cli:
            id_c1 = upload(cli, payload_c1, "compact-me.bin")
            full_id = upload(cli, os.urandom(CHUNK * 2), "full-stripes.bin")
            files = {f["id"]: f for f in cli.get("/api/files").json()["files"]}
            check("末条带未写满的文件标记为可压实",
                  files[id_c1]["compactable"] is True)
            check("末条带写满的文件不可压实",
                  files[full_id]["compactable"] is False
                  and files[full_id]["compacted"] is False)
            check("空文件不可压实", files[id_e]["compactable"] is False)

            r = cli.post(f"/api/files/{full_id}/compact")
            check("满条带文件压实返回 409", r.status_code == 409, r.text)
            r = cli.post(f"/api/files/{id_e}/compact")
            check("空文件压实返回 409", r.status_code == 409, r.text)
            r = cli.post(f"/api/files/{'0' * 32}/compact")
            check("未知文件压实返回 404", r.status_code == 404, r.text)

            last_idx = vol().db.get_stripes(id_c1)[-1]["stripe_index"]
            r = cli.post(f"/api/files/{id_c1}/compact")
            check("压实请求成功", r.status_code == 200, r.text)
            check("合计节省字节数 = 3 × 零填充",
                  r.json()["bytes_saved"] == 3 * (CHUNK - last_len), r.text)
            check("压实后下载逐字节一致", download(cli, id_c1) == payload_c1)
            check("三个块文件都缩短到有效长度",
                  block_sizes(id_c1, last_idx) == [last_len] * 3,
                  str(block_sizes(id_c1, last_idx)))
            check("前面满条带的块保持原长",
                  block_sizes(id_c1, 0) == [CHUNK] * 3,
                  str(block_sizes(id_c1, 0)))
            files = {f["id"]: f for f in cli.get("/api/files").json()["files"]}
            check("列表显示已压实与节省字节",
                  files[id_c1]["compacted"] is True
                  and files[id_c1]["saved_bytes"] == 3 * (CHUNK - last_len)
                  and files[id_c1]["compactable"] is False)
            r = cli.post(f"/api/files/{id_c1}/compact")
            check("重复压实幂等（already）",
                  r.status_code == 200 and r.json()["already"] is True
                  and r.json()["bytes_saved"] == 3 * (CHUNK - last_len), r.text)

        # ---- compaction: fault tolerance + offline gating -------------
        section("压实后单盘下线与换盘重建；离线时拒绝压实")
        with client() as cli:
            payload_c2 = os.urandom(CHUNK + 50)
            id_c2 = upload(cli, payload_c2, "compact-later.bin")
            cli.post("/api/disks/0/offline").raise_for_status()
            r = cli.post(f"/api/files/{id_c2}/compact")
            check("盘离线时压实被拒(503)", r.status_code == 503, r.text)
            check("压实后单盘下线仍可下载", download(cli, id_c1) == payload_c1)
            cli.post("/api/disks/0/replace").raise_for_status()
            check("压实后换盘重建成功", wait_rebuild().get("state") == "done")
            check("重建后压实文件下载仍一致", download(cli, id_c1) == payload_c1)
            last = vol().db.get_stripes(id_c1)[-1]
            role0 = next(role for role, d in (("d0", last["d0_disk"]),
                                              ("d1", last["d1_disk"]),
                                              ("p", last["p_disk"])) if d == 0)
            b = vol().db.get_block(id_c1, last["stripe_index"], role0)
            size0 = os.path.getsize(
                storage.block_path(vol().roots[0], b["relpath"]))
            check("换盘补出的块保持压实后的长度", size0 == last_len,
                  f"{size0} != {last_len}")
            check("重建后三个块仍是压实长度",
                  block_sizes(id_c1, last["stripe_index"]) == [last_len] * 3)
            r = cli.post(f"/api/files/{id_c2}/compact")
            check("恢复三盘在线后可压实",
                  r.status_code == 200
                  and r.json()["bytes_saved"] == 3 * (CHUNK - 50), r.text)
            check("该文件压实后下载一致", download(cli, id_c2) == payload_c2)

        # ---- compaction: concurrent downloads -------------------------
        section("压实期间并发下载")
        payload_c3 = os.urandom(CHUNK * 4 + 500)
        with client() as cli:
            id_c3 = upload(cli, payload_c3, "compact-concurrent.bin")
            volmod._COMPACT_BLOCK_DELAY = 0.25
            compact_box: dict = {}

            def run_compact() -> None:
                with client() as c2:
                    try:
                        compact_box["r"] = c2.post(f"/api/files/{id_c3}/compact")
                    except Exception as exc:  # noqa: BLE001
                        compact_box["exc"] = exc

            t = threading.Thread(target=run_compact, daemon=True)
            t.start()
            time.sleep(0.15)  # first block(s) trimmed, compaction in flight
            with concurrent.futures.ThreadPoolExecutor(max_workers=4) as ex:
                results = list(ex.map(lambda _: download(cli, id_c3), range(8)))
            check("压实期间并发下载全部逐字节一致",
                  all(r == payload_c3 for r in results))
            t.join(timeout=30)
            volmod._COMPACT_BLOCK_DELAY = 0
            check("并发下载期间压实完成",
                  compact_box.get("r") is not None
                  and compact_box["r"].status_code == 200,
                  str(compact_box.get("r") or compact_box.get("exc")))
            check("压实完成后再下载仍一致", download(cli, id_c3) == payload_c3)

        # ---- compaction interrupted by restart, then retried ----------
        section("压实中断：重启后按原编号下载一致，可重试")
        tail = 77
        payload_c4 = os.urandom(CHUNK * 3 + tail)
        with client() as cli:
            id_c4 = upload(cli, payload_c4, "compact-crash.bin")
        volmod._COMPACT_BLOCK_DELAY = 0.3
        crash_box: dict = {}

        def run_compact4() -> None:
            with client() as c2:
                try:
                    crash_box["r"] = c2.post(f"/api/files/{id_c4}/compact")
                except Exception as exc:  # noqa: BLE001
                    crash_box["exc"] = exc

        t = threading.Thread(target=run_compact4, daemon=True)
        t.start()
        time.sleep(0.45)  # 1-2 blocks trimmed, stripe not yet flagged
        restart_service()
        t.join(timeout=30)
        volmod._COMPACT_BLOCK_DELAY = 0
        with client() as cli:
            check("中断的压实未显示完成",
                  crash_box.get("r") is None
                  or crash_box["r"].status_code != 200,
                  str(crash_box.get("r")))
            stripes4 = vol().db.get_stripes(id_c4)
            check("中断后条带未标记压实（三块未全缩短不显示完成）",
                  stripes4[-1]["compacted"] == 0)
            mixed = block_sizes(id_c4, stripes4[-1]["stripe_index"])
            check("中断时压实已开始（至少一块已截断）", min(mixed) == tail,
                  str(mixed))
            check("中断重启后按原编号下载逐字节一致",
                  download(cli, id_c4) == payload_c4)
            files = {f["id"]: f for f in cli.get("/api/files").json()["files"]}
            check("中断后列表仍显示可压实",
                  files[id_c4]["compactable"] is True
                  and files[id_c4]["compacted"] is False)
            r = cli.post(f"/api/files/{id_c4}/compact")
            check("压实中断可重试并成功",
                  r.status_code == 200
                  and r.json()["bytes_saved"] == 3 * (CHUNK - tail), r.text)
            check("重试后下载仍逐字节一致", download(cli, id_c4) == payload_c4)
            check("重试后三个块都缩短到有效长度",
                  block_sizes(id_c4, stripes4[-1]["stripe_index"])
                  == [tail] * 3)
    finally:
        vol().shutdown()
        server.should_exit = True
        thread.join(timeout=10)
        shutil.rmtree(_TMP, ignore_errors=True)

    print(f"\n结果: {PASS} 通过, {FAIL} 失败")
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
