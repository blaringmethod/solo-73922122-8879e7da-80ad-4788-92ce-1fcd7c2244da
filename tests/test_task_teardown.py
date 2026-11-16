"""任务收尾协作的管理器级回归（假重算协程，不依赖 LibreOffice）。

复现并锁定需求中的关键不变量：
- 运行中取消 / 重算超时：不发布判定、终止本批、清完暂存才落终态并释放名额；
- 两批名额占满后，卡住的批次被取消/超时释放名额，排队批次能继续推进；
- 一批取消不影响另一批的核对与状态查询；不做全局杀进程、不串行化。
"""

from __future__ import annotations

import asyncio
import io

import openpyxl
import pytest

from app import config, manager as manager_mod
from app.manager import TaskManager
from app.models import TaskStatus
from app.recalc import RecalcCancelled, RecalcTimeout


def _build_xlsx() -> bytes:
    wb = openpyxl.Workbook()
    wb.active.title = "标定表"
    ws = wb.create_sheet("样本表")
    ws["A1"] = "样本号"
    ws["B1"] = "判定"
    for i in range(2, 5):
        ws.cell(row=i, column=1, value=f"S{i}")
        ws.cell(row=i, column=2, value=f'=IF(C{i}>=标定表!$B$2,"PASS","FAIL")')
        ws.cell(row=i, column=3, value=20.0)
    buf = io.BytesIO()
    wb.save(buf)
    return buf.getvalue()


def _build_cached_xlsx() -> bytes:
    """带真实缓存 PASS/FAIL 的工作簿，模拟 LibreOffice 重算产物。"""
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "样本表"
    ws["A1"] = "样本号"
    ws["B1"] = "判定"
    for i in range(2, 5):
        ws.cell(row=i, column=1, value=f"S{i}")
        ws.cell(row=i, column=2, value="PASS")  # 已重算缓存
    buf = io.BytesIO()
    wb.save(buf)
    return buf.getvalue()


@pytest.fixture()
def mgr_env(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "DATA_DIR", tmp_path)
    monkeypatch.setattr(config, "TASKS_DIR", tmp_path / "tasks")
    monkeypatch.setattr(config, "STAGING_DIR", tmp_path / "staging")
    (tmp_path / "tasks").mkdir()
    (tmp_path / "staging").mkdir()
    yield tmp_path


def _install_dispatch(monkeypatch, routing: dict, default):
    """按任务暂存目录（含 task_id）把 run_recalc 分派到不同假实现。"""

    async def dispatch(task_dir, input_name="input.xlsx", cancel_event=None):
        for key, impl in routing.items():
            if key in str(task_dir):
                return await impl(task_dir, input_name, cancel_event)
        return await default(task_dir, input_name, cancel_event)

    monkeypatch.setattr(manager_mod, "run_recalc", dispatch)


async def _create(mgr: TaskManager, name: str):
    rec = await mgr.create_task(_build_xlsx(), name)
    return rec["task_id"]


@pytest.mark.asyncio
async def test_cancel_releases_slot_and_queued_batch_proceeds(mgr_env, monkeypatch):
    mgr = TaskManager()
    assert mgr.slots._capacity == 2

    first_cancelled = asyncio.Event()
    slow_started = asyncio.Event()
    gate = asyncio.Event()          # 两个在跑批次共同的放行门
    second_started = asyncio.Event()

    async def slow_recalc(task_dir, input_name="input.xlsx", cancel_event=None):
        # 卡住第一批，直到取消或放行；模拟计算迟迟不结束
        slow_started.set()
        while not gate.is_set():
            await asyncio.sleep(0.02)
            if cancel_event is not None and cancel_event.is_set():
                first_cancelled.set()
                raise RecalcCancelled("cancelled")
        return task_dir / "output" / input_name

    async def blocking_ok_recalc(task_dir, input_name="input.xlsx",
                                 cancel_event=None):
        # 第二批占住另一个名额，待放行后再产出有效重算结果
        second_started.set()
        while not gate.is_set():
            await asyncio.sleep(0.02)
        out = task_dir / "output"
        out.mkdir(parents=True, exist_ok=True)
        (out / input_name).write_bytes(_build_cached_xlsx())
        return out / input_name

    async def ok_recalc(task_dir, input_name="input.xlsx", cancel_event=None):
        out = task_dir / "output"
        out.mkdir(parents=True, exist_ok=True)
        (out / input_name).write_bytes(_build_cached_xlsx())
        await asyncio.sleep(0.05)
        return out / input_name

    # 按创建顺序分派：第一批 slow、第二批 blocking_ok、第三批（排队）快速 ok。
    # 分派在任何任务创建前安装，避免协程抢先执行造成竞态。
    order = {"n": 0}

    async def ordered_dispatch(task_dir, input_name="input.xlsx",
                               cancel_event=None):
        idx = order["n"]
        order["n"] += 1
        impl = (slow_recalc, blocking_ok_recalc, ok_recalc)[idx]
        return await impl(task_dir, input_name, cancel_event)

    monkeypatch.setattr(manager_mod, "run_recalc", ordered_dispatch)
    id1 = await _create(mgr, "a.xlsx")
    id2 = await _create(mgr, "b.xlsx")
    id3 = await _create(mgr, "c.xlsx")

    # 等前两批真正占住两个名额，第三批在排队
    await asyncio.wait_for(slow_started.wait(), timeout=5)
    await asyncio.wait_for(second_started.wait(), timeout=5)
    await asyncio.sleep(0.1)
    assert (await mgr.get(id1))["status"] == TaskStatus.RUNNING.value
    assert (await mgr.get(id2))["status"] == TaskStatus.RUNNING.value
    assert (await mgr.get(id3))["status"] == TaskStatus.QUEUED.value
    assert mgr.slots.in_use == 2

    # 取消卡住的第一批：立即受理，终态在收尾后落下
    _, accepted = await mgr.request_cancel(id1)
    assert accepted is True
    await asyncio.wait_for(mgr._jobs[id1], timeout=5)
    await first_cancelled.wait()

    final1 = await mgr.get(id1)
    assert final1["status"] == TaskStatus.CANCELLED.value
    assert final1["samples"] is None                       # 取消不发布判定
    assert not (config.STAGING_DIR / id1).exists()         # 暂存清完才落终态

    # 名额释放后，排队的第三批获得名额并完成；第二批不受影响
    gate.set()
    deadline = asyncio.get_running_loop().time() + 10
    while asyncio.get_running_loop().time() < deadline:
        if (await mgr.get(id3))["status"] == TaskStatus.COMPLETED.value:
            break
        await asyncio.sleep(0.05)
    assert (await mgr.get(id3))["status"] == TaskStatus.COMPLETED.value
    assert (await mgr.get(id2))["status"] == TaskStatus.COMPLETED.value

    # 全部收尾后名额归零、暂存清空
    await asyncio.gather(*list(mgr._jobs.values()))
    assert mgr.slots.in_use == 0
    assert not any(config.STAGING_DIR.iterdir())


@pytest.mark.asyncio
async def test_timeout_publishes_no_result_and_frees_slot(mgr_env, monkeypatch):
    mgr = TaskManager()

    async def timeout_recalc(task_dir, input_name="input.xlsx", cancel_event=None):
        raise RecalcTimeout("重算超过 10 秒")

    monkeypatch.setattr(manager_mod, "run_recalc", timeout_recalc)
    tid = await _create(mgr, "heavy.xlsx")
    await asyncio.wait_for(mgr._jobs[tid], timeout=5)

    rec = await mgr.get(tid)
    assert rec["status"] == TaskStatus.FAILED.value
    assert rec["error_code"] == "RECALC_TIMEOUT"
    assert rec["samples"] is None
    assert not (config.STAGING_DIR / tid).exists()
    assert mgr.slots.in_use == 0


@pytest.mark.asyncio
async def test_cancel_does_not_touch_other_batch(mgr_env, monkeypatch):
    """一批被取消，另一批仍能完成并可查询；不存在全局杀进程/串行化。"""
    mgr = TaskManager()
    started = asyncio.Event()

    async def cancel_me(task_dir, input_name="input.xlsx", cancel_event=None):
        started.set()
        for _ in range(200):
            await asyncio.sleep(0.02)
            if cancel_event is not None and cancel_event.is_set():
                raise RecalcCancelled("cancelled")
        return task_dir / "output" / input_name

    async def healthy(task_dir, input_name="input.xlsx", cancel_event=None):
        out = task_dir / "output"
        out.mkdir(parents=True, exist_ok=True)
        (out / input_name).write_bytes(_build_cached_xlsx())
        return out / input_name

    # 第一批为可取消任务；装分派后第二批走 healthy
    monkeypatch.setattr(
        manager_mod, "run_recalc",
        lambda *a, **k: cancel_me(*a, **k),
    )
    id_cancel = await _create(mgr, "x.xlsx")
    _install_dispatch(monkeypatch, {id_cancel: cancel_me}, default=healthy)
    id_ok = await _create(mgr, "y.xlsx")

    await started.wait()
    _, accepted = await mgr.request_cancel(id_cancel)
    assert accepted is True
    await asyncio.wait_for(mgr._jobs[id_cancel], timeout=6)

    # 健康批次照常完成，状态随时可查（只读）
    ok_rec = await mgr.get(id_ok)
    assert ok_rec["status"] == TaskStatus.COMPLETED.value
    assert len(ok_rec["samples"]) == 3

    cancelled_rec = await mgr.get(id_cancel)
    assert cancelled_rec["status"] == TaskStatus.CANCELLED.value
    assert cancelled_rec["samples"] is None
    assert mgr.slots.in_use == 0
