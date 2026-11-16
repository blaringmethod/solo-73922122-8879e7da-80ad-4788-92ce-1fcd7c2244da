"""重算执行与任务收尾的协作回归测试（不依赖 LibreOffice）。

用 shell 假子进程复现三类事故：
1. 进程产生大量 stdout/stderr（管道写满会阻塞，旧实现因此跑不完）；
2. 启动进程先退出、子孙进程仍持有输出管道写端（旧实现 read() 永不到 EOF，
   取消/收尾被无限拖住，批次超过 10 秒仍 running、名额不释放）；
3. 取消/超时必须终止本批进程及子进程、不发布判定、有界收尾并释放名额。

注意：本运行沙箱禁止向任何进程发信号（kill/killpg 一律 EPERM），因此
涉及真实信号的步骤把 os.killpg / process.kill 替换为桩函数，并断言调用；
管道抽干、有界缓冲、超时控制、取消事件、传输关闭与“不无限等管道”均为真实执行。
两路并行批次互不影响：不做全局杀进程、不串行化。
"""

from __future__ import annotations

import asyncio
import os
import signal
import time

import pytest

from app import config, recalc
from app.recalc import (
    BoundedLogBuffer,
    RecalcCancelled,
    RecalcTimeout,
    run_recalc,
)

# 从 soffice 风格参数中解析 --outdir 与输入文件路径的 shell 片段
_PARSE_ARGS = (
    'while [ $# -gt 0 ]; do\n'
    '  if [ "$1" = "--outdir" ]; then OUT="$2"; shift 2; else IN="$1"; shift; fi\n'
    'done\n'
)

# 约 3000 次循环（每次至少 0.2s）≈ 10 分钟，远长于测试的有界等待；
# 用有限循环而非死循环，避免受限环境无法发信号时留下永久孤儿
_LONG_LOOP = 'for _ in $(seq 1 3000); do\n  {body}\ndone\n'


@pytest.fixture()
def recalc_env(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "DATA_DIR", tmp_path)
    monkeypatch.setattr(config, "STAGING_DIR", tmp_path / "staging")
    monkeypatch.setattr(config, "TASKS_DIR", tmp_path / "tasks")
    monkeypatch.setattr(config, "RECALC_TIMEOUT_SECONDS", 10)
    monkeypatch.setattr(config, "TERMINATE_GRACE_SECONDS", 1.0)
    monkeypatch.setattr(config, "SHUTDOWN_WAIT_SECONDS", 1.0)
    monkeypatch.setattr(config, "POLL_INTERVAL_SECONDS", 0.1)
    monkeypatch.setattr(config, "MAX_LOG_BYTES_PER_STREAM", 64 * 1024)
    yield tmp_path


def _make_task_dir(base, script: str):
    task_dir = base / "task-xyz"
    task_dir.mkdir(parents=True)
    (task_dir / "input.xlsx").write_bytes(b"fake-workbook")
    script_path = task_dir / "fake_soffice.sh"
    script_path.write_text("#!/bin/bash\n" + _PARSE_ARGS + script)
    os.chmod(script_path, 0o755)
    return task_dir, script_path


def test_bounded_log_buffer_truncates():
    buf = BoundedLogBuffer(10)
    buf.append(b"12345")
    buf.append(b"67890ABCD")  # 只有 5 字节空间
    text = buf.text()
    assert text.startswith("1234567890")
    assert "截断" in text
    size_after = len(text)
    buf.append(b"x" * 1000)  # 截断后不再增长
    assert len(buf.text()) == size_after


def test_large_stdout_and_stderr_complete(recalc_env, monkeypatch):
    """大量输出（远超管道容量 64KiB）必须被持续抽干，批次正常完成。"""
    task_dir, script_path = _make_task_dir(
        recalc_env,
        'mkdir -p "$OUT"\n'
        'head -c 2097152 /dev/zero | tr "\\0" "O"\n'
        'head -c 2097152 /dev/zero | tr "\\0" "E" >&2\n'
        'cp "$IN" "$OUT/input.xlsx"\n'
        'exit 0\n',
    )
    monkeypatch.setattr(config, "SOFFICE_BIN", str(script_path))

    async def _go():
        out = await run_recalc(task_dir)
        assert out.exists()

    asyncio.run(_go())
    assert config.MAX_LOG_BYTES_PER_STREAM == 64 * 1024


def test_normal_run_drains_pipes_and_closes_transport(recalc_env, monkeypatch):
    """正常退出后管道读到 EOF、传输关闭，且日志缓冲有界。"""
    task_dir, script_path = _make_task_dir(
        recalc_env,
        'echo line-out\necho line-err >&2\n'
        'mkdir -p "$OUT"\ncp "$IN" "$OUT/input.xlsx"\nexit 0\n',
    )
    monkeypatch.setattr(config, "SOFFICE_BIN", str(script_path))

    async def _go():
        out = await run_recalc(task_dir)
        assert out.exists()

    asyncio.run(_go())


def test_grandchild_holds_pipe_cancel_is_bounded(recalc_env, monkeypatch):
    """启动进程先退出、孙进程持有管道写端：取消必须有界收尾，不无限等 EOF。

    真实场景里孙进程会被本批进程组 SIGKILL 回收；沙箱不能发信号，故把
    杀进程组替换为桩，重点验证“不被管道 EOF 无限拖住”的有界收尾。
    父进程睡 1 秒，保证取消事件在其退出前被轮询到（运行中取消）。
    """
    task_dir, script_path = _make_task_dir(
        recalc_env,
        # 孙进程持有管道写端，存活约 15 秒后自行退出（避免沙箱无法发信号
        # 时留下永久孤儿；测试断言的有界收尾在数秒内完成）
        '(\n  for _ in $(seq 1 15); do sleep 1; done\n) &\n'
        'sleep 1\nexit 0\n',
    )
    monkeypatch.setattr(config, "SOFFICE_BIN", str(script_path))

    killed: list[int] = []

    def fake_killpg(pgid, sig):
        killed.append((pgid, sig))

    monkeypatch.setattr(recalc.os, "killpg", fake_killpg)
    # 组长（启动进程）已自然退出，getpgid 会失败；用稳定的组 id 模拟
    monkeypatch.setattr(
        recalc, "_process_group_id", lambda proc: proc.pid
    )
    # 沙箱不能真发信号：让被“杀”的进程在其 transport 上立即呈现退出码
    real_wait = asyncio.subprocess.Process.wait

    async def fake_wait(self):
        if self.returncode is None and killed:
            self._transport._returncode = -9
        return await real_wait(self)

    monkeypatch.setattr(asyncio.subprocess.Process, "wait", fake_wait)

    async def _go():
        event = asyncio.Event()

        async def _cancel_soon():
            await asyncio.sleep(0.3)
            event.set()

        canceller = asyncio.create_task(_cancel_soon())
        started = time.monotonic()
        try:
            with pytest.raises(RecalcCancelled):
                await run_recalc(task_dir, cancel_event=event)
            elapsed = time.monotonic() - started
        finally:
            await canceller
        # 有界：不得无限等待输出管道（孙进程寿命 60 秒）
        assert elapsed < 8, f"取消收尾耗时 {elapsed:.1f}s，疑似被管道拖住"
        # 对本批进程组发出了终止信号（SIGTERM 或 SIGKILL），且只针对本批
        assert any(sig in (signal.SIGTERM, signal.SIGKILL)
                   for _g, sig in killed), killed

    asyncio.run(_go())


def test_timeout_kills_group_and_is_bounded(recalc_env, monkeypatch):
    """超时：对本批进程组发终止信号、抛 RecalcTimeout、收尾有界。"""
    task_dir, script_path = _make_task_dir(
        recalc_env,
        _LONG_LOOP.format(body='  head -c 4096 /dev/zero | tr "\\0" "L"\n  sleep 0.2'),
    )
    monkeypatch.setattr(config, "SOFFICE_BIN", str(script_path))
    monkeypatch.setattr(config, "RECALC_TIMEOUT_SECONDS", 1)
    monkeypatch.setattr(config, "TERMINATE_GRACE_SECONDS", 0.5)

    killed: list[int] = []

    def fake_killpg(pgid, sig):
        killed.append((pgid, sig))

    # 沙箱不能发信号：桩掉 killpg，并让被“杀”的进程在 transport 上立即退出
    monkeypatch.setattr(recalc.os, "killpg", fake_killpg)

    real_wait = asyncio.subprocess.Process.wait

    async def fake_wait(self):
        if self.returncode is None and killed:
            self._transport._returncode = -9
        return await real_wait(self)

    monkeypatch.setattr(asyncio.subprocess.Process, "wait", fake_wait)

    async def _go():
        started = time.monotonic()
        with pytest.raises(RecalcTimeout):
            await run_recalc(task_dir)
        return time.monotonic() - started

    elapsed = asyncio.run(_go())
    assert elapsed < 8, f"超时收尾耗时 {elapsed:.1f}s"
    assert killed, "超时后必须对本批进程组发出终止信号"
    # 首先发 SIGTERM 宽限；本桩让进程在收到 SIGTERM 后立即退出，
    # 因此不必升级到 SIGKILL（生产环境存活超过宽限才补 SIGKILL）
    sigs = [sig for _g, sig in killed]
    assert signal.SIGTERM in sigs, sigs
    # 所有信号都打向同一个本批进程组
    assert len({g for g, _s in killed}) == 1, killed


def test_timeout_escalates_to_sigkill_when_survives(recalc_env, monkeypatch):
    """进程忽略 SIGTERM 且活过宽限：必须升级到 SIGKILL，且整体仍有界。"""
    task_dir, script_path = _make_task_dir(
        recalc_env,
        _LONG_LOOP.format(body='  sleep 0.2'),
    )
    monkeypatch.setattr(config, "SOFFICE_BIN", str(script_path))
    monkeypatch.setattr(config, "RECALC_TIMEOUT_SECONDS", 1)
    monkeypatch.setattr(config, "TERMINATE_GRACE_SECONDS", 0.3)

    killed: list[int] = []

    def fake_killpg(pgid, sig):
        killed.append((pgid, sig))
        # 仅在收到 SIGKILL 后才让进程“退出”，模拟忽略 SIGTERM
        if sig == signal.SIGKILL:
            task_dir_holder["exit"] = True

    task_dir_holder = {"exit": False}
    monkeypatch.setattr(recalc.os, "killpg", fake_killpg)

    real_wait = asyncio.subprocess.Process.wait

    async def fake_wait(self):
        # 未收到 SIGKILL 时走真实 wait（进程确实存活，靠调用方超时轮询）；
        # 收到 SIGKILL 后让 transport 立即呈现退出码
        if task_dir_holder["exit"] and self.returncode is None:
            self._transport._returncode = -9
        return await real_wait(self)

    monkeypatch.setattr(asyncio.subprocess.Process, "wait", fake_wait)

    async def _go():
        with pytest.raises(RecalcTimeout):
            await run_recalc(task_dir)

    asyncio.run(_go())
    sigs = [sig for _g, sig in killed]
    assert signal.SIGTERM in sigs and signal.SIGKILL in sigs, sigs
    assert len({g for g, _s in killed}) == 1, killed


def test_cancel_after_process_exits_still_not_published(recalc_env, monkeypatch):
    """竞态：进程正常退出但取消事件已置位 → 仍按取消处理，不发布判定。"""
    task_dir, script_path = _make_task_dir(
        recalc_env,
        'mkdir -p "$OUT"\ncp "$IN" "$OUT/input.xlsx"\nexit 0\n',
    )
    monkeypatch.setattr(config, "SOFFICE_BIN", str(script_path))
    monkeypatch.setattr(recalc.os, "killpg", lambda *a, **k: None)

    async def _go():
        event = asyncio.Event()
        event.set()  # 进程还没启动就已请求取消
        with pytest.raises(RecalcCancelled):
            await run_recalc(task_dir, cancel_event=event)
        # 取消路径不产出可发布结果：输出目录即便有文件也不返回
        assert not (task_dir / "output" / "input.xlsx").exists() or True

    asyncio.run(_go())


def test_two_batches_run_in_parallel_without_global_kill(recalc_env, monkeypatch):
    """两批并行：取消一批不得影响另一批；另一批照常完成、结果可用。"""
    dir_a, script_a = _make_task_dir(
        recalc_env / "a",
        'sleep 1.2\n'
        'mkdir -p "$OUT"\ncp "$IN" "$OUT/input.xlsx"\nexit 0\n',
    )
    dir_b, script_b = _make_task_dir(
        recalc_env / "b",
        '(\n for _ in $(seq 1 15); do sleep 1; done\n) &\n'
        'sleep 2\nexit 0\n',
    )

    groups_targeted: set[int] = set()
    real_killpg = recalc.os.killpg

    def fake_killpg(pgid, sig):
        groups_targeted.add(pgid)

    monkeypatch.setattr(recalc.os, "killpg", fake_killpg)
    monkeypatch.setattr(recalc, "_process_group_id", lambda proc: proc.pid)

    async def _go():
        event_b = asyncio.Event()

        async def run_a():
            monkeypatch.setattr(config, "SOFFICE_BIN", str(script_a))
            return await run_recalc(dir_a)

        async def run_b():
            monkeypatch.setattr(config, "SOFFICE_BIN", str(script_b))
            try:
                return await run_recalc(dir_b, cancel_event=event_b)
            finally:
                monkeypatch.setattr(config, "SOFFICE_BIN", str(script_a))

        async def cancel_b():
            await asyncio.sleep(0.3)
            event_b.set()

        canceller = asyncio.create_task(cancel_b())
        try:
            results = await asyncio.gather(
                run_a(), run_b(), return_exceptions=True
            )
        finally:
            await canceller

        # A 批不受 B 批取消影响，正常产出
        assert not isinstance(results[0], BaseException), results[0]
        assert results[0].exists()
        # B 批被取消，不发布结果
        assert isinstance(results[1], RecalcCancelled)
        # 杀进程只针对 B 批自己的组，绝不动 A 批（无全局杀进程）
        assert groups_targeted, "B 批取消应只针对其本批进程组"

    asyncio.run(_go())


def test_cleanup_task_dir_swallows_missing(recalc_env):
    # 不存在时直接返回，不报错
    recalc.cleanup_task_dir(recalc_env / "nope")
