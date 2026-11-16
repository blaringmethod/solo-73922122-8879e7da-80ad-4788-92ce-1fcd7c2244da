"""LibreOffice headless 重算。

隔离措施（每批互不影响）：
- 独立暂存目录、独立 UserInstallation profile；
- 独立进程组（os.setsid），取消/超时时 kill 整个进程组，含 soffice 子进程；
- 精简后的干净环境变量，不继承父进程的可疑变量；
- 每个 profile 内置 registrymodifications.xcu，强制 OOXML 载入即重算，
  并把宏安全级别调到最高（Very High，不执行任何宏）。

输出管道协作（修复要点）：
- stdout/stderr 在进程启动后立即由两个后台协程持续抽干，写入**有界**缓冲区
  （每路最多保留 MAX_LOG_BYTES_PER_STREAM 字节，超出截断并标记），
  因此即使计算进程产生大量输出也不会撑满管道而自我阻塞，正常批次一定能跑完；
- 取消/超时只走有界流程：SIGTERM 进程组 → 宽限 → SIGKILL，再回收进程、
  取消抽干协程并关闭管道传输。任何一步都带超时，绝不无限等待输出管道
  （启动进程先退出、子孙进程仍持有管道写端时，靠关闭读端传输兜底解阻塞）；
- 进程正常退出后用已缓冲的日志判定错误，不再对管道做可能无限阻塞的 read()。
"""

from __future__ import annotations

import asyncio
import os
import shutil
import signal
import sys
from pathlib import Path

from . import config

# 强制重算 + 宏全禁用的 profile 配置
REGISTRY_XCU = """<?xml version="1.0" encoding="UTF-8"?>
<oor:items xmlns:oor="http://openoffice.org/2001/registry"
 xmlns:xs="http://www.w3.org/2001/XMLSchema"
 xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
 <item oor:path="/org.openoffice.Office.Calc/Formula/Load">
  <prop oor:name="OOXMLRecalcMode" oor:op="fuse"><value>0</value></prop>
 </item>
 <item oor:path="/org.openoffice.Office.Calc/Formula/Load">
  <prop oor:name="ODFRecalcMode" oor:op="fuse"><value>0</value></prop>
 </item>
 <item oor:path="/org.openoffice.Office.Common/Security/Scripting">
  <prop oor:name="MacroSecurityLevel" oor:op="fuse"><value>3</value></prop>
 </item>
</oor:items>
"""

# 抽干管道时单次读取的块大小
_READ_CHUNK = 4096
_TRUNCATED_MARKER = "\n...[输出超出上限，已截断]..."


class RecalcTimeout(Exception):
    """重算超过单批时限。"""


class RecalcCancelled(Exception):
    """重算过程中收到取消。"""


class RecalcFailed(Exception):
    """LibreOffice 进程异常退出或未产出文件。"""


class BoundedLogBuffer:
    """有界字节缓冲：只保留前 limit 字节，超出后不再累积（日志占用有界）。"""

    def __init__(self, limit: int):
        self._limit = max(0, limit)
        self._chunks: list[bytes] = []
        self._size = 0
        self._truncated = False

    def append(self, data: bytes) -> None:
        if not data or self._size >= self._limit:
            if data:
                self._truncated = True
            return
        room = self._limit - self._size
        if len(data) > room:
            self._chunks.append(data[:room])
            self._size += room
            self._truncated = True
        else:
            self._chunks.append(data)
            self._size += len(data)

    def text(self) -> str:
        body = b"".join(self._chunks).decode("utf-8", "ignore")
        if self._truncated:
            body += _TRUNCATED_MARKER
        return body


def prepare_profile(profile_dir: Path) -> Path:
    user_dir = profile_dir / "user"
    user_dir.mkdir(parents=True, exist_ok=True)
    (user_dir / "registrymodifications.xcu").write_text(
        REGISTRY_XCU, encoding="utf-8"
    )
    return profile_dir


def _build_env() -> dict[str, str]:
    """干净的子进程环境，仅保留运行必需变量。"""
    env = {
        "PATH": "/usr/local/bin:/usr/bin:/bin",
        "LANG": os.environ.get("LANG", "C.UTF-8"),
        "LC_ALL": os.environ.get("LC_ALL", "C.UTF-8"),
        "HOME": os.environ.get("HOME", "/tmp"),
        "TMPDIR": os.environ.get("TMPDIR", "/tmp"),
    }
    # 本地便携版 LibreOffice 需要 LD_LIBRARY_PATH（容器内 apt 安装不需要）
    extra = os.environ.get("SOFFICE_LD_LIBRARY_PATH")
    if extra:
        env["LD_LIBRARY_PATH"] = extra
    return env


async def _pump_stream(stream, buffer: BoundedLogBuffer) -> None:
    """持续抽干一路管道直到 EOF，写入有界缓冲，避免管道写满阻塞计算进程。"""
    if stream is None:
        return
    try:
        while True:
            chunk = await stream.read(_READ_CHUNK)
            if not chunk:
                return
            buffer.append(chunk)
    except (asyncio.CancelledError, RuntimeError, ConnectionResetError,
            BrokenPipeError):
        # 取消/传输关闭时正常结束，缓冲中已读到的内容仍可用于错误信息
        return
    except Exception:
        # 抽干失败不能影响任务终态流转
        return


async def _wait_bounded(awaitable, timeout: float):
    """带超时地等待；超时返回 None，绝不无限等待。"""
    try:
        return await asyncio.wait_for(awaitable, timeout=max(0.05, timeout))
    except asyncio.TimeoutError:
        return None


def _process_group_id(process: asyncio.subprocess.Process) -> int | None:
    try:
        return os.getpgid(process.pid)
    except (ProcessLookupError, PermissionError, OSError):
        return None


def _signal_group(pgid: int, sig: int) -> bool:
    """向进程组发信号；进程已消失返回 True，无权限返回 False。"""
    try:
        os.killpg(pgid, sig)
        return True
    except ProcessLookupError:
        return True
    except PermissionError:
        return False


async def terminate_process_group(
    process: asyncio.subprocess.Process,
    grace: float | None = None,
) -> None:
    """有界地终止本批计算进程及其全部子孙（同一进程组）。

    SIGTERM 整组 → 宽限等待 → 仍存活则 SIGKILL 整组 → 回收。
    受限环境下 killpg 被拒（PermissionError）退化为直接终止子进程。
    所有等待均有超时：不会因为某个子孙进程不退出而无限卡住，
    也不会触碰其它批次的进程（只针对本进程组 / 本子进程）。
    """
    if process.returncode is not None:
        return
    if grace is None:
        grace = config.TERMINATE_GRACE_SECONDS

    pgid = _process_group_id(process)
    if pgid is not None and _signal_group(pgid, signal.SIGTERM):
        # 给整组 SIGTERM 宽限；仍不退出再 SIGKILL
        await _wait_bounded(process.wait(), grace)
        if process.returncode is None:
            _signal_group(pgid, signal.SIGKILL)
    else:
        # 拿不到进程组或 killpg 被拒：退化为直接终止子进程本身
        try:
            process.terminate()
        except (ProcessLookupError, PermissionError):
            pass
        await _wait_bounded(process.wait(), grace)
        if process.returncode is None:
            try:
                process.kill()
            except (ProcessLookupError, PermissionError):
                pass

    if process.returncode is None:
        await _wait_bounded(process.wait(), config.SHUTDOWN_WAIT_SECONDS)


def _close_transport(process: asyncio.subprocess.Process) -> None:
    """主动关闭子进程管道传输。

    启动进程先退出而子孙进程仍持有管道写端时，read() 永远等不到 EOF；
    关闭读端传输可让抽干协程立即解阻塞，避免无限等待输出管道。
    同步调用，不依赖事件循环调度。
    """
    transport = getattr(process, "_transport", None)
    if transport is None or transport.is_closing():
        return
    try:
        transport.close()
    except Exception:
        pass


async def _stop_pumps(
    pump_tasks: dict[str, asyncio.Task],
    process: asyncio.subprocess.Process,
    group_id: int | None = None,
) -> None:
    """有界地停止抽干协程并关闭管道，保证收尾不被输出管道拖住。

    若抽干协程在有界等待后仍未见到 EOF，说明启动进程虽已退出、却有子孙进程
    仍持有管道写端（本故障的根因）。此时对本批专用进程组补 SIGKILL
    （进程组在 setsid 下组长即子进程 pid，组长退出后组仍可被 killpg 定位），
    再关闭读端传输兜底，全程有界、且只针对本批进程，不波及其它批次。
    """
    pending = [t for t in pump_tasks.values() if not t.done()]
    for task in pending:
        task.cancel()
    if pending:
        await _wait_bounded(
            asyncio.gather(*pending, return_exceptions=True),
            config.SHUTDOWN_WAIT_SECONDS,
        )

    stuck = [t for t in pump_tasks.values() if not t.done()]
    if stuck:
        # 管道仍被子孙进程持有：先清理本批残留进程组（若进程本身还活着）
        if process.returncode is None:
            await terminate_process_group(process)
        # 组长可能已退出而组内仍有子孙：用记录的组 id 直接补杀整组
        if group_id is not None:
            try:
                os.killpg(group_id, signal.SIGKILL)
            except (ProcessLookupError, PermissionError, OSError):
                pass
        await _wait_bounded(process.wait(), config.SHUTDOWN_WAIT_SECONDS)
        _close_transport(process)
        await _wait_bounded(
            asyncio.gather(*stuck, return_exceptions=True),
            config.SHUTDOWN_WAIT_SECONDS,
        )
    # 无论协程状态如何，最后确保传输关闭、文件描述符不泄漏
    _close_transport(process)


async def run_recalc(
    task_dir: Path,
    input_name: str = "input.xlsx",
    cancel_event: asyncio.Event | None = None,
) -> Path:
    """执行一次 headless 转换重算。

    task_dir 下需已有 input.xlsx；输出到 task_dir/output/input.xlsx。
    超时或取消时有界地终止整个进程组并保证管道收尾后抛出对应异常，
    两种情况都不会产出/发布任何判定结果。
    """
    input_path = task_dir / input_name
    output_dir = task_dir / "output"
    profile_dir = task_dir / "lo-profile"
    output_dir.mkdir(parents=True, exist_ok=True)
    prepare_profile(profile_dir)

    cmd = [
        config.SOFFICE_BIN,
        "--headless",
        "--norestore",
        "--nolockcheck",
        "--nodefault",
        "--nofirststartwizard",
        "--invisible",
        f"-env:UserInstallation=file://{profile_dir.resolve()}",
        "--convert-to",
        "xlsx",
        "--outdir",
        str(output_dir.resolve()),
        str(input_path.resolve()),
    ]

    process = await asyncio.create_subprocess_exec(
        *cmd,
        cwd=str(task_dir),
        env=_build_env(),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        start_new_session=True,  # 独立进程组，便于整组 kill
    )

    # 进程一启动就持续抽干两路管道，缓冲有界，防止输出撑爆管道造成死锁
    stdout_buf = BoundedLogBuffer(config.MAX_LOG_BYTES_PER_STREAM)
    stderr_buf = BoundedLogBuffer(config.MAX_LOG_BYTES_PER_STREAM)
    pump_tasks = {
        "stdout": asyncio.create_task(
            _pump_stream(process.stdout, stdout_buf), name="pump-stdout"
        ),
        "stderr": asyncio.create_task(
            _pump_stream(process.stderr, stderr_buf), name="pump-stderr"
        ),
    }
    # start_new_session=True 时组长即子进程 pid，组长退出后组 id 仍可用于
    # killpg，提前记录，避免后续 getpgid 因组长消失而失败
    group_id = _process_group_id(process)

    deadline = asyncio.get_running_loop().time() + config.RECALC_TIMEOUT_SECONDS
    timed_out = False
    cancelled = False

    async def _watch() -> None:
        """轮询进程退出、取消事件与超时。

        整体包在 shield 中：外部协程被取消时，这里仍能完成进程组回收，
        再由调用方根据 cancel_event 转成 RecalcCancelled。
        """
        nonlocal timed_out, cancelled
        while True:
            if cancel_event is not None and cancel_event.is_set():
                cancelled = True
                return
            remaining = deadline - asyncio.get_running_loop().time()
            if remaining <= 0:
                timed_out = True
                return
            try:
                # 外层已用 shield 包裹；这里只做带超时的退出等待
                await asyncio.wait_for(
                    process.wait(),
                    timeout=min(config.POLL_INTERVAL_SECONDS, remaining),
                )
                return  # 进程正常退出
            except asyncio.TimeoutError:
                continue

    try:
        try:
            await asyncio.shield(_watch())
        except asyncio.CancelledError:
            # 外层任务被取消（正常取消走 event；这里做防御性兜底）
            if cancel_event is not None:
                cancel_event.set()
            cancelled = True

        # 取消或满 10 秒超时：有界终止本批进程组（含子孙），不发布判定
        if cancelled:
            await terminate_process_group(process)
            await _stop_pumps(pump_tasks, process, group_id)
            raise RecalcCancelled("任务已取消")
        if timed_out:
            await terminate_process_group(process)
            await _stop_pumps(pump_tasks, process, group_id)
            raise RecalcTimeout(
                f"重算超过 {config.RECALC_TIMEOUT_SECONDS} 秒"
            )

        # 正常退出：有界地等抽干协程读到 EOF（输出已被持续抽走，不会死锁）；
        # 万一子孙进程仍持有写端导致无 EOF，关闭传输兜底，同样有界
        await _stop_pumps(pump_tasks, process, group_id)
    finally:
        # 防御性兜底：任何路径下进程都必须被回收，管道都必须被关闭
        if process.returncode is None:
            await terminate_process_group(process)
        await _stop_pumps(pump_tasks, process, group_id)

    # 竞态兜底：启动进程可能在取消事件/超时被轮询到之前就已退出。
    # 只要取消或超时已发生，即使进程是正常退出，也绝不发布该批判定。
    if cancelled or (cancel_event is not None and cancel_event.is_set()):
        raise RecalcCancelled("任务已取消")
    if timed_out:
        raise RecalcTimeout(
            f"重算超过 {config.RECALC_TIMEOUT_SECONDS} 秒"
        )

    # 以下为正常退出路径：直接使用已缓冲的有界日志，不再对管道做阻塞 read()
    if process.returncode != 0:
        raise RecalcFailed(
            f"LibreOffice 退出码 {process.returncode}："
            f"{stderr_buf.text()[:300]}"
        )

    # soffice 即使 rc=0 也可能在输出阶段静默失败（如输入输出同路径），
    # 检查 stdout/stderr 中的 Error 标记
    combined = f"{stdout_buf.text()}\n{stderr_buf.text()}"
    if "Error:" in combined or "failed:" in combined.lower():
        raise RecalcFailed(f"LibreOffice 转换报错：{combined.strip()[:300]}")

    out_file = output_dir / input_name
    if not out_file.exists():
        raise RecalcFailed(
            f"未找到重算输出文件，soffice 输出：{stdout_buf.text()[:300]}"
        )
    return out_file


def _on_rm_error(func, path, exc_info):
    """shutil.rmtree 的 onerror（Python ≤3.11）：只读文件改权限后重试。"""
    try:
        os.chmod(path, 0o700)
        func(path)
    except (PermissionError, FileNotFoundError, OSError):
        # 最后兜底：环境不允许删除时不阻断终态流转
        pass


def _on_rm_error_312(func, path, exc):
    """shutil.rmtree 的 onexc（Python ≥3.12）。"""
    _on_rm_error(func, path, None)


def cleanup_task_dir(task_dir: Path) -> None:
    """任务终态后清理暂存（原件、profile、输出全部删除）。"""
    if not task_dir.exists():
        return
    # 子进程可能留下只读文件，先恢复写权限再删除
    for root, dirs, files in os.walk(task_dir):
        for name in dirs + files:
            try:
                os.chmod(os.path.join(root, name), 0o700)
            except (PermissionError, FileNotFoundError):
                pass
    # Python 3.12 起 onerror 改名为 onexc
    if sys.version_info >= (3, 12):
        shutil.rmtree(task_dir, onexc=_on_rm_error_312)
    else:
        shutil.rmtree(task_dir, onerror=_on_rm_error)
