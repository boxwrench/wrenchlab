"""Isolated, bounded supervisor for configured health/owner/service commands.

Separate subreaper prevents helper cleanup from touching the experiment's tree.
The inner deadline survives loss of the caller. No arbitrary inherited secrets.
"""
import ctypes
import json
import os
from pathlib import Path
import resource
import signal
import subprocess
import sys
import tempfile
import time
from execution_worker import clean_processes


def main():
    if ctypes.CDLL(None, use_errno=True).prctl(36, 1, 0, 0, 0) != 0:
        raise RuntimeError('probe subreaper unavailable')
    resource.setrlimit(resource.RLIMIT_FSIZE, (1024 * 1024, 1024 * 1024))
    command = None
    end = time.monotonic() + float(sys.argv[1])
    stopped = False
    def stop(*_):
        nonlocal stopped
        stopped = True
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    with tempfile.TemporaryFile() as out, tempfile.TemporaryFile() as err:
        try:
            command = subprocess.Popen(sys.argv[2:], stdout=out, stderr=err,
                                       stdin=subprocess.DEVNULL, start_new_session=True)
            while True:
                if stopped or time.monotonic() >= end:
                    raise TimeoutError('configured helper deadline exceeded')
                status = os.waitid(os.P_PID, command.pid, os.WEXITED | os.WNOHANG | os.WNOWAIT)
                if status:
                    code = status.si_status if status.si_code == os.CLD_EXITED else -status.si_status
                    break
                time.sleep(.02)
        finally:
            clean_processes(command)
        out.seek(0); err.seek(0)
        print(json.dumps({'returncode': code, 'stdout': out.read().decode(errors='replace'),
                          'stderr': err.read().decode(errors='replace')}))


if __name__ == '__main__':
    main()
