import signal
import subprocess
import sys
import time
from typing import Dict


CHILDREN: Dict[str, subprocess.Popen] = {}


def terminate_children() -> None:
    for process in CHILDREN.values():
        if process.poll() is None:
            process.terminate()

    deadline = time.time() + 10
    while time.time() < deadline:
        if all(process.poll() is not None for process in CHILDREN.values()):
            return
        time.sleep(0.2)

    for process in CHILDREN.values():
        if process.poll() is None:
            process.kill()


def handle_signal(signum, _frame) -> None:
    print(f"Received signal {signum}. Stopping web and worker processes...")
    terminate_children()
    sys.exit(0)


def main() -> None:
    commands = {
        "web": [sys.executable, "run_api.py"],
        "worker": [sys.executable, "run_bot.py"],
    }

    for name, command in commands.items():
        print(f"Starting {name}: {' '.join(command)}")
        CHILDREN[name] = subprocess.Popen(command)

    while True:
        for name, process in CHILDREN.items():
            exit_code = process.poll()
            if exit_code is not None:
                print(f"{name} exited with code {exit_code}. Shutting down service...")
                terminate_children()
                # Exit non-zero so Railway restarts the container.
                sys.exit(exit_code if exit_code != 0 else 1)
        time.sleep(1)


if __name__ == "__main__":
    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)
    main()
