"""Run the frozen official addon and MCP server inside one isolated task container."""

import os
import socket
import subprocess
import sys
import time
from pathlib import Path


def main() -> None:
    Path(os.environ["HOME"]).mkdir(parents=True, exist_ok=True)
    with open("/tmp/display.log", "w") as display_log, open("/tmp/blender.log", "w") as log:
        display = subprocess.Popen(
            ["Xvfb", ":99", "-screen", "0", "1280x720x24"],
            stdout=display_log,
            stderr=subprocess.STDOUT,
        )
        blender = subprocess.Popen(
            ["/opt/blender/blender", "--factory-startup", "--python", "/opt/adapter/addon.py"],
            stdout=log,
            stderr=subprocess.STDOUT,
        )
        try:
            deadline = time.monotonic() + 90
            while time.monotonic() < deadline:
                if blender.poll() is not None:
                    raise RuntimeError(
                        "Blender exited before addon readiness; inspect /tmp/blender.log"
                    )
                try:
                    with socket.create_connection(("127.0.0.1", 9876), timeout=1):
                        break
                except OSError:
                    time.sleep(0.5)
            else:
                raise TimeoutError("Blender addon did not become ready")
            # MCP owns stdin/stdout; Blender logs never corrupt the protocol stream.
            result = subprocess.run([sys.executable, "/opt/adapter/server.py"], check=False)
            raise SystemExit(result.returncode)
        finally:
            for proc in (blender, display):
                if proc.poll() is None:
                    proc.terminate()
                    try:
                        proc.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        proc.kill()
                        proc.wait()


if __name__ == "__main__":
    main()
