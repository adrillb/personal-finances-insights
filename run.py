"""Run either Streamlit or Flask UI based on environment config."""

from __future__ import annotations

import os
import subprocess
import sys

from dotenv import load_dotenv


def _run_streamlit() -> int:
    port = os.getenv("STREAMLIT_PORT", "8501")
    command = [
        sys.executable,
        "-m",
        "streamlit",
        "run",
        "app.py",
        "--server.port",
        str(port),
    ]
    print(f"[run.py] UI_MODE=streamlit -> starting Streamlit on http://localhost:{port}")
    completed = subprocess.run(command, check=False)
    return completed.returncode


def _run_flask() -> int:
    host = os.getenv("FLASK_HOST", "0.0.0.0")
    port = os.getenv("FLASK_PORT", "8080")
    local_hint = f"http://localhost:{port}" if host == "0.0.0.0" else f"http://{host}:{port}"
    print(f"[run.py] UI_MODE=flask -> starting Flask dashboard on {local_hint}")
    from app_flask import run as run_flask

    run_flask()
    return 0


def main() -> int:
    load_dotenv()
    mode = os.getenv("UI_MODE", "streamlit").strip().lower()
    if mode == "flask":
        return _run_flask()
    if mode == "streamlit":
        return _run_streamlit()

    print(f"[run.py] Unknown UI_MODE={mode!r}. Valid values: streamlit, flask.")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
