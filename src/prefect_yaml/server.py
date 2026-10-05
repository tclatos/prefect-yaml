"""Prefect server process lifecycle management."""

from __future__ import annotations

import os
import signal
import subprocess
import time
import webbrowser
from pathlib import Path
from typing import Any

import httpx
from loguru import logger
from pydantic import BaseModel


class PrefectServer(BaseModel):
    """Local Prefect server process manager."""

    host: str = "127.0.0.1"
    port: int = 4200
    api_url: str | None = None
    pid_file: Path | None = None

    model_config = {"arbitrary_types_allowed": True}

    def model_post_init(self, context: Any, /) -> None:
        """Initialize host and port from environment if set."""
        if not self.api_url and "PREFECT_API_URL" in os.environ:
            self.api_url = os.environ["PREFECT_API_URL"]

    @property
    def resolved_api_url(self) -> str:
        """Full API URL for the Prefect server."""
        if self.api_url:
            return self.api_url
        return f"http://{self.host}:{self.port}/api"

    @property
    def ui_url(self) -> str:
        """Browser dashboard URL."""
        return f"http://{self.host}:{self.port}"

    @property
    def resolved_pid_file(self) -> Path:
        """Path to PID tracking file."""
        if self.pid_file:
            return self.pid_file
        path = Path.home() / ".cache" / "prefect_yaml" / "prefect.pid"
        path.parent.mkdir(parents=True, exist_ok=True)
        return path

    def is_running(self) -> bool:
        """Check whether the Prefect server is active and responding.

        Returns:
            True if running and healthy, False otherwise.
        """
        pid = self._read_pid()
        if pid is not None:
            try:
                os.kill(pid, 0)
            except (ProcessLookupError, PermissionError):
                self.resolved_pid_file.unlink(missing_ok=True)

        try:
            client = httpx.Client(proxy=None, trust_env=False)
            resp = client.get(f"{self.resolved_api_url}/health", timeout=3.0)
            return resp.status_code == 200
        except Exception:
            return False

    def start(self, *, foreground: bool = False) -> None:
        """Start the Prefect server process.

        Args:
            foreground: Block current process if True, run detached daemon if False.
        """
        if self.is_running():
            logger.info("Prefect server already running at {}", self.ui_url)
            return

        cmd = [
            "prefect",
            "server",
            "start",
            "--host",
            self.host,
            "--port",
            str(self.port),
        ]

        if foreground:
            logger.info("Starting Prefect server at {} (foreground)...", self.ui_url)
            subprocess.run(cmd, check=False)
            return

        logger.info("Starting Prefect server daemon at {}...", self.ui_url)
        log_file = self.resolved_pid_file.with_suffix(".log")
        with open(log_file, "w") as out:
            proc = subprocess.Popen(
                cmd,
                stdout=out,
                stderr=subprocess.STDOUT,
                start_new_session=True,
            )
        self.resolved_pid_file.write_text(str(proc.pid))

        # Wait up to 10 seconds for healthy status
        for _ in range(20):
            time.sleep(0.5)
            if self.is_running():
                logger.info("Prefect server ready at {}", self.ui_url)
                return

    def stop(self) -> None:
        """Stop the running Prefect server daemon."""
        pid = self._read_pid()
        if pid is not None:
            try:
                os.kill(pid, signal.SIGTERM)
                time.sleep(1.0)
                if self.is_running():
                    os.kill(pid, signal.SIGKILL)
            except Exception as exc:
                logger.debug("Error stopping server pid {}: {}", pid, exc)
            self.resolved_pid_file.unlink(missing_ok=True)
            logger.info("Prefect server stopped.")

    def open_ui(self) -> None:
        """Open the Prefect UI in a web browser."""
        webbrowser.open(self.ui_url)

    def _read_pid(self) -> int | None:
        if self.resolved_pid_file.exists():
            try:
                return int(self.resolved_pid_file.read_text().strip())
            except Exception:
                return None
        return None
