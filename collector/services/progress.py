"""Step-based progress logging for long-running batch loops."""

import logging
import time
from collections.abc import Callable


class ProgressLogger:
    """Log aggregate progress at fixed percentage steps.

    Tracks elapsed time internally so callers only report counts done.
    """

    def __init__(
        self,
        logger: logging.Logger,
        step_pct: int = 5,
        level: int = logging.INFO,
    ):
        """Create logger that logs at fixed ``step_pct`` intervals."""
        self._logger = logger
        self._step_pct = step_pct
        self._level = level
        self._started = time.monotonic()
        self._next_log_pct = step_pct

    def maybe_log(
        self,
        done: int,
        total: int,
        render: Callable[[int, int, int, float], str],
        force: bool = False,
    ) -> None:
        """Log when ``done / total`` crosses the next percentage step.

        Args:
            done: Completed items so far.
            total: Total items to process.
            render: Formats the log line from (pct, done, total, elapsed_seconds).
            force: Log regardless of step threshold (e.g. final summary).
        """
        pct = done * 100 // total if total else 100
        if not force and pct < self._next_log_pct:
            return
        elapsed = time.monotonic() - self._started
        self._logger.log(self._level, render(pct, done, total, elapsed))
        self._next_log_pct = pct + self._step_pct
