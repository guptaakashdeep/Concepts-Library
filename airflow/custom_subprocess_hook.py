from __future__ import annotations

import contextlib
import os
import signal
from collections import namedtuple
from subprocess import PIPE, STDOUT, Popen
from tempfile import TemporaryDirectory, gettempdir

from airflow.hooks.subprocess import SubprocessHook

SubprocessResult = namedtuple("SubprocessResult", ["exit_code", "output"])


class ExtendedSubprocessHook(SubprocessHook):
    """Hook for running processes with the ``subprocess`` module."""

    def __init__(self, log_search_kw, **kwargs) -> None:
        super().__init__(**kwargs)
        self.log_search_kw = log_search_kw

    def run_command(
        self,
        command: list[str],
        env: dict[str, str] | None = None,
        output_encoding: str = "utf-8",
        cwd: str | None = None,
    ) -> SubprocessResult:
        """
        Execute the command.

        If ``cwd`` is None, execute the command in a temporary directory which will be cleaned afterwards.
        If ``env`` is not supplied, ``os.environ`` is passed

        :param command: the command to run
        :param env: Optional dict containing environment variables to be made available to the shell
            environment in which ``command`` will be executed.  If omitted, ``os.environ`` will be used.
            Note, that in case you have Sentry configured, original variables from the environment
            will also be passed to the subprocess with ``SUBPROCESS_`` prefix. See
            :doc:`/administration-and-deployment/logging-monitoring/errors` for details.
        :param output_encoding: encoding to use for decoding stdout
        :param cwd: Working directory to run the command in.
            If None (default), the command is run in a temporary directory.
        :return: :class:`namedtuple` containing ``exit_code`` and ``output``, the last line from stderr
            or stdout
        """
        self.log.info("Tmp dir root location: %s", gettempdir())
        with contextlib.ExitStack() as stack:
            if cwd is None:
                cwd = stack.enter_context(TemporaryDirectory(prefix="airflowtmp"))

            def pre_exec():
                # Restore default signal disposition and invoke setsid
                for sig in ("SIGPIPE", "SIGXFZ", "SIGXFSZ"):
                    if hasattr(signal, sig):
                        signal.signal(getattr(signal, sig), signal.SIG_DFL)
                os.setsid()

            self.log.info("Running command: %s", command)

            self.sub_process = Popen(
                command,
                stdout=PIPE,
                stderr=STDOUT,
                cwd=cwd,
                env=env if env or env == {} else os.environ,
                preexec_fn=pre_exec,
            )

            self.log.info("Output:")
            line = ""
            if self.sub_process is None:
                raise RuntimeError("The subprocess should be created here and is None!")
            if self.sub_process.stdout is not None:
                for raw_line in iter(self.sub_process.stdout.readline, b""):
                    log_line = raw_line.decode(output_encoding, errors="backslashreplace").rstrip()
                    self.log.info("%s", log_line)
                    # Filter for a specific line, e.g., lines containing "custom_response"
                    if self.log_search_kw in log_line:
                        # Optionally, assign the filtered line to a variable
                        line = log_line
                        self.log.info("%s", line)

            self.sub_process.wait()

            self.log.info("Command exited with return code %s", self.sub_process.returncode)
            return_code: int = self.sub_process.returncode

        return SubprocessResult(exit_code=return_code, output=line)