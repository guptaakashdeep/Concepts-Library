from __future__ import annotations

from airflow.operators.bash import BashOperator
from custom_subprocess_hook import ExtendedSubprocessHook

import os
import shutil
import warnings
from functools import cached_property
from typing import Sequence, cast

from airflow.exceptions import AirflowException, AirflowSkipException

from airflow.models.taskinstance import TaskInstance
from airflow.utils.context import Context


template_fields: Sequence[str] = ("bash_command", "env", "cwd")
template_fields_renderers = {"bash_command": "bash", "env": "json"}
template_ext: Sequence[str] = (".sh", ".bash")
ui_color = "#f0ede4"


class CustomBashOperator(BashOperator):
    """
    CustomBashOperator is a custom implementation of the BashOperator that allows for additional functionality
    such as searching for specific keywords in the logs.

    Attributes:
        search_kw (str): Keyword to search for in the logs.

    Methods:
        subprocess_hook: Returns an ExtendedSubprocessHook for running the bash command.
        execute(context: Context): Executes the bash command in a subprocess, handling environment setup and error checking.
        on_kill(): Sends a SIGTERM signal to the subprocess to terminate it.

    Raises:
        AirflowException: If the current working directory (cwd) is invalid or if the bash command fails.
        AirflowSkipException: If the bash command returns an exit code that indicates the task should be skipped.
    """

    def __init__(self, kw, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.search_kw = kw

    @cached_property
    def subprocess_hook(self):
        """Returns hook for running the bash command."""
        return ExtendedSubprocessHook(log_search_kw=self.search_kw)


    def execute(self, context: Context):
        bash_path = shutil.which("bash") or "bash"
        if self.cwd is not None:
            if not os.path.exists(self.cwd):
                raise AirflowException(f"Can not find the cwd: {self.cwd}")
            if not os.path.isdir(self.cwd):
                raise AirflowException(f"The cwd {self.cwd} must be a directory")
        env = super().get_env(context)

        # Because the bash_command value is evaluated at runtime using the @tash.bash decorator, the
        # RenderedTaskInstanceField data needs to be rewritten and the bash_command value re-rendered -- the
        # latter because the returned command from the decorated callable could contain a Jinja expression.
        # Both will ensure the correct Bash command is executed and that the Rendered Template view in the UI
        # displays the executed command (otherwise it will display as an ArgNotSet type).
        if self._init_bash_command_not_set:
            ti = cast("TaskInstance", context["ti"])
            super().refresh_bash_command(ti)

        result = self.subprocess_hook.run_command(
            command=[bash_path, "-c", self.bash_command],
            env=env,
            output_encoding=self.output_encoding,
            cwd=self.cwd,
        )
        if result.exit_code in self.skip_on_exit_code:
            raise AirflowSkipException(f"Bash command returned exit code {result.exit_code}. Skipping.")
        elif result.exit_code != 0:
            raise AirflowException(
                f"Bash command failed. The command returned a non-zero exit code {result.exit_code}."
            )

        return result.output

    def on_kill(self) -> None:
        self.subprocess_hook.send_sigterm()
