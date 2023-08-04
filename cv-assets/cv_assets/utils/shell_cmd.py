import subprocess
from string import Template
from typing import Any, Optional


def run_shell_cmd(cmd: Template, **substitutions: Optional[Any]) -> None:
    subprocess.run(
        args=cmd.substitute(**substitutions),
        shell=True,  # Allows args to be passed as a string
        check=True,  # Prevents cmd from failing silently
    )
