import subprocess
from string import Template
from typing import Any, Optional


def run_shell_cmd(cmd: Template, **substitutions: Optional[Any]) -> None:
    """
    Runs a shell command with optional template substitutions.

    Args:
        cmd (Template): The command template with optional placeholders.
        **substitutions (Optional[Any]): Optional key-value pairs to substitute into the command template.

    Returns:
        None: This function does not return any value.
    """
    subprocess.run(
        args=cmd.substitute(**substitutions),
        shell=True,  # Allows args to be passed as a string
        check=True,  # Prevents cmd from failing silently
    )
