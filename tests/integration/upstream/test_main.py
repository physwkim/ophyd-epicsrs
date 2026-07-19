import subprocess
import sys

from ophyd import __version__

import ophyd  # noqa: E402  # isort:skip — version gate

OPHYD_1_6 = ophyd.__version__.startswith("1.6.")


@pytest.mark.skipif(
    OPHYD_1_6,
    reason="upstream ophyd changed this behavior after 1.6; project pins ophyd==1.6.*",
)
def test_cli_version():
    cmd = [sys.executable, "-m", "ophyd", "--version"]
    assert subprocess.check_output(cmd).decode().strip() == __version__
