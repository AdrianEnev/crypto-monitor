"""Load shared credentials and project settings without rewriting either file."""

import os
import re
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_ENV_FILE = PROJECT_ROOT / ".env"
DEFAULT_DATABASE = PROJECT_ROOT / "monitor.sqlite3"
SHARED_ENV_FILE = PROJECT_ROOT.parent / ".env"


def read_env_values(path, *, required=False):
    """Read the project's existing strict NAME=value format without interpolation."""
    path = Path(path).expanduser().resolve()
    if not path.is_file():
        if required:
            raise ValueError(f"required environment file not found: {path}")
        return {}
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except (OSError, UnicodeError) as exc:
        raise ValueError(f"cannot read environment file: {path}") from exc
    values = {}
    for number, line in enumerate(lines, 1):
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[7:].strip()
        if "=" not in line:
            raise ValueError(f"{path.name}:{number}: expected NAME=value")
        key, value = (part.strip() for part in line.split("=", 1))
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", key):
            raise ValueError(f"{path.name}:{number}: invalid environment variable name")
        if key in values:
            raise ValueError(f"{path.name}:{number}: duplicate setting {key}")
        if value[:1] in ("'", '"'):
            if len(value) < 2 or value[-1] != value[0]:
                raise ValueError(f"{path.name}:{number}: unclosed quote")
            value = value[1:-1]
        else:
            value = value.split(" #", 1)[0].strip()
        values[key] = value
    return values


def load_environment(project_file=DEFAULT_ENV_FILE, *, shared_file=None):
    """Precedence: explicit process variables, project file, shared parent file.

    The shared path is anchored to this package, independent of the working
    directory or an optional custom project settings file. Called once during
    CLI startup; exported values persist in this process. No import-time I/O.
    """
    shared_file = SHARED_ENV_FILE if shared_file is None else shared_file
    # Parse both before exporting anything, so malformed input cannot partially
    # update the process environment.
    shared = read_env_values(shared_file)
    project = read_env_values(project_file)
    for key, value in {**shared, **project}.items():
        os.environ.setdefault(key, value)


def email_region():
    """Use an explicit AWS region or the region in an SES SMTP endpoint."""
    region = os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION")
    if region:
        return region
    match = re.fullmatch(r"email-smtp\.([a-z0-9-]+)\.amazonaws\.com(?:\.cn)?",
                         os.environ.get("SES_SMTP_HOST", "").strip())
    return match.group(1) if match else None
