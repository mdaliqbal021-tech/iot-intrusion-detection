import os
from pathlib import Path

import kagglehub


def dataset_root() -> Path:
    """Return the directory containing CICIOT23."""
    configured = os.environ.get("CICIOT2023_DATA_DIR")
    if configured:
        return Path(configured).expanduser().resolve()
    return Path(kagglehub.dataset_download("himadri07/ciciot2023/versions/1"))
