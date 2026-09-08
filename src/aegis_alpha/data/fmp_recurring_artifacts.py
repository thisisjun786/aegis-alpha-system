"""Bounded filesystem loading for detached recurring authority artifacts."""

from __future__ import annotations

import stat
from pathlib import Path
from typing import Final

from aegis_alpha.data.fmp_recurring_errors import RecurringAuthorityError

_MAX_AUTHORITY_BYTES: Final = 4096
_SIGNATURE_BYTES: Final = 64


def load_recurring_authority(path: Path, signature_path: Path) -> tuple[bytes, bytes]:
    """Read bounded detached authority artifacts without accepting non-files."""

    try:
        authority_metadata = path.lstat()
        signature_metadata = signature_path.lstat()
        if not stat.S_ISREG(authority_metadata.st_mode) or not stat.S_ISREG(
            signature_metadata.st_mode
        ):
            raise RecurringAuthorityError("recurring authority artifacts must be regular files")
        if authority_metadata.st_size > _MAX_AUTHORITY_BYTES:
            raise RecurringAuthorityError("recurring authority exceeds 4096 bytes")
        if signature_metadata.st_size != _SIGNATURE_BYTES:
            raise RecurringAuthorityError("recurring authority signature must contain 64 bytes")
        return path.read_bytes(), signature_path.read_bytes()
    except OSError as error:
        raise RecurringAuthorityError("cannot read recurring authority artifacts") from error
