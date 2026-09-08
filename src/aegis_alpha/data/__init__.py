"""Provider-neutral data contracts and storage primitives."""

from aegis_alpha.data.contracts import SourceSnapshot, ValidationStatus
from aegis_alpha.data.data_root import (
    DATA_ROOT_ENV_VAR,
    DataRoot,
    DataRootError,
    configured_data_root,
    resolve_data_root,
)
from aegis_alpha.data.raw_store import ContentAddressedRawStore

__all__ = [
    "DATA_ROOT_ENV_VAR",
    "ContentAddressedRawStore",
    "DataRoot",
    "DataRootError",
    "SourceSnapshot",
    "ValidationStatus",
    "configured_data_root",
    "resolve_data_root",
]
