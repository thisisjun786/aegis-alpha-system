from __future__ import annotations

from aegis_alpha.modules import aegis, alpha, hedge


def module_catalog() -> list[dict[str, object]]:
    """Describe responsibilities; no module currently executes a strategy."""
    return [
        {
            "module": module.MODULE_ID.value,
            "responsibility": module.RESPONSIBILITY,
            "strategy_execution": "not_implemented",
            "allocation_model": "funded_long_only",
            "instrument_type_validation": False,
        }
        for module in (aegis, alpha, hedge)
    ]
