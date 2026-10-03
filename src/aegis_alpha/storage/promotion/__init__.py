"""Promotion of source-library tables into typed market generations (``aas-promotion-v1``).

``dev-notes/design/data-vertical.md`` owns the contract. ``spec`` parses the hashed
document, ``mappers`` holds the registered source shapes, ``time_rules`` and
``decimal_rules`` the versioned rules, ``formats`` the frozen hash formats, and
``engine`` plans, applies, recovers and verifies a promotion.
"""
