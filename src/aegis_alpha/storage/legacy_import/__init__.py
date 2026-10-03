"""``aas import legacy``: retain legacy originals in ``raw/`` as content-addressed sources.

A hashed ``aas-legacy-import-v1`` manifest (``manifest``) names legacy locations and the
registered loader that reads each one (``loaders``, ``norgate``, ``public``). ``engine``
plans without writing, applies unit by unit through the source library's content path, and
verifies that every planned source is committed, identical and backed by intact raw bytes.
"""
