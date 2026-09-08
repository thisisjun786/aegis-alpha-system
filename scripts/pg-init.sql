-- Provision the container ledger database to satisfy the migration
-- ownership guard (ADR 0014): the database name and its comment must
-- match the aas_owned_<token>_test convention.
COMMENT ON DATABASE aas_owned_0123456789abcdef0123456789abcdef_test IS '0123456789abcdef0123456789abcdef';
