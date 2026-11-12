-- A seed's column fingerprint: the hash of its CSV header and column_types as
-- its release's compile reported them; empty when the release carried no compile
-- extras for the seed's service.
ALTER TABLE topology_nodes ADD COLUMN seed_columns_hash text NOT NULL DEFAULT '';
-- The column fingerprint of the version last loaded, written with every recorded
-- load and adoption; empty for a load recorded without one.
ALTER TABLE node_materializations ADD COLUMN seed_columns_hash text NOT NULL DEFAULT '';
