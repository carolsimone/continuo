-- current_prod records the manifests_uri of the promoted release so the next
-- release's validation defer-state base points at exactly where that release's
-- manifests were uploaded. Reconstructing the URI from a hardcoded bucket would
-- diverge from the submitted location (e.g. a different bucket per environment).
ALTER TABLE current_prod ADD COLUMN manifests_uri text NOT NULL DEFAULT '';
