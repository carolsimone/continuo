-- A bootstrap release promotes without validation (seeds current_prod + swaps
-- topology directly). The flag is immutable per release; default false.
ALTER TABLE releases ADD COLUMN bootstrap boolean NOT NULL DEFAULT false;
