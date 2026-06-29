-- compile_in_continuo: when true, the release runs continuo's dbt-compile leg
-- (status `compiling`) before parsing; when false (default), the producer's
-- uploaded manifest is used and the release goes straight Received->Parsing.
-- Immutable per release; default false (opt-in, rollout-safe).
ALTER TABLE releases ADD COLUMN compile_in_continuo boolean NOT NULL DEFAULT false;
