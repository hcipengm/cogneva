-- Migration: 085_drop_tables_no_code_names
--
-- Nine tables are created here and named by no code path in this repository
-- and by no script in the private one: no SELECT, no INSERT, no DELETE. They
-- are not scaffolding waiting for a feature; they are the schema claiming
-- capabilities the platform does not have. A reader who finds model_pricing
-- concludes the platform prices model calls, and a zero row count reads as
-- "nobody has used it yet" rather than "nothing can use it".
--
-- The rule this migration applies: a table stays only if some code names it.
--
-- memory_raw_sources, memory_schema_entries and memory_summary_entries go
-- because the memory crate owns that data under its own names -- it creates
-- schema_entries and summary_entries at startup and writes those -- so these
-- three are a second, permanently empty copy of the same intent.
--
-- token_face_beat is not in this list on purpose: a scheduled job on the host
-- writes it, so its name does exist outside this tree.
--
-- The down migrations restore each shape verbatim, so the decision is
-- reversible without archaeology.

DROP TABLE IF EXISTS agent_performance;
DROP TABLE IF EXISTS model_pricing;
DROP TABLE IF EXISTS user_quotas;
DROP TABLE IF EXISTS user_devices;
DROP TABLE IF EXISTS user_biometric_keys;
DROP TABLE IF EXISTS memory_raw_sources;
DROP TABLE IF EXISTS memory_schema_entries;
DROP TABLE IF EXISTS memory_summary_entries;
DROP TABLE IF EXISTS principle_debt;
