-- Migration: 026_drop_unused_memory_tables
-- 006_memory created memory_raw_sources / memory_schema_entries /
-- memory_summary_entries, but nothing has ever written them. The memory layers
-- run against tables they create themselves: schema_entries (cog-memory
-- postgres_schema.rs) and summary_entries (postgres_entry_store.rs), while the
-- Raw layer stores objects through the ObjectBackend. The 006 shapes cannot
-- carry the current types either -- memory_summary_entries has no namespace
-- column and keeps the embedding as BLOB.
--
-- Keeping them is not inert: they are a second, permanently empty copy of the
-- same concept, so any row count taken on them reads 0 even when memory is
-- healthy. Drop them so the tables that describe memory state are the ones the
-- process actually uses.

DROP TABLE IF EXISTS memory_summary_entries;
DROP TABLE IF EXISTS memory_schema_entries;
DROP TABLE IF EXISTS memory_raw_sources;
