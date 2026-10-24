-- Migration: 082_datasets
-- See the Postgres migration of the same name for why the catalog exists, why
-- a version is unique per name, and why row_count and the coverage bounds are
-- nullable rather than zero-filled.
--
-- Dialect differences, and only these: no regex check on the digest (MySQL 8
-- would allow one, but the column width is the part that matters here), and
-- lengths are bounded because MySQL keys have to be.
CREATE TABLE IF NOT EXISTS datasets (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(191) NOT NULL,
    version VARCHAR(191) NOT NULL,
    object_key VARCHAR(512) NOT NULL,
    content_sha256 VARCHAR(80) NOT NULL,
    bytes BIGINT NOT NULL,
    row_count BIGINT NULL,
    coverage_from TIMESTAMP NULL,
    coverage_to TIMESTAMP NULL,
    source VARCHAR(255) NOT NULL,
    media_type VARCHAR(255) NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY datasets_one_row_per_version (name, version),
    KEY idx_datasets_name_created (name, created_at)
);
