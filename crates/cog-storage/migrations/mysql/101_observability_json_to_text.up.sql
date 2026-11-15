ALTER TABLE cog_observability_events MODIFY payload LONGTEXT NOT NULL;
ALTER TABLE cog_observability_logs   MODIFY metadata LONGTEXT NOT NULL;
ALTER TABLE cog_observability_squads MODIFY agents   LONGTEXT NOT NULL;
