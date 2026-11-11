-- Back to the name the column carried before 099. A downgrade only works on a
-- database whose rows did not outlive the rename; the value is the same either
-- way.
ALTER TABLE task_defined RENAME COLUMN declared_duration_seconds TO timeout_seconds;

ALTER TABLE tasks RENAME COLUMN declared_duration_seconds TO timeout_seconds;
