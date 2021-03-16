--LAUNCH

DROP INDEX IF EXISTS launch_project_idx;
DROP INDEX IF EXISTS launch_user_idx;
DROP INDEX IF EXISTS launch_uuid_idx;

--TEST_ITEM

DROP INDEX IF EXISTS ti_parent_idx;
DROP INDEX IF EXISTS ti_launch_idx;
DROP INDEX IF EXISTS ti_retry_of_idx;
DROP INDEX IF EXISTS ti_uuid_idx;
DROP INDEX IF EXISTS test_item_unique_id_idx;
DROP INDEX IF EXISTS item_test_case_id_idx;
DROP INDEX IF EXISTS test_item_unique_id_launch_id_idx;
DROP INDEX IF EXISTS item_test_case_id_launch_id_idx;
DROP INDEX IF EXISTS path_gist_idx;
DROP INDEX IF EXISTS path_idx;
DROP INDEX IF EXISTS test_case_hash_idx;
DROP INDEX IF EXISTS test_case_hash_launch_id_idx;

ALTER TABLE test_item
    DROP CONSTRAINT IF EXISTS test_item_retry_of_fkey;

--LOG
DROP INDEX IF EXISTS log_ti_idx;
DROP INDEX IF EXISTS log_message_trgm_idx;
DROP INDEX IF EXISTS log_uuid_idx;
DROP INDEX IF EXISTS log_attach_id_idx;
ALTER TABLE log
    DROP CONSTRAINT IF EXISTS log_check,
    DROP CONSTRAINT IF EXISTS log_attachment_id_fkey;

--STATISTICS

ALTER TABLE statistics
    DROP CONSTRAINT IF EXISTS statistics_statistics_field_id_fkey,
    DROP CONSTRAINT IF EXISTS unique_stats_item,
    DROP CONSTRAINT IF EXISTS unique_stats_launch,
    DROP CONSTRAINT IF EXISTS statistics_check;
DROP INDEX IF EXISTS statistics_ti_idx;
DROP INDEX IF EXISTS statistics_launch_idx;

ALTER TABLE issue
    DROP CONSTRAINT IF EXISTS issue_issue_id_fkey;
DROP INDEX IF EXISTS issue_it_idx;
DROP INDEX IF EXISTS ticket_submitter_idx;
ALTER TABLE public.issue_ticket
    DROP CONSTRAINT IF EXISTS issue_ticket_issue_id_fkey,
    DROP CONSTRAINT IF EXISTS issue_ticket_ticket_id_fkey;

DROP INDEX IF EXISTS parameter_ti_idx;

DROP INDEX IF EXISTS item_attr_ti_idx;
DROP INDEX IF EXISTS item_attr_launch_idx;
ALTER TABLE item_attribute
    DROP CONSTRAINT IF EXISTS item_attribute_check;


ALTER TABLE issue
    SET (AUTOVACUUM_ENABLED = FALSE);
ALTER TABLE test_item_results
    SET (AUTOVACUUM_ENABLED = FALSE);
ALTER TABLE test_item
    SET (AUTOVACUUM_ENABLED = FALSE);
ALTER TABLE item_attribute
    SET (AUTOVACUUM_ENABLED = FALSE);
ALTER TABLE parameter
    SET (AUTOVACUUM_ENABLED = FALSE);
ALTER TABLE statistics
    SET (AUTOVACUUM_ENABLED = FALSE);
ALTER TABLE log
    SET (AUTOVACUUM_ENABLED = FALSE);