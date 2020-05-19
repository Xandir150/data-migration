--LAUNCH

CREATE INDEX IF NOT EXISTS launch_project_idx ON launch (project_id);
CREATE INDEX IF NOT EXISTS launch_user_idx ON launch (user_id);

--TEST_ITEM

CREATE INDEX IF NOT EXISTS ti_parent_idx ON test_item (parent_id NULLS LAST);
CREATE INDEX IF NOT EXISTS ti_launch_idx ON test_item (launch_id NULLS LAST);
CREATE INDEX IF NOT EXISTS ti_retry_of_idx ON test_item (retry_of NULLS LAST);
CREATE INDEX IF NOT EXISTS test_item_unique_id_launch_id_idx ON test_item (unique_id, launch_id);
CREATE INDEX IF NOT EXISTS item_test_case_id_launch_id_idx ON test_item (test_case_id, launch_id);
CREATE INDEX IF NOT EXISTS path_gist_idx ON test_item USING gist (path);
CREATE INDEX IF NOT EXISTS path_idx ON test_item USING btree (path);
CREATE INDEX IF NOT EXISTS test_case_hash_launch_id_idx ON test_item (test_case_hash, launch_id);

ALTER TABLE test_item
    ADD CONSTRAINT test_item_retry_of_fkey FOREIGN KEY (retry_of) REFERENCES test_item (item_id) ON DELETE CASCADE;

--LOG
CREATE INDEX IF NOT EXISTS log_ti_idx ON log (item_id);
CREATE INDEX IF NOT EXISTS log_message_trgm_idx ON log USING gin (log_message gin_trgm_ops);
CREATE INDEX IF NOT EXISTS log_attach_id_idx ON log (attachment_id);
ALTER TABLE log
    ADD CONSTRAINT log_check CHECK ((item_id IS NOT NULL AND launch_id IS NULL) OR (item_id IS NULL AND launch_id IS NOT NULL)),
    ADD CONSTRAINT log_attachment_id_fkey FOREIGN KEY (attachment_id) REFERENCES attachment (id) ON DELETE SET NULL;

--STATISTICS

ALTER TABLE statistics
    ADD CONSTRAINT statistics_statistics_field_id_fkey FOREIGN KEY (statistics_field_id) REFERENCES statistics_field (sf_id) ON DELETE CASCADE,
    ADD CONSTRAINT unique_stats_item UNIQUE (statistics_field_id, item_id),
    ADD CONSTRAINT unique_stats_launch UNIQUE (statistics_field_id, launch_id),
    ADD CONSTRAINT statistics_check
        CHECK (statistics.s_counter >= 0 AND ((item_id IS NOT NULL AND launch_id IS NULL) OR (launch_id IS NOT NULL AND item_id IS NULL)));

CREATE INDEX IF NOT EXISTS statistics_ti_idx ON statistics (item_id NULLS LAST);
CREATE INDEX IF NOT EXISTS statistics_launch_idx ON statistics (launch_id NULLS LAST);

ALTER TABLE issue
    ADD CONSTRAINT issue_issue_id_fkey
        FOREIGN KEY (issue_id) REFERENCES test_item_results (result_id) ON DELETE CASCADE;
CREATE INDEX IF NOT EXISTS issue_it_idx ON issue (issue_type);
CREATE INDEX IF NOT EXISTS ticket_submitter_idx ON ticket (submitter);
ALTER TABLE reportportal.public.issue_ticket
    ADD CONSTRAINT issue_ticket_issue_id_fkey FOREIGN KEY (issue_id) REFERENCES issue (issue_id) ON DELETE CASCADE,
    ADD CONSTRAINT issue_ticket_ticket_id_fkey FOREIGN KEY (ticket_id) REFERENCES ticket (id);

CREATE INDEX IF NOT EXISTS parameter_ti_idx
    ON parameter (item_id);

ALTER TABLE item_attribute
    ADD CONSTRAINT item_attribute_check CHECK ((item_id IS NOT NULL AND launch_id IS NULL) OR (item_id IS NULL AND launch_id IS NOT NULL));
CREATE INDEX IF NOT EXISTS item_attr_ti_idx ON item_attribute (item_id NULLS LAST);
CREATE INDEX IF NOT EXISTS item_attr_launch_idx ON item_attribute (launch_id NULLS LAST);


ALTER TABLE issue
    SET (AUTOVACUUM_ENABLED = TRUE);
ALTER TABLE pattern_template_test_item
    SET (AUTOVACUUM_ENABLED = TRUE);
ALTER TABLE parameter
    SET (AUTOVACUUM_ENABLED = TRUE);
ALTER TABLE item_attribute
    SET (AUTOVACUUM_ENABLED = TRUE);
ALTER TABLE statistics
    SET (AUTOVACUUM_ENABLED = TRUE);
ALTER TABLE test_item_results
    SET (AUTOVACUUM_ENABLED = TRUE);
ALTER TABLE log
    SET (AUTOVACUUM_ENABLED = TRUE);
ALTER TABLE test_item
    SET (AUTOVACUUM_ENABLED = TRUE);

ANALYSE acl_class;
ANALYSE acl_entry;
ANALYSE acl_object_identity;
ANALYSE acl_sid;
ANALYSE attachment;
ANALYSE attribute;
ANALYSE content_field;
ANALYSE dashboard;
ANALYSE dashboard_widget;
ANALYSE filter;
ANALYSE filter_condition;
ANALYSE filter_sort;
ANALYSE integration;
ANALYSE integration_type;
ANALYSE issue;
ANALYSE issue_ticket;
ANALYSE issue_type;
ANALYSE issue_type_project;
ANALYSE item_attribute;
ANALYSE launch;
ANALYSE launch_attribute_rules;
ANALYSE launch_names;
ANALYSE launch_number;
ANALYSE log;
ANALYSE oauth_access_token;
ANALYSE parameter;
ANALYSE project;
ANALYSE project_attribute;
ANALYSE project_user;
ANALYSE recipients;
ANALYSE sender_case;
ANALYSE server_settings;
ANALYSE shareable_entity;
ANALYSE statistics;
ANALYSE statistics_field;
ANALYSE test_item;
ANALYSE test_item_results;
ANALYSE ticket;
ANALYSE user_preference;
ANALYSE users;
ANALYSE widget;
ANALYSE widget_filter;

SELECT setval('test_item_item_id_seq', max(item_id)) FROM test_item;
SELECT setval('log_id_seq', max(id)) FROM log;