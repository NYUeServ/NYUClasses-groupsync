CREATE TABLE groupsync_source_state (source_id varchar2(255), group_id varchar2(255), last_sync_time NUMBER default 0, failure_count integer default 0, primary key (source_id, group_id));
CREATE TABLE groupsync_target_state (target_id varchar2(255), group_id varchar2(255), username varchar2(255), role varchar2(30), primary key (target_id, group_id, username));

CREATE TABLE groupsync_target_store (target_id varchar2(255), set_name varchar2(255), value varchar2(1024));
CREATE INDEX groupsync_target_store_set on groupsync_target_store (target_id, set_name);

ALTER TABLE groupsync_target_state rename column username to user_id;
