package edu.nyu.classes.groupsync.main;

import edu.nyu.classes.groupsync.api.Differences;
import edu.nyu.classes.groupsync.api.Group;
import edu.nyu.classes.groupsync.api.GroupSet;
import edu.nyu.classes.groupsync.api.GroupTarget;
import edu.nyu.classes.groupsync.api.TargetStore;
import edu.nyu.classes.groupsync.main.db.DB;
import edu.nyu.classes.groupsync.main.db.DBAction;
import edu.nyu.classes.groupsync.main.db.DBConnection;
import edu.nyu.classes.groupsync.main.db.DBPreparedStatement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;


public class ReplicationState implements TargetStore {
    private static Logger logger = LoggerFactory.getLogger(ReplicationState.class);

    private DataSource db;

    public ReplicationState(DataSource db) {
        this.db = db;
    }

    public long lastUpdateForSource(String sourceId) throws Exception {
        return lastSyncTimeForGroup(sourceId, dummyGroupForSource(sourceId));
    }

    public void markSourceAsUpdated(String sourceId, long now) throws Exception {
        markGroupAsSynced(sourceId, dummyGroupForSource(sourceId), now);
    }

    private Group dummyGroupForSource(String sourceId) {
        return new Group("_GROUPSYNC_ALL_GROUPS_", "");
    }


    public long lastSyncTimeForGroup(final String sourceId, Group group) throws Exception {
        return DB.transaction(db, new DBAction<Long>() {
            @Override
            public Long call(DBConnection c) throws SQLException {
                DBPreparedStatement select = c.run("select last_sync_time from groupsync_source_state" +
                        " where source_id = ? AND group_id = ?");
                select.param(sourceId);
                select.param(group.getName());

                long result = 0;

                for (ResultSet rs : select.executeQuery()) {
                    result = rs.getLong("last_sync_time");
                }

                return result;
            }
        });
    }


    public void storeRemoteMemberships(final String targetId, final GroupSet groups) throws Exception {
        DB.transaction(db, new DBAction<Void>() {
            @Override
            public Void call(DBConnection c) throws SQLException {
                DBPreparedStatement delete = c.run("delete from groupsync_target_state" +
                        " where target_id = ? AND group_id = ?");

                DBPreparedStatement insert = c.run("insert into groupsync_target_state" +
                        " (target_id, group_id, user_id, role) VALUES (?, ?, ?, ?)");

                try {
                    for (Group g : groups) {
                        delete.clearParameters();
                        delete.param(targetId);
                        delete.param(g.getName());
                        delete.addBatch();
                        delete.executeBatch();

                        Set<String> seenUserIds = new HashSet<>();

                        for (Group.Membership m : g.getMembers()) {
                            // Generally we expect our group target to return us
                            // membership lists that are free of duplicates, but
                            // sometimes we're disappointed.
                            if (seenUserIds.contains(m.userId)) {
                                logger.warn(String.format("Skipped userid '%s' in group '%s' with role '%s' because we've already seen this user.",
                                                          m.userId, targetId, m.role.toString()));
                                continue;
                            } else {
                                seenUserIds.add(m.userId);
                            }

                            insert.clearParameters();
                            insert.param(targetId);
                            insert.param(g.getName());
                            insert.param(m.userId);
                            insert.param(m.role.toString());

                            insert.addBatch();
                        }

                        insert.executeBatch();
                    }

                    c.commit();
                } finally {
                    delete.close();
                    insert.close();
                }

                return null;
            }
        });
    }

    public void applyDiffs(final String targetId, Collection<Differences.Difference> diffs) throws Exception {
        DB.transaction(db, new DBAction<Void>() {
            @Override
            public Void call(DBConnection c) throws SQLException {
                DBPreparedStatement insert = c.run("insert into groupsync_target_state" +
                        " (target_id, group_id, user_id, role) VALUES (?, ?, ?, ?)");

                DBPreparedStatement delete = c.run("delete from groupsync_target_state" +
                        " where target_id = ? AND group_id = ? AND user_id = ?");

                DBPreparedStatement change = c.run("update groupsync_target_state" +
                        " set role = ? where target_id = ? AND group_id = ? AND user_id = ?");

                for (Differences.Difference d : diffs) {
                    if (d instanceof Differences.MemberAdd) {
                        Differences.MemberAdd diff = (Differences.MemberAdd) d;
                        insert.clearParameters();
                        insert.param(targetId);
                        insert.param(diff.group.getName());
                        insert.param(diff.userId);
                        insert.param(diff.role.toString());

                        insert.addBatch();
                    } else if (d instanceof Differences.MemberDrop) {
                        Differences.MemberDrop diff = (Differences.MemberDrop) d;
                        delete.clearParameters();
                        delete.param(targetId);
                        delete.param(diff.group.getName());
                        delete.param(diff.userId);

                        delete.addBatch();
                    } else if (d instanceof Differences.MemberRoleChange) {
                        Differences.MemberRoleChange diff = (Differences.MemberRoleChange) d;
                        change.clearParameters();
                        change.param(diff.role.toString());
                        change.param(targetId);
                        change.param(diff.group.getName());
                        change.param(diff.userId);

                        change.addBatch();
                    }
                }

                insert.executeBatch();
                delete.executeBatch();
                change.executeBatch();

                c.commit();

                return null;
            }
        });
    }


    public void incrementFailureCount(final String sourceId, Group group) throws Exception {
        DB.transaction(db, new DBAction<Void>() {
            @Override
            public Void call(DBConnection c) throws SQLException {
                DBPreparedStatement update = c.run("update groupsync_source_state" +
                        " set failure_count = failure_count + 1 where source_id = ? AND group_id = ?");

                DBPreparedStatement insert = c.run("insert into groupsync_source_state" +
                        " (source_id, group_id, failure_count) values (?, ?, 1)");

                update.param(sourceId);
                update.param(group.getName());

                if (update.executeUpdate() == 0) {
                    insert.param(sourceId);
                    insert.param(group.getName());
                    insert.executeUpdate();
                }

                c.commit();

                return null;
            }
        });
    }

    public void markGroupAsSynced(final String sourceId, Group group, long now) throws Exception {
        DB.transaction(db, new DBAction<Void>() {
            @Override
            public Void call(DBConnection c) throws SQLException {
                DBPreparedStatement delete = c.run("delete from groupsync_source_state" +
                        " where source_id = ? AND group_id = ?");

                DBPreparedStatement insert = c.run("insert into groupsync_source_state" +
                        " (source_id, group_id, last_sync_time, failure_count) values (?, ?, ?, 0)");

                delete.param(sourceId);
                delete.param(group.getName());
                delete.executeUpdate();

                insert.param(sourceId);
                insert.param(group.getName());
                insert.param(now);
                insert.executeUpdate();

                c.commit();

                return null;
            }
        });
    }

    public Set<String> readSet(GroupTarget target, String setName) throws Exception {
        return DB.transaction(db, new DBAction<Set<String>>() {
            @Override
            public Set<String> call(DBConnection c) throws SQLException {
                DBPreparedStatement select = c.run("select value from groupsync_target_store" +
                        " where target_id = ? AND set_name = ?");
                select.param(target.getId());
                select.param(setName);

                Set<String> result = new HashSet<>();

                for (ResultSet rs : select.executeQuery()) {
                    result.add(rs.getString("value"));
                }

                return result;
            }
        });
    }

    public void writeSet(GroupTarget target, String setName, Set<String> set) throws Exception {
        DB.transaction(db, new DBAction<Void>() {
            @Override
            public Void call(DBConnection c) throws SQLException {
                DBPreparedStatement delete = c.run("delete from groupsync_target_store" +
                        " where target_id = ? AND set_name = ?");

                DBPreparedStatement insert = c.run("insert into groupsync_target_store" +
                        " (target_id, set_name, value) values (?, ?, ?)");

                delete.param(target.getId());
                delete.param(setName);
                delete.executeUpdate();

                for (String value : set) {
                    insert.clearParameters();
                    insert.param(target.getId());
                    insert.param(setName);
                    insert.param(value);
                    insert.addBatch();
                }

                insert.executeBatch();

                c.commit();

                return null;
            }
        });
    }

    public void clearSet(GroupTarget target, String setName) throws Exception {
        DB.transaction(db, new DBAction<Void>() {
            @Override
            public Void call(DBConnection c) throws SQLException {
                DBPreparedStatement delete = c.run("delete from groupsync_target_store" +
                        " where target_id = ? AND set_name = ?");

                delete.param(target.getId());
                delete.param(setName);
                delete.executeUpdate();

                c.commit();

                return null;
            }
        });
    }

}
