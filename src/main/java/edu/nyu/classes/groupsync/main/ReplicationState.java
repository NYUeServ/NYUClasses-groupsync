package edu.nyu.classes.groupsync.main;

import edu.nyu.classes.groupsync.api.*;
import edu.nyu.classes.groupsync.main.db.DB;
import edu.nyu.classes.groupsync.main.db.DBAction;
import edu.nyu.classes.groupsync.main.db.DBConnection;
import edu.nyu.classes.groupsync.main.db.DBPreparedStatement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.List;
import java.util.stream.Collectors;


public class ReplicationState implements TargetStore, UserProvisionerState {
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
                        delete.executeUpdate();

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

    public void addPlaceholders(String targetId, List<String> userIds) {
        DB.transaction(db, new DBAction<Void>() {
            @Override
            public Void call(DBConnection c) throws SQLException {
                long now = System.currentTimeMillis();

                for (String userId : userIds) {
                    DBPreparedStatement insert = c.run("insert into groupsync_user_status (target_id, username, status, created_time) values (?, ?, ?, ?)");

                    insert.param(targetId);
                    insert.param(userId);
                    insert.param("unchecked");
                    insert.param(now);

                    try {
                        insert.executeUpdate();
                    } catch (SQLException e) {
                        if (e.getSQLState().startsWith("23")) {
                            // Integrity violation.  We expect this to fail for users who were already there
                        } else {
                            throw e;
                        }
                    }
                }

                c.commit();

                return null;
            }
        });
    }

    public List<String> getUsersToCheck(String targetId, UserProvisionerState.CheckPredicate predicate) {
        return DB.transaction(db, new DBAction<List<String>>() {
            @Override
            public List<String> call(DBConnection c) throws SQLException {
                DBPreparedStatement select = c.run("select username, created_time, last_checked_time from groupsync_user_status where status != 'provisioned' AND target_id = ?");
                select.param(targetId);

                List<String> result = new ArrayList<>();

                for (ResultSet rs : select.executeQuery()) {
                    long createdTime = rs.getLong("created_time");
                    long lastCheckedTime = rs.getLong("last_checked_time");
                    String username = rs.getString("username");

                    if (predicate.check(createdTime, lastCheckedTime)) {
                        result.add(username);
                    }
                }

                return result;
            }
        });
    }

    public void markUsersAsProvisioned(String targetId, long now, List<String> userIds) {
        markUsersWithStatus(targetId, now, userIds, "provisioned");
    }

    public void markUsersAsPending(String targetId, long now, List<String> userIds) {
        markUsersWithStatus(targetId, now, userIds, "pending");
    }

    public Set<String> selectProvisionedUsers(String targetId, List<String> userIds) {
        final Set<String> result = new HashSet<>();

        DB.transaction(db, new DBAction<Void>() {
            @Override
            public Void call(DBConnection c) throws SQLException {
                int pageSize = 256;
                for (int i = 0; i < userIds.size(); i += pageSize) {
                    int upper = Math.min(i + pageSize, userIds.size());

                    List<String> subset = userIds.subList(i, upper);
                    String placeholders = subset.stream().map(_p -> "?").collect(Collectors.joining(","));

                    DBPreparedStatement query = c.run("select username from groupsync_user_status" +
                                                      " where status = 'provisioned' " +
                                                      " AND target_id = ?" +
                                                      " AND username in (" + placeholders + ")");


                    query.param(targetId);

                    for (String userId : subset) {
                        query.param(userId);
                    }

                    for (ResultSet rs : query.executeQuery()) {
                        result.add(rs.getString("username"));
                    }
                }

                return null;
            }
        });

        return result;
    }

    private void markUsersWithStatus(String targetId, long now, List<String> userIds, String status) {
        if (userIds.isEmpty()) {
            return;
        }

        DB.transaction(db, new DBAction<Void>() {
            @Override
            public Void call(DBConnection c) throws SQLException {
                for (String userId : userIds) {
                    DBPreparedStatement update = c.run("update groupsync_user_status" +
                                                       " set status = ?," +
                                                       " last_checked_time = ?" +
                                                       " where username = ? AND target_id = ?");

                    update.param(status);
                    update.param(now);
                    update.param(userId);
                    update.param(targetId);

                    update.executeUpdate();
                }

                c.commit();

                return null;
            }
        });
    }
}

