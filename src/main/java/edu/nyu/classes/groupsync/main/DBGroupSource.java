package edu.nyu.classes.groupsync.main;

import edu.nyu.classes.groupsync.api.Group;
import edu.nyu.classes.groupsync.api.GroupSet;
import edu.nyu.classes.groupsync.api.GroupSource;
import edu.nyu.classes.groupsync.main.db.DB;
import edu.nyu.classes.groupsync.main.db.DBAction;
import edu.nyu.classes.groupsync.main.db.DBConnection;
import edu.nyu.classes.groupsync.main.db.DBPreparedStatement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Locale;

public class DBGroupSource implements GroupSource {
    private static Logger logger = LoggerFactory.getLogger(DBGroupSource.class);

    private DataSource db;
    private String id;
    private String groupInfoTable;
    private String memberInfoTable;
    private String syncStatusTable;


    public DBGroupSource(DataSource db,
                         String id,
                         String groupInfoTable,
                         String memberInfoTable,
                         String syncStatusTable) {
        this.db = db;
        this.id = id;
        this.groupInfoTable = groupInfoTable;
        this.memberInfoTable = memberInfoTable;
        this.syncStatusTable = syncStatusTable;
    }

    public String getId() {
        return id;
    }

    // FIXME: probably want to rename 'netid' to 'eid' at some point
    public GroupSet updatedGroupsSince(long time) {
        logger.info("Return updates for {} since {}", id, time);

        final GroupSet result = new GroupSet();

        DB.transaction(db, new DBAction<Void>() {
            @Override
            public Void call(DBConnection c) throws SQLException {
                // Add all groups -- even the ones with no members
                DBPreparedStatement groups = c.run("select lower(replace(ggd.group_id, ':', '-')) group_id, ggd.description, ggd.ready_for_sync_time" +
                        " from " + groupInfoTable + " ggd " +
                        " where ggd.ready_for_sync_time >= ?");

                groups.param(time);

                for (ResultSet rs : groups.executeQuery()) {
                    String groupName = rs.getString("group_id");
                    String description = rs.getString("description");
                    long lastModifiedTime = rs.getLong("ready_for_sync_time");

                    Group group = result.createOrGetGroup(new Group(groupName, description));
                    group.setLastModifiedTime(lastModifiedTime);
                }

                // Populate any members for non-deleted groups
                DBPreparedStatement members = c.run("select lower(replace(ggu.group_id, ':', '-')) group_id, ggu.role, ggu.email" +
                        " from " + groupInfoTable + " ggd " +
                        " inner join " + memberInfoTable + " ggu " +
                        " on ggd.group_id = ggu.group_id" +
                        " where ggd.ready_for_sync_time >= ? AND ggd.deleted = 0");

                members.param(time);

                for (ResultSet rs : members.executeQuery()) {
                    String groupName = rs.getString("group_id");
                    String email = rs.getString("email");
                    String role = mapRole(rs.getString("role"));

                    Group group = result.get(groupName);
                    group.addMembership(email, role);
                }

                return null;
            }
        });

        return result;
    }

    public void markGroupAsSynced(Group group) throws Exception {
        final String table = syncStatusTable;

        DB.transaction(db, new DBAction<Void>() {
            @Override
            public Void call(DBConnection c) throws SQLException {
                String sql = null;

                if (c.isOracle()) {
                    sql = "update " + table + " set status = 'synced', update_mtime = systimestamp where lower(replace(group_id, ':', '-')) = ?";
                } else if (c.isMySQL()) {
                    sql = "update " + table + " set status = 'synced', update_mtime = current_timestamp() where lower(replace(group_id, ':', '-')) = ?";
                } else {
                    throw new RuntimeException("No update SQL provided for this database type");
                }

                DBPreparedStatement update = c.run(sql);

                update.param(group.getName());
                update.executeUpdate();

                c.commit();

                return null;
            }
        });
    }

    private String mapRole(String role) {
        if ("viewer".equals(role)) {
            return "member";
        } else {
            return role;
        }
    }
}
