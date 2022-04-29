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

import java.sql.ResultSet;
import java.sql.SQLException;

import edu.nyu.classes.groupsync.main.brightspace.*;
import edu.nyu.classes.groupsync.api.Role;
import java.util.*;
import java.util.concurrent.*;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class BrightspaceGroupSource implements GroupSource {
    private static Logger logger = LoggerFactory.getLogger(BrightspaceGroupSource.class);

    private String id;
    private BrightspaceClient brightspace;

    private ConcurrentHashMap<String, String> groupNameToCourseOfferingId;

    public BrightspaceGroupSource(String id, BrightspaceClient brightspace) {
        this.id = id;
        this.brightspace = brightspace;

        this.groupNameToCourseOfferingId = new ConcurrentHashMap<>();
    }

    public String getId() {
        return id;
    }

    public String sanitizeName(String name) {
        return name.replaceAll("[^a-zA-z0-9-_]", "_").toLowerCase(Locale.ROOT);
    }

    public GroupSet updatedGroupsSince(long time) {
        logger.info("Return updates for {} since {}", id, time);

        final GroupSet result = new GroupSet();

        long now = System.currentTimeMillis();

        for (String courseOfferingId : brightspace.listSitesForSync()) {
            try {
                BrightspaceClient.CourseOfferingData siteInfo = brightspace.fetchCourseData(courseOfferingId);
                List<BrightspaceClient.BrightspaceSiteUser> users = brightspace.getSiteUsers(courseOfferingId);

                Group newGroup = result.createOrGetGroup(new Group(sanitizeName(String.format("%s_%s", siteInfo.code, courseOfferingId)),
                                                                   chopDescription(String.format("All Members - %s", siteInfo.title), 72)));

                groupNameToCourseOfferingId.put(newGroup.getName(), courseOfferingId);

                for (BrightspaceClient.BrightspaceSiteUser user : users) {
                    if (!siteInfo.isPublished && Role.MEMBER.equals(user.role)) {
                        continue;
                    }

                    newGroup.addMembership(user.email, user.role.toString().toLowerCase(Locale.ROOT));
                }
            } catch (Exception e) {
                logger.error("Skipped group {} due to exception", courseOfferingId, e);
            }
        }

        return result;
    }


    private String chopDescription(String description, int length) {
        if (description.length() <= length) {
            return description;
        }

        return description.substring(0, length - 3) + "...";
    }

    public void markGroupAsSynced(Group group) throws Exception {
        String courseOfferingId = groupNameToCourseOfferingId.get(group.getName());

        if (courseOfferingId == null) {
            logger.error("BUG: Couldn't map group {} back to its course offering ID", group.getName());
            return;
        }

        brightspace.markSiteAsSynced(courseOfferingId);
    }

}
