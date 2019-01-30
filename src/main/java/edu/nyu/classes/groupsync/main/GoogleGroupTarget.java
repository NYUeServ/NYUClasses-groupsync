package edu.nyu.classes.groupsync.main;

import com.google.api.client.googleapis.batch.BatchRequest;
import com.google.api.client.googleapis.batch.json.JsonBatchCallback;
import com.google.api.client.googleapis.json.GoogleJsonError;
import com.google.api.client.googleapis.services.AbstractGoogleClient;
import com.google.api.client.googleapis.services.json.AbstractGoogleJsonClientRequest;
import com.google.api.client.http.HttpHeaders;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.services.admin.directory.Directory;
import com.google.api.services.admin.directory.model.Member;
import com.google.api.services.admin.directory.model.Members;
import com.google.api.services.groupssettings.Groupssettings;
import com.google.api.services.groupssettings.model.Groups;
import edu.nyu.classes.groupsync.api.Differences;
import edu.nyu.classes.groupsync.api.Group;
import edu.nyu.classes.groupsync.api.GroupSet;
import edu.nyu.classes.groupsync.api.GroupTarget;
import edu.nyu.classes.groupsync.api.Role;
import edu.nyu.classes.groupsync.api.TargetStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

public class GoogleGroupTarget implements GroupTarget {
    private static Logger logger = LoggerFactory.getLogger(GoogleGroupTarget.class);

    private GoogleClient google;
    private String id;
    private int requestsPerBatch;
    private String defaultGroupDescription;
    private RateLimiter rateLimiter;

    public GoogleGroupTarget(String id, int requestsPerBatch, String defaultGroupDescription, RateLimiter rateLimiter, GoogleClient google) {
        this.id = id;
        this.google = google;
        this.requestsPerBatch = requestsPerBatch;
        this.defaultGroupDescription = defaultGroupDescription;
        this.rateLimiter = rateLimiter;
    }

    public String getId() {
        return id;
    }

    private static String SETTINGS_STATE_KEY = "GROUPS_NEEDING_SETTINGS";

    public void createNewGroups(Collection<Group> newGroups, TargetStore state) {
        // Creating a group and settings its members isn't an atomic operation,
        // so we need to store some state to act as a write ahead log.  If we
        // crash between creating the group and applying its settings, we can
        // pick up where we left off.
        Set<String> groupsNeedingSettings = null;

        try {
            groupsNeedingSettings = state.readSet(this, SETTINGS_STATE_KEY);

            for (Group g : newGroups) {
                groupsNeedingSettings.add(g.getName() + "@" + google.getDomain());
            }

            state.writeSet(this, SETTINGS_STATE_KEY, groupsNeedingSettings);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        // Create the groups (if any)
        if (!newGroups.isEmpty()) {
            try {
                Directory directory = google.getDirectory();
                Directory.Groups groups = directory.groups();

                LimitedBatchRequest batch = new LimitedBatchRequest(directory);

                for (Group g : newGroups) {
                    com.google.api.services.admin.directory.model.Group googleGroup =
                            new com.google.api.services.admin.directory.model.Group();
                    googleGroup.setName(g.getDescription());
                    googleGroup.setDescription(g.getDescription());
                    googleGroup.setEmail(g.getName() + "@" + google.getDomain());

                    logger.info("Creating new group in Google: {}", g.getName());

                    Directory.Groups.Insert groupRequest = groups.insert(googleGroup);
                    batch.queue(groupRequest, new GroupCreateHandler(g));
                }

                batch.execute();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        if (!groupsNeedingSettings.isEmpty()) {
            try {
                Groupssettings settings = google.getGroupSettings();
                Groupssettings.Groups groups = settings.groups();

                LimitedBatchRequest batch = new LimitedBatchRequest(settings);

                for (String groupKey : groupsNeedingSettings) {
                    Groups groupSettings = new Groups();
                    groupSettings.setWhoCanPostMessage("ALL_MEMBERS_CAN_POST");
                    groupSettings.setAllowExternalMembers("true");
                    groupSettings.setWhoCanJoin("INVITED_CAN_JOIN");
                    groupSettings.setIsArchived("true");
                    groupSettings.setDescription(defaultGroupDescription);
                    groupSettings.setWhoCanViewMembership("ALL_MANAGERS_CAN_VIEW");
                    groupSettings.setWhoCanContactOwner("ALL_MANAGERS_CAN_CONTACT");

                    Groupssettings.Groups.Patch settingsRequest = groups.patch(groupKey, groupSettings);
                    batch.queue(settingsRequest, new GroupSettingsHandler(groupKey));
                }

                batch.execute();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        try {
            state.clearSet(this, SETTINGS_STATE_KEY);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public GroupSet fetchGroupsForNames(Collection<String> groupNames) {
        List<Group> groupList = new ArrayList<>();

        logger.info("Fetching group metadata for {} groups", groupNames.size());

        // Fetch the groups themselves
        try {
            Directory directory = google.getDirectory();
            Directory.Groups groups = directory.groups();

            LimitedBatchRequest batch = new LimitedBatchRequest(directory);

            for (String groupName : groupNames) {
                Directory.Groups.Get groupRequest = groups.get(domainKey(groupName));
                batch.queue(groupRequest, new GroupHandler(groupName, groupList));
            }

            batch.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }


        GroupSet result = new GroupSet();

        for (Group group : groupList) {
            result.createOrGetGroup(group);
        }

        logger.info("Fetching group memberships for {} groups", groupList.size());

        // This is a little complicated.  We have M groups, and each group might
        // require N pages to be fetched.  So, keep a work queue of groups to
        // pages and keep firing batches of requests until we've got everything.
        try {
            Directory directory = google.getDirectory();
            Directory.Members members = directory.members();

            Map<Group, String> groupsToFetch = new HashMap<>(groupList.size());
            for (Group group : groupList) {
                // Start with our initial set of groups.  Empty string means "no page token yet".
                groupsToFetch.put(group, "");
            }

            while (!groupsToFetch.isEmpty()) {
                LimitedBatchRequest batch = new LimitedBatchRequest(directory);

                for (Group group : groupsToFetch.keySet()) {
                    Directory.Members.List membersRequest = members.list(domainKey(group));
                    if (!groupsToFetch.get(group).isEmpty()) {
                        // apply the next page token
                        membersRequest.setPageToken(groupsToFetch.get(group));
                    }

                    // Our handler will take responsibility for either updating
                    // the page token or removing the group from the work queue.
                    batch.queue(membersRequest, new MemberListHandler(group, groupsToFetch));
                }

                batch.execute();
            }

            return result;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    private String domainKey(Group group) {
        return domainKey(group.getName());
    }

    private String domainKey(String name) {
        return name + "@" + google.getDomain();
    }


    public Collection<Differences.Difference> applyDiffs(Collection<Differences.Difference> diffs, TargetStore state) {
        List<Differences.Difference> appliedDiffs = new ArrayList<>();

        Directory directory = null;
        try {
            directory = google.getDirectory();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        // Handle metadata changes first
        handleMetadataChanges(diffs, appliedDiffs);

        // Membership changes
        try {
            Directory.Members members = directory.members();

            LimitedBatchRequest batch = new LimitedBatchRequest(directory);

            //
            // Insertions
            for (Differences.Difference d : diffs) {
                if (!(d instanceof Differences.MemberAdd)) {
                    continue;
                }

                logger.info("Adding group member: {}", d);

                Member m = new Member();
                m.setEmail(((Differences.MemberAdd) d).userId);
                m.setRole(((Differences.MemberAdd) d).role.toString());

                Directory.Members.Insert insertRequest = members.insert(domainKey(d.group), m);

                batch.queue(insertRequest, new MemberDiffAppliedHandler(d, appliedDiffs));
            }

            //
            // Role changes
            for (Differences.Difference d : diffs) {
                if (!(d instanceof Differences.MemberRoleChange)) {
                    continue;
                }

                logger.info("Changing user role: {}", d);

                Member m = new Member();
                m.setEmail(domainKey(((Differences.MemberRoleChange) d).userId));
                m.setRole(((Differences.MemberRoleChange) d).role.toString());

                Directory.Members.Patch updateRequest = members.patch(domainKey(d.group),
                        ((Differences.MemberRoleChange) d).userId,
                        m);

                batch.queue(updateRequest, new MemberDiffAppliedHandler(d, appliedDiffs));
            }


            //
            // Deletions
            for (Differences.Difference d : diffs) {
                if (!(d instanceof Differences.MemberDrop)) {
                    continue;
                }

                logger.info("Deleting user membership: {}", d);

                Directory.Members.Delete deleteRequest = members.delete(domainKey(d.group),
                        ((Differences.MemberDrop) d).userId);

                batch.queue(deleteRequest, new MemberDiffAppliedHandler(d, appliedDiffs));
            }

            //
            // Bam!
            batch.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return appliedDiffs;
    }

    private void handleMetadataChanges(Collection<Differences.Difference> diffs, List<Differences.Difference> appliedDiffs) {
        // For each metadata change, we'll fetch the settings for that group (to
        // get its description, just in case the instructor has changed it).
        // Then, combine that description to the name change and update the
        // whole shebang.

        // Fetch existing descriptions
        Map<Group, String> groupDescriptions = new HashMap<>();

        try {
            Groupssettings settings = google.getGroupSettings();
            Groupssettings.Groups groups = settings.groups();

            LimitedBatchRequest batch = new LimitedBatchRequest(settings);

            for (Differences.Difference d : diffs) {
                if (!(d instanceof Differences.MetadataChange)) {
                    continue;
                }

                Groupssettings.Groups.Get groupSettingsRequest = groups.get(domainKey(d.group.getName()));
                batch.queue(groupSettingsRequest, new GroupDescriptionsHandler(d.group, groupDescriptions));
            }

            batch.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        try {
            Directory directory = google.getDirectory();
            Directory.Groups groups = directory.groups();

            LimitedBatchRequest batch = new LimitedBatchRequest(directory);

            for (Differences.Difference d : diffs) {
                if (!(d instanceof Differences.MetadataChange)) {
                    continue;
                }

                logger.info("Applying group metadata change: {}", d);

                com.google.api.services.admin.directory.model.Group googleGroup =
                        new com.google.api.services.admin.directory.model.Group();
                googleGroup.setName(d.group.getDescription());
                googleGroup.setDescription(groupDescriptions.get(d.group));

                Directory.Groups.Patch updateRequest = groups.patch(domainKey(d.group),
                        googleGroup);
                batch.queue(updateRequest, new GroupDiffAppliedHandler(d, appliedDiffs));
            }

            batch.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    private void executeBatch(BatchRequest batch) throws Exception {
        if (batch.size() > 0) {
            long start = System.currentTimeMillis();

            logger.info("Executing batch of size: {}", batch.size());
            batch.execute();
            logger.info("Done in {} ms", System.currentTimeMillis() - start);
        }
    }

    private class LimitedBatchRequest {

        // According to the docs, Google sets their maximum to 1000, but we
        // can't really use that without hitting the rate limit.  See
        // RateLimiter above.
        private LinkedList<AbstractGoogleJsonClientRequest> requests = new LinkedList<>();
        private LinkedList<JsonBatchCallback> callbacks = new LinkedList<>();
        private AbstractGoogleClient client;


        public LimitedBatchRequest(AbstractGoogleClient client) throws Exception {
            requests = new LinkedList<AbstractGoogleJsonClientRequest>();
            callbacks = new LinkedList<JsonBatchCallback>();

            this.client = client;
        }

        public void queue(AbstractGoogleJsonClientRequest request, JsonBatchCallback callback) {
            requests.add(request);
            callbacks.add(callback);
        }

        public void execute() throws Exception {
            if (requests.isEmpty()) {
                return;
            }

            while (executeNextBatch()) {
                // To glory!
            }
        }

        private boolean executeNextBatch() throws Exception {
            if (requests.isEmpty()) {
                return false;
            }

            BatchRequest batch = client.batch(new HttpRequestInitializer() {
                @Override
                public void initialize(HttpRequest request) {
                    request.setConnectTimeout(15 * 60000);
                    request.setReadTimeout(15 * 60000);
                }
            });

            for (int i = 0; !requests.isEmpty() && i < GoogleGroupTarget.this.requestsPerBatch; i++) {
                AbstractGoogleJsonClientRequest request = requests.pop();
                JsonBatchCallback callback = callbacks.pop();

                request.queue(batch, callback);
            }

            if (batch.size() > 0) {
                GoogleGroupTarget.this.rateLimiter.wantQueries(batch.size());

                long start = System.currentTimeMillis();
                logger.info("Executing batch of size: {}", batch.size());
                batch.execute();
                logger.info("Batch finished in {} ms", System.currentTimeMillis() - start);
            }

            return !requests.isEmpty();
        }
    }

    private class GroupDescriptionsHandler extends JsonBatchCallback<com.google.api.services.groupssettings.model.Groups> {
        private Group group;
        private Map<Group, String> groupDescriptions;

        public GroupDescriptionsHandler(Group group, Map<Group, String> groupDescriptions) {
            this.group = group;
            this.groupDescriptions = groupDescriptions;
        }

        public void onSuccess(com.google.api.services.groupssettings.model.Groups groupSettings, HttpHeaders responseHeaders) {
            groupDescriptions.put(group, groupSettings.getDescription());
        }

        public void onFailure(GoogleJsonError e, HttpHeaders responseHeaders) {
            if (e.getCode() == 403) {
                GoogleGroupTarget.this.rateLimiter.rateLimitHit();
            }

            if (e.getCode() == 404) {
                // One of the groups couldn't be found on the
                // Google side, which is normal for groups that
                // haven't ever been synced.
            } else {
                throw new RuntimeException("Failed during Google lookup for group settings: " + group.getName() + " " + e);
            }
        }
    }

    private class GroupHandler extends JsonBatchCallback<com.google.api.services.admin.directory.model.Group> {
        private String groupName;
        private List<Group> groups;

        public GroupHandler(String groupName, List<Group> groups) {
            this.groupName = groupName;
            this.groups = groups;
        }

        public void onSuccess(com.google.api.services.admin.directory.model.Group group, HttpHeaders responseHeaders) {
            Group g = new Group(emailToGroupName(group.getEmail()), group.getDescription());

            groups.add(g);
        }

        public void onFailure(GoogleJsonError e, HttpHeaders responseHeaders) {
            if (e.getCode() == 403) {
                GoogleGroupTarget.this.rateLimiter.rateLimitHit();
            }

            if (e.getCode() == 404) {
                // One of the groups couldn't be found on the
                // Google side, which is normal for groups that
                // haven't ever been synced.
            } else {
                throw new RuntimeException("Failed during Google lookup for group: " + groupName + " " + e);
            }
        }

        private String emailToGroupName(String email) {
            return email.substring(0, email.indexOf("@")).toLowerCase(Locale.ROOT);
        }
    }

    private class MemberListHandler extends JsonBatchCallback<Members> {
        private Map<String, String> roleMapping;

        private Group group;
        private Map<Group, String> groupsToFetch;

        public MemberListHandler(Group group, Map<Group, String> groupsToFetch) {
            this.group = group;
            this.groupsToFetch = groupsToFetch;

            roleMapping = new HashMap<>();

            // We'll map owners on the Google side to managers on our side.
            // Generally this won't happen unless someone messes with the group
            // manually via the Google UI.
            roleMapping.put("OWNER", "MANAGER");
        }

        public void onSuccess(Members members, HttpHeaders responseHeaders) {
            if (members.getMembers() == null) {
                // No members...
                groupsToFetch.remove(group);
                return;
            }

            for (Member m : members.getMembers()) {
                String role = m.getRole();

                if (roleMapping.containsKey(role)) {
                    role = roleMapping.get(role);
                }

                group.addMembership(m.getEmail(), role);
            }

            if (members.getNextPageToken() == null) {
                // No more pages
                groupsToFetch.remove(group);
            } else {
                groupsToFetch.put(group, members.getNextPageToken());
            }
        }

        public void onFailure(GoogleJsonError e, HttpHeaders responseHeaders) {
            groupsToFetch.remove(group);

            if (e.getCode() == 403) {
                GoogleGroupTarget.this.rateLimiter.rateLimitHit();
            }

            if (e.getCode() == 404) {
                // One of the groups couldn't be found on the
                // Google side, which is normal for groups that
                // haven't ever been synced.
            } else {
                System.out.println("Error Message: " + e.getMessage());
                throw new RuntimeException("Failed during Google lookup for group: " + group.getName() + " " + e);
            }
        }
    }

    private class GroupCreateHandler extends JsonBatchCallback<com.google.api.services.admin.directory.model.Group> {
        private Group group;

        public GroupCreateHandler(Group group) {
            this.group = group;
        }

        public void onSuccess(com.google.api.services.admin.directory.model.Group group, HttpHeaders responseHeaders) {
            logger.info("Successfully created group '{}'", group.getName());
        }

        public void onFailure(GoogleJsonError e, HttpHeaders responseHeaders) {
            if (e.getCode() == 403) {
                GoogleGroupTarget.this.rateLimiter.rateLimitHit();
            }

            logger.info("Failed while creating group '{}': {}", group.getName(), e.getMessage());
            throw new RuntimeException("Failed during creation for group: " + group.getName() + " " + e);
        }
    }

    private class GroupSettingsHandler extends JsonBatchCallback<com.google.api.services.groupssettings.model.Groups> {
        private String groupKey;

        public GroupSettingsHandler(String groupKey) {
            this.groupKey = groupKey;
        }

        public void onSuccess(com.google.api.services.groupssettings.model.Groups groupSettings, HttpHeaders responseHeaders) {
            logger.info("Successfully configured group '{}'", groupKey);
        }

        public void onFailure(GoogleJsonError e, HttpHeaders responseHeaders) {
            if (e.getCode() == 403) {
                GoogleGroupTarget.this.rateLimiter.rateLimitHit();
            }

            logger.info("Failed while creating group '{}': {}", groupKey, e.getMessage());
            throw new RuntimeException("Failed while configuring group: " + groupKey + " " + e);
        }
    }

    private class GroupDiffAppliedHandler extends JsonBatchCallback<com.google.api.services.admin.directory.model.Group> {
        private Differences.Difference diff;
        private List<Differences.Difference> appliedDiffs;

        public GroupDiffAppliedHandler(Differences.Difference diff, List<Differences.Difference> appliedDiffs) {
            this.appliedDiffs = appliedDiffs;
            this.diff = diff;
        }

        public void onSuccess(com.google.api.services.admin.directory.model.Group group, HttpHeaders responseHeaders) {
            appliedDiffs.add(diff);
        }

        public void onFailure(GoogleJsonError e, HttpHeaders responseHeaders) {
            if (e.getCode() == 403) {
                GoogleGroupTarget.this.rateLimiter.rateLimitHit();
            }

            logger.info("Failed to apply diff: {}", diff);
            logger.info("Error from Google was: {}", e);
        }
    }

    private class MemberDiffAppliedHandler extends JsonBatchCallback<Member> {
        private Differences.Difference diff;
        private List<Differences.Difference> appliedDiffs;

        public MemberDiffAppliedHandler(Differences.Difference diff, List<Differences.Difference> appliedDiffs) {
            this.appliedDiffs = appliedDiffs;
            this.diff = diff;
        }

        public void onSuccess(Member member, HttpHeaders responseHeaders) {
            appliedDiffs.add(diff);
        }

        public void onFailure(GoogleJsonError e, HttpHeaders responseHeaders) {
            if (e.getCode() == 403) {
                GoogleGroupTarget.this.rateLimiter.rateLimitHit();
            }

            if (diff instanceof Differences.MemberAdd &&
                e.getErrors() != null &&
                e.getErrors().size() == 1 &&
                "duplicate".equals(e.getErrors().get(0).getReason())) {

                // We already had this user.  Google's member listing often lags reality, so we'll treat this as a non-error.
                appliedDiffs.add(diff);

                logger.info("Caught 'duplicate user' error from Google.  No need to add this user: {}", e);

                return;
            }

            logger.info("Failed to apply diff: {}", diff);
            logger.info("Error from Google was: {}", e);
        }
    }

}
