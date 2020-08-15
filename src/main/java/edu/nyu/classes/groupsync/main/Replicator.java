package edu.nyu.classes.groupsync.main;

import edu.nyu.classes.groupsync.api.Differences;
import edu.nyu.classes.groupsync.api.Group;
import edu.nyu.classes.groupsync.api.GroupSet;
import edu.nyu.classes.groupsync.api.GroupSource;
import edu.nyu.classes.groupsync.api.GroupTarget;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Replicator extends Thread {
    private static Logger logger = LoggerFactory.getLogger(Replicator.class);

    private long pollIntervalMs;
    private GroupSource source;
    private GroupTarget target;
    private ReplicationState state;
    private Config config;

    private class Failure {
        public long failureCount = 0;
        public long currentPenalty = 0;
    }

    private Map<String, Failure> failureCountsByGroupName = new HashMap<>();

    // Backdate our time a little just to avoid the risk of slow transactions, etc.
    private static long UPDATE_MARGIN_MS = 5000;

    public Replicator(long pollIntervalMs, GroupSource source, GroupTarget target, ReplicationState state, Config config) {
        this.pollIntervalMs = pollIntervalMs;
        this.state = state;
        this.source = source;
        this.target = target;
        this.config = config;

        if (this.pollIntervalMs <= 0) {
            logger.warn("Bogus value for replicator from {} to {} ({}).  Defaulting to one minute",
                    source.getId(),
                    target.getId(),
                    this.pollIntervalMs);

            this.pollIntervalMs = 60000;
        }
    }

    public void run() {
        while (true) {
            try {
                Thread.sleep(pollIntervalMs);
            } catch (InterruptedException e) {
            }

            try {
                long now = System.currentTimeMillis();

                // Fetch the groups that were updated since we last checked
                long lastSourceUpdateTime = state.lastUpdateForSource(source.getId());

                lastSourceUpdateTime = Math.max(0, lastSourceUpdateTime - UPDATE_MARGIN_MS);

                logger.info("Looking for updates from '{}' since {}", source.getId(), lastSourceUpdateTime);

                GroupSet updatedGroups = source.updatedGroupsSince(lastSourceUpdateTime);

                logger.debug("Groups from source {}: {}", source.getId(), updatedGroups.summary());

                // It's possible that we already synced some of these groups on
                // a previous run, since the source update time is only bumped
                // when *all* groups have succeeded.  However, we don't need to
                // resync the groups that we did already.
                List<Group> groupsToSkip = new ArrayList<>();
                for (Group g : updatedGroups) {
                    long groupSyncTime = state.lastSyncTimeForGroup(source.getId(), g);

                    if (groupSyncTime > g.getLastModifiedTime()) {
                        // We can skip this group
                        logger.info("Skipping previously synced group: {}", g.getName());
                        groupsToSkip.add(g);
                    }
                }

                updatedGroups.removeAll(groupsToSkip);

                if (updatedGroups.isEmpty()) {
                    logger.info("No group updates were found for source {}", source.getId());
                    state.markSourceAsUpdated(source.getId(), now);
                    continue;
                }

                // Groups that previously failed should have their penalties
                // applied.  When a group fails to sync a few times in a row,
                // we'll back off how frequently we retry.
                for (Group g : updatedGroups) {
                    Failure f = failureCountsByGroupName.get(g.getName());

                    if (f != null && f.currentPenalty > 0) {
                        f.currentPenalty -= 1;
                        groupsToSkip.add(g);

                        logger.info("Skipping group for current run due to failure penalty: {} ({} rounds left until next run)",
                                    g.getName(),
                                    f.currentPenalty);
                    }
                }

                updatedGroups.removeAll(groupsToSkip);

                // If everything else got skipped, nothing more to do on this
                // run, but don't mark the group source as up-to-date yet.
                if (updatedGroups.isEmpty()) {
                    continue;
                }

                // Pull their group memberships
                GroupSet groupsFromTarget = target.fetchGroupsForNames(updatedGroups.groupNames());

                // Store the remote memberships as they currently stand.  This
                // allows our REST queries to return relatively up-to-date
                // information about who has and hasn't been synced yet.
                state.storeRemoteMemberships(target.getId(), groupsFromTarget);

                // These groups exist in our source but not the target.
                List<Group> newGroups = new ArrayList<>();

                for (Group g : updatedGroups) {
                    if (!groupsFromTarget.hasGroup(g.getName())) {
                        newGroups.add(g);

                        // For the sake of our upcoming membership comparison,
                        // we'll treat this as an empty group.
                        groupsFromTarget.createOrGetGroup(new Group(g.getName(), g.getDescription()));
                    }
                }

                logger.debug("Groups from target {}: {}", target.getId(), groupsFromTarget.summary());

                target.createNewGroups(newGroups, state);

                Collection<Differences.Difference> diffs = new GroupSetDiffer().diff(groupsFromTarget, updatedGroups);

                logger.debug("Calculated differences: {}", diffs);

                Collection<Differences.Difference> appliedDiffs = target.applyDiffs(diffs, state);

                state.applyDiffs(target.getId(), appliedDiffs);

                // Any diffs that weren't successfully applied should prevent
                // that group from being marked as synced.  We'll retry those
                // groups on a subsequent run.
                //
                List<Group> failedGroups = new ArrayList<>();
                for (Differences.Difference diff : diffs) {
                    if (!appliedDiffs.contains(diff) && !failedGroups.contains(diff.group)) {
                        failedGroups.add(diff.group);
                    }
                }

                // Mark off the successfully synced groups
                long allowableFailures = config.getLong("sakai_google.allowable_failures", 3);
                long penalty = config.getLong("sakai_google.failure_penalty", 30);

                for (Group g : updatedGroups) {
                    if (failedGroups.contains(g)) {
                        if (!failureCountsByGroupName.containsKey(g.getName())) {
                            // First failure for this group
                            failureCountsByGroupName.put(g.getName(), new Failure());
                        }

                        Failure f = failureCountsByGroupName.get(g.getName());
                        f.failureCount += 1;

                        if (f.failureCount > allowableFailures) {
                            // Apply a penalty
                            f.currentPenalty = penalty;

                            logger.info("Group {} has failed more than {} times.  Skipping the next {} iterations",
                                        g.getName(),
                                        allowableFailures,
                                        penalty);
                        }

                        state.incrementFailureCount(source.getId(), g);
                    } else {
                        failureCountsByGroupName.remove(g.getName());

                        state.markGroupAsSynced(source.getId(), g, now);
                        source.markGroupAsSynced(g);
                    }
                }

                // If there were no errors, we can mark the source as fully updated
                if (failedGroups.isEmpty() && failureCountsByGroupName.isEmpty()) {
                    logger.info("Full sync for source {} succeeded", source.getId());
                    state.markSourceAsUpdated(source.getId(), now);
                } else {
                    logger.info("{} groups failed to fully sync while syncing source {}", failedGroups.size() + failureCountsByGroupName.size(), source.getId());
                }

            } catch (Exception e) {
                if (isCriticalException(e)) {
                    Monitoring.recordException(e);
                }

                logger.error("Caught an exception in replicator from {} to {}: {}",
                        source.getId(), target.getId(), e.getMessage(), e);
            }
        }
    }


    private boolean isCriticalException(Exception e) {
        if (e.toString().indexOf("backendError") >= 0 || e.toString().indexOf("502 Bad Gateway") >= 0) {
            // Google intermittently throws these and there's not much we can do
            // about it.
            return false;
        }

        return true;
    }

}
