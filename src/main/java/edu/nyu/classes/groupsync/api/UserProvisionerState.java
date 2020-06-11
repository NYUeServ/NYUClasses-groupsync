package edu.nyu.classes.groupsync.api;

import java.util.List;
import java.util.Set;

public interface UserProvisionerState {
    interface CheckPredicate {
        boolean check(long createdTime, long lastCheckedTime);
    }

    public void addPlaceholders(String targetId, List<String> userIds);
    public List<String> getUsersToCheck(String targetId, CheckPredicate predicate);

    public void markUsersAsProvisioned(String targetId, long now, List<String> userIds);
    public void markUsersAsPending(String targetId, long now, List<String> userIds);

    public Set<String> selectProvisionedUsers(String targetId, List<String> userIds);
}
