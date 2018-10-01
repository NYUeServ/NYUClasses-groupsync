package edu.nyu.classes.groupsync.api;

import java.util.Collection;

public interface GroupTarget {
    public String getId();

    public GroupSet fetchGroupsForNames(Collection<String> GroupNames);

    public void createNewGroups(Collection<Group> newGroups, TargetStore state);

    public Collection<Differences.Difference> applyDiffs(Collection<Differences.Difference> diffs, TargetStore state);
}
