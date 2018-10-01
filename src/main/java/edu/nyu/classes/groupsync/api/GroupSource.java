package edu.nyu.classes.groupsync.api;

public interface GroupSource {
    public String getId();

    public GroupSet updatedGroupsSince(long time);

    public void markGroupAsSynced(Group group) throws Exception;
}
