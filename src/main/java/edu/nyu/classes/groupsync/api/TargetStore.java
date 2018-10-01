package edu.nyu.classes.groupsync.api;

import java.util.Set;

public interface TargetStore {
    public Set<String> readSet(GroupTarget target, String setName) throws Exception;
    public void writeSet(GroupTarget target, String setName, Set<String> value) throws Exception;
    public void clearSet(GroupTarget target, String setName) throws Exception;
}
