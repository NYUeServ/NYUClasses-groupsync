package edu.nyu.classes.groupsync.api;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

public class GroupSet implements Iterable<Group> {

    private HashMap<String, Group> groups = new HashMap<>();

    public Group createOrGetGroup(Group group) {
        if (groups.containsKey(group.getName())) {
            return groups.get(group.getName());
        } else {
            groups.put(group.getName(), group);
            return group;
        }
    }

    public boolean hasGroup(String groupName) {
        return groups.containsKey(groupName);
    }

    public Group get(String groupName) {
        Group result = groups.get(groupName);

        if (result == null) {
            throw new RuntimeException("Group not found: " + groupName);
        }

        return result;
    }

    public Collection<String> groupNames() {
        return groups.keySet();
    }

    public Iterator<Group> iterator() {
        return groups.values().iterator();
    }

    public void removeAll(List<Group> groupList) {
        for (Group g : groupList) {
            groups.remove(g.getName());
        }
    }

    public String toString() {
        return groups.toString();
    }

    public boolean isEmpty() {
        return groups.isEmpty();
    }
}
