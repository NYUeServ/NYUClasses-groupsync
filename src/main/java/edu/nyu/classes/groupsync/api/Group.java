package edu.nyu.classes.groupsync.api;

import java.util.ArrayList;
import java.util.List;

public class Group {
    private String name;
    private String description;
    private long lastModifiedTime;

    private List<Membership> members = new ArrayList<>();

    public Group(String name, String description) {
        this.name = name;
        this.description = description;
        this.lastModifiedTime = System.currentTimeMillis();
    }

    public void addMembership(String userId, String role) {
        members.add(new Membership(userId, role));
    }

    public String getName() {
        return name;
    }

    public String getDescription() {
        return description;
    }

    public List<Membership> getMembers() {
        return members;
    }

    public long getLastModifiedTime() {
        return lastModifiedTime;
    }

    public void setLastModifiedTime(long time) {
        lastModifiedTime = time;
    }

    public String toString() {
        return String.format("#<Group '%s' (%d members: %s)>",
                name,
                members.size(),
                members.toString());
    }

    public class Membership {
        public Role role;
        public String userId;

        public Membership(String userId, String role) {
            this.userId = userId;
            this.role = Role.valueOf(role.toUpperCase(java.util.Locale.ROOT));
        }

        public String toString() {
            return String.format("%s <%s>", userId, role);
        }
    }
}
