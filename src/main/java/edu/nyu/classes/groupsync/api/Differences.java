package edu.nyu.classes.groupsync.api;

public class Differences {

    public static class Difference {
        public Group group;

        public Difference(Group group) {
            if (group == null) {
                throw new RuntimeException("Group wasn't set!");
            }

            this.group = group;
        }
    }

    public static class MemberDifference extends Difference {
        public String userId;

        public MemberDifference(Group group, String userId) {
            super(group);

            if (userId == null) {
                throw new RuntimeException("UserId wasn't set!");
            }

            this.userId = userId;
        }
    }


    public static class MetadataChange extends Difference {
        public MetadataChange(Group group) {
            super(group);
        }

        public String toString() {
            return String.format("#<MetadataChange for group: %s>", group.getName());
        }
    }


    public static class MemberAdd extends MemberDifference {
        public Role role;

        public MemberAdd(Group group, String userId, Role role) {
            super(group, userId);
            this.role = role;
        }

        public String toString() {
            return String.format("#<MemberAdd: %s[%s] group: %s>", userId, role, group.getName());
        }
    }

    public static class MemberDrop extends MemberDifference {
        public MemberDrop(Group group, String userId) {
            super(group, userId);
        }

        public String toString() {
            return String.format("#<MemberDrop: %s group: %s>", userId, group.getName());
        }

    }

    public static class MemberRoleChange extends MemberDifference {
        public Role role;

        public MemberRoleChange(Group group, String userId, Role newRole) {
            super(group, userId);
            this.role = newRole;
        }

        public String toString() {
            return String.format("#<MemberRoleChange: %s[%s] group: %s>", userId, role, group.getName());
        }

    }
}
