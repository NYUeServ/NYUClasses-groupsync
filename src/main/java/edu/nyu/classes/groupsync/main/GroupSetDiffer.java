package edu.nyu.classes.groupsync.main;

import edu.nyu.classes.groupsync.api.Differences;
import edu.nyu.classes.groupsync.api.Group;
import edu.nyu.classes.groupsync.api.GroupSet;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class GroupSetDiffer {

    // Describe the minimal set of changes required to transform `target` into
    // `authoritive`.
    //
    // In each group, users may need to be added, dropped or have their roles
    // changed.
    public Collection<Differences.Difference> diff(GroupSet target, GroupSet authoritive) {
        Collection<Differences.Difference> result = new ArrayList<>();

        for (Group authoritiveGroup : authoritive) {
            Group targetGroup = target.get(authoritiveGroup.getName());

            if (!authoritiveGroup.getDescription().equals(targetGroup.getDescription())) {
                // The description needs updating
                result.add(new Differences.MetadataChange(authoritiveGroup));
            }
        }

        // Calculate membership differences
        for (Group authoritiveGroup : authoritive) {
            String groupName = authoritiveGroup.getName();
            Group targetGroup = target.get(groupName);

            Map<String, Group.Membership> authoritiveMembershipsByUserId = membershipsMap(authoritiveGroup.getMembers());
            Map<String, Group.Membership> targetMembershipsByUserId = membershipsMap(targetGroup.getMembers());

            for (Group.Membership m : authoritiveGroup.getMembers()) {
                if (targetMembershipsByUserId.containsKey(m.userId)) {
                    // We already have an entry for the user.  Has their role changed?
                    if (!m.role.equals(targetMembershipsByUserId.get(m.userId).role)) {
                        // It has!
                        result.add(new Differences.MemberRoleChange(authoritiveGroup, m.userId, m.role));
                    }
                } else {
                    // This user is missing from our target.
                    result.add(new Differences.MemberAdd(authoritiveGroup, m.userId, m.role));
                }
            }

            // We now have the users we need to add and the users whose roles
            // have changed.  Add the drops too.
            for (Group.Membership m : targetGroup.getMembers()) {
                if (!authoritiveMembershipsByUserId.containsKey(m.userId)) {
                    result.add(new Differences.MemberDrop(authoritiveGroup, m.userId));
                }
            }
        }

        return result;
    }


    private Map<String, Group.Membership> membershipsMap(Collection<Group.Membership> members) {
        Map<String, Group.Membership> result = new HashMap<>();

        for (Group.Membership m : members) {
            result.put(m.userId, m);
        }

        return result;
    }

}
