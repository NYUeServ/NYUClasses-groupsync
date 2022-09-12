package edu.nyu.classes.groupsync.main;

import edu.nyu.classes.groupsync.main.brightspace.BrightspaceClient;

import java.util.Locale;
import java.util.Map;

public class EmailMapper {
    private Map<String, String> mappings;

    public EmailMapper(BrightspaceClient brightspace) {
        this.mappings = brightspace.loadEmailAddressMappings();
    }

    public String map(String fromAddress) {
        return this.mappings.getOrDefault(fromAddress.trim().toLowerCase(Locale.ROOT), fromAddress);
    }
}
