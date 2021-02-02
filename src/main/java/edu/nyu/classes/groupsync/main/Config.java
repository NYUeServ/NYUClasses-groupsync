package edu.nyu.classes.groupsync.main;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;

public class Config {

    private Properties properties;

    public Config(String path) throws Exception {
        InputStream is = new FileInputStream(path);
        properties = new Properties();
        properties.load(is);
        is.close();
    }

    public Collection<String> replicationSets() {
        return Arrays.asList(getString("replication_sets").split(", *"));
    }

    public Group readGroup(String group, String prefix) {
        return new Group(group + "." + prefix);
    }

    public String getString(String property) {
        String value = getString(property, null);

        if (value == null) {
            throw new RuntimeException("Missing property: " + property);
        }

        return value;
    }

    public long getLong(String property, long defaultValue) {
        String value = getString(property, "");

        if ("".equals(value)) {
            return defaultValue;
        } else {
            return Long.valueOf(value);
        }
    }

    public String getString(String property, String defaultValue) {
        String value = (String) properties.get(property);

        if (value == null || value.isEmpty()) {
            return defaultValue;
        }

        return value;
    }

    public class Group {
        private String prefix;

        public Group(String prefix) {
            this.prefix = prefix;
        }

        public String getString(String name) {
            return Config.this.getString(prefix + "." + name);
        }

        public String getString(String name, String defaultValue) {
            return Config.this.getString(prefix + "." + name, defaultValue);
        }

        public long getLong(String name, long defaultValue) {
            String s = getString(name, "");

            if (s.isEmpty()) {
                return defaultValue;
            }

            return Long.valueOf(s);
        }


    }

}
