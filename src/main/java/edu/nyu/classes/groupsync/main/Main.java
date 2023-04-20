package edu.nyu.classes.groupsync.main;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.zaxxer.hikari.HikariDataSource;

// import org.slf4j.Logger;
// import org.slf4j.LoggerFactory;

import edu.nyu.classes.groupsync.api.GroupSource;
import edu.nyu.classes.groupsync.api.GroupTarget;
import edu.nyu.classes.groupsync.main.brightspace.BrightspaceClient;

public class Main {
    // private static Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        Config config = null;

        try {
            config = new Config(args[0]);
        } catch (Exception e) {
            System.err.println("Usage: Main config/config.properties");
            System.exit(1);
        }

        // Our Oracle queries should generally be short.  Don't hang on a failed
        // Oracle connection indefinitely...
        long timeout = 600000;
        System.setProperty("oracle.net.READ_TIMEOUT", String.valueOf(timeout));
        System.setProperty("oracle.jdbc.ReadTimeout", String.valueOf(timeout));

        HikariDataSource replication_ds = new HikariDataSource();

        replication_ds.setJdbcUrl(config.getString("replication_state.jdbc_url"));
        replication_ds.setUsername(config.getString("replication_state.jdbc_user"));
        replication_ds.setPassword(config.getString("replication_state.jdbc_pass"));

        Map<String, RateLimiter> rateLimiters = new HashMap<>();
        List<Replicator> replicators = new ArrayList<>();

        try {
            for (String set : config.replicationSets()) {
                Config.Group sourceConfig = config.readGroup(set, "source");
                Config.Group targetConfig = config.readGroup(set, "target");

                // Set up the source
                GroupSource source = null;
                if ("db".equals(sourceConfig.getString("type"))) {
                    HikariDataSource ds = new HikariDataSource();

                    ds.setJdbcUrl(sourceConfig.getString("jdbc_url"));
                    ds.setUsername(sourceConfig.getString("jdbc_user"));
                    ds.setPassword(sourceConfig.getString("jdbc_pass"));

                    source = new DBGroupSource(ds,
                            sourceConfig.getString("id"),
                            sourceConfig.getString("group_def_table"),
                            sourceConfig.getString("group_users_table"),
                            sourceConfig.getString("sync_status_table"));

                } else if ("brightspace".equals(sourceConfig.getString("type"))) {
                    HikariDataSource ds = new HikariDataSource();

                    ds.setJdbcUrl(sourceConfig.getString("darkside_jdbc_url"));
                    ds.setUsername(sourceConfig.getString("darkside_jdbc_user"));
                    ds.setPassword(sourceConfig.getString("darkside_jdbc_pass"));

                    BrightspaceClient brightspace = new BrightspaceClient(sourceConfig, ds);

                    source = new BrightspaceGroupSource(sourceConfig.getString("id"), brightspace);
                } else {
                    throw new RuntimeException("Unknown source type: " + sourceConfig.getString("type"));
                }


                // And the target
                GroupTarget target = null;
                if ("google".equals(targetConfig.getString("type"))) {
                    // Rate limits are applied at the user-level, so we should coordinate them.
                    String targetAccountKey = Optional.ofNullable(targetConfig.getString("oauth_user", null))
                        .orElse(targetConfig.getString("service_account_user"));

                    RateLimiter rateLimiter = rateLimiters.get(targetAccountKey);
                    if (rateLimiter == null) {
                        rateLimiter = new RateLimiter(Long.valueOf(targetConfig.getString("queries_per_timestep")),
                                                      Long.valueOf(targetConfig.getString("ratelimit_timestep_ms")));

                        rateLimiters.put(targetAccountKey, rateLimiter);
                    }

                    target = new GoogleGroupTarget(targetConfig.getString("id"),
                            Integer.valueOf(targetConfig.getString("batchSize", "50")),
                            targetConfig.getString("groupDescription", "auto-created group"),
                            rateLimiter,
                            new GoogleClient(targetConfig.getString("domain"),
                                    targetConfig.getString("oauth_user"),
                                    targetConfig.getString("oauth_secret"),
                                    targetConfig.getString("credentials_path")));
                } else {
                    throw new RuntimeException("Unknown target type: " + targetConfig.getString("type"));
                }

                ReplicationState replicationState = new ReplicationState(replication_ds);

                replicators.add(new Replicator(Long.valueOf(config.getString(set + ".frequency_ms")), source, target, replicationState, config));
            }
        } catch (Exception e) {
            throw new RuntimeException("Errors while setting up replicators", e);
        }

        for (Replicator r : replicators) {
            r.start();
        }

        for (Replicator r : replicators) {
            try {
                r.join();
            } catch (InterruptedException e) {
            }
        }
    }
}
