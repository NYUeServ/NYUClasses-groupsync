package edu.nyu.classes.groupsync.main.brightspace;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.URLEncoder;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import javax.sql.DataSource;

import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.nyu.classes.groupsync.main.Config;
import edu.nyu.classes.groupsync.main.db.DB;
import edu.nyu.classes.groupsync.main.db.DBConnection;


public class OAuth {

    private static final Logger LOG = LoggerFactory.getLogger(OAuth.class);

    private String accessToken;

    private static final String APP_ID = "groupsync";

    private static final String AUTH_URL = "https://auth.brightspace.com/oauth2/auth";
    private static final String TOKEN_URL = "https://auth.brightspace.com/core/connect/token";
    private static final String SCOPE = "core:*:* enrollment:*:* reporting:*:* datahub:*:*";

    private Config.Group config;
    private DataSource dataSource;

    private static class OAuthApplication {
        public String clientId;
        public String clientSecret;

        public OAuthApplication(String clientId, String clientSecret) {
            this.clientId = clientId;
            this.clientSecret = clientSecret;
        }
    }

    private static class RefreshToken {
        public OAuthApplication application;
        public String refreshToken;

        public RefreshToken(OAuthApplication application, String refreshToken) {
            this.application = application;
            this.refreshToken = refreshToken;
        }
    }

    private AtomicReference<Map<String, OAuthApplication>> oauthApplicationByClientId = new AtomicReference<>(new HashMap<>());

    public OAuth(Config.Group config, DataSource ds) {
        this.config = config;
        this.dataSource = ds;

        reloadOauthClientProperties();
    }

    private void reloadOauthClientProperties() {
        Map<String, OAuthApplication> newProperties = new HashMap<>();

        for (int i = 0; i < 100; i++) {
            String clientId = config.getString(String.format("oauth_client_id.%d", i), "");
            String secret = config.getString(String.format("oauth_client_secret.%d", i), "");

            if (!clientId.isEmpty()) {
                newProperties.put(clientId, new OAuthApplication(clientId, secret));
            }
        }

        oauthApplicationByClientId.set(newProperties);
    }

    private class DudRefreshToken extends Exception {
        public RefreshToken dudToken;

        public DudRefreshToken(RefreshToken refreshToken) {
            this.dudToken = refreshToken;
        }
    }

    private void loadTokens() {
        reloadOauthClientProperties();

        try {
            for (;;) {
                try {
                    RefreshToken refreshToken = readRefreshToken();
                    accessToken = redeemRefreshToken(refreshToken);
                    break;
                } catch (DudRefreshToken error) {
                    LOG.info("Refresh token wasn't valid.  Retrying...");

                    deleteRefreshToken(error.dudToken.refreshToken);

                    try {
                        Thread.sleep((long)Math.floor(Math.random() * 5000));
                    } catch (InterruptedException e) {}
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public String accessToken() {
        return accessToken(null);
    }

    // Return an access token that isn't this one!
    public synchronized String accessToken(String knownInvalidToken) {
        if (accessToken == null || (knownInvalidToken != null && accessToken.equals(knownInvalidToken))) {
            loadTokens();
        }

        return accessToken;
    }

    public class AuthRedirect {
        public String clientId;
        public String url;
    }

    // Find an application needing a refresh token generated for the first time.  Null if there isn't one.
    public AuthRedirect buildAuthRedirectUrl(String redirectUrl) {
        try {
            List<String> happyClientIds = listClientIdsWithRefreshTokens();

            for (String clientId : oauthApplicationByClientId.get().keySet()) {
                if (happyClientIds.contains(clientId)) {
                    continue;
                }

                AuthRedirect result = new AuthRedirect();

                result.url = AUTH_URL + String.format("?response_type=code&client_id=%s&redirect_uri=%s&scope=%s",
                                                      clientId,
                                                      URLEncoder.encode(redirectUrl, "UTF-8"),
                                                      URLEncoder.encode(SCOPE, "UTF-8"));
                result.clientId = clientId;

                return result;
            }

            return null;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private String redeemRefreshToken(RefreshToken refreshToken) throws Exception {
        try (CloseableHttpClient client = HttpClients.createDefault()) {
            HttpPost req = new HttpPost(TOKEN_URL);
            req.setEntity(new UrlEncodedFormEntity(Arrays.asList(new BasicNameValuePair("grant_type", "refresh_token"),
                                                                 new BasicNameValuePair("refresh_token", refreshToken.refreshToken),
                                                                 new BasicNameValuePair("scope", SCOPE),
                                                                 new BasicNameValuePair("client_id", refreshToken.application.clientId),
                                                                 new BasicNameValuePair("client_secret", refreshToken.application.clientSecret))));

            try (CloseableHttpResponse response = client.execute(req)) {
                if (response.getStatusLine().getStatusCode() == 200) {
                    // OK
                    JSON tokens = JSON.parse(EntityUtils.toString(response.getEntity()));
                    String newRefreshToken = tokens.path("refresh_token").asString(null);

                    if (newRefreshToken != null) {
                        storeRefreshToken(new RefreshToken(refreshToken.application, newRefreshToken));
                    }

                    return tokens.path("access_token").asStringOrDie();
                } else {
                    if (response.getStatusLine().getStatusCode() == 400) {
                        JSON error = JSON.parse(EntityUtils.toString(response.getEntity()));

                        if ("invalid_grant".equals(error.path("error").asString(""))) {
                            throw new DudRefreshToken(refreshToken);
                        }
                    }

                    throw new RuntimeException(String.format("Failed redeeming refresh token: %d (%s)",
                                                             response.getStatusLine().getStatusCode(),
                                                             EntityUtils.toString(response.getEntity())));
                }
            }
        }
    }

    public String redeemAuthCode(String clientId, String authCode, String redirectUri) {
        OAuthApplication app = oauthApplicationByClientId.get().get(clientId);

        if (app == null) {
            throw new RuntimeException("Client ID not recognized: " + clientId);
        }

        try (CloseableHttpClient client = HttpClients.createDefault()) {
            HttpPost req = new HttpPost(TOKEN_URL);
            req.setEntity(new UrlEncodedFormEntity(Arrays.asList(new BasicNameValuePair("grant_type", "authorization_code"),
                                                                 new BasicNameValuePair("code", authCode),
                                                                 new BasicNameValuePair("scope", SCOPE),
                                                                 new BasicNameValuePair("redirect_uri", redirectUri),
                                                                 new BasicNameValuePair("client_id", clientId),
                                                                 new BasicNameValuePair("client_secret", app.clientSecret))));

            try (CloseableHttpResponse response = client.execute(req)) {
                if (response.getStatusLine().getStatusCode() == 200) {
                    // OK
                    JSON tokens = JSON.parse(EntityUtils.toString(response.getEntity()));
                    String newRefreshToken = tokens.path("refresh_token").asString(null);

                    if (app != null && newRefreshToken != null) {
                        storeRefreshToken(new RefreshToken(app, newRefreshToken));
                    }

                    return tokens.path("access_token").asStringOrDie();
                } else {
                    throw new RuntimeException(String.format("Failed redeeming auth code: %d (%s)",
                                                             response.getStatusLine().getStatusCode(),
                                                             EntityUtils.toString(response.getEntity())));
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private RefreshToken readRefreshToken() {
        return DB.transaction
            (this.dataSource,
             "Get an available refresh token",
             (DBConnection db) -> {
                AtomicReference<RefreshToken> result = new AtomicReference<>(null);

                db.run("SELECT client_id, refresh_token from nyu_t_brightspace_oauth where system = ? order by client_id")
                    .param(APP_ID)
                    .executeQuery()
                    .each((row) -> {
                            if (result.get() == null) {
                                OAuthApplication app = oauthApplicationByClientId.get().get(row.getString("client_id"));

                                if (app != null) {
                                    result.set(new RefreshToken(app, row.getString("refresh_token")));
                                }
                            }
                        });

                if (result.get() != null) {
                    return result.get();
                } else {
                    throw new RuntimeException("Failed to find an active refresh token for system: " + APP_ID);
                }
            });
    }

    private void storeRefreshToken(RefreshToken newToken) {
        DB.transaction
            (this.dataSource,
             "Write the latest refresh token",
             (DBConnection db) -> {
                db.run("delete from nyu_t_brightspace_oauth where client_id = ?")
                    .param(newToken.application.clientId)
                    .executeUpdate();

                db.run("insert into nyu_t_brightspace_oauth (client_id, refresh_token, system) values (?, ?, ?)")
                    .param(newToken.application.clientId)
                    .param(newToken.refreshToken)
                    .param(APP_ID)
                    .executeUpdate();

                db.commit();

                return null;
            });
    }

    private void deleteRefreshToken(String dudToken) {
        DB.transaction
            (this.dataSource,
             "Delete an invalidated refresh token",
             (DBConnection db) -> {
                db.run("delete from nyu_t_brightspace_oauth where refresh_token = ?")
                    .param(dudToken)
                    .executeUpdate();

                db.commit();

                return null;
            });
    }

    private List<String> listClientIdsWithRefreshTokens() {
        return DB.transaction
            (this.dataSource,
             "List clients with refresh tokens",
             (DBConnection db) -> {
                return db.run("SELECT client_id from nyu_t_brightspace_oauth where system = ?")
                    .param(APP_ID)
                    .executeQuery()
                    .getStringColumn("client_id");
            });
    }


    static class BasicDataSource implements DataSource {
        private String url;
        private String user;
        private String pass;

        public BasicDataSource(String url, String user, String pass) {
            this.url = url;
            this.user = user;
            this.pass = pass;
        }

        public Connection getConnection() {
            try {
                return DriverManager.getConnection(url, user, pass);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        public Connection getConnection(String username, String password) {
            return getConnection();
        }

        public int getLoginTimeout() { return 0; }
        public PrintWriter getLogWriter() { return null; }
        public java.util.logging.Logger getParentLogger() { return null; }
        public void setLoginTimeout(int seconds) {}
        public void setLogWriter(PrintWriter out) {}

 	public boolean isWrapperFor(Class<?> iface) { return false; }
        public <T> T unwrap(Class<T> iface) { return null; }
    }


    public static void main(String[] args) {
        try {
            String configPath = args[0];
            String replicationSet = args[1];

            BufferedReader stdin = new BufferedReader(new InputStreamReader(System.in));

            String spoof_redirect_url = "https://www.nyu.edu/";

            Config config = new Config(configPath);
            Config.Group sourceConfig = config.readGroup(replicationSet, "source");

            DataSource ds = new BasicDataSource(sourceConfig.getString("oauth_jdbc_url"),
                                                sourceConfig.getString("oauth_jdbc_user"),
                                                sourceConfig.getString("oauth_jdbc_pass"));


            OAuth oauth = new OAuth(sourceConfig, ds);

            for (;;) {
                AuthRedirect redirect = oauth.buildAuthRedirectUrl(spoof_redirect_url);

                if (redirect == null) {
                    // All done!
                    break;
                }

                System.err.println("\nHit the following URL in your browser and paste the auth_code value here (auth-code.XXXXXXXXXXXXXXXXXXXXXXXXXXXXXX).");

                System.err.println(redirect.url);

                String token = stdin.readLine().trim();

                oauth.redeemAuthCode(redirect.clientId, token, spoof_redirect_url);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
