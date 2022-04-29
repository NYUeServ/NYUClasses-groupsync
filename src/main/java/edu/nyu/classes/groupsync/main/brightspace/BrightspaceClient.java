package edu.nyu.classes.groupsync.main.brightspace;

import java.io.*;
import java.util.*;

import edu.nyu.classes.groupsync.main.Config;

import org.apache.http.client.config.RequestConfig;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.*;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.*;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;

import edu.nyu.classes.groupsync.main.db.*;
import edu.nyu.classes.groupsync.api.*;

// import org.sakaiproject.component.cover.HotReloadConfigurationService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;
import java.util.stream.*;

import javax.sql.DataSource;
import java.sql.SQLException;



public class BrightspaceClient {

    private static final Logger LOG = LoggerFactory.getLogger(BrightspaceClient.class);

    private Config.Group config;

    private OAuth tokens;

    private ExecutorService requestPool;

    private static Map<String, String> apiVersions;

    private DataSource darksideDataSource;

    static {
        apiVersions = new HashMap<>();
        apiVersions.put("lp", "1.25");
        apiVersions.put("le", "1.25");
    }

    public BrightspaceClient(Config.Group config, DataSource dataSource) {
        this.config = config;

        this.darksideDataSource = dataSource;

        tokens = new OAuth(config, dataSource);
        requestPool = Executors.newFixedThreadPool(32);
    }

    public class CourseOfferingData {
        public String title;
        public String code;
        public boolean isPublished;
    }

    public CourseOfferingData fetchCourseData(String courseOfferingId) {
        try {
            Future<HTTPResponse> courseOfferingLookup =
                httpGet(endpoint("lp", String.format("/courses/%s", courseOfferingId)));

            try (HTTPResponse response = courseOfferingLookup.get()) {
                JSON courseData = response.json(JSON.parse("{}"));

                CourseOfferingData data = new CourseOfferingData();
                data.title = courseData.path("Name").asString("Untitled");
                data.code = courseData.path("Code").asStringOrDie();
                data.isPublished = courseData.path("IsActive").asBoolean(false);

                return data;
            }
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }


    public class BrightspaceSiteUser {
        public String email;
        public Role role;

        public BrightspaceSiteUser(String email, Role role) {
            this.email = email;
            this.role = role;
        }

    }

    public List<String> listSitesForSync() {
        Set<String> result = new HashSet<>();

        String needsSyncOrgUnit = config.getString("needs_sync_org_unit_id");
        String hasSyncedOrgUnit = config.getString("has_synced_org_unit_id");

        String orgUnitTypeId = config.getString("course_offering_type_id");

        try {
            for (String orgUnitOfInterest : Arrays.asList(needsSyncOrgUnit, hasSyncedOrgUnit)) {
                String bookmark = "";
                for (;;) {
                    Future<HTTPResponse> request = httpGet(endpoint("lp", String.format("/orgstructure/%s/descendants/paged/",
                                                                                        orgUnitOfInterest)),
                                                           "ouTypeId", orgUnitTypeId,
                                                           "bookmark", bookmark);

                    try (HTTPResponse response = request.get()) {
                        JSON orgUnitData = response.json(JSON.parse("{\"Items\": []}"));

                        for (JSON orgUnit : orgUnitData.path("Items").asJSONList()) {
                            result.add(orgUnit.path("Identifier").asStringOrDie());
                        }

                        if (orgUnitData.path("PagingInfo > HasMoreItems").asBoolean(false)) {
                            bookmark = orgUnitData.path("PagingInfo > Bookmark").asStringOrDie();
                        } else {
                            break;
                        }
                    }
                }
            }
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }

        return new ArrayList<>(result);
    }

    public void markSiteAsSynced(String courseOfferingId) {
        String needsSyncOrgUnit = config.getString("needs_sync_org_unit_id");
        String hasSyncedOrgUnit = config.getString("has_synced_org_unit_id");

        try {
            Future<HTTPResponse> syncedResponse = httpPost(endpoint("lp", String.format("/orgstructure/%s/parents/", courseOfferingId)),
                                                           "text/json",
                                                           hasSyncedOrgUnit);

            syncedResponse.get().assertOK();

            Future<HTTPResponse> removeResponse = httpDelete(endpoint("lp", String.format("/orgstructure/%s/parents/%s", courseOfferingId,
                                                                                          needsSyncOrgUnit)));

            removeResponse.get().assertOK();

            DB.transaction
                (this.darksideDataSource,
                 "Mark this group as migrated in Darkside",
                 (DBConnection db) -> {
                    db.run("DELETE from nyu_t_darkside_org_unit_props where name = 'google-group-synced' AND org_unit_id = ?")
                        .param(courseOfferingId)
                        .executeUpdate();

                    db.run("INSERT INTO nyu_t_darkside_org_unit_props (org_unit_id, name, value) values (?, ?, ?)")
                        .param(courseOfferingId)
                        .param("google-group-synced")
                        .param("true")
                        .executeUpdate();

                    db.commit();

                    return null;
                });
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    public List<BrightspaceSiteUser> getActiveSiteUsers(String courseOfferingId) {
        try {
            List<BrightspaceSiteUser> result = new ArrayList<>();

            String bookmark = "";

            for (;;) {
                Future<HTTPResponse> usersRequest = httpGet(endpoint("lp", String.format("/enrollments/orgUnits/%s/users/", courseOfferingId)),
                                                            "bookmark", bookmark);

                try (HTTPResponse usersResponse = usersRequest.get()) {
                    JSON userData = usersResponse.json(JSON.parse("{\"Items\": []}"));

                    for (JSON orgUnitUser : userData.path("Items").asJSONList()) {
                        JSON userInfo = orgUnitUser.path("User");
                        JSON roleInfo = orgUnitUser.path("Role");

                        String email = userInfo.path("EmailAddress").asString(null);
                        String brightspaceRole = roleInfo.path("Name").asString(null);

                        if (email == null || brightspaceRole == null) {
                            LOG.warn(String.format("Skipped user because email or role is null: %s", orgUnitUser));
                            continue;
                        }

                        Role role = null;

                        if (brightspaceRole.toLowerCase(Locale.ROOT).matches("^.*(instructor|course site admin|course admin).*$")) {
                            role = Role.MANAGER;
                        } else if (brightspaceRole.toLowerCase(Locale.ROOT).matches("^.*(student|teaching assistant).*$")) {
                            role = Role.MEMBER;
                        }

                        if (brightspaceRole.toLowerCase(Locale.ROOT).matches("^.*(withdrawn).*$")) {
                            LOG.info(String.format("User withdrawn from %s: %s", courseOfferingId, email));
                        }

                        if (role != null) {
                            result.add(new BrightspaceSiteUser(email, role));
                        }
                    }

                    if (userData.path("PagingInfo > HasMoreItems").asBoolean(false)) {
                        bookmark = userData.path("PagingInfo > Bookmark").asStringOrDie();
                    } else {
                        break;
                    }
                }
            }

            return result;
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    private class HTTPResponse implements AutoCloseable {
        private CloseableHttpClient client;
        public CloseableHttpResponse response;
        private String requestId;
        public Throwable error;

        public HTTPResponse(CloseableHttpClient client, String requestId, CloseableHttpResponse origResponse) {
            this.client = client;
            this.requestId = requestId;
            this.response = origResponse;
        }

        public HTTPResponse(CloseableHttpClient client, String requestId, Throwable error) {
            this.client = client;
            this.requestId = requestId;
            this.error = error;
        }

        private void assertOK() {
            if (this.error != null) {
                throw new RuntimeException(this.error);
            }
        }

        public JSON json() {
            try {
                successOrDie();
                return JSON.parse(bodyString());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        public JSON json(JSON dflt) {
            try {
                if (response != null && response.getStatusLine() != null && response.getStatusLine().getStatusCode() == 404) {
                    // Brightspace returns 404 for "nothing here"
                    return dflt;
                }

                successOrDie();

                return JSON.parse(bodyString());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        public void successOrDie() {
            assertOK();

            if (response.getStatusLine().getStatusCode() == 200) {
                // Hooray
                return;
            }

            String body = "";

            try {
                body = EntityUtils.toString(response.getEntity());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            throw new RuntimeException(String.format("Failed request %s: %d (%s)",
                                                     this.requestId,
                                                     response.getStatusLine().getStatusCode(),
                                                     body));
        }

        public String bodyString() throws IOException {
            assertOK();
            return EntityUtils.toString(this.response.getEntity());
        }

        public void close() {
            if (response != null) {
                try { response.close(); } catch (IOException e) {}
            }

            if (client != null) {
                try { client.close(); } catch (IOException e) {}
            }
        }
    }

    private final int MAX_TOKEN_ATTEMPTS = 3;

    private Future<HTTPResponse> httpGet(String uri, String ...params) {
        try {
            URIBuilder builder = new URIBuilder(uri);

            for (int i = 0; i < params.length; i += 2) {
                builder.setParameter(params[i], params[i + 1]);
            }

            HttpGet request = new HttpGet(builder.build());

            return httpRequest("GET " + uri, request);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private Future<HTTPResponse> httpPost(String uri, String contentType, String body) {
        try {
            URIBuilder builder = new URIBuilder(uri);
            HttpPost request = new HttpPost(builder.build());

            request.setEntity(new StringEntity(body, ContentType.create(contentType)));

            return httpRequest("POST " + uri, request);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private Future<HTTPResponse> httpDelete(String uri) {
        try {
            URIBuilder builder = new URIBuilder(uri);
            HttpDelete request = new HttpDelete(builder.build());
            return httpRequest("DELETE " + uri, request);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    private Future<HTTPResponse> httpRequest(String request_id, HttpRequestBase req) {
        return this.requestPool.submit(() -> {
                CloseableHttpClient client = null;

                try {
                    for (int attempt = 0; ; attempt++) {
                        if (attempt > 0) {
                            try {
                                LOG.info("Retrying request");
                                Thread.sleep((long)Math.floor(1000 + (5000 * Math.random())));
                            } catch (InterruptedException ie) {}
                        }

                        RequestConfig config = RequestConfig
                            .custom()
                            .setConnectTimeout(60000)
                            .setConnectionRequestTimeout(60000)
                            .setSocketTimeout(60000)
                            .build();

                        client = HttpClients
                            .custom()
                            .setDefaultRequestConfig(config)
                            .build();

                        String accessToken = this.tokens.accessToken();

                        req.setHeader("Authorization", String.format("Bearer %s", accessToken));

                        CloseableHttpResponse response = client.execute(req);

                        int code = response.getStatusLine().getStatusCode();
                        if ((attempt + 1) < MAX_TOKEN_ATTEMPTS && (code == 401 || code == 403)) {
                            // Bad token.  Probably expired?  Force a refresh if we have attempts left.
                            this.tokens.accessToken(accessToken);
                        } else {
                            return new HTTPResponse(client, request_id, response);
                        }
                    }
                } catch (Exception e) {
                    return new HTTPResponse(client, request_id, e);
                }
            });
    }

    private String endpoint(String product, String uri) {
        String baseURL = config.getString("brightspace_api_url", "https://brightspace.nyu.edu/d2l/api").replace("/+", "");

        if (product.isEmpty()) {
            return String.format("%s%s", baseURL, uri);
        } else {
            String productVersion = apiVersions.get(product);

            return String.format("%s/%s/%s%s",
                                 baseURL,
                                 product,
                                 productVersion,
                                 uri);
        }
    }
}
