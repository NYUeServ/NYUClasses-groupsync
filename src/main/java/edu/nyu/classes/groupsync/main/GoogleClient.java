package edu.nyu.classes.groupsync.main;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.auth.oauth2.CredentialRefreshListener;
import com.google.api.client.auth.oauth2.DataStoreCredentialRefreshListener;
import com.google.api.client.auth.oauth2.TokenErrorResponse;
import com.google.api.client.auth.oauth2.TokenResponse;
import com.google.api.client.googleapis.auth.oauth2.GoogleAuthorizationCodeFlow;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.util.store.FileDataStoreFactory;
import com.google.api.services.admin.directory.Directory;
import com.google.api.services.admin.directory.DirectoryScopes;
import com.google.api.services.groupssettings.Groupssettings;
import com.google.api.services.groupssettings.GroupssettingsScopes;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.HashSet;
import java.util.Set;

public class GoogleClient {
    private static Logger logger = LoggerFactory.getLogger(GoogleClient.class);

    private static String APPLICATION = "GroupSyncGoogleClient";

    private String user;
    private String secret;
    private String credentialsPath;

    private HttpTransport httpTransport;
    private JacksonFactory jsonFactory;
    private String domain;

    public GoogleClient(String domain, String user, String secret, String credentialsPath) throws Exception {
        httpTransport = GoogleNetHttpTransport.newTrustedTransport();
        jsonFactory = JacksonFactory.getDefaultInstance();

        this.domain = domain;
        this.user = user;
        this.secret = secret;
        this.credentialsPath = credentialsPath;
    }

    public static Set<String> requiredScopes() {
        Set<String> result = new HashSet<>();

        result.add(DirectoryScopes.ADMIN_DIRECTORY_GROUP);
        result.addAll(GroupssettingsScopes.all());

        return result;
    }


    public String getDomain() {
        return domain;
    }

    private Credential getCredential() throws Exception {
        File dataStoreLocation = new File(credentialsPath);
        FileDataStoreFactory store = new FileDataStoreFactory(dataStoreLocation);

        // General idea: create the auth flow backed by a credential store;
        // check whether the store already has credentials for the user we
        // want.  If it doesn't, we go through the auth process.
        GoogleAuthorizationCodeFlow auth = new GoogleAuthorizationCodeFlow.Builder(httpTransport, jsonFactory,
                user, secret,
                GoogleClient.requiredScopes())
                .setAccessType("offline")
                .setDataStoreFactory(store)
                .build();

        Credential storedCredential = null;

        storedCredential = auth.loadCredential(user);

        if (storedCredential == null) {
            throw new RuntimeException("No stored credential was found for user: " + user);
        }

        // Take our credential and wrap it in a GoogleCredential.  As far as
        // I can tell, all this gives us is the ability to update our stored
        // credentials as they get refreshed (using the
        // DataStoreCredentialRefreshListener).
        Credential credential = new GoogleCredential.Builder()
                .setTransport(httpTransport)
                .setJsonFactory(jsonFactory)
                .setClientSecrets(user, secret)
                .addRefreshListener(new CredentialRefreshListener() {
                    public void onTokenErrorResponse(Credential credential, TokenErrorResponse tokenErrorResponse) {
                        logger.error("OAuth token refresh error: " + tokenErrorResponse);
                    }

                    public void onTokenResponse(Credential credential, TokenResponse tokenResponse) {
                        logger.info("OAuth token was refreshed");
                    }
                })
                .addRefreshListener(new DataStoreCredentialRefreshListener(user, store))
                .build();

        credential.setAccessToken(storedCredential.getAccessToken());
        credential.setRefreshToken(storedCredential.getRefreshToken());

        return credential;
    }

    public Directory getDirectory() throws Exception {
        return new Directory.Builder(httpTransport, jsonFactory, getCredential())
                .setApplicationName(APPLICATION)
                .build();
    }

    public Groupssettings getGroupSettings() throws Exception {
        return new Groupssettings.Builder(httpTransport, jsonFactory, getCredential())
                .setApplicationName(APPLICATION)
                .build();
    }
}
