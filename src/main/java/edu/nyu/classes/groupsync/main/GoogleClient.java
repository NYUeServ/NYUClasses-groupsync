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
import com.google.api.client.util.store.AbstractDataStoreFactory;
import com.google.api.client.util.store.DataStore;
import com.google.api.client.util.store.DataStoreFactory;
import com.google.api.client.util.store.FileDataStoreFactory;
import com.google.api.services.admin.directory.Directory;
import com.google.api.services.admin.directory.DirectoryScopes;
import com.google.api.services.groupssettings.Groupssettings;
import com.google.api.services.groupssettings.GroupssettingsScopes;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
        NYUDataStoreFactory store = new NYUDataStoreFactory(dataStoreLocation);

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


    public class NYUDataStoreFactory extends AbstractDataStoreFactory {
        private File dataStoreLocation;
        private FileDataStoreFactory wrappedFactory;

        public NYUDataStoreFactory(File dataStoreLocation) throws IOException {
            this.dataStoreLocation = dataStoreLocation;
            this.wrappedFactory = new FileDataStoreFactory(dataStoreLocation);
        }

        protected <V extends Serializable> DataStore<V> createDataStore(String id) throws IOException {
            return new NYUFileDataStore<>(new File(dataStoreLocation, "StoredCredential"),
                                          this,
                                          wrappedFactory.getDataStore(id));
        }
    }

    private class NYUFileDataStore<V extends Serializable> implements DataStore<V> {
        private File credentialFile;
        private DataStoreFactory factory;
        private DataStore<V> wrappedDataStore;
        private Lock lock;

        public NYUFileDataStore(File credentialFile, DataStoreFactory factory, DataStore<V> wrappedDataStore) {
            this.lock = new ReentrantLock();
            this.credentialFile = credentialFile;
            this.factory = factory;
            this.wrappedDataStore = wrappedDataStore;
        }

        /// All of these just wrap the original data store.
        public DataStoreFactory getDataStoreFactory() {
            return this.factory;
        }
        public String getId() {
            return this.wrappedDataStore.getId();
        }

        public int size() throws IOException {
            return this.wrappedDataStore.size();
        }

        public boolean isEmpty() throws IOException {
            return this.wrappedDataStore.isEmpty();
        }

        public boolean containsKey(String key) throws IOException {
            return this.wrappedDataStore.containsKey(key);
        }

        public boolean containsValue(V value) throws IOException {
            return this.wrappedDataStore.containsValue(value);
        }

        public Set<String> keySet() throws IOException {
            return this.wrappedDataStore.keySet();
        }

        public Collection<V> values() throws IOException {
            return this.wrappedDataStore.values();
        }

        public V get(String key) throws IOException {
            return this.wrappedDataStore.get(key);
        }

        /// Deal with disk space issues by making a copy of our credential store
        /// prior to adding to it.  If all else fails, atomically move the
        /// backup over the top of the (now empty) version.
        public DataStore<V> set(String key, V value) throws IOException {
            backupCredentialFile();
            try {
                return this.wrappedDataStore.set(key, value);
            } catch (IOException e) {
                recoverCredentialFile();
                throw e;
            }
        }

        public DataStore<V> clear() throws IOException {
            backupCredentialFile();
            try {
                return this.wrappedDataStore.clear();
            } catch (IOException e) {
                recoverCredentialFile();
                throw e;
            }
        }

        public DataStore<V> delete(String key) throws IOException {
            backupCredentialFile();
            try {
                return this.wrappedDataStore.delete(key);
            } catch (IOException e) {
                recoverCredentialFile();
                throw e;
            }
        }

        private void backupCredentialFile() throws IOException {
            lock.lock();
            try {
                Files.copy(this.credentialFile.toPath(),
                           new File(this.credentialFile.getPath() + ".bak").toPath(),
                           StandardCopyOption.REPLACE_EXISTING,
                           StandardCopyOption.COPY_ATTRIBUTES);
            } finally {
                lock.unlock();
            }
        }

        private void recoverCredentialFile() throws IOException {
            lock.lock();
            try {
                Files.move(new File(this.credentialFile.getPath() + ".bak").toPath(),
                           this.credentialFile.toPath(),
                           StandardCopyOption.REPLACE_EXISTING,
                           StandardCopyOption.ATOMIC_MOVE);
            } finally {
                lock.unlock();
            }
        }
    }
}
