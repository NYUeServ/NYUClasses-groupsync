/**********************************************************************************
 *
 * Copyright (c) 2019 The Sakai Foundation
 *
 * Original developers:
 *
 *   New York University
 *   Payten Giles
 *   Mark Triggs
 *
 * Licensed under the Educational Community License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.osedu.org/licenses/ECL-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 **********************************************************************************/

package edu.nyu.classes.groupsync.main.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.UUID;
import java.util.Collection;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A database connection with commit/rollback tracking.
 */
public class DBConnection {
    private static final Logger LOG = LoggerFactory.getLogger(DBConnection.class);

    private final Connection connection;
    private boolean resolved;
    private boolean dirty;
    private long logQueryThresholdMS = -1;

    public DBConnection(Connection connection) {
        this.connection = connection;
        this.dirty = false;
        this.resolved = false;
    }

    public void setTimingEnabled() {
        this.logQueryThresholdMS = 0;
    }

    public void setTimingEnabled(long msThreshold) {
        this.logQueryThresholdMS = msThreshold;
    }

    public void setTimingDisabled() {
        this.logQueryThresholdMS = -1;
    }

    public void commit() throws SQLException {
        connection.commit();
        resolved = true;
    }

    public void rollback() throws SQLException {
        connection.rollback();
        resolved = true;
    }

    /**
     * Record the fact that this connection has updated the database (so commit or rollback is required)
     */
    public void markAsDirty() {
        this.dirty = true;
    }

    /**
     * True if the database wasn't updated or a commit or rollback was performed.
     */
    public boolean wasResolved() {
        if (dirty) {
            return resolved;
        } else {
            return true;
        }
    }

    public PreparedStatement prepareStatement(String sql) throws SQLException {
        return connection.prepareStatement(sql);
    }

    public DBPreparedStatement run(String sql) throws SQLException {
        return new DBPreparedStatement(connection.prepareStatement(sql), sql, this);
    }

    public String uuid() {
        return UUID.randomUUID().toString();
    }

    public String placeholders(Collection<?> coll) {
        return coll.stream().map(_p -> "?").collect(Collectors.joining(","));
    }

    public boolean isConstraintViolation(SQLException e) {
        return e.getSQLState().startsWith("23");
    }

    public void maybeLogTime(String sql, long timeMs) {
        if (logQueryThresholdMS >= 0 && timeMs >= logQueryThresholdMS) {
            LOG.info(String.format("DBConnection query took %d ms: %s", timeMs, sql));
        }
    }

    public boolean isOracle() throws SQLException {
        return connection.getMetaData().getDatabaseProductName().contains("Oracle");
    }

    public boolean isMySQL() throws SQLException {
        return connection.getMetaData().getDatabaseProductName().contains("MySQL");
    }


}
