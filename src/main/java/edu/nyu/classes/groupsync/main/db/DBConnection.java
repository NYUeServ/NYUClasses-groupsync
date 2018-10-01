package edu.nyu.classes.groupsync.main.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * A database connection with commit/rollback tracking.
 */
public class DBConnection {

    private final Connection connection;
    private boolean resolved;
    private boolean dirty;

    public DBConnection(Connection connection) {
        this.connection = connection;
        this.dirty = false;
        this.resolved = false;
    }

    public void commit() throws SQLException {
        connection.commit();
        resolved = true;
    }

    public void rollback() throws SQLException {
        connection.rollback();
        resolved = true;
    }

    public boolean isOracle() throws SQLException {
        return connection.getMetaData().getDatabaseProductName().contains("Oracle");
    }

    public boolean isMySQL() throws SQLException {
        return connection.getMetaData().getDatabaseProductName().contains("MySQL");
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
        return new DBPreparedStatement(connection.prepareStatement(sql), this);
    }
}
