package edu.nyu.classes.groupsync.main.db;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Provide an iterator over a ResultSet.
 */
public class DBResults implements Iterable<ResultSet>, Iterator<ResultSet>, AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(DBResults.class);
    private final PreparedStatement originalStatement;
    private final ResultSet resultSet;
    private boolean hasRowReady;

    public DBResults(ResultSet rs, PreparedStatement originalStatement) {
        this.resultSet = rs;
        this.originalStatement = originalStatement;
    }

    @Override
    public void close() throws SQLException {
        resultSet.close();
        originalStatement.close();
    }

    @Override
    public boolean hasNext() {
        try {
            if (!hasRowReady) {
                hasRowReady = resultSet.next();
            }

            return hasRowReady;
        } catch (SQLException e) {
            LOG.warn("SQLException while calling hasNext", e);
            return false;
        }
    }

    @Override
    public ResultSet next() {
        if (!hasRowReady) {
            throw new NoSuchElementException("Read past end of results");
        }

        hasRowReady = false;
        return resultSet;
    }

    @Override
    public Iterator<ResultSet> iterator() {
        return this;
    }
}
