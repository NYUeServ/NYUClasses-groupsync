package edu.nyu.classes.groupsync.main.db;

import java.io.Reader;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Wrap a PreparedStatement, providing nicer parameter handling and transaction commit/rollback checks.
 */
public class DBPreparedStatement {

    private final DBConnection dbConnection;
    private final PreparedStatement preparedStatement;
    private int paramCount;

    public DBPreparedStatement(PreparedStatement preparedStatement, DBConnection dbc) {
        this.dbConnection = dbc;
        this.preparedStatement = preparedStatement;
        this.paramCount = 1;
    }

    public DBPreparedStatement param(String parameter) throws SQLException {
        try {
            preparedStatement.setString(paramCount(), parameter);
            return this;
        } catch (SQLException e) {
            close();
            throw e;
        }
    }

    public DBPreparedStatement param(Long parameter) throws SQLException {
        try {
            preparedStatement.setLong(paramCount(), parameter);
            return this;
        } catch (SQLException e) {
            close();
            throw e;
        }
    }

    public DBPreparedStatement param(Integer parameter) throws SQLException {
        try {
            preparedStatement.setInt(paramCount(), parameter);
            return this;
        } catch (SQLException e) {
            close();
            throw e;
        }
    }

    public DBPreparedStatement param(Reader reader) throws SQLException {
        try {
            preparedStatement.setClob(paramCount(), reader);
            return this;
        } catch (SQLException e) {
            close();
            throw e;
        }
    }

    public DBPreparedStatement param(Reader reader, long length) throws SQLException {
        try {
            int param = paramCount();
            try {
                preparedStatement.setCharacterStream(param, reader, length);
            } catch (java.lang.AbstractMethodError e) {
                // Older JDBC versions don't support the method with a long
                // argument.  Use an int instead.
                preparedStatement.setCharacterStream(param, reader, (int) length);
            }

            return this;
        } catch (SQLException e) {
            close();
            throw e;
        }
    }

    public DBPreparedStatement clearParameters() throws SQLException {
        preparedStatement.clearParameters();
        this.paramCount = 1;

        return this;
    }

    public void addBatch() throws SQLException {
        preparedStatement.addBatch();
    }

    public int[] executeBatch() throws SQLException {
        return preparedStatement.executeBatch();
    }

    public int executeUpdate() throws SQLException {
        dbConnection.markAsDirty();
        return preparedStatement.executeUpdate();
    }

    public DBResults executeQuery() throws SQLException {
        return new DBResults(preparedStatement.executeQuery(),
                preparedStatement);
    }

    public void close() {
        try {
            preparedStatement.close();
        } catch (SQLException e) {
        }
    }

    private int paramCount() {
        return this.paramCount++;
    }

}
