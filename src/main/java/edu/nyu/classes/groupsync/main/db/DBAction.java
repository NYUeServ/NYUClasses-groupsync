package edu.nyu.classes.groupsync.main.db;

import java.sql.SQLException;

/**
 * Interface for performing a database query within a transaction.
 */
public interface DBAction<E> {
    public E call(DBConnection db) throws SQLException;
}
