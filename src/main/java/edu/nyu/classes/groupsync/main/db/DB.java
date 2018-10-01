package edu.nyu.classes.groupsync.main.db;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * Logic for borrowing and returning DB connections.
 */
public class DB {

    private static final Logger LOG = LoggerFactory.getLogger(DB.class);

    private DB() {
        // No public constructor
    }

    /**
     * Run some database queries within a transaction.
     */
    public static <E> E transaction(DataSource ds, DBAction<E> action) throws RuntimeException {
        return transaction(ds, action.toString(), action);
    }

    /**
     * Run some database queries within a transaction with a helpful message if something goes wrong.
     */
    public static <E> E transaction(DataSource ds, String actionDescription, DBAction<E> action) throws RuntimeException {
        try {
            Connection db = ds.getConnection();
            DBConnection dbc = new DBConnection(db);
            boolean autocommit = db.getAutoCommit();

            try {
                db.setAutoCommit(false);

                return action.call(dbc);
            } finally {

                if (!dbc.wasResolved()) {
                    LOG.warn("**************\nDB Transaction was neither committed nor rolled back.  Committing for you.");
                    new Throwable().printStackTrace();
                    dbc.commit();
                }

                if (autocommit) {
                    db.setAutoCommit(true);
                }

                db.close();
            }

        } catch (SQLException e) {
            throw new RuntimeException("Failure in database action: " + actionDescription, e);
        }
    }
}
