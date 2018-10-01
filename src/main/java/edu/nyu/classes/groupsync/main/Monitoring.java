package edu.nyu.classes.groupsync.main;

import java.io.IOException;
import java.io.FileWriter;

class Monitoring {

    // Dump an exception to a file so we can monitor it through health checks/Nagios/whatever.
    public static synchronized void recordException(Throwable e) {
        FileWriter log = null;

        try {
            log = new FileWriter("logs/last_exception.log");

            log.write(e.getMessage());
            log.write("\n");

            for (StackTraceElement frame : e.getStackTrace()) {
                log.write(frame.toString());
                log.write("\n");
            }
        } catch (IOException e2) {
            // Well, we tried.
        } finally {
            if (log != null) {
                try {
                    log.close();
                } catch (IOException e3) {}
            }
        }
    }

}
