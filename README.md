Groupsync daemon
----------------

This is a daemon that can replicate group and membership information
from a relational database to Google Groups.

Notable files:

  * config/config.properties -- Contains DB and Google credentials.

  * config/logback.xml -- Logging configuration.  Logs to `logs/` by default.

  * build.sh -- Build the code.

  * run.sh -- Run the daemon in the foreground.

  * init.sh -- An init script for running on startup.

  * authorize_oauth.sh -- Sets up OAuth tokens for Google API access.


Getting started
---------------

  * Unpack the code somewhere

  * Configure your DB and Google settings under config.properties

  * Authorize your Google account by running:

      ./authorize_oauth.sh 'yourgoogleuser.apps.googleusercontent.com' 'usersecret'

    These values are available under the 'Credentials' area of the Google API console.

  * Apply the appropriate database commands for your database (see the
    `migrations` directory)

  * Start it up (either with `run.sh` or via the init script)
