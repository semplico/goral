* 0.1.17
    * transfer the repository to @semplico github org

* 0.1.16
    * ssh access log - RFC3339 support

* 0.1.15
    * bugfix for rows counts assertion
    * update dependencies

* 0.1.14
    * exponential backoff for rules as well
    * upgrade all dependencies

* 0.1.13
    * sheets storage refactoring
    * bugfix of the truncation algorithm

* 0.1.12
    * spreadsheet links with a specific row for healthchecks and rules triggers
    * upgrade all dependencies

* 0.1.11
    * refactor Github flows
    * scrape-push rule for queues sizes takes into account max append duration timeout
    * http status code for http failed healthchecks prepends the response
    * fix the truncation algorithm
    * different logs detailization for log levels

* 0.1.10
    * upgrade all dependencies (except sysinfo)
    * refactor http client and server usage

* 0.1.9
    * reorder system logs fields for charts
    * ssh versions checks (for ubuntu)
    * system support check (for ubuntu)

* 0.1.8
    * fix ssh logs parsing

* 0.1.7
    * ssh log monitoring
    * rules for text now support "is" and "is not" conditions
    * more helpful message about usage limits
    * remove access/refresh tokens for google oauth from logs at the debug level

* 0.1.6
    * safe numbers conversions
    * ids collision tests
    * latency measurements for healthchecks

* 0.1.5
    * increase max body size for latest release check
    * KV server shutdown message fix

* 0.1.4
    * minor improvements for notifications
    * urls with specified domains do not require ports

* 0.1.3
    * fix version message for telegram
    * releases for other platforms
    * fine grained notifications control
    * installer shell script https://maksimryndin.github.io/goral/install.sh
    * fix port validation for domain-specified urls
    * a separate website for docs https://maksimryndin.github.io/goral

* 0.1.2
    * improve an append error handling and reporting
    * rules are applied also at shutdown
    * a welcome service message is sent to a service messenger instead of a general service messenger
    * rules update warn is sent to a service messenger first
    * rules update interval is increased
    * the append timeout is set to the maximum backoff
    * rule fetch timeout is decreased to 2000ms (from 3000ms)

* 0.1.1
    * no panic for Google API access failure - just send an error to a messenger
    * rule fetch timeout is increased to 3000ms (from 1000ms)
    * if process user cannot be retrieved, NA is returned
    * fix fetch of a user id of a process
    * fix exponential backoff algorithm (decrease jittered)
    * fix repetitive truncation warning and truncation algorithm
    * binary size is reduced (by stripping debug info)

* 0.1.0
    * first public release
