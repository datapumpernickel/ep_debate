## European Parliamentary Debates

The following repository contains data to
* scrape the calendar information on sessions of the EP (`01_raw_data/calendar`)
* scrape the respective Table of Contents (`01_raw_data/tocs`)
* scrape the verbatim debate reports of the sessions (`01_raw_data/debates`)

The debates are identified by:
- table of content points ending in (debate)
- excluding most table of content points that occured more than 10 times (adjounrment of the session, opening of the session, etc.)
- excluding some specific words related to proceedings that are not debates (votes, voting, minutes, etc.)

A rough visual inspection was done to confirm this. This resulted in about 22892 debates being collected.


