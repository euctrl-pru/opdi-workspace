# Portal edits and short tracks in the flight list (2026-11-19)

## The owner's request (verbatim, numbering as given)

1. The Overview page title should be "OPDI Data".
2. The warning "Release v2.0.0 is the upcoming release. This section describes it. Its files are being produced, ... see Release v0.0.2" can be removed.
3. The pages table, the "Page" column should be wider horizontally. Currently it's cutting off words.
4. Call the menu items as follows: Access -> Data Access. Algorithms -> OPDI Algorithms.
5. Order of the pages under OPDI Data: 1. Overview; 2. Data Access; 3. Data dictionary; 4. Data Quality; 5. Data preview; 6. OPDI Algorithms; 7. Releases.
6. On the overview page:
   1. "Where the data are: Trino (...) and the daily files, read with Python, R, DuckDB or any Parquet tool." -> "Where the data can be accessed and how."
   2. "What changed in release v2.0.0 since v0.0.2, ... how it was computed." -> "Release information."
   3. "Charts made from the data." -> "A preview of what is included in the OPDI data and what can be done with it."
   4. "When the data as a whole, and each item, are missing or wrong." -> "An overview of which limitations apply to the flight list, flight events and measurements."
   5. "How often each item exists and how close it is to the records, overall and at the 100 busiest airports in Europe and worldwide (Airports)." -> "An overview of the data quality, incl. availability and accuracy. Benchmarking is done using EUROCONTROL data."
   6. "OPDI starts from the position reports that aircraft broadcast by radio" -> by ADS-B (don't mention radio).
   7. ", and the columns of its flight, so one row stands alone." -> delete.
   8. "how each is computed is explained in [Algorithms]" -> OPDI Algorithms.
9. Don't mention the release version anywhere in text; e.g. "Release v2.0.0 has two files per day:" -> "The data is published in daily files."
10. The paragraph "A few events belong to a track with no row in the flight list (3,338 of 6,691,769 events ...)": flights without departure or destination must also be in the flight list. Add them again to the flight list (opdi). Remove the paragraph.
10b. Coverage paragraph: don't mention the version.
11. Data Access: don't mention opdi-reader for now. Cut away most of the longer examples. Remove the Python, DuckDB and R sections. No need to show how to get the v0.0.2 version.
12. Remove the version details site-wide; mention version details only in Releases.

"Apply this and then I'll review." Execution: subagent-driven.

## Findings (measured 2026-11-19 on the 14 June 2026 scratch run `runs/next_day`)

- The flight list already holds flights with neither departure nor destination
  (22,740 of 161,967 working rows): step 03's overflight pass
  (`FlightListProcessor._fetch_overflights`) appends them.
- The events with no flight-list row (2,670 of 6,067,536 in that run) belong to
  1,141 tracks whose reports span less than 5 minutes (median 190 s, max 299 s).
  The overflight pass drops them with a fixed rule `last - first >= 300 s`.
  Their events: first_seen and last_seen 1,141 each, top-of-climb/descent 175 each,
  a few level and flight-level crossings.
- So item 10 means: every track that has events gets a flight-list row, by
  making the 300 s rule a setting that is 0 by default and 300 under the
  v0.0.2 settings (`legacy()`), whose golden output must stay byte-identical.

## Calls made for the owner (to check on review)

- Releases keeps all version detail. The Roadmap keeps its release-history
  table and its "Release v2.0.0 delivers" plan, because listing releases is that
  page's purpose; its status callout loses the version wording like the rest.
- Version strings that are data values (the `version` column values such as
  `time_v0.0.2`, `events_v0.5.0`; the `release` column) stay on the Data
  dictionary as values, without "release v2.0.0" prose around them.
- opdi-reader is removed from Data Access, and also from Resources ("for now").
- Data Access keeps one short Trino example (flights between two airports).
- Overview day counts become rounded ("about 160,000 flights and 6.7 million
  events"), since the step 03 inputs of 14 June were deleted and cannot be
  re-run cheaply; the rounded values stay true with the ~0.7% added rows.
- No version-string change for the flight list: v6.0.0/v6.1.0 are not yet
  published (release v2.0.0 is upcoming).
