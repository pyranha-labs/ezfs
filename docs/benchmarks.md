## Benchmarks

## Table Of Contents

  * [Compressor Native Calls vs Optimized](#compressor-native-calls-vs-optimized)
  * [SQLite3 Read/Write Performance By File Size](#sqlite3-readwrite-performance-by-file-size)
  * [DuckDB vs SQLite3 DB Adapter Backend](#duckdb-vs-sqlite3-db-adapter-backend)


### Compressor Native Calls vs Optimized

**Test details:**
- 1 Read/Write == 1 entire "file"
- 64 byte text "file"
- 5M iterations of reads or writes
- Averaged over 3 loops
- ezfs 1.1.0
- zstandard 0.22.0

**Test results:**

| Operations                                | Time   | Write/s | Read/s   |
|-------------------------------------------|--------|---------|----------|
| zstandard.compress/decompress             | 22.50s | 222,222 | 304,136  |
| zstandard.ZstdCompressor/ZstdDecompressor | 19.40s | 257,731 | 341,763  |

**Takeaways:**
- Custom compressors with cached objects are faster, but require additional setup
  - High transaction workflows benefit more
- Optimized compressors are not always thread safe
  - Per `zstandard` documentation, decompressors should not be shared across threads
  - Depending on module, multi-thread workflows may require additional setup to ensure no shared resources


### SQLite3 Read/Write Performance By File Size

**Test details:**
- 1 Read/Write == 1 entire "file"
- Files 64 to 2M in size
  - Small: 64 byte text "file", uncompressed
  - Medium: 300K byte binary "file", uncompressed
  - Large: 2M byte binary "file", uncompressed
- Iterations 10K to 1M
  - 1M iterations of reads or writes for small
  - 100K iterations of reads or writes for medium
  - 10K iterations of reads or writes for large 
- Averaged over 3 loops
- sqlite3 from Python 3.10.11
- Write == INSERT
- Read == SELECT

**Test results:**

| Operations                                           | Write Time | Write/s   | Read/s    |
|------------------------------------------------------|------------|-----------|-----------|
| Small, sqlite3, :memory:                             | 4.87s      | 205,338   | 330,033   |
| Small, sqlite3, SSD                                  | 317.81s    | 3,154     | 147,928   |
| Small, sqlite3, :memory:<br/>1000 insert per commit  | 3.04s      | 328,947s  | No change |
| Small, sqlite3, SSD<br/>1000 insert per commit       | 3.63s      | 275,482s  | No change |
| Medium, sqlite3, :memory:                            | 19.78s     | 5055      | 600       |
| Medium, sqlite3, SSD                                 | 70.40s     | 1420      | 2500      |
| Medium, sqlite3, :memory:<br/>1000 insert per commit | No change  | No change | No change |
| Medium, sqlite3, SSD<br/>1000 insert per commit      | 28.85s     | 3466      | No change |
| Large, sqlite3, :memory:                             | 14.86s     | 672       | 277       |
| Large, sqlite3, SSD                                  | 27.17s     | 368       | 781       |
| Large, sqlite3, :memory:<br/>1000 insert per commit  | No change  | No change | No change |
| Large, sqlite3, SSD<br/>1000 insert per commit       | No change  | No change | No change |

**Takeaways:**
- SQLite is insanely fast for small, transactional, work
- Performance slows the larger the file, which is expected
- Waiting to commit matters less and less the larger the files
  - Probably around 50-100K it starts making little to no difference between commit on write
- Interestingly, reading from disk is faster than reading from memory, with larger files
  - Medium/large in-memory tests were impacted by swap usage due to high RAM consumption, even though RAM was available


### DuckDB vs SQLite3 DB Adapter Backend

Additional actions required, depending on test:
```
self._connection = duckdb.connect(database)  # To swap backend for DuckDB
duckdb.execute('INSTALL sqlite;')  # To read/write to SQLite in DuckDB + SQLite3 tests.
```

**Test details:**
- 1 Read/Write == 1 entire "file"
- 64 byte text "file", uncompressed
- 1M iterations of reads or writes
- Averaged over 3 loops
- sqlite3 from Python 3.10.11
- duckdb 0.10.2
- Write == INSERT
- Read == SELECT

**Test results:**

| Operations                                   | Write Time    | Write/s   | Read/s       |
|----------------------------------------------|---------------|-----------|--------------|
| sqlite3, :memory:                            | 4.87s         | 205,338   | 330,033      |
| sqlite3, SSD                                 | 317.81s       | 3,154     | 147,928      |
| sqlite3, :memory:<br/>100 insert per commit  | 3.03s         | 330,033   | No change    |
| sqlite3, SSD<br/>100 insert per commit       | 7.01s         | 142,653   | No change    |
| sqlite3, :memory:<br/>1000 insert per commit | 3.04s         | 328,947s  | No change    |
| sqlite3, SSD<br/>1000 insert per commit      | 3.63s         | 275,482s  | No change    |
| duckdb, :memory:                             | 303.90s       | 3,300     | 4,658        |
| duckdb, SSD                                  | 356.81s       | 2,808     | No change    |
| duckdb, :memory:<br/>1000 insert per commit  | No change     | No change | No change    |
| duckdb, SSD<br/>1000 insert per commit       | No change     | No change | No change    |
| duckdb + sqlite3 DB, SSD                     | 600s + abort  | -         | 600s + abort |

**Takeaways:**
- DuckDB is insanely fast for analytics, but slow for regular database transactions. SQLite wins hands down.
- SQLite is insanely fast for basic transactional work, but slower for analytics. Analytics not applicable to EZFS.
