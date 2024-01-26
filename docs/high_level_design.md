# HLD v1:

Design diag: https://whimsical.com/rover-v1-2cPN1nvV8fGCBwQfaiDwPA

This version supports very basic level of querying with manually ingested data (multiple tables). Metadata store is in-mem hashtable. Only allows for fetch, and row selection within 1 single table. Sync queries only.

## Tech Choices

1. Apache Spark as compute work-horse.
2. In-mem HashTables as metadata store.
3. Ingestion format: csv only.

