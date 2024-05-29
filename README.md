For now bikeshed is just a collection of queries and scripts. An end goal would be to bundle these things up into a single CLI for easy installation and use.

# Requirements

- [clickhouse client](https://clickhouse.tech/docs/en/getting-started/install/) for running queries
- [jless](https://jless.io/) for viewing query results as JSON trees

# Queries

## Query Log By Normalized Hash

`clickhouse client --queries-file queries/query_log_by_normalized_hash.sql | jless`
