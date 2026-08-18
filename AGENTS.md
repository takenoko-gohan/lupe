# Lupe

A Rust CLI that loads AWS ALB / S3 access logs into DuckDB for local SQL analysis. Human-facing usage lives in `README.md`.

## Architecture

The same binary is both the CLI client and a long-running server. Public subcommands are `load` / `query` / `clean`. `server` is clap-`hide`n and is not a user-facing command.

1. `load` starts `lupe server` as a child process if the Unix socket (`$TMPDIR/lupe.sock`, often `/tmp/lupe.sock`) is missing
2. The client calls tonic gRPC over that UDS (dummy endpoint `http://[::]:0` plus a custom connector)
3. The server holds an **in-memory DuckDB**. Nothing is persisted
4. S3 reads use DuckDB `httpfs` and `CREATE SECRET (TYPE S3, PROVIDER CREDENTIAL_CHAIN, CHAIN 'config;sts;sso;env')`
5. `query` does not start the server. It fails if the socket is missing
6. `clean` does not `DROP` tables. It sends a Shutdown RPC, stops the server, and removes the socket (in-memory data goes with the process)

```
src/main.rs          clap entry. `--debug` enables `lupe=debug`
src/cmd/             subcommands (gRPC client side)
src/pb/              gRPC server impl (Management / Operation)
src/repo/            DuckDB init, log table creation, Arrow-to-string conversion
src/repo/alb.rs      ALB access logs
src/repo/s3.rs       S3 access logs
src/util/uds.rs      socket path and Channel setup (up to 3 retries)
proto/db.proto       service definition; compiled by `build.rs` via tonic-prost-build
```

`.devcontainer/` is not part of lupe itself. It is an isolated Cursor Agent environment with an mitm proxy. Do not touch it when changing application behavior.

## Setup and commands

- Build: `cargo build`
- Help: `cargo run -- --help`
- Format: `cargo fmt`
- Lint: `cargo clippy --all-targets -- -D warnings`
- DuckDB is `bundled`, so the first build is slow
- Rebuilding is required after changing `proto/db.proto`
- `load` needs AWS credentials in the environment (config / STS / SSO / env vars)
- There is no test crate or CI workflow yet

## Code conventions

- Rust edition 2021. Modules are `pub(crate)`
- Each subcommand lives in `src/cmd/<name>.rs` as `pub(crate) async fn run(...) -> Result<(), Box<dyn std::error::Error>>`
- Assemble structs with `typed-builder`
- Log with `tracing` (`info` / `debug` / `error`). Reserve `println!` for user output such as query results
- clap uses derive. New public flags are `--kebab-case`
- gRPC business errors use `Status` (`internal` / `invalid_argument`). Clients return `e.message()`
- Commit messages follow the existing history: English, Conventional Commits style (`feat:` / `fix:` / `chore:`)
- Do not change README, dependencies, or `.devcontainer` unless the user asked

## Adding a log type

When adding a `--table-type`, update all of the following. Numeric values must match `TableType` in `proto/db.proto`.

| Location | What to do |
|---|---|
| `proto/db.proto` | Add a value to `enum TableType` |
| `src/cmd/load.rs` | clap `TableType`, `From<TableType> for i32`, default table name |
| `src/repo/<type>.rs` | Implement `Client` and pin the schema with `CREATE TABLE ... AS` |
| `src/repo/mod.rs` | Export the module |
| `src/pb/mod.rs` | Add a branch to the `match req.table_type` in `create_table` |

Existing mapping: `0 = ALB` (default table `alb_logs`), `1 = S3` (`s3_logs`).

DuckDB `CREATE TABLE` interpolates `table_name` and `s3_uri` with `format!`. Identifiers are not escaped. Keep treating table names as trusted input.

Arrow-to-string conversion in `src/repo/mod.rs` is an explicit per-type match. Unsupported types error. If a new DuckDB / Arrow type shows up in query results, extend that match.

## Query results

`repo::raw_query` collects Arrow `RecordBatch`es column-wise, reshapes them into rows, and returns `RawQueryReply`. The `query` command prints with `comfy-table`. NULLs become the string `"NULL"`.

## Boundaries

- Always: run `cargo fmt` and `cargo clippy` after changes. If you touch proto, confirm generated code still builds
- Ask first: breaking public CLI changes, adding or updating dependencies, or redesigning DuckDB to persist on disk
- Never: commit secrets or `target/`. Do not loosen `.devcontainer` allowlists or CA handling unless asked
