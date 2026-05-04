# Development notes

## Lockfile and installs

The repo uses **[uv](https://docs.astral.sh/uv/)** with a checked-in **`uv.lock`**. After changing `pyproject.toml`:

```bash
uv lock
uv sync
```

`uv sync` installs the project (via **hatchling**) into `.venv` and wires up console scripts (`garmin-fetch`, `garmin-mcp-influx`, etc.).

Rebuild the Docker image after lockfile changes so the image’s `uv sync` matches.

## MCP servers (local)

Requires the `mcp` package (declared in `pyproject.toml`). From the repo root with `uv` or a venv:

```bash
# example: Influx must be reachable (e.g. 127.0.0.1:8086 with compose ports uncommented)
export INFLUXDB_HOST=127.0.0.1
garmin-mcp-influx
```

See `docs/claude-mcp-integration.md` for Claude Desktop configuration.
