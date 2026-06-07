# KuzuDB — Maintained by Vela Partners

**Embedded graph database for AI agent memory.** Built for speed. Concurrent multi-writer support. MIT licensed.

KuzuDB is an embedded property graph database optimized for complex analytical workloads on very large graphs. This fork, maintained by [Vela Partners](https://vela.partners), extends the original with concurrent write support for multi-agent AI systems.

> **Looking for KuzuDB?** The original project by Kuzu Inc. (University of Waterloo) was [archived in October 2025](https://github.com/kuzudb/kuzu). This is an actively maintained fork. See [Fork Context](#fork-context) below.

[![MIT License](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

---

## What's Different in This Fork

The original KuzuDB enforces a single-writer constraint. In multi-agent AI systems, multiple agents need to write to the context graph simultaneously. Serialized writes bottleneck the entire system and worsen as agent count grows.

**This fork adds concurrent multi-writer support**, enabling architectures where many AI agents read and write to a shared graph memory in parallel.

Everything else from the original KuzuDB remains intact: Cypher queries, vector search, full-text search, columnar storage, WASM bindings, and all language bindings (Python, Node.js, Rust, Go, Java, C/C++).

## Why KuzuDB for AI Agents

AI agents need memory that captures relationships, not just documents. When an agent traces a chain like `Founder → Company → Round → Outcome`, that is a multi-hop graph traversal. KuzuDB runs these **374x faster** than Neo4j on path queries (0.009s vs 3.22s) while requiring **zero infrastructure** (no server, no Docker, no connection pool).

| Capability | KuzuDB | Neo4j | Vector DB |
|---|---|---|---|
| Multi-hop path queries | 0.009s | 3.22s | Not supported |
| Infrastructure required | None (in-process) | Server + config | Server + config |
| Concurrent writes | Yes (this fork) | Yes | Yes |
| Causal chain traversal | Native | Native | Approximate (embedding) |
| License | MIT | GPLv3 / Commercial | Varies |

For the full benchmark methodology, see [P. Rao's kuzudb-study](https://github.com/prrao87/kuzudb-study) (100K nodes, 2.4M edges).

**Read the full technical writeup:** [KuzuDB for Production AI Agents: Our Fork, the Benchmarks, and Why Graph Memory Works](https://www.vela.partners/blog/kuzudb-ai-agent-memory-graph-database)

## Quick Start

```bash
pip install kuzu
```

```python
import kuzu

db = kuzu.Database("./agent_memory")
conn = kuzu.Connection(db)

# Create schema
conn.execute("CREATE NODE TABLE Entity(name STRING, type STRING, PRIMARY KEY(name))")
conn.execute("CREATE REL TABLE RELATES_TO(FROM Entity TO Entity, relation STRING)")

# Add knowledge
conn.execute("CREATE (e:Entity {name: 'Acme AI', type: 'company'})")
conn.execute("CREATE (e:Entity {name: 'Jane Smith', type: 'founder'})")
conn.execute("""
    MATCH (a:Entity {name: 'Jane Smith'}), (b:Entity {name: 'Acme AI'})
    CREATE (a)-[:RELATES_TO {relation: 'founded'}]->(b)
""")

# Query: who founded what?
result = conn.execute("""
    MATCH (f:Entity)-[r:RELATES_TO {relation: 'founded'}]->(c:Entity)
    RETURN f.name, c.name
""")
print(result.get_as_df())
```

No server. No Docker. Just `pip install` and query.

## Core Features

- **Property Graph Model** with openCypher query language (same as Neo4j)
- **Embedded, in-process** execution with sub-millisecond latency
- **Concurrent multi-writer support** for multi-agent architectures (this fork)
- **Vector search** and **full-text search** built in
- **Columnar disk-based storage** with CSR adjacency indices
- **Vectorized and factorized query processor** (up to 374x faster than Neo4j on path queries)
- **Worst-case optimal join algorithms** for complex many-to-many traversals
- **Multi-core parallelism** across all available CPU cores
- **Serializable ACID transactions**
- **WebAssembly bindings** for browser execution
- **Language bindings:** Python, Node.js, Rust, Go, Java, C/C++

## Documentation

- **Docs:** [kuzudb.github.io/docs](https://kuzudb.github.io/docs)
- **Blog (archive):** [kuzudb.github.io/blog](https://kuzudb.github.io/blog)
- **Technical deep-dive:** [Vela Partners blog post](https://www.vela.partners/blog/kuzudb-ai-agent-memory-graph-database)
- **Research paper:** [KÙZU GDBMS, CIDR 2023](https://www.cidrdb.org/cidr2023/papers/p48-jin.pdf)
- **Getting started:** [kuzudb.github.io/docs/get-started](https://kuzudb.github.io/docs/get-started/)

## Extensions

KuzuDB v0.11.3+ bundles four commonly used extensions (`algo`, `fts`, `json`, `vector`). No manual installation needed.

The Vela-maintained fork publishes FTS extension artifacts to a static GitHub Pages registry. FTS installs use this registry by default:

```sql
INSTALL fts;
LOAD fts;
```

You can also set the registry explicitly:

```bash
export KUZU_EXTENSION_REPO=https://vela-engineering.github.io/kuzu/
```

You can also install FTS directly from the registry in SQL:

```sql
INSTALL fts FROM 'https://vela-engineering.github.io/kuzu/';
```

For additional extensions, you can host your own extension server:

```bash
docker pull ghcr.io/kuzudb/extension-repo:latest
docker run -d -p 8080:80 ghcr.io/kuzudb/extension-repo:latest
```

Then install from your server:

```sql
INSTALL <EXTENSION_NAME> FROM 'http://localhost:8080/';
```

## Build from Source

See the [developer guide](https://kuzudb.github.io/docs/developer-guide) for build instructions.

## Fork Context

KuzuDB was originally developed by Kùzu Inc., founded by [Semih Salihoglu](https://cs.uwaterloo.ca/~ssalMDiho/) and his team at the University of Waterloo. The project was archived in October 2025. The engineering quality of the original codebase is exceptional, grounded in serious database research including worst-case optimal joins and factorized execution, published at [CIDR 2023](https://www.cidrdb.org/cidr2023/papers/p48-jin.pdf). Everything this fork builds on exists because of the foundation the Waterloo team laid.

### Other Active Forks

| Fork | Maintainer | Focus |
|---|---|---|
| **[Vela-Engineering/kuzu](https://github.com/Vela-Engineering/kuzu)** (this repo) | [Vela Partners](https://vela.partners) | Multi-agent AI systems, concurrent writes |
| [LadybugDB](https://github.com/LadybugDB/ladybug) | Arun Sharma | General-purpose KuzuDB continuation |
| [Bighorn](https://github.com/Kineviz/bighorn) | Kineviz | Graph visualization, server mode |

## Contributing

We welcome contributions. Priority areas:

1. **Multi-agent memory patterns** — if you're building AI agents with graph memory, share your use cases
2. **Bug fixes and stability** — ensuring core functionality is rock-solid
3. **CI/CD and testing** — cross-platform automated testing
4. **Documentation** — tutorials, examples, migration guides
5. **Extension ecosystem** — packaging and distribution

Open an issue or submit a PR. We review quickly.

## About Vela Partners

[Vela Partners](https://vela.partners) is an AI-native quantitative venture capital firm based in San Francisco. We invest in seed-stage AI startups and publish peer-reviewed research with the University of Oxford (50+ papers). We operate multi-agent AI systems across sourcing, evaluation, and portfolio monitoring. KuzuDB is the graph memory layer for these agents.

## Releases

| Version | Date | Highlights |
|---|---|---|
| [v0.12.0-vela](https://github.com/Vela-Engineering/kuzu/releases/tag/v0.12.0-vela.87bf0be) | Mar 2026 | Concurrent multi-writer support, rebased on upstream 0.12.0 |
| v0.11.3-vela | Nov 2025 | Initial fork, bundled extensions (algo, fts, json, vector) |

See [all releases](https://github.com/Vela-Engineering/kuzu/releases).

## License

MIT License. See [LICENSE](LICENSE).
