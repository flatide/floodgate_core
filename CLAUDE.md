# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build Commands

```bash
# Build JAR with dependencies copied to target/lib/
mvn clean package

# Output: target/floodgate_core-1.2.1.jar + target/lib/*.jar
```

There are no tests in this project. No lint or checkstyle configuration exists.

## Project Overview

Floodgate Core is a metadata-driven data integration library (not a standalone application) built on Java 8 / Spring Boot 2.7.3. It orchestrates data flows between heterogeneous sources (databases, files, FTP/SFTP) using configurable pipelines defined in metadata tables. Source code comments include Korean.

All source code is under `src/main/java/com/flatide/floodgate/`. There are no resources, test files, or configuration properties in this module — it is a pure library JAR meant to be embedded in host applications.

## Architecture

### Processing Pipeline

```
API Request → ChannelAgent.process()
  → Load API metadata from MetaManager
  → Resolve targets (supports concurrent execution via ThreadPoolExecutor)
  → For each target: ChannelJob.call()
    → Flow.prepare() → Flow.process()
      → Module chain: processBefore() → process() → processAfter()
        → Connector performs READ/CREATE/UPDATE/DELETE
        → MappingRules transform data between source and target schemas
```

### Singleton Managers (all accessed via `.shared()`)

| Manager | Purpose |
|---------|---------|
| `ConfigurationManager` | Hierarchical config with dot-notation keys |
| `MetaManager` | Loads/caches metadata tables (API, Flow, Datasource, Template) |
| `LoggingManager` | Manages log data sources |
| `FloodgateHandlerManager` | Lifecycle callbacks (CHANNEL_IN/OUT, FLOW_IN/OUT, MODULE_IN/OUT) |
| `ConnectorFactory` | Creates connectors by type (JDBC, FILE, FTP, SFTP) |
| `SpoolingManager` | Async job spooling |

### Connector System

`ConnectorFactory` creates connectors based on the `CONNECTOR` field in metadata:
- **JDBC** → `ConnectorDB` (HikariCP pooling). Supported `DBTYPE` values: oracle, mysql, mysql_old, mariadb, postgresql, greenplum, mssql, db2, tibero
- **FILE** → `ConnectorFile`
- **FTP** → `ConnectorFTP` (Commons-Net)
- **SFTP** → `ConnectorSFTP` (JSch)

All extend `ConnectorBase` implementing the `Connector` interface with lifecycle methods: connect, beforeRead/afterRead, beforeCreate/afterCreate, read, create, update, delete, commit, rollback, close.

### Data Streaming

```
FGInputStream → Carrier (interface) → Pipe implementations
  ├── JSONPipe  (Jackson streaming for large JSON)
  ├── ListPipe  (in-memory List-based)
  └── BytePipe  (binary data)
```

`FGInputStream` supports multi-subscriber distribution via `Payload`. Variants: `FGBlockingInputStream`, `FGSharableInputStream`.

### Context Hierarchy

`Context` → `AgentContext` → `FlowContext` → `ModuleContext`

Contexts pass state through the pipeline using `CONTEXT_KEY` enum values and support dot-notation path evaluation for nested Map/List access. Expressions like `{KEY.OUTPUT}` are resolved at runtime.

### Metadata Tables

All pipeline behavior is driven by four metadata tables configured via `FloodgateConstants`:

| Config Key | Purpose |
|------------|---------|
| `meta.source.tableForAPI` | API definitions (targets, concurrency settings) |
| `meta.source.tableForFlow` | Flow definitions (modules, rules, entry points) |
| `meta.source.tableForDatasource` | Connector configurations |
| `meta.source.tableForTemplate` | Document templates for SQL/protocol generation |

Metadata storage backends implement `FDataSource`: `FDataSourceDB` (JDBC), `FDataSourceFile` (filesystem), `FDataSourceDefault` (in-memory).

### MappingRule Actions

Rules in `MappingRuleItem` support these actions: `system` (DB parameter placeholder), `reference` (column reference), `literal` (value with `$KEY$` substitution), `function` (FunctionProcessor evaluation), `order` (positional index).

### Key Configuration Keys

| Key | Purpose |
|-----|---------|
| `channel.meta.datasource` | Datasource for metadata storage |
| `channel.log.datasource` | Datasource for logging |
| `channel.spooling.folder` | Spool directory for async jobs |
| `channel.payload.folder` | Payload backup directory |

### Initialization

Host applications must call `Floodgate.init()` to set up all singleton managers before using the framework. The main entry point for processing is `ChannelAgent.process()`.

## Package Map

| Package | Key Classes |
|---------|-------------|
| `floodgate` | `Floodgate`, `ConfigurationManager`, `FloodgateConstants` |
| `agent` | `ChannelAgent`, `ChannelJob`, `Context`, `AgentContext` |
| `agent.connector` | `ConnectorFactory`, `ConnectorDB`, `ConnectorFile`, `ConnectorFTP`, `ConnectorSFTP` |
| `agent.flow` | `Flow`, `FlowContext`, `FlowTag` |
| `agent.flow.module` | `Module`, `ModuleContext` |
| `agent.flow.rule` | `MappingRule`, `MappingRuleItem`, `FunctionProcessor` |
| `agent.flow.stream` | `FGInputStream`, `Payload`, `FGBlockingInputStream` |
| `agent.flow.stream.carrier.pipe` | `JSONPipe`, `ListPipe`, `BytePipe` |
| `agent.handler` | `FloodgateHandlerManager`, `FloodgateAbstractHandler` |
| `agent.meta` | `MetaManager`, `MetaTable` |
| `agent.template` | `DocumentTemplate` |
| `system.datasource` | `FDataSource`, `FDataSourceDB`, `FDataSourceFile` |
| `system.utils` | `PropertyMap`, `DBUtils`, `HttpUtils` |
