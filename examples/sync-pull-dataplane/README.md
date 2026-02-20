# Sync PULL Dataplane Example

This example demonstrates a synchronous pull-based data transfer scenario using the dataplane-sdk. In this scenario, the provider creates an access token and endpoint, which the consumer uses to fetch data on-demand.

## Overview

The pull transfer model works as follows:
1. Consumer's control plane prepares the data flow on the consumer dataplane
2. Provider's control plane starts the data flow on the provider dataplane, which creates an access token
3. Provider returns a `DataAddress` containing the endpoint URL and access token
4. Consumer's control plane notifies the consumer dataplane with the `DataAddress`
5. Consumer application fetches data using the token for authentication

## Architecture

### Directory Structure

```
sync-pull-dataplane/
├── src/
│   ├── api/
│   │   ├── public.rs       # Public API for data access (requires token)
│   │   └── tokens.rs       # Token management API
│   ├── tokens/
│   │   ├── manager.rs      # Token creation and management
│   │   ├── model.rs        # Token data model
│   │   └── repo/           # Database implementations
│   │       ├── postgres.rs
│   │       └── sqlite.rs
│   ├── bin/
│   │   ├── dataplane.rs    # Dataplane server binary
│   │   └── scenario.rs     # Scenario simulator binary
│   ├── app_client.rs       # Client for fetching data
│   ├── config.rs           # Configuration parsing
│   ├── handler.rs          # DataFlow handler implementation
│   └── launcher.rs         # Server startup logic
├── migrations/             # SQL migrations for token table
├── provider.config.toml    # Provider dataplane configuration
├── consumer.config.toml    # Consumer dataplane configuration
└── scenario.config.toml    # Scenario simulator configuration
```

### Components

- **Provider Dataplane**: Handles `START` requests by creating access tokens and returning a `DataAddress` with the endpoint and token
- **Consumer Dataplane**: Handles `PREPARE` requests and `STARTED` notifications, storing the received token for data access
- **Scenario Simulator**: Simulates control plane operations to orchestrate the data flow between provider and consumer

### Database Backends

The example supports both SQLite and PostgreSQL:
- **SQLite**: In-memory or file-based, suitable for development and testing
- **PostgreSQL**: Production-ready persistent storage

## Prerequisites

- Rust toolchain (cargo)
- Docker (required for PostgreSQL backend)
- Available ports:
  - Provider: 3000 (signaling), 8789 (public API), 8790 (token API)
  - Consumer: 3001 (signaling), 8791 (public API), 8792 (token API)


## Running the Example

From the root of the project:

### 1. Start PostgreSQL (if using PostgreSQL backend, by default SQLite in memory) 

```bash
docker run -d --name dp-postgres \
  -e POSTGRES_USER=cp \
  -e POSTGRES_PASSWORD=controlplane \
  -e POSTGRES_DB=dp \
  -p 5432:5432 \
  postgres:16
```

### 2. Start the Provider Dataplane


```bash
cargo run --bin dataplane -- examples/sync-pull-dataplane/provider.config.toml
```

### 3. Start the Consumer Dataplane (in a new terminal)

```bash
cargo run --bin dataplane examples/sync-pull-dataplane/consumer.config.toml
```

### 4. Run the Scenario (in a new terminal)

```bash
cargo run --bin scenario examples/sync-pull-dataplane/scenario.config.toml
```

## API Endpoints

### Signaling API (Control Plane Interface)

The signaling API implements the Data Plane Signaling API for control plane communication.

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/v1/dataflows/prepare` | POST | Prepare a data flow (consumer side) |
| `/api/v1/dataflows/start` | POST | Start a data flow and create access token (provider side) |
| `/api/v1/dataflows/{processId}/started` | POST | Notify that data flow has started with DataAddress |

### Public API (Data Access)

The public API provides authenticated access to dataset data.

| Endpoint | Method | Auth | Description |
|----------|--------|------|-------------|
| `/datasets/{dataset_id}` | GET | Bearer token | Fetch dataset data |

### Token API (Token Management)

The token API provides internal token management capabilities.

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/tokens/{dataset_id}` | GET | List tokens for a dataset |

## Pull Scenario Flow


1. **Consumer Prepare**: Consumer's control plane sends a `PREPARE` message to the consumer dataplane, which creates a data flow record in `PREPARED` state

2. **Provider Start**: Provider's control plane sends a `START` message to the provider dataplane, which:
   - Creates a data flow record
   - Generates an access token
   - Returns a `DataAddress` containing:
     - `endpoint`: URL of the public API (e.g., `http://localhost:8789`)
     - `access_token`: Bearer token for authentication

3. **Consumer Started**: Consumer's control plane sends a `STARTED` notification to the consumer dataplane with the `DataAddress` received from the provider. The consumer dataplane extracts and stores the token.

4. **Data Fetch**: Consumer application retrieves the token and uses it to fetch data from the provider's public API endpoint.


## Handler Implementation

The `TokenHandler` implements the `DataFlowHandler` trait:

- **`on_prepare`**: Returns a response with `PREPARED` state (consumer side)
- **`on_start`**: Creates an access token, stores it, and returns a `DataAddress` (provider side)
- **`on_started`**: Extracts and stores the token from the received `DataAddress` (consumer side)


This implementation of a `DataFlowHandler`showcase how an handler can be implemented in order to be agnostic of the `TransactionalContext` and `Transaction`
which is managed by the `DataPlaneSdk`
