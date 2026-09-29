# VAULT — Distributed Object Storage

VAULT is a distributed object storage system where clients interact with a single logical object store while data is distributed and replicated across independent storage nodes.

The **Coordinator** manages metadata, replica placement, node health, failure detection, repair, reconciliation, and rebalancing. **Storage nodes** are responsible for storing and serving object data from their local disks.

## What it provides

* Distributed object storage
* Configurable replication
* Multiple independent storage nodes
* Persistent metadata with PostgreSQL
* SHA-256 object checksums
* Node heartbeats and failure detection
* Automatic replica repair
* Metadata/storage reconciliation
* Replica rebalancing when the cluster topology changes

---

## Architecture

VAULT is split into a **control plane** and a **data plane**.

```mermaid
flowchart TB

    CLIENT[Client]

    subgraph CONTROL["VAULT Control Plane"]

        COORD[Coordinator]

        META[(PostgreSQL<br/>Metadata)]

        MEMBERSHIP[Membership<br/>Heartbeats]

        PLACEMENT[Replica<br/>Placement]

        FAILURE[Failure<br/>Detection]

        REPAIR[Replica<br/>Repair]

    end

    subgraph DATA["VAULT Data Plane"]

        N1[Storage Node 1]

        N2[Storage Node 2]

        N3[Storage Node 3]

    end

    subgraph STORAGE["Persistent Storage"]

        D1[(Node 1 Disk)]

        D2[(Node 2 Disk)]

        D3[(Node 3 Disk)]

    end

    CLIENT -->|HTTP| COORD

    COORD --> MEMBERSHIP
    COORD --> PLACEMENT
    COORD --> FAILURE
    COORD --> REPAIR

    COORD <--> META

    MEMBERSHIP -.->|Heartbeat| N1
    MEMBERSHIP -.->|Heartbeat| N2
    MEMBERSHIP -.->|Heartbeat| N3

    PLACEMENT -->|Object Data| N1
    PLACEMENT -->|Object Data| N2

    N1 --> D1
    N2 --> D2
    N3 --> D3

    FAILURE --> REPAIR
    REPAIR -->|Recover Replica| N3
```

### Control Plane

The Coordinator handles cluster-level decisions:

* Storage-node membership and heartbeats
* Replica placement
* Object and replica metadata
* Failure detection
* Replica repair
* Reconciliation
* Rebalancing

PostgreSQL stores the persistent metadata used by the Coordinator.

### Data Plane

Storage nodes are independent services with their own persistent disks.

They are responsible for:

* Storing object bytes
* Reading objects
* Receiving replica transfers
* Verifying checksums
* Reporting health

The client does not need to know where an object's replicas are located.

---

## Object Write

A client sends an object to the Coordinator. The Coordinator determines the required replica destinations, stores the object on the selected nodes, and records the resulting replica metadata.

```mermaid
sequenceDiagram
    participant C as Client
    participant CO as Coordinator
    participant DB as PostgreSQL
    participant N1 as Storage Node 1
    participant N2 as Storage Node 2

    C->>CO: PUT /objects/{id}
    CO->>DB: Get replica placement
    DB-->>CO: Replica destinations
    CO->>N1: Store object
    CO->>N2: Store replica
    N1-->>CO: Checksum
    N2-->>CO: Checksum
    CO->>DB: Save object + replicas
    CO-->>C: Object metadata
```

Object bytes remain on storage nodes while PostgreSQL stores the metadata describing them.

---

## Object Read

The Coordinator looks up the object's replicas and retrieves the object from an available replica.

```mermaid
sequenceDiagram
    participant C as Client
    participant CO as Coordinator
    participant DB as PostgreSQL
    participant N as Storage Node

    C->>CO: GET /objects/{id}
    CO->>DB: Lookup replicas
    DB-->>CO: Replica locations
    CO->>N: Read object
    N-->>CO: Object + checksum
    CO-->>C: Object data
```

Multiple replicas allow the system to continue serving an object when another replica is unavailable.

---

## Failure Detection and Repair

Storage nodes periodically send heartbeats to the Coordinator. If a node stops reporting within the configured failure-detection window, the Coordinator can mark it unavailable and repair affected replicas.

```mermaid
flowchart TD
    HB["Storage Node Heartbeat"]
    CHECK{"Node healthy?"}
    FAIL["Mark node unavailable"]
    FIND["Find affected objects"]
    PLACE["Select replacement node"]
    COPY["Transfer replica"]
    VERIFY["Verify checksum"]
    META["Update replica metadata"]

    HB --> CHECK
    CHECK -->|Yes| HB
    CHECK -->|No| FAIL
    FAIL --> FIND
    FIND --> PLACE
    PLACE --> COPY
    COPY --> VERIFY
    VERIFY --> META
```

Repair is driven by the replica metadata maintained by the Coordinator.

---

## Reconciliation

Reconciliation compares the state expected by the Coordinator with the state that actually exists on storage nodes.

It can detect missing replicas, unexpected objects, and replica metadata inconsistencies, allowing the system to move back toward a consistent state.

```text
Coordinator metadata
        |
        v
Expected state
        |
        v
Compare with
        |
        v
Actual storage state
        |
        v
Repair inconsistencies
```

---

## Rebalancing

When the storage topology changes, the desired placement of replicas may change.

The Coordinator can move replicas toward the new placement without requiring clients to know about the change.

```mermaid
flowchart LR
    A["Topology changes"]
    B["Calculate desired placement"]
    C["Transfer replicas"]
    D["Verify data"]
    E["Remove unnecessary replicas"]
    F["Update metadata"]

    A --> B --> C --> D --> E --> F
```

---

## Running Locally

### Requirements

* Python 3.11+
* Docker Desktop
* Docker Compose
* Git

### Python environment

```cmd
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

Create the environment file:

```cmd
copy .env.example .env
```

Default local database configuration:

```env
DATABASE_URL=postgresql+psycopg://vault:vault@localhost:5433/vault
```

### Start the cluster

```cmd
docker compose build
docker compose up
```

Or:

```cmd
docker compose up -d
```

### Local services

| Service        | Port | Role          |
| -------------- | ---: | ------------- |
| Coordinator    | 8000 | Control plane |
| Storage Node 1 | 8100 | Data plane    |
| Storage Node 2 | 8101 | Data plane    |
| Storage Node 3 | 8102 | Data plane    |
| Storage Node 4 | 8103 | Data plane    |
| PostgreSQL     | 5433 | Metadata      |

Health endpoints:

```text
http://localhost:8000/health
http://localhost:8100/health
```

Run the test suite with:

```cmd
pytest -q
```

---

## Project Structure

```text
.
├── coordinator/
│   ├── api/
│   ├── domain/
│   ├── repositories/
│   └── services/
├── storage_node/
│   ├── api/
│   ├── domain/
│   └── services/
├── tests/
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
├── .env.example
└── README.md
```

The code separates API handling, domain logic, persistence, and cluster-management operations so that control-plane and storage responsibilities remain independent.

---

## API

The Coordinator exposes the client-facing object API and internal cluster-management endpoints.

```text
GET  /health
PUT  /objects/{object_id}
GET  /objects/{object_id}

GET  /internal/storage-nodes
```

Storage-node APIs are used internally for object operations, health checks, and replica transfers.

---

## Design Principles

**Coordinator decides, storage nodes store.**
Global placement and cluster state belong to the Coordinator. Storage nodes own their local data.

**Metadata and data are separate.**
PostgreSQL stores object and replica metadata; object bytes remain on storage nodes.

**Replicas are explicit.**
The Coordinator tracks which nodes contain each object's replicas.

**Checksums protect data.**
SHA-256 checksums are used to verify object integrity during storage and replica operations.

**Cluster maintenance stays behind the Coordinator.**
Failure repair, reconciliation, and rebalancing do not require clients to understand storage topology.

---

## Development Phases

VAULT was built incrementally in five phases:

1. **Storage Foundation** — Coordinator, PostgreSQL, storage nodes, object persistence, checksums and Docker setup.
2. **Placement & Replication** — Replica metadata, placement decisions and distributed object storage.
3. **Failure Detection & Repair** — Heartbeats, failure detection and automatic replica replacement.
4. **Reconciliation** — Detection and correction of metadata/storage inconsistencies.
5. **Rebalancing** — Redistribution of replicas when cluster topology changes.

The resulting lifecycle is:

**Store → Replicate → Detect Failure → Repair → Reconcile → Rebalance**

---

## Technology

**Python · FastAPI · PostgreSQL · Psycopg · Docker · Docker Compose · Pytest**
