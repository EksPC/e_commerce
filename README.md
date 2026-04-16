# e-Commerce Backend

A Python-based e-commerce backend built on a microservices architecture implementing the **SAGA orchestration pattern** for distributed transactions. Originally developed for the Distributed Data Systems course at TU Delft and continuously improved.

---

## Table of Contents

- [Services](#services)
  - [Order Service](#order-service)
  - [Stock Service](#stock-service)
  - [Payment Service](#payment-service)
  - [Orchestrator Service](#orchestrator-service)
- [Infrastructure Components](#infrastructure-components)
- [Key Design Patterns](#key-design-patterns)
- [Database Schema](#database-schema)
- [API Reference](#api-reference)
- [Deployment](#deployment)
- [Testing](#testing)

---

## Services
Each service is split into three independent processes:
- **`{service}-service`** – The REST API (FastAPI served via Gunicorn + UvicornWorker)
- **`{service}-producer`** – The outbox relay: polls the DB outbox table and publishes events to Kafka
- **`{service}-consumer`** – Subscribes to Kafka topics and processes incoming commands/events

### Order Service

Manages the lifecycle of customer orders.

**Responsibilities:**
- Create orders for a given user
- Add items (with prices fetched from the Stock service) to an order
- Initiate checkout, which kicks off the SAGA
- Listen for saga completion results via Redis pub/sub and return the final HTTP response

**Key endpoints:**

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/create/{user_id}` | Create a new empty order for a user |
| `GET`  | `/find/{order_id}` | Retrieve order details |
| `POST` | `/addItem/{order_id}/{item_id}/{quantity}` | Add an item to an order |
| `POST` | `/checkout/{order_id}` | Trigger checkout (starts the SAGA) |
| `POST` | `/batch_init/{n}/{n_items}/{n_users}/{item_price}` | Bulk-create orders for load testing |

**Database tables:** `orders`, `outbox`, `log`

---

### Stock Service

Manages item inventory and handles stock reservation/release during sagas.

**Responsibilities:**
- Create stock items with a price and initial quantity
- Add or subtract stock
- Reserve stock atomically when commanded by the orchestrator (pessimistic locking)
- Release (roll back) reserved stock if a saga compensates

**Key endpoints:**

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/item/create/{price}` | Create a new item with given price |
| `GET`  | `/find/{item_id}` | Get current stock and price for an item |
| `POST` | `/add/{item_id}/{amount}` | Add stock to an item |
| `POST` | `/subtract/{item_id}/{amount}` | Subtract stock from an item |
| `GET`  | `/items` | List all items |
| `POST` | `/batch_init/{n}/{starting_stock}/{item_price}` | Bulk-create items for load testing |

**Database tables:** `item_snapshots`, `received_events`, `outbox`

---

### Payment Service

Manages user credit accounts and handles payment processing/refunds during sagas.

**Responsibilities:**
- Create user accounts with an initial credit balance
- Add credit to user accounts
- Deduct credit when commanded by the orchestrator (atomic, idempotent)
- Refund credit if a saga rolls back

**Key endpoints:**

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/create_user` | Create a new user with zero credit |
| `GET`  | `/find_user/{user_id}` | Get a user's current credit balance |
| `POST` | `/add_funds/{user_id}/{amount}` | Add credit to a user |
| `POST` | `/pay/{user_id}/{amount}` | Direct HTTP credit deduction |
| `GET`  | `/users` | List all users |
| `POST` | `/batch_init/{n}/{starting_money}` | Bulk-create users for load testing |

**Database tables:** `user_snapshots`, `received_events`, `outbox`

---

### Orchestrator Service

Coordinates the distributed checkout SAGA by reacting to integration events from all participant services.

**Responsibilities:**
- Create and manage saga state machines in the database
- Command the Stock service to reserve items
- Command the Payment service to charge the user (after stock is confirmed)
- Trigger compensating transactions (stock release, payment refund) on failure
- Notify the Order service of the final outcome (success or failure)
- Use optimistic concurrency control (versioned saga rows) to prevent race conditions

**Database tables:** `sagas`, `received_events`, `outbox`

---

## Infrastructure Components

| Component | Technology | Purpose |
|-----------|-----------|---------|
| API Gateway | Nginx 1.25 | Path-based routing to microservices |
| Message Broker | Apache Kafka (KRaft, no Zookeeper) | Async event transport between services |
| Cache / Pub-Sub | Redis 7 | Delivers saga results to waiting HTTP requests |
| Databases | CitusDB 13.0 (PostgreSQL) | One isolated database per service |
| Connection Pooler | PgBouncer | Reduces DB connection overhead for the Orchestrator |

---

## Key Design Patterns

### SAGA (Orchestration)
The Orchestrator service drives all cross-service transactions. It reacts to integration events from Stock and Payment, advances the saga state machine, and dispatches compensating commands when a step fails.

### Transactional Outbox
Rather than calling Kafka directly from within a database transaction (which would risk partial failures), each service writes outgoing events to a local `outbox` table **in the same database transaction** as the domain state change. A separate **producer** process polls this table and relays unsent rows to Kafka, marking them as sent only after receiving a broker acknowledgement.

### Idempotent Consumers
Each service maintains a `received_events` table. Before processing a command, the consumer inserts the event ID with an `ON CONFLICT DO NOTHING` guard. If the row already exists (duplicate delivery), the service re-sends the previously computed response without re-applying the side effect.

### Optimistic Concurrency Control
The Orchestrator's `sagas` table includes a `version` column. Every update increments `version` and checks the expected old version; a zero-rowcount result signals a concurrent modification and triggers a retry at the Kafka consumer level.

### Result Pattern
Shared service utilities return `Success[T] | Failure[E]` union types rather than raising exceptions, enabling explicit, type-safe error handling throughout business logic.

### Redis Pub/Sub for Saga Completion
When a client calls `POST /checkout/{order_id}`, the Order service:
1. Subscribes to a Redis channel `saga:{saga_id}` **before** writing the checkout event
2. Waits (with a 30-second timeout) for a message on that channel
3. The Order consumer forwards the Kafka `end_checkout` command to Redis, waking the waiting HTTP handler

---


## Deployment

### Docker Compose (local development)

```bash
docker compose up --build
```

The gateway will be available at `http://localhost:8000`.

Environment files (not committed) are expected under `env/`:
- `env/order_citus.env`, `env/stock_citus.env`, `env/payment_citus.env`, `env/orchestrator_citus.env`
- `env/kafka_settings.env`


## Testing

The `test/` directory contains:

| File | Purpose |
|------|---------|
| `locust_test.py` | Load / stress testing with [Locust](https://locust.io/) |
| `test_microservices.py` | Integration tests for the full checkout flow |
| `asyncio_test.py` | Async unit/integration tests |
| `utils.py` | Shared test helpers |

Install test dependencies:

```bash
pip install -r requirements.txt
```
