<div align="center">
  <img src="docs/assets/logo.png" alt="Mercury Logo" width="800" height="auto"/>
  <h1>Mercury</h1>
  <p><strong>Autonomous Trading Agent</strong></p>
</div>

---
 
 > *Named after the Roman god of commerce, financial gain, and messages — the swift messenger who bridges the gap between intelligence and action.*
 
Mercury is an autonomous cryptocurrency trading agent built using Python. It relies on a microservice architecture communicating exclusively over a NATS JetStream message broker. Mercury is designed to securely decouple the intelligence layer (which analyzes data and makes decisions) from the execution layer (which holds sensitive API keys and touches funds).

Mercury is the sister project to [Nephtys](https://github.com/AndreaBozzo/Nephtys) (the data pipeline connector). Nephtys handles connecting to Binance webhooks, formatting data into a normalized stream, and publishing it to NATS. Mercury subscribes to those streams.

## Architecture

Mercury is split into two completely isolated microservices:

1. **The Brain (Intelligence Layer)**
   - Listens to tick-by-tick `MarketEvent` streams published to `nephtys.stream.crypto.*` wrappers via JetStream durable subscriptions.
   - Evaluates a configurable rolling state strategy (e.g., Simple Moving Average logic inside `brain/strategies/moving_average.py`).
   - Generates and publishes normalized `TradeSignal` objects back to NATS, enriched with `confidence_score` and `allocation_percentage`.
   - Has **NO** knowledge of any exchange API keys.

2. **The Executor (Risk & Execution Layer)**
   - Subscribes to NATS `trade.signals` via a durable consumer (so it never misses a trade, even if it goes offline temporarily).
   - Passes every incoming signal through the `risk_manager.py` risk vault.
   - Uses persistent state managed natively inside a NATS Key-Value bucket (`mercury_wallet_state`) to strictly enforce maximum exposure vs a virtual balance (to avoid the "Amnesia" bug if restarted).
   - If a signal passes risk checks, executes the trade asynchronously against the Binance Sandbox via `ccxt.async_support`.

## Technologies Used

- **Python 3.11+**
- **uv** (Fast Python package and project manager)
- **NATS JetStream** (Message broker, durability, pub/sub, and KV store)
- **CCXT** (Cryptocurrency exchange trading library)
- **Pydantic** (Data validation and payload modeling)
- **Docker & Docker Compose**

## Setup and Running

Mercury requires `uv` to manage its dependencies and a running instance of NATS JetStream (which can be the one provided by Nephtys).

### Local Quickstart

1. **Install Dependencies**
   ```bash
   uv sync
   ```

2. **Configure Environment**
   Populate your `.env` file with your API keys:
   ```env
   BINANCE_API_KEY=your_sandbox_api_key
   BINANCE_SECRET_KEY=your_sandbox_secret_key
   NATS_URL=nats://localhost:4222
   ```

3. **Orchestration (Auto-provisioning Nephtys)**
   Ensure Nephtys is running. You can run the startup script to automatically register the required streams via its REST API:
   ```bash
   uv run python -m scripts.provision_nephtys
   ```

4. **Run Services (Terminal 1)**
   Start the Brain:
   ```bash
   uv run python -m brain.main
   ```

5. **Run Services (Terminal 2)**
   Start the Executor:
   ```bash
   uv run python -m executor.main
   ```

### Docker
Alternatively, you can run the entire cluster using Docker Compose:
```bash
docker-compose up -d
```
*(Note: Be sure your NATS network topology is configured correctly if you are sharing a NATS container between the Nephtys docker cluster and the Mercury docker cluster).*

## Data Contracts (Schemas)

All communication between Nephtys `->` Brain `->` Executor is strictly typed using Pydantic models defined in `core/schemas.py`.
