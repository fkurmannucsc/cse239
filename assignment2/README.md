# Distributed MapReduce Implementation

This code implements a distributed MapReduce system for word counting using multiple Docker containers communicating with RPyC (remote Python calls).

- **Map Phase**: The input text is split into chunks and distributed to workers via RPyC calls. Workers tokenizes and count words individually.
- **Shuffle Phase**: The coordinator groups intermediate results by word and partitions them for reduce workers using hash-based partitioning.
- **Reduce Phase**: Key/value data is sent to workers to sum counts per word.
- **Aggregation**: The coordinator combines all reduce results and sorts by frequency

## Architecture

- **Coordinator**: Manages MapReduce workflow, distributes tasks to workers, handles failures, and aggregates results.
- **Workers**: Execute Map and Reduce tasks remotely via RPyC.
- **Docker Networking**: Uses hostnames (worker-1, worker-2, worker-3) for service discovery.

## Files

- `coordinator.py`: Main coordinator that orchestrates MapReduce workflow.
- `worker.py`: Worker service that exposes Map and Reduce RPC methods.
- `Dockerfile`: Builds container image with Python and dependencies.
- `docker-compose.yml`: Orchestrates specified number of workers and 1 coordinator on a shared network.

## How to Run

### 1. Configure Workers

Edit the `.env` file to set the number of workers you want to run:

```
# .env
NUM_WORKERS=3
```

### 2. Build and Run

```bash
docker-compose up --build
```

This will start the coordinator and the number of worker containers specified in the `.env` file.

To use a different dataset:

- Edit `docker-compose.yml` and change the `DATASET_URL` environment variable:
or
- Run the coordinator and manually pass a URL
```bash
docker-compose run coordinator python coordinator.py https://example.com/data.zip
```

## Configuring Workers

Environment variables can be set in `docker-compose.yml`:

```yaml
environment:
  - TASK_TIMEOUT=20        # Timeout in seconds for task completion
```

## Clean Up

```bash
docker-compose down
```

To also remove volumes and downloaded files:

```bash
docker-compose down -v
rm -rf txt/ enwik*.zip
```