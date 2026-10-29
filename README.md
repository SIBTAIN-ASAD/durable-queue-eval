# Durable SQLite queue

A Python backend task: build a job queue that handles retries, worker crashes, leases, and concurrent requests.

Start with [the requirements](durable-queue/instruction.md). Implement `environment/workspace/queue.py` inside the task folder.

## Run

Install and start Docker Desktop. No local Python is needed.

```sh
docker compose run --build --rm oracle
docker compose run --build --rm nop
```

Run these from the repository folder. The first run downloads the Python image.

`oracle` tests the reference solution (expected score: 1). `nop` tests the empty starter (expected score: 0; failed tests are normal). Each command exits successfully only when its expected score is returned.

Reports are saved in `results/oracle/` and `results/nop/`. These commands run the verifier directly in Docker; they are not Harbor CLI runs. The task folder remains compatible with Harbor.

## Files

- `instruction.md`: requirements.
- `task.toml`: Harbor settings.
- `environment/`: agent container and starter.
- `tests/`: separate verifier container.
- `solution/`: reference implementation.

Tests and the solution are excluded from the solver container.
