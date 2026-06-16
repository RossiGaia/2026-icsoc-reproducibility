"""
Experiment runner for DT State Reproducibility paper.

Experiments:
  1. Rebuild time vs number of events
  2. Runtime overhead of logging (per-event MongoDB write latency)
  3. ODTE recovery time after rebuild

Usage:
  python experiment_runner.py --experiment 1 --rounds 10
  python experiment_runner.py --experiment 2 --rounds 10
  python experiment_runner.py --experiment 3 --rounds 10

Setup for each experiment:
  - MongoDB must be running and accessible
  - DT must be running with STARTUP_MQTT_CONNECTION=0
  - PT must be running and publishing events
  - For experiment 2: run at each desired event rate manually
"""

import argparse
import csv
import logging
import time
import requests
import pymongo

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

DT_URL              : str   = "http://localhost:5000"
MONGO_URL           : str   = "mongodb://user:pass@localhost:27017"
MONGO_DB            : str   = "dt"
MONGO_COLLECTION    : str   = "events"

ODTE_THRESHOLD      : float = 0.9
ODTE_POLL_INTERVAL  : float = 0.5
ODTE_TIMEOUT        : float = 120.0

# event counts used in experiments 1 and 3
EVENT_COUNTS        : list  = [100, 200, 500, 1000, 2000]

# seconds to wait between rounds for the DT to stabilize
STABILIZATION_WAIT  : float = 3.0

# ---------------------------------------------------------------------------
# MongoDB helpers
# ---------------------------------------------------------------------------

def _collection():
    client = pymongo.MongoClient(MONGO_URL)
    return client[MONGO_DB][MONGO_COLLECTION]

def count_events() -> int:
    return _collection().count_documents({})

def clear_mongo():
    _collection().delete_many({})
    logger.info("MongoDB cleared.")

def wait_for_n_events(n: int, poll_interval: float = 1.0):
    logger.info(f"Waiting for {n} events in MongoDB...")
    while True:
        count = count_events()
        logger.info(f"  {count}/{n} events accumulated.")
        if count >= n:
            break
        time.sleep(poll_interval)

# ---------------------------------------------------------------------------
# DT endpoint helpers
# ---------------------------------------------------------------------------
def set_mongo_rebuild_size(size: int):
    resp = requests.post(
        f"{DT_URL}/mongo_rebuild_size",
        json={"mongo_messages_no": size},
        timeout=5
    )
    if resp.status_code != 200:
        raise RuntimeError(f"Set mongo rebuild size failed: {resp.text}")
    logger.info(f"Mongo rebuild size set to {size}.")

def reset_overhead_buffer():
    resp = requests.post(f"{DT_URL}/logging_overhead/reset", timeout=5)
    if resp.status_code != 200:
        raise RuntimeError(f"Reset logging overhead buffer failed: {resp.text}")
    logger.info("Logging overhead buffer reset.")

def trigger_rebuild() -> float:
    logger.info("Triggering rebuild...")
    start = time.time()
    resp = requests.post(f"{DT_URL}/rebuild", timeout=600)
    elapsed = time.time() - start
    if resp.status_code != 200:
        raise RuntimeError(f"Rebuild failed: {resp.text}")
    logger.info(f"Rebuild completed in {elapsed:.3f}s")
    return elapsed

def disconnect_dt():
    resp = requests.post(f"{DT_URL}/disconnect", timeout=10)
    if resp.status_code != 200:
        raise RuntimeError(f"Disconnect failed: {resp.text}")
    logger.info("DT disconnected from PT.")

def reconnect_dt():
    resp = requests.post(f"{DT_URL}/reconnect", timeout=10)
    if resp.status_code != 200:
        raise RuntimeError(f"Reconnect failed: {resp.text}")
    logger.info("DT reconnected to PT.")

def get_odte() -> float:
    resp = requests.get(f"{DT_URL}/odte", timeout=5)
    return float(resp.text.strip().split()[-1])

def get_logging_overhead_stats() -> dict:
    resp = requests.get(f"{DT_URL}/logging_overhead", timeout=5)
    return resp.json()

def wait_for_odte_recovery() -> float:
    logger.info(f"Waiting for ODTE >= {ODTE_THRESHOLD}...")
    start = time.time()
    while True:
        elapsed = time.time() - start
        if elapsed > ODTE_TIMEOUT:
            logger.warning(f"ODTE recovery timed out after {ODTE_TIMEOUT}s.")
            return float("inf")
        odte = get_odte()
        logger.info(f"  ODTE = {odte:.3f} (elapsed {elapsed:.1f}s)")
        if odte >= ODTE_THRESHOLD:
            logger.info(f"ODTE recovered in {elapsed:.3f}s")
            return elapsed
        time.sleep(ODTE_POLL_INTERVAL)

def write_csv(path: str, header: list, rows: list):
    with open(path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(rows)
    logger.info(f"Results written to {path}")

# ---------------------------------------------------------------------------
# Experiment 1: Rebuild time vs number of events
# ---------------------------------------------------------------------------

def experiment_1(rounds: int):
    """
    For each event count N and each round:
      - Clear MongoDB
      - Connect DT to PT, accumulate N events, disconnect
      - Trigger rebuild, record duration
      - Reconnect DT
    """
    logger.info("=== Experiment 1: Rebuild time vs number of events ===")
    rows = []

    for n in EVENT_COUNTS:
        set_mongo_rebuild_size(n)  
        for r in range(rounds):
            logger.info(f"--- N={n}, round={r+1}/{rounds} ---")
            try:
                clear_mongo()
                reconnect_dt()
                wait_for_n_events(n)
                disconnect_dt()
                rebuild_time = trigger_rebuild()
                rows.append([n, r + 1, round(rebuild_time, 4)])
            except Exception as e:
                logger.error(f"Round failed: {e}")
                rows.append([n, r + 1, None])
            finally:
                time.sleep(STABILIZATION_WAIT)
    write_csv(
        "experiment1_rebuild_time.csv",
        ["n_events", "round", "rebuild_time_s"],
        rows
    )

# ---------------------------------------------------------------------------
# Experiment 2: Runtime overhead of logging
# ---------------------------------------------------------------------------

def experiment_2(rounds: int, n: int = 100):
    """
    Collect per-event MongoDB write latency while the DT is running normally.
    Run this experiment at each desired event rate (change PT config manually).
    The script collects stats over the specified number of rounds, each round
    separated by a fixed interval to sample different time windows.

    Record the event rate label manually via --rate argument.
    """
    logger.info("=== Experiment 2: Runtime overhead of logging ===")
    rows = []

    for r in range(rounds):
        logger.info(f"--- round={r+1}/{rounds} ---")
        try:
            clear_mongo()
            reconnect_dt()
            wait_for_n_events(n)
            stats = get_logging_overhead_stats()
            if stats["count"] == 0:
                logger.warning("No overhead samples collected yet.")
                continue
            rows.append([
                r + 1,
                round(stats["average_s"] * 1000, 4),  # convert to ms
                round(stats["min_s"] * 1000, 4),
                round(stats["max_s"] * 1000, 4),
                stats["count"],
                stats["values"]
            ])
            logger.info(
                f"  avg={stats['average_s']*1000:.2f}ms "
                f"min={stats['min_s']*1000:.2f}ms "
                f"max={stats['max_s']*1000:.2f}ms "
                f"count={stats['count']}"
            )
            disconnect_dt()
            reset_overhead_buffer()
        except Exception as e:
            logger.error(f"Round failed: {e}")
            rows.append([r + 1, None, None, None, None])

    write_csv(
        "experiment2_logging_overhead.csv",
        ["round", "avg_write_ms", "min_write_ms", "max_write_ms", "sample_count", "values"],
        rows
    )

# ---------------------------------------------------------------------------
# Experiment 3: ODTE recovery time after rebuild
# ---------------------------------------------------------------------------

def experiment_3(rounds: int):
    """
    For each event count N and each round:
      - Clear MongoDB
      - Connect DT to PT, accumulate N events, disconnect
      - Trigger rebuild
      - Reconnect DT, measure time until ODTE >= threshold
    """
    logger.info("=== Experiment 3: ODTE recovery time after rebuild ===")
    rows = []

    for n in EVENT_COUNTS:
        for r in range(rounds):
            logger.info(f"--- N={n}, round={r+1}/{rounds} ---")
            try:
                clear_mongo()
                reconnect_dt()
                wait_for_n_events(n)
                disconnect_dt()
                rebuild_time = trigger_rebuild()
                reconnect_dt()
                recovery_time = wait_for_odte_recovery()
                rows.append([
                    n, r + 1,
                    round(rebuild_time, 4),
                    round(recovery_time, 4) if recovery_time != float("inf") else "timeout"
                ])
            except Exception as e:
                logger.error(f"Round failed: {e}")
                rows.append([n, r + 1, None, None])
            finally:
                disconnect_dt()
                time.sleep(STABILIZATION_WAIT)

    write_csv(
        "experiment3_odte_recovery.csv",
        ["n_events", "round", "rebuild_time_s", "odte_recovery_time_s"],
        rows
    )

# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="DT State Reproducibility experiment runner."
    )
    parser.add_argument(
        "--experiment", type=int, required=True, choices=[1, 2, 3],
        help="Which experiment to run (1, 2, or 3)"
    )
    parser.add_argument(
        "--rounds", type=int, default=10,
        help="Number of repetitions per configuration (default: 10)"
    )
    args = parser.parse_args()

    if args.experiment == 1:
        experiment_1(args.rounds)
    elif args.experiment == 2:
        experiment_2(args.rounds)
    elif args.experiment == 3:
        experiment_3(args.rounds)