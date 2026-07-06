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
import signal
import time

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

def graceful_shutdown(signum, frame):
    logger.info("Shutting down...")
    exit(0)

signal.signal(signal.SIGINT, graceful_shutdown)
signal.signal(signal.SIGTERM, graceful_shutdown)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
ODTE_THRESHOLD      : float = 0.9
ODTE_POLL_INTERVAL  : float = 0.5
ODTE_TIMEOUT        : float = 120.0

# event counts used in experiments 1 and 3
EVENT_COUNTS        : list  = [100, 200, 500, 1000, 2000, 5000, 10000]
UPDATES_PER_SECOND  : list  = [1, 5, 10, 20, 50, 100, 200]

# seconds to wait between rounds for the DT to stabilize
STABILIZATION_WAIT  : float = 3.0

EXPERIMENT2_EVENTS_NO = 500

EXCLUDED_FIELDS = ["recv_timestamp", "processing_time_s", "key"]

SORT_KEY = "commit_seq_no"

# ---------------------------------------------------------------------------
# MongoDB helpers
# ---------------------------------------------------------------------------

def _collection():
    return _mongo_client[MONGO_DB][MONGO_COLLECTION]

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
# PT endpoint helpers
# ---------------------------------------------------------------------------

def start_pt():
    resp = requests.post(f"{PT_URL}/start", timeout=10)
    if resp.status_code != 200:
        raise RuntimeError(f"Start failed: {resp.text}")
    logger.info("PT started successfully.")

def stop_pt():
    resp = requests.post(f"{PT_URL}/stop", timeout=10)
    if resp.status_code != 200:
        raise RuntimeError(f"Stop failed: {resp.text}")
    logger.info("PT stopped successfully.")

def set_message_limit(limit: int):
    resp = requests.post(
        f"{PT_URL}/message_limit",
        json={"limit": limit},
        timeout=5
    )
    if resp.status_code != 200:
        raise RuntimeError(f"Set messages limit failed: {resp.text}")
    logger.info(f"Message limit set to {limit}.")

def pt_set_updates_per_second(updates_per_second: int):
    resp = requests.post(
    f"{PT_URL}/updates_per_second",
        json={"updates_per_second": updates_per_second},
        timeout=5
    )
    if resp.status_code != 200:
        raise RuntimeError(f"Set updates per second failed: {resp.text}")
    logger.info(f"Updates per second set to {updates_per_second}.")

# ---------------------------------------------------------------------------
# DT state comparison helpers
# ---------------------------------------------------------------------------

def normalize_entry(entry: dict) -> dict:
    return {k: v for k, v in entry.items() if k not in EXCLUDED_FIELDS}

def normalize_buffer(buffer: list) -> list:
    return [normalize_entry(entry) for entry in buffer]

def check_list_equality(list1: list, list2: list, sortkey: str | None) -> tuple[bool, dict]:
    differences_dict = {}

    if sortkey:
        list1.sort(key=lambda x: x[sortkey])
        list2.sort(key=lambda x: x[sortkey])

    if len(list1) != len(list2):
        return False, differences_dict
    for i in range(len(list1)):
        if list1[i] != list2[i]:
            differences_dict[i] = {"list1_value": list1[i], "list2_value": list2[i]}
    if len(differences_dict) > 0:
        return False, differences_dict
    return True, differences_dict


def check_state_equality(state1: dict, state2: dict) -> tuple[bool, float, dict]:
    is_equal = True
    difference = 0.0
    differences_dict = {}
    vars_no = len(state1)
    different_vars = 0
    if state1["state_max_size"] != state2["state_max_size"]:
        is_equal = False
        different_vars += 1
        differences_dict["state_max_size"] = (state1["state_max_size"], state2["state_max_size"])
    if state1["connection_buffer_maxlen"] != state2["connection_buffer_maxlen"]:
        is_equal = False
        different_vars += 1
        differences_dict["connection_buffer_maxlen"] = (state1["connection_buffer_maxlen"], state2["connection_buffer_maxlen"])
    if state1["processing_buffer_maxlen"] != state2["processing_buffer_maxlen"]:
        is_equal = False
        different_vars += 1
        differences_dict["processing_buffer_maxlen"] = (state1["processing_buffer_maxlen"], state2["processing_buffer_maxlen"])
    if state1["conveyor_params"] != state2["conveyor_params"]:
        is_equal = False
        different_vars += 1
        differences_dict["conveyor_params"] = (state1["conveyor_params"], state2["conveyor_params"])
    ok, differences = check_list_equality(normalize_buffer(state1["connection_buffer"]), normalize_buffer(state2["connection_buffer"]), SORT_KEY)
    if not ok:
        is_equal = False
        different_vars += 1
        differences_dict["connection_buffer"] = differences
    ok, differences = check_list_equality(normalize_buffer(state1["processing_buffer"]), normalize_buffer(state2["processing_buffer"]), SORT_KEY)
    if not ok:
        is_equal = False
        different_vars += 1
        differences_dict["processing_buffer"] = differences

    difference = different_vars / vars_no if vars_no > 0 else 0
    return is_equal, difference, differences_dict

# ---------------------------------------------------------------------------
# DT endpoint helpers
# ---------------------------------------------------------------------------
def dt_set_updates_per_second(updates_per_second: int):
    resp = requests.post(
    f"{DT_URL}/updates_per_second",
        json={"updates_per_second": updates_per_second},
        timeout=5
    )
    if resp.status_code != 200:
        raise RuntimeError(f"Set updates per second failed: {resp.text}")
    logger.info(f"Updates per second set to {updates_per_second}.")

def get_dt_state() -> dict:
    resp = requests.get(f"{DT_URL}/state", timeout=10)
    if resp.status_code != 200:
        raise RuntimeError(f"DT state request failed: {resp.text}")
    logger.info("DT state acquired successfully.")
    return resp.json()["state"]

def restart_dt():
    resp = requests.post(f"{DT_URL}/restart", timeout=10)
    if resp.status_code != 200:
        raise RuntimeError(f"Restart failed: {resp.text}")
    logger.info("DT restarted successfully.")

def reset_logging_overhead_buffer():
    resp = requests.post(f"{DT_URL}/logging_overhead/reset", timeout=5)
    if resp.status_code != 200:
        raise RuntimeError(f"Reset logging overhead buffer failed: {resp.text}")
    logger.info("Logging overhead buffer reset.")

def reset_processing_overhead_buffer():
    resp = requests.post(f"{DT_URL}/processing_overhead/reset", timeout=5)
    if resp.status_code != 200:
        raise RuntimeError(f"Reset processing overhead buffer failed: {resp.text}")
    logger.info("Processing overhead buffer reset.")

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

def get_processing_overhead_stats() -> dict:
    resp = requests.get(f"{DT_URL}/processing_overhead", timeout=5)
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
        set_message_limit(n)  
        for r in range(rounds):
            logger.info(f"--- N={n}, round={r+1}/{rounds} ---")
            try:
                restart_dt()
                reconnect_dt()
                time.sleep(STABILIZATION_WAIT)
                start_pt()
                wait_for_n_events(n)
                disconnect_dt()
                time.sleep(STABILIZATION_WAIT)
                real_state = get_dt_state()
                stop_pt()
                restart_dt()
                time.sleep(STABILIZATION_WAIT)
                rebuild_time = trigger_rebuild()
                time.sleep(STABILIZATION_WAIT)
                rebuilt_state = get_dt_state()
                ok, _, _ = check_state_equality(real_state, rebuilt_state)
                if ok:
                    logger.info("DT rebuilt state is equal to previous state")
                else:
                    logger.error("DT rebuilt state is NOT equal to previous state")
                rows.append([n, r + 1, round(rebuild_time, 4), ok])
            except Exception as e:
                logger.error(f"Round failed: {e}")
                rows.append([n, r + 1, None, None])
            finally:
                time.sleep(STABILIZATION_WAIT)
                clear_mongo()
    write_csv(
        f"experiment1_rebuild_time{current_timestamp}.csv",
        ["n_events", "round", "rebuild_time_s", "equal"],
        rows
    )

# ---------------------------------------------------------------------------
# Experiment 2: Runtime overhead of logging
# ---------------------------------------------------------------------------

def experiment_2(rounds: int):
    """
    Collect per-event MongoDB write latency while the DT is running normally.
    Run this experiment at each desired event rate (change PT config manually).
    The script collects stats over the specified number of rounds, each round
    separated by a fixed interval to sample different time windows.
    """
    logger.info("=== Experiment 2: Runtime overhead of logging ===")
    rows = []
    set_message_limit(EXPERIMENT2_EVENTS_NO)
    for n in UPDATES_PER_SECOND:
        pt_set_updates_per_second(n)
        dt_set_updates_per_second(n)
        for r in range(rounds):
            logger.info(f"--- round={r+1}/{rounds} ---")
            try:
                restart_dt()
                reconnect_dt()
                time.sleep(STABILIZATION_WAIT)
                start_pt()
                wait_for_n_events(EXPERIMENT2_EVENTS_NO)
                stop_pt()
                logging_stats = get_logging_overhead_stats()
                if logging_stats["count"] == 0:
                    logger.warning("No logging overhead samples collected yet.")
                    continue
                rows.append([
                    r + 1,
                    n,
                    "logging",
                    round(logging_stats["average_s"] * 1000, 4),  # convert to ms
                    round(logging_stats["min_s"] * 1000, 4),
                    round(logging_stats["max_s"] * 1000, 4),
                    logging_stats["count"],
                    logging_stats["values"]
                ])
                logger.info(
                    f"  avg={logging_stats['average_s']*1000:.2f}ms "
                    f"min={logging_stats['min_s']*1000:.2f}ms "
                    f"max={logging_stats['max_s']*1000:.2f}ms "
                    f"count={logging_stats['count']}"
                )
                processing_stats = get_processing_overhead_stats()
                if processing_stats["count"] == 0:
                    logger.warning("No processing overhead samples collected yet.")
                    continue
                rows.append([
                    r + 1,
                    n,
                    "processing",
                    round(processing_stats["average_s"] * 1000, 4),  # convert to ms
                    round(processing_stats["min_s"] * 1000, 4),
                    round(processing_stats["max_s"] * 1000, 4),
                    processing_stats["count"],
                    processing_stats["values"]
                ])
                logger.info(
                    f"  avg={processing_stats['average_s']*1000:.2f}ms "
                    f"min={processing_stats['min_s']*1000:.2f}ms "
                    f"max={processing_stats['max_s']*1000:.2f}ms "
                    f"count={processing_stats['count']}"
                )
                disconnect_dt()
                reset_logging_overhead_buffer()
                reset_processing_overhead_buffer()
            except Exception as e:
                logger.error(f"Round failed: {e}")
                rows.append([r + 1, None, None, None, None, None])
            finally:
                time.sleep(STABILIZATION_WAIT)
                clear_mongo()  

    write_csv(
        f"experiment2_logging_overhead{current_timestamp}.csv",
        ["round", "updates_per_sec", "overhead_type", "avg_write_ms", "min_write_ms", "max_write_ms", "sample_count", "values"],
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
                set_message_limit(n)  
                restart_dt()
                reconnect_dt()
                time.sleep(STABILIZATION_WAIT)
                start_pt()
                wait_for_n_events(n)
                disconnect_dt()
                time.sleep(STABILIZATION_WAIT)
                real_state = get_dt_state()
                stop_pt()
                restart_dt()
                time.sleep(STABILIZATION_WAIT)
                rebuild_time = trigger_rebuild()
                time.sleep(STABILIZATION_WAIT)
                rebuilt_state = get_dt_state()
                ok, _, _ = check_state_equality(real_state, rebuilt_state)
                if ok:
                    logger.info("DT rebuilt state is equal to previous state")
                else:
                    logger.error("DT rebuilt state is NOT equal to previous state")
                reconnect_dt()
                set_message_limit(999999)
                time.sleep(STABILIZATION_WAIT)
                start_pt()
                recovery_time = wait_for_odte_recovery()
                stop_pt()
                disconnect_dt()
                rows.append([
                    n, r + 1,
                    round(rebuild_time, 4),
                    round(recovery_time, 4) if recovery_time != float("inf") else "timeout",
                    ok
                ])
            except Exception as e:
                logger.error(f"Round failed: {e}")
                rows.append([n, r + 1, None, None, None])
            finally:
                time.sleep(STABILIZATION_WAIT)
                clear_mongo()  
    write_csv(
        f"experiment3_odte_recovery_{current_timestamp}.csv",
        ["n_events", "round", "rebuild_time_s", "odte_recovery_time_s", "equal"],
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
    parser.add_argument(
        "--dt-port", type=int,
        help="Port to DT endpoints", required=True
    )
    parser.add_argument(
        "--pt-port", type=int,
        help="Port to PT endpoints", required=True
    )
    parser.add_argument(
        "--mongo-port", type=int,
        help="Port to MongoDB", required=True
    )
    parser.add_argument(
        "--mongo-user", type=str, 
        help="MongoDB username", required=True
    )
    parser.add_argument(
        "--mongo-password", type=str, 
        help="MongoDB password", required=True
    )
    args = parser.parse_args()


    dt_port         : int = args.dt_port
    pt_port         : int = args.pt_port
    mongo_port      : int = args.mongo_port
    mongo_user      : str = args.mongo_user
    mongo_password  : str = args.mongo_password


    DT_URL              : str   = f"http://localhost:{dt_port}"
    PT_URL              : str   = f"http://localhost:{pt_port}"
    MONGO_URL           : str   = f"mongodb://{mongo_user}:{mongo_password}@localhost:{mongo_port}"
    MONGO_DB            : str   = "dt"
    MONGO_COLLECTION    : str   = "events"


    _mongo_client = pymongo.MongoClient(MONGO_URL)

    current_timestamp = str(time.time()).replace(".", "_")

    if args.experiment == 1:
        experiment_1(args.rounds)
    elif args.experiment == 2:
        experiment_2(args.rounds)
    elif args.experiment == 3:
        experiment_3(args.rounds)