import requests
import time
import signal

rounds     :int   = 100
interval_s :float = 1.0

original_dt_endpoint : str = "http://localhost:5000/state"
replay_dt_endpoint   : str = "http://localhost:5001/state"

output_file = "state_comparison_results.csv"

excluded_fields = ["recv_timestamp", "processing_time_s"]

sort_key = "seq_id"

def graceful_exit(signum, frame):
    print("Exiting gracefully...")
    exit(0)

signal.signal(signal.SIGINT, graceful_exit)
signal.signal(signal.SIGTERM, graceful_exit)

def normalize_entry(entry: dict) -> dict:
    return {k: v for k, v in entry.items() if k not in excluded_fields}

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
    ok, differences = check_list_equality(normalize_buffer(state1["connection_buffer"]), normalize_buffer(state2["connection_buffer"]), sort_key)
    if not ok:
        is_equal = False
        different_vars += 1
        differences_dict["connection_buffer"] = differences
    ok, differences = check_list_equality(normalize_buffer(state1["processing_buffer"]), normalize_buffer(state2["processing_buffer"]), sort_key)
    if not ok:
        is_equal = False
        different_vars += 1
        differences_dict["processing_buffer"] = differences

    difference = different_vars / vars_no if vars_no > 0 else 0
    return is_equal, difference, differences_dict

i = 0
first_csv_line = "round,is_equal,difference\n"
lines = []

while i < rounds:
    try:
        original_dt_state = requests.get(original_dt_endpoint).json()["state"]
        replay_dt_state = requests.get(replay_dt_endpoint).json()["state"]
        # print(original_dt_state)
        # print(replay_dt_state)
        is_equal, difference, differences = check_state_equality(original_dt_state, replay_dt_state)
        if is_equal:
            print("States are equal.")
        else:
            print(f"States are NOT equal. Difference value: {difference}.\nDifferences: {differences}")

        line = f"{i},{is_equal},{difference}\n"
        lines.append(line)
    except Exception as e:
        print(f"Error while fetching states: {e}")


    i += 1
    time.sleep(interval_s)

with open(output_file, "a") as f:
    f.write(first_csv_line)
    f.writelines(lines)
