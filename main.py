from connections import MqttConnection
from process import Processing
import threading
import collections
import signal
from flask import Flask, jsonify, Response, request
import yaml
import logging
import time
import os
from prometheus_client import Gauge, generate_latest, CONTENT_TYPE_LATEST

# metrics
odte_metric = Gauge(
    "odte",
    "Overall Digital Twin Entanglement."
)

mqtt_active_ts = Gauge(
    "mqtt_active_ts",
    "Timestamp for MQTT connection. 0 if not set."
)

mqtt_inactive_ts = Gauge(
    "mqtt_inactive_ts",
    "Timestamp for MQTT connection. 0 if not set."
)

processing_active_ts = Gauge(
    "processing_active_ts",
    "Timestamp processing module started. 0 if not set."
)
rebuild_duration_time = Gauge(
    "rebuild_duration_time",
    "Time to complete the rebuild phase. 0 if not set."
)

mqtt_active_ts.set(0)
mqtt_inactive_ts.set(0)
processing_active_ts.set(0)
rebuild_duration_time.set(0)

# conf_path = "/app/dt/configs/config.yaml"
conf_path = "./config.yaml"
confs = yaml.safe_load(open(conf_path))
dt_name = confs["name"]
flask_port = int(confs["flask"]["port"])
process_conf = confs["process"]
process_buffer_conf = process_conf["buffer"]["size"]
process_burn_worker = process_conf["burn"]["workers"]
process_burn_work = process_conf["burn"]["work"]
process_do_periodic_checkpoints = process_conf["file_checkpoints"]["use"]
process_periodic_checkpoints_interval = None
process_periodic_checkpoints_file = None

if process_do_periodic_checkpoints:
    process_periodic_checkpoints_interval = process_conf["file_checkpoints"]["interval"]
    process_periodic_checkpoints_file = process_conf["file_checkpoints"]["path"]

connection_conf = confs["connections"]
connection_buffer_conf = connection_conf["buffer"]["size"]
connection_mqtt_conf = connection_conf["mqtt"]
connection_mongo_conf = connection_conf["mongodb"]
connection_mongo_url = connection_mongo_conf["url"]
connection_mongo_db = connection_mongo_conf["database"]
connection_mongo_collection = connection_mongo_conf["collection"]

state_max_size = confs["state"]["max_size_mb"]

logger_conf = confs["logger"]
level_conf = logger_conf["level"]
shell_level = level_conf["shell"]
file_level = level_conf["file"]
mongo_level = level_conf["mongo"]

random_seed = confs["random"]["seed"]

def setup_logging():
    fmt = logging.Formatter(
        fmt="%(asctime)s,%(msecs)03d %(name)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    root = logging.getLogger()
    root.setLevel(logging.DEBUG)

    sh = logging.StreamHandler()
    sh.setFormatter(fmt)
    sh.setLevel(getattr(logging, shell_level.upper(), logging.DEBUG))
    root.addHandler(sh)

    log_process = logging.getLogger("process")
    log_connections = logging.getLogger("connections")
    log_main = logging.getLogger("__main__")

    log_process.propagate = True
    log_connections.propagate = True
    log_main.propagate = True

    logging.getLogger("pymongo").setLevel(getattr(logging, mongo_level.upper(), logging.INFO))

setup_logging()
logger = logging.getLogger(__name__)

def graceful_shutdown(signum, frame):
    processing.stop()
    try:
        processing_t.join()
    except:
        pass

    mqtt_connection.stop()
    try:
        if mqtt_t:
            mqtt_t.join()
    except:
        pass

    logger.info(f"Processing stopped at: {time.time()}")
    exit(0)

signal.signal(signal.SIGINT, graceful_shutdown)
signal.signal(signal.SIGTERM, graceful_shutdown)


app = Flask(__name__)

processing_buffer = collections.deque(maxlen=process_buffer_conf)
connection_buffer = collections.deque(maxlen=connection_buffer_conf)
messages_buffer = collections.deque(maxlen=connection_buffer_conf)

mqtt_connection = MqttConnection(
    connection_buffer=connection_buffer,
    mqtt_conf=connection_mqtt_conf,
    mongo_url=connection_mongo_url,
    mongo_db=connection_mongo_db,
    mongo_collection=connection_mongo_collection,
    messages_buffer=messages_buffer,
    random_seed=random_seed
)
processing = Processing(
    connection_buffer=connection_buffer,
    processing_buffer=processing_buffer,
    state_max_size=state_max_size,
    worker=process_burn_worker,
    work=process_burn_work,
    mongo_url=connection_mongo_url,
    mongo_db=connection_mongo_db,
    mongo_collection=connection_mongo_collection,
    messages_buffer=messages_buffer,
)

startup_mqtt_connection = int(os.environ.get("STARTUP_MQTT_CONNECTION", 1))
mqtt_t: threading.Thread
processing_t: threading.Thread

@app.route("/rebuild", methods=["POST"])
def rebuild():
    rebuild_start_time = time.time()
    try:
        processing.rebuild()
    except:
        return jsonify({"message": "Error in rebuild process."}), 500

    rebuild_total_time = time.time() - rebuild_start_time
    rebuild_duration_time.set(rebuild_total_time)
    return jsonify({"message": f"Rebuild success. Total time: {rebuild_total_time}"})


@app.route("/state", methods=["GET"])
def get_state():
    dump_start_timestamp = time.time()
    state = processing.serialize_state()
    dump_end_timestamp = time.time()
    # logger.info(f"Dumping total time: {dump_end_timestamp - dump_start_timestamp}. Started at {dump_start_timestamp}, ended at {dump_end_timestamp}.")
    return jsonify({"state": state})


@app.route("/disconnect", methods=["POST"])
def disconnect():
    disconnection_time = time.time()
    logger.info(f"Disconnecting from mqtt. Time: {disconnection_time}")
    mqtt_inactive_ts.set(disconnection_time)
    try:
        mqtt_connection.stop()
        if mqtt_t:
            mqtt_t.join()
    except:
        return jsonify({"status": "error"})

    return jsonify({"status": "success"})


@app.route("/reconnect", methods=["POST"])
def reconnect():
    global mqtt_t, mqtt_connection
    try:
        if mqtt_t and mqtt_t.is_alive():
            mqtt_connection.stop()
            mqtt_t.join()
    except:
        pass

    mqtt_connection.reset()
    mqtt_t = threading.Thread(target=mqtt_connection.run)
    mqtt_t.start()
    reconnection_time = time.time()
    mqtt_active_ts.set(reconnection_time)
    logger.info(f"Reconnecting to mqtt. Time: {reconnection_time}")
    return jsonify({"status": "success"}), 200


@app.route("/healthd")
def health_check():
    return jsonify({"status": "healthy"}), 200


@app.route("/odte")
def odte():
    body = f"odte {float(processing.get_odte())}"
    return Response(
        body,
        mimetype="text/plain; version=0.0.4; charset=utf-8"
    )


@app.route("/metrics")
def get_metrics():
    global odte_metric
    odte_metric.set(processing.get_odte())
    return Response(generate_latest(), mimetype=CONTENT_TYPE_LATEST)

@app.route("/logging_overhead", methods=["GET"])
def get_logging_overhead():
    stats = mqtt_connection.get_logging_overhead_stats()
    return jsonify(stats)

@app.route("/logging_overhead/reset", methods=["POST"])
def reset_logging_overhead():
    mqtt_connection.reset_logging_overhead_buffer()
    return jsonify({"status": "success"})

@app.route("/restart", methods=["POST"])
def restart():
    global mqtt_t, processing_t, mqtt_connection, processing, processing_buffer, connection_buffer, messages_buffer
    mqtt_connection.stop()
    processing.stop()

    try:
        if mqtt_t and mqtt_t.is_alive():
            mqtt_t.join()
    except:
        pass

    try:
        if processing_t and processing_t.is_alive():
            processing_t.join()
    except:
        pass

    connection_buffer.clear()
    processing_buffer.clear()
    messages_buffer.clear()

    mqtt_connection = MqttConnection(
        connection_buffer=connection_buffer,
        mqtt_conf=connection_mqtt_conf,
        mongo_url=connection_mongo_url,
        mongo_db=connection_mongo_db,
        mongo_collection=connection_mongo_collection,
        messages_buffer=messages_buffer,
        random_seed=random_seed
    )
    processing = Processing(
        connection_buffer=connection_buffer,
        processing_buffer=processing_buffer,
        state_max_size=state_max_size,
        worker=process_burn_worker,
        work=process_burn_work,
        mongo_url=connection_mongo_url,
        mongo_db=connection_mongo_db,
        mongo_collection=connection_mongo_collection,
        messages_buffer=messages_buffer,
    )
    processing_t = threading.Thread(target=processing.run)

    processing_t.start()
    logger.debug("Processing restarted.")
    return jsonify({"status": "restart success with no mqtt connection. Call reconnect to start mqtt connection."})

@app.route("/updates_per_second", methods=["POST"])
def set_odte_updates_per_second():
    body = request.get_json()
    updates_per_second = int(body.get("updates_per_second"))
    processing.cfg.odte_expected_msg_sec = updates_per_second
    logger.info(f"Updates per second set to {updates_per_second}.")
    return jsonify({"status": "success", "updates_per_second": updates_per_second})


if __name__ == "__main__":
    logger.debug("Started main.")

    if startup_mqtt_connection != 0:
        mqtt_t = threading.Thread(target=mqtt_connection.run)
        mqtt_t.start()
        mqtt_connected_ts = time.time()
        mqtt_active_ts.set(mqtt_connected_ts)
        logger.info(f"Connected to mqtt. Time: {mqtt_connected_ts}")

    processing_t = threading.Thread(target=processing.run)
    processing_t.start()
    processing_start_ts = time.time()
    processing_active_ts.set(processing_start_ts)

    logger.info(f"Processing started at: {processing_start_ts}")

    app.run(host="0.0.0.0", port=flask_port)
