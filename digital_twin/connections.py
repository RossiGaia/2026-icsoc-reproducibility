import paho.mqtt.client as mqtt
import logging
import threading
import time
import json
import bson
import random
import string
import pymongo
import collections
import sys
from paho.mqtt.enums import CallbackAPIVersion

logger = logging.getLogger(__name__)

class MqttConnection:

    def __init__(
        self,
        *,
        connection_buffer: collections.deque,
        mqtt_conf,
        mongo_url: str,
        mongo_db: str,
        mongo_collection: str,
        messages_buffer: collections.deque,
        random_seed: int,
    ):
        self.rng = random.Random(random_seed)
        self.mqtt_loop_run = True
        self.mqtt_client: mqtt.Client

        self.connection_buffer = connection_buffer
        self.messages_buffer = messages_buffer
        self.logging_overhead_buffer = collections.deque(maxlen=10000)

        self.mqtt_broker_url = mqtt_conf["broker_url"]
        self.mqtt_port = mqtt_conf["port"]
        self.mqtt_topics = mqtt_conf["topics"]
        self.mongo_url = mongo_url
        self.mongo_db = mongo_db
        self.mongo_collection = mongo_collection
        self.mongo_client: pymongo.MongoClient
        self.determinant_padding_size_bytes = 0
        self.determinants_document_sizes_buffer = collections.deque(maxlen=10000)

        try:
            self.mongo_client = pymongo.MongoClient(self.mongo_url)
        except:
            logger.error("Could not connect to mongo.")
            sys.exit(1)

        # add index on commit_seq_no for faster queries
        self.mongo_client[self.mongo_db][self.mongo_collection].create_index(
            [("commit_seq_no", pymongo.ASCENDING)]
        )

        self.commit_seq_no = 0
        self.commit_seq_no_lock = threading.Lock()

    def new_client(self):
        self.mqtt_client = mqtt.Client(CallbackAPIVersion.VERSION2, client_id="DT-1", clean_session=False)
        self.mqtt_client.on_connect = self.on_connect
        self.mqtt_client.on_message = self.on_message

    def on_connect(self, client, userdata, flags, reason_code, properties):
        logger.info(f"connected to {self.mqtt_broker_url} at port {self.mqtt_port}.")
        for topic in self.mqtt_topics:
            logger.debug(f"subscribing to {topic}.")
            self.mqtt_client.subscribe(topic)

    def on_message(self, client, userdata, msg):
        with self.commit_seq_no_lock:
            commit_seq_no = self.commit_seq_no
            self.commit_seq_no += 1


        data = {
            "topic": msg.topic,
            "payload": json.loads(msg.payload.decode("UTF-8")),
            "recv_timestamp": time.time(),
            "key": "".join(
                self.rng.choices(string.ascii_uppercase + string.digits, k=10)
            ),
            "commit_seq_no": commit_seq_no,
        }

        if self.determinant_padding_size_bytes > 0:
            data["padding"] = self._compute_padding(data)
        self.determinants_document_sizes_buffer.append(len(bson.encode(data)))

        write_start_time = time.time()
        # pessimist logging
        try:
            self.mongo_client[self.mongo_db][self.mongo_collection].insert_one(data)
            write_duration_s = time.time() - write_start_time
            logger.debug(f"Determinant persisted for seq_id: {data['payload']['seq_id']}")
            logger.debug(f"Write duration: {write_duration_s} seconds")
            self.logging_overhead_buffer.append(write_duration_s)
            self.connection_buffer.append(data)
            self.messages_buffer.append(data)
        except Exception as e:
            logger.error(f"Failed to persist determinant: {e}. Event will not be processed.")
            return
        
    def _compute_padding(self, data: dict) -> str:
        current_size = len(bson.encode(data))
        padding_needed = self.determinant_padding_size_bytes - current_size
        if padding_needed <= 0:
            return ""
        return "0" * padding_needed


    def set_determinant_padding_size_bytes(self, size: int):
        self.determinant_padding_size_bytes = size
        logger.info(f"Determinant padding size set to {size}.")

    def run(self):
        self.new_client()
        logger.info(f"connecting to {self.mqtt_broker_url} at {self.mqtt_port}.")
        self.mqtt_client.connect(self.mqtt_broker_url, self.mqtt_port)
        self.mqtt_client.loop_start()

        while self.mqtt_loop_run:
            time.sleep(0.05)

        logger.debug("mqtt disconnection.")
        try:
            self.mqtt_client.disconnect()
        finally:
            self.mqtt_client.loop_stop()

        return

    def stop(self):
        self.mqtt_loop_run = False
        logger.info(f"stop requested. mqtt_loop_run = {self.mqtt_loop_run}")

    def get_logging_overhead_stats(self) -> dict:
        if len(self.logging_overhead_buffer) == 0:
            return {"average_s": None, "max_s": None, "min_s": None, "count": 0}
        values = list(self.logging_overhead_buffer)
        average = sum(values) / len(values)
        return {
            "average_s": average,
            "max_s": max(values),
            "min_s": min(values),
            "count": len(values),
            "values": values,
        }

    def reset_logging_overhead_buffer(self):
        self.logging_overhead_buffer.clear()

    def get_determinants_document_sizes_stats(self) -> dict:
        if len(self.determinants_document_sizes_buffer) == 0:
            return {"average_bytes": None, "max_bytes": None, "min_bytes": None, "count": 0}
        values = list(self.determinants_document_sizes_buffer)
        average = sum(values) / len(values)
        return {
            "average_bytes": average,
            "max_bytes": max(values),
            "min_bytes": min(values),
            "count": len(values),
            "values": values,
        }
    
    def reset_determinants_document_sizes_buffer(self):
        self.determinants_document_sizes_buffer.clear()

    def reset(self):
        with self.commit_seq_no_lock:
            self.commit_seq_no = 0
        self.mqtt_loop_run = True
        logger.info("MqttConnection reset.")