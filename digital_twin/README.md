# Digital twin

This component can be run with Python or Docker.

## Config file

Before running the Digital Twin, create the `config.yaml`.

The used configuration:

```yaml
name: "DT1_ABCDEF_d"

connections:
  mqtt:
    broker_url: "mqtt"
    port: 1883
    topics:
    - "test/#"
    - "control"
  mongodb:
    use: yes
    url: "mongodb://user:pass@mongo:27017"
    database: "dt"
    collection: "events"

  buffer:
    size: 100000

process:
  buffer:
    size: 100000
  burn:
    workers: 0
    work: 1000

state:
  max_size_mb: 1

flask:
  port: 5000

logger:
  level:
    shell: debug
    file: debug
    mongo: debug

random:
  seed: 10
```

> Make sure the configuration points to the correct MQTT broker and, if enabled, to the correct MongoDB instance.

## Python run

Install the required Python packages:

```bash
pip install -r requirements.txt
```

Run the component with:

```bash
python3 main.py
```

## Dockerize

The Dockerfile is provided as `Dockerfile`.
Build the image:

```bash
docker build -t digital-twin .
```

Run the container:

```bash
docker run --name dt -p <port_in_config>:<port_in_config> digital-twin
```

For example, if flask.port in config.yaml is set to 5000:

```bash
docker run --name dt -p 5000:5000 digital-twin
```