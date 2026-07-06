# Physical twin

This component can be run with Python or Docker.

## Config file

Before running the Physical Twin, create the `config.yaml`.

The used configuration configuration:

```yaml
sensors:
  updates_per_second: 5
  mqtt:
    broker_url: "mqtt"
    port: 1883
    topic: "test/cnv1"

web:
  port: 5001
```

> Make sure that the configuration file points to the correct MQTT broker instance.

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
docker build -t physical-twin .
```

Run the container:

```bash
docker run --name pt -p <port_in_config>:<port_in_config> physical-twin
```

For example, if flask.port in config.yaml is set to 5001:

```bash
docker run --name pt -p 5001:5001 physical-twin