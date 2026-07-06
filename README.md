# Artifacts for State Reproducibility in Digital Twin Services

---

This repository contains the relevant artifacts used to evaluate State Reproducibility in Digital Twin Services.

This repository is structured as follows:
```
./
├── digital_twin/                       # Source code of the DT
├── mosquitto/                          # Config file for mosquitto deployment
├── physical_twin/                      # Source code of the PT emulator
├── results/                            # Scripts to run experiments, generate graphs, and the CSV files used to generate the graphs
├── docker-compose.yaml                 # Docker compose file to deploy the cluster
├── LICENSE                             # License
├── README.md                           # This file
└── requirements.txt                    # Requirements to run experiment scripts
```

## Deploy the Services
To deploy the services, use Docker Compose:
```
docker compose -p cluster-test up
```

## Delete the Services
To remove the deployed services:
```
docker compose -p cluster-test down
```

## Run the Experiments:
To run the experiments, first check the ports on the host machine of the deployed Services with:
```
docker ps
```

Create the virtual environment, activate it and install the required depenedencies:
```
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

To run the script:
```
cd results
python3 experiments.py --experiment <choose 1,2,3> --rounds <rounds_no> --dt-port <dt_port> --pt-port <pt_port> --mongo-port <mongo_port> --mongo-user <mongo_user> --mongo-password <mongo_password> 
```