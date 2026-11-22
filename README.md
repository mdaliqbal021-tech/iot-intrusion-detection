# CICIoT2023 intrusion detection

These notebooks inspect the CICIoT2023 dataset and audit stratified subsets of its existing train, validation, and test splits.

## Clone the project and set up Python

From a Linux or WSL shell, clone the branch containing the serving code. The example uses `dev`. Install Python 3.12, Python 3.10, Docker Compose, and `curl` before running the walkthrough.

If you received the source ZIP, extract it, open the extracted `iot-intrusion-detection/` directory, and start at `python3.12 -m venv .venv` below.

```sh
git clone --branch dev https://github.com/mdaliqbal021-tech/iot-intrusion-detection.git
cd iot-intrusion-detection
python3.12 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
python3.10 -m venv --without-pip .venv/replay-310
```

The Python 3.12 `.venv` runs notebooks and the local batch tools. The replay launcher uses `.venv/replay-310` for host-side replay checks. Docker supplies Python 3.10 to the Spark driver and workers and Python 3.12 to FastAPI and Streamlit. No host-installed Cassandra or Spark is required.

For interactive notebooks in VS Code, select the project's Python 3.12 `.venv` kernel. The repo root and `notebooks/` are both supported as kernel working directories.

## Get the dataset and run the notebooks

Both notebooks use version 1 of the [CICIoT2023 dataset on Kaggle](https://www.kaggle.com/datasets/himadri07/ciciot2023). The first dataset-loading cell downloads it through KaggleHub or reuses its local cache. If Kaggle prompts for authentication, follow the [KaggleHub authentication instructions](https://github.com/Kaggle/kagglehub#authenticate). Allow space for the roughly 2.2 GiB of extracted CSV files and the generated subsets.

To use an existing extraction, set `CICIOT2023_DATA_DIR` before launching your notebook editor. Point it to the directory containing `CICIOT23/`, which contains `train/train.csv`, `validation/validation.csv`, and `test/test.csv`.

```sh
export CICIOT2023_DATA_DIR="/path/to/extracted/dataset"
```

1. Run [`01_dataset_inspection.ipynb`](notebooks/01_dataset_inspection.ipynb) from top to bottom for dataset inspection.
2. Run [`02_eda_restart.ipynb`](notebooks/02_eda_restart.ipynb) from top to bottom to recreate the subsets. The **Stratified random subsets and class proportions** section uses seed 42 and writes 100,000 training rows, 25,000 validation rows, and 25,000 test rows to `data/subsets/`. The following cells report class proportions and audit the saved CSVs.

The sampling cell refuses to overwrite existing CSVs. To reuse them, skip the cell that writes the subsets. To regenerate them, move the three existing CSVs out of `data/subsets/` before running that cell.

The virtual environment and dataset files are local to each checkout.

### Regenerate the model files for serving

From the repo root, activate the Python 3.12 environment. Run notebook 02 on a fresh checkout to generate the three subset CSVs. If the subsets already exist, start with notebook 07.

```sh
source .venv/bin/activate
jupyter execute --timeout=600 notebooks/02_eda_restart.ipynb
jupyter execute --timeout=600 notebooks/07_final_test_evaluation.ipynb
```

Notebook 07 writes the Git-ignored export and metadata (`xgboost_final.ubj` and `xgboost_final_metadata.json`) and the 32-row check files (`xgboost_final_check_input.csv` and `xgboost_final_check_expected.csv`) to `data/models/`. The terminal commands leave the checked-in notebook outputs intact.

## Notebook conventions

Introduce each analysis with a short explanation, show its code and saved outputs, then interpret the actual findings and limitations underneath. Mark any missing result as unverified.

## Run the binary baseline from the terminal

The batch training and validation modules and their shared helpers are in `src/batch/`.

With the saved training and validation CSVs in `data/subsets/`, run these commands from the repository root using the project virtual environment.

```sh
source .venv/bin/activate
python -m src.batch.train
python -m src.batch.validate
```

Training writes the fitted scaler and logistic regression pipeline and its ordered feature list to `data/models/binary_baseline.pkl`. Validation loads that file and prints confusion matrix counts and classification metrics for both the always-attack reference and logistic regression at the 0.5 threshold. It reports results for the full validation subset and for rows with no exact match in training across the 46 original features.

## Check the exported XGBoost model locally

From the repo root, compare the batch prediction output with the reference from notebook 07.

```sh
.venv/bin/python -m src.batch.predict data/models/xgboost_final_check_input.csv \
  | diff - data/models/xgboost_final_check_expected.csv
```

## Replay the final XGBoost check through Kafka and Spark

With Python 3.10, Docker Compose, and the four XGBoost artifacts from notebook 07 in `data/models/`, run:

```sh
python3.10 -m venv --without-pip .venv/replay-310
bash infra/run_replay.sh
```

The replay code is in `src/streaming/`. Docker Compose, the Spark image definition, and the Cassandra schema are under `infra/`. The script assigns IDs 0–31 in CSV row order, publishes the rows to a fresh Kafka topic, and runs a finite Spark Structured Streaming query. Spark reads the exported model and metadata through read-only mounts. Predictions and checkpoints stay in a fresh Git-ignored directory under `data/replay/`. The comparison requires every ID exactly once, exact class agreement, and a maximum absolute score difference of `1e-7`.

Run `bash infra/run_replay.sh --cassandra` to also store the predictions in the `iot_replay.predictions` table. Cassandra uses a dedicated volume and an internal Compose network. The script queries the run after the initial write and after replaying the same topic with a fresh checkpoint and the same run ID. Both queries must return the same 32 matching predictions. The primary key combines the run ID and replay row ID.

Stop and remove the local containers with `docker compose -f infra/compose.yaml down`. The Cassandra volume and Git-ignored replay files persist.

## Start the prediction API and dashboard

With the exported artifacts in place, start the services and run the Cassandra replay from the repo root in Bash.

```sh
export IOT_API_PORT=18080 IOT_DASHBOARD_PORT=18501
docker compose -f infra/compose.yaml up -d --build --wait kafka cassandra api dashboard
curl -fsS "http://127.0.0.1:$IOT_API_PORT/health"

mkdir -p data/replay
set -o pipefail
bash infra/run_replay.sh --cassandra | tee data/replay/dashboard-smoke.log
RUN_ID=$(sed -n 's|^Replay files: .*/||p' data/replay/dashboard-smoke.log)
printf 'Dashboard: http://127.0.0.1:%s\nRun ID: %s\n' "$IOT_DASHBOARD_PORT" "$RUN_ID"
```

Open the printed dashboard URL, enter the printed run ID, and select **Load predictions**. The dashboard reads only from FastAPI over HTTP. Its Compose default is `http://api:8000`. Override it with `IOT_API_BASE_URL` if needed. The dashboard defaults to one CPU and 512 MB of memory. Set `IOT_DASHBOARD_CPUS` or `IOT_DASHBOARD_MEMORY` before starting Compose to change these limits.

To query FastAPI directly:

```sh
curl -fsS "http://127.0.0.1:$IOT_API_PORT/runs/$RUN_ID/predictions"
curl -sS -i "http://127.0.0.1:$IOT_API_PORT/runs/unknown-run/predictions"
```

FastAPI and Streamlit publish on localhost. Set `IOT_API_PORT` or `IOT_DASHBOARD_PORT` to a free port if needed. Cassandra stays on the internal Compose network. A healthy response is `{"status":"ok"}`. A known run returns its ID and predictions in `replay_id` order. An unknown run returns HTTP 404, and a Cassandra outage returns HTTP 503.

```sh
docker compose -f infra/compose.yaml down
```
