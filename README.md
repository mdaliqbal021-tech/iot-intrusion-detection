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

## Replay test rows through Kafka and Spark

With Python 3.10, Docker Compose, and the four XGBoost artifacts from notebook 07 in `data/models/`, run:

```sh
python3.10 -m venv --without-pip .venv/replay-310
bash infra/run_replay.sh
```

The replay code is in `src/streaming/`. Docker Compose, the Spark image definition, and the Cassandra schema are under `infra/`. The launcher assigns zero-based IDs in CSV row order, publishes the rows to a fresh Kafka topic, and runs a finite Spark Structured Streaming query in microbatches of at most 1,000 rows. Spark reads the exported model and metadata through read-only mounts. Predictions and checkpoints stay in a fresh Git-ignored directory under `data/replay/`.

Add `--cassandra` to store and verify the predictions in `iot_replay.predictions`. The comparison requires every ID exactly once, exact class agreement, and a maximum absolute score difference of `1e-7`. The smoke mode also compares with the saved 32-row reference and verifies a retry with the same run ID. Cassandra uses a dedicated volume and an internal Compose network. Its prediction primary key combines the run ID and replay row ID.

Stop and remove the local containers with `docker compose -f infra/compose.yaml down`. The Cassandra volume and Git-ignored replay files persist.

## Start the prediction API and dashboard

From the repository root in WSL, build and start the dashboard and replay worker. The held-out CSV and deployed model files are mounted read-only. Spark's Kafka connector JARs are resolved during the image build and reused at replay time.

```sh
docker compose -f infra/compose.yaml up -d --build --wait dashboard replay-worker
```

Open [the dashboard](http://127.0.0.1:18501), enter a **Test source record ID** from 0 to 24,999, preview the recorded label and features, then click **Start replay**. The source ID is the zero-based row position in the fixed seed-42 test subset. Every submission has its own run ID and one within-run event, with event ID 0. **Completed run** opens a saved result, including earlier CLI runs. The model comparison uses the saved notebook artifacts in `figures/`.

The equivalent API submission returns a queued run ID. Poll its replay status until it completes, then read its Cassandra result through the existing predictions endpoint.

```sh
curl -fsS localhost:18080/replays -H 'Content-Type: application/json' -d '{"source_record_id":132}'
curl -fsS "localhost:18080/replays/$run_id"
curl -fsS "localhost:18080/runs/$run_id/predictions"
```

Set `run_id` to the ID returned by the submission. The **Cassandra proof** panel supplies the exact read-only command for the displayed prediction. For a selected single-record replay, the command is:

```sh
docker compose -f infra/compose.yaml exec -T cassandra cqlsh -e "SELECT run_id, replay_id, attack_score, predicted_class FROM iot_replay.predictions WHERE run_id = '$run_id' AND replay_id = 0;"
```

The batch CLI remains available:

```sh
bash infra/run_replay.sh --cassandra --smoke  # 32 fixed rows and an idempotency check
bash infra/run_replay.sh --cassandra          # first 1,000 rows of the saved test subset
bash infra/run_replay.sh --cassandra --full   # all 25,000 rows of the saved test subset
```

FastAPI and Streamlit listen on localhost. Set `IOT_API_PORT` or `IOT_DASHBOARD_PORT` to change the host ports. The dashboard uses `http://api:8000` unless `IOT_API_BASE_URL` overrides it. Its default limits are one CPU and 512 MiB, configurable with `IOT_DASHBOARD_CPUS` and `IOT_DASHBOARD_MEMORY`. Cassandra stays on the internal Compose network. Selected replay status, Kafka inputs, and worker logs are under `data/replay/selected/<run_id>/`.

```sh
docker compose -f infra/compose.yaml down
```
