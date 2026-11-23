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

## Measure end-to-end replay

The timed CLI publishes through the bundled Kafka client at a paced rate, waits for publication to finish, then starts a fresh Spark process. It records a Linux monotonic timestamp immediately before each Kafka send call. The Cassandra acknowledgement is shared by the rows in each micro-batch and recorded after the concurrent write helper returns successfully. Notebook 09 reports latency from send to this acknowledgement and throughput from first send to last acknowledgement. It measures Spark startup separately within that interval. Notebook 06 measures local model calls.

From Bash in the repo root, start the prepared stack and run a fresh benchmark. The following commands retain the existing saved results in `figures/`:

```bash
docker compose -f infra/compose.yaml build dashboard api replay-worker spark
docker compose -f infra/compose.yaml up -d --no-build --wait dashboard replay-worker
bench=$(mktemp -d)
set -o pipefail
bash infra/run_replay.sh --cassandra --smoke --timing-rate=200 | tee "$bench/smoke.log"
for trial in 1 2 3; do
  bash infra/run_replay.sh --cassandra --timing-rate=200 | tee "$bench/trial-$trial.log" || break
done
.venv/bin/python -m src.streaming.summarize_replay --output "$bench" \
  --smoke "$(sed -n 's/^Replay files: //p' "$bench/smoke.log")" \
  "$(sed -n 's/^Replay files: //p' "$bench/trial-1.log")" \
  "$(sed -n 's/^Replay files: //p' "$bench/trial-2.log")" \
  "$(sed -n 's/^Replay files: //p' "$bench/trial-3.log")"
```

The 1,000-row repetitions use the same first 1,000 held-out test records. Inspect the saved per-run comparison in [notebook 09](notebooks/09_streaming_performance.ipynb) or under **Model results → Measured streaming replay performance**.

## Start the prediction API and dashboard

From the repository root in WSL, build and start the dashboard and replay worker. The held-out CSV and deployed model files are mounted read-only. Spark's Kafka connector JARs are resolved during the image build and reused at replay time.

```sh
docker compose -f infra/compose.yaml up -d --build --wait dashboard replay-worker
```

Open [the dashboard](http://127.0.0.1:18501). **Choose a test record** defaults to a held-out benign example. Example names describe the recorded labels. To select another row, open **Browse all 25,000 test records**, enable **Use the record number below**, and enter a number from 1 to 25,000. The **Will replay** line shows which selection takes effect. Preview the flow and click **Start replay**.

Record numbers are one-based. The technical source-record ID is the zero-based row position paired with the SHA-256 of the fixed seed-42 CSV (`9a6b68f43ac6da53c56acd0833c65c93eaa56ab0ea150b12ea61d06d49866b12` for the evaluated subset). Each new replay has its own run ID and one within-run event with ID 0. Retries of the same submission reuse the run ID. **Completed run** opens saved results, including CLI runs. **Prediction history** shows recent Cassandra results. The model comparison uses the saved notebook artifacts in `figures/`.

For the viva, start **Example 1 · Benign traffic** in **Replay**, wait for **Completed**, then select **Explain prediction**. The bars show the largest signed contributions in raw log-odds from the model and exact input used by Spark. **Verify the score reconstruction** shows the bias, sum across all features, model margin, and score checked against Cassandra. Switch to **Example 2 · DDoS traffic** and start another replay to explain the attack example. To explain an older single-record run, select its test record and replay it to capture the explanation.

Open **Model results** to review the validation comparison from notebook 05, the full held-out test results from notebook 07, and the two-thread local CPU batch benchmark from notebook 06. This view loads the saved artifacts from `figures/`.

The equivalent API submission returns a queued run ID. Poll its replay status until it completes, then read its Cassandra result through the existing predictions endpoint.

```sh
curl -fsS localhost:18080/replays -H 'Content-Type: application/json' -d '{"source_record_id":132}'
curl -fsS "localhost:18080/replays/$run_id"
curl -fsS "localhost:18080/runs/$run_id/predictions"
```

Set `run_id` to the ID returned by the submission. API clients can send a UUIDv4 `request_id` and the catalog's `dataset_sha256` with the source ID for safe submission retries. **Technical details** supplies the exact read-only Cassandra command for the displayed prediction. For a selected single-record replay, the command is:

```sh
docker compose -f infra/compose.yaml exec -T cassandra cqlsh -e "SELECT run_id, replay_id, attack_score, predicted_class FROM iot_replay.predictions WHERE run_id = '$run_id' AND replay_id = 0;"
```

The batch CLI remains available. Its 32-row smoke fixture is a fixed sample used for score parity, separate from the 25,000-row test catalog:

```sh
bash infra/run_replay.sh --cassandra --smoke  # 32 fixed rows and an idempotency check
bash infra/run_replay.sh --cassandra          # first 1,000 rows of the saved test subset
bash infra/run_replay.sh --cassandra --full   # all 25,000 rows of the saved test subset
```

FastAPI and Streamlit listen on localhost. Set `IOT_API_PORT` or `IOT_DASHBOARD_PORT` to change the host ports. The dashboard uses `http://api:8000` unless `IOT_API_BASE_URL` overrides it. Its default limits are one CPU and 512 MiB, configurable with `IOT_DASHBOARD_CPUS` and `IOT_DASHBOARD_MEMORY`. Cassandra stays on the internal Compose network. Selected replay status, Kafka inputs, and worker logs are under `data/replay/selected/<run_id>/`.

```sh
docker compose -f infra/compose.yaml down
```
