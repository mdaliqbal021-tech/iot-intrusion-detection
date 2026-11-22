# CICIoT2023 intrusion detection

These notebooks inspect the CICIoT2023 dataset and audit stratified subsets of its existing train, validation, and test splits.

## Set up Python

From the repository root, use Python 3.12.

```sh
python3.12 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
```

On Windows, use `py -3.12 -m venv .venv` and activate with `.venv\Scripts\Activate.ps1` in PowerShell. Open the repository folder in a notebook editor such as VS Code with its Jupyter extension. Select the project's `.venv` kernel and open the notebooks in `notebooks/`. The notebooks support either the repository root or `notebooks/` as the kernel working directory.

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

## Notebook conventions

Introduce each analysis with a short explanation, show its code and saved outputs, then interpret the actual findings and limitations underneath. Mark any missing result as unverified.

## Run the binary baseline from the terminal

The batch training and validation modules and their shared helpers are in `Batch/`.

With the saved training and validation CSVs in `data/subsets/`, run these commands from the repository root using the project virtual environment.

```sh
source .venv/bin/activate
python -m Batch.train
python -m Batch.validate
```

Training writes the fitted scaler and logistic regression pipeline and its ordered feature list to `data/models/binary_baseline.pkl`. Validation loads that file and prints confusion matrix counts and classification metrics for both the always-attack reference and logistic regression at the 0.5 threshold. It reports results for the full validation subset and for rows with no exact match in training across the 46 original features.
