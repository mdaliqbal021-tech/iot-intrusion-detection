# CICIoT2023 intrusion detection

These notebooks inspect the CICIoT2023 dataset and audit stratified subsets of its existing train, validation, and test splits.

## Set up Python

From the repository root, use Python 3.12.

```sh
python3.12 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
```

On Windows, use `py -3.12 -m venv .venv` and activate with `.venv\Scripts\Activate.ps1` in PowerShell. Open the repository folder in a notebook editor such as VS Code with its Jupyter extension. Select the project's `.venv` kernel and use the repository root as the notebook working directory.

## Get the dataset and run the notebooks

Both notebooks use version 1 of the [CICIoT2023 dataset on Kaggle](https://www.kaggle.com/datasets/himadri07/ciciot2023). The first dataset-loading cell downloads it through KaggleHub or reuses its local cache. If Kaggle prompts for authentication, follow the [KaggleHub authentication instructions](https://github.com/Kaggle/kagglehub#authenticate). Allow space for the roughly 2.2 GiB of extracted CSV files and the generated subsets.

To use an existing extraction, set `CICIOT2023_DATA_DIR` before launching your notebook editor. Point it to the directory containing `CICIOT23/`, which contains `train/train.csv`, `validation/validation.csv`, and `test/test.csv`.

```sh
export CICIOT2023_DATA_DIR="/path/to/extracted/dataset"
```

1. Run `01_dataset_inspection.ipynb` from top to bottom for dataset inspection.
2. Run `02_eda_restart.ipynb` from top to bottom to create and audit the three saved subsets in `data/subsets/`. If they already exist, skip the cell that writes the subsets and run the cells before and after it. The audit section can also run by itself once the three subset CSVs exist.

The virtual environment and dataset files are local to each checkout.
