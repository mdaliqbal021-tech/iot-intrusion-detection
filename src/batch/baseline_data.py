"""Load the saved CICIoT2023 subsets used by the binary baseline."""

from pathlib import Path

import numpy as np
import pandas as pd
from pandas.api.types import is_numeric_dtype

SUBSET_DIR = Path(__file__).resolve().parents[2] / "data" / "subsets"
TRAINING_PATH = SUBSET_DIR / "ciciot2023_train_stratified_100000_seed42.csv"
VALIDATION_PATH = SUBSET_DIR / "ciciot2023_validation_stratified_25000_seed42.csv"
MODEL_PATH = SUBSET_DIR.parent / "models" / "binary_baseline.pkl"
ATTACK_THRESHOLD = 0.5

# CSV spellings of the 33 attack types listed by CIC and the dataset paper.
ATTACK_LABELS = {
    "DDoS-ACK_Fragmentation", "DDoS-HTTP_Flood", "DDoS-ICMP_Flood",
    "DDoS-ICMP_Fragmentation", "DDoS-PSHACK_Flood", "DDoS-RSTFINFlood",
    "DDoS-SYN_Flood", "DDoS-SlowLoris", "DDoS-SynonymousIP_Flood",
    "DDoS-TCP_Flood", "DDoS-UDP_Flood", "DDoS-UDP_Fragmentation",
    "DoS-HTTP_Flood", "DoS-SYN_Flood", "DoS-TCP_Flood", "DoS-UDP_Flood",
    "Recon-HostDiscovery", "Recon-OSScan", "Recon-PingSweep",
    "Recon-PortScan", "VulnerabilityScan",
    "Backdoor_Malware", "BrowserHijacking", "CommandInjection",
    "SqlInjection", "Uploading_Attack", "XSS", "DictionaryBruteForce",
    "DNS_Spoofing", "MITM-ArpSpoofing",
    "Mirai-greeth_flood", "Mirai-greip_flood", "Mirai-udpplain",
}

LABEL_MAP = {"BenignTraffic": 0, **dict.fromkeys(ATTACK_LABELS, 1)}


def load_features(path):
    frame = pd.read_csv(path, low_memory=False, skip_blank_lines=False, dtype={"label": "string"})
    features = frame.drop(columns="label")
    if features.shape[1] != 46:
        raise ValueError(f"{path.name}: expected 46 feature columns, found {features.shape[1]}.")
    if not all(map(is_numeric_dtype, features.dtypes)):
        raise ValueError(f"{path.name}: nonnumeric feature columns.")
    if not np.isfinite(features.to_numpy()).all():
        raise ValueError(f"{path.name}: nonfinite feature values.")
    return features, frame["label"]


def binary_targets(labels, split):
    missing = labels.isna() | labels.str.strip().eq("").fillna(False)
    unknown = sorted(labels[~missing & ~labels.isin(LABEL_MAP)].unique())
    if missing.any() or unknown:
        raise ValueError(f"{split} labels: {missing.sum():,} missing; unknown labels: {unknown!r}.")
    return labels.map(LABEL_MAP).astype(int)


def select_model_features(training):
    dropped = ["ece_flag_number", "cwr_flag_number", "Telnet", "SMTP", "IRC", "DHCP", "Srate"]
    if training["IPv"].eq(training["LLC"]).all():
        dropped.append("LLC")
    return training.columns.drop(dropped).tolist()


def validation_overlap(training, validation):
    """Match exact tuples across all original features in training column order."""
    if not validation.columns.equals(training.columns):
        raise ValueError("Validation feature columns differ from the training schema.")

    training_patterns = set(training.itertuples(index=False, name=None))
    shared = set(validation.itertuples(index=False, name=None)) & training_patterns
    matches = np.fromiter(
        (row in shared for row in validation.itertuples(index=False, name=None)),
        dtype=bool,
        count=len(validation),
    )
    return len(shared), matches
