from __future__ import annotations
from dataclasses import dataclass, asdict
from typing import Any, Dict, List, Optional
import pandas as pd
import json
from pathlib import Path

@dataclass
class DatasetProfile:
    filename: str
    format: str
    shape: Dict[str, int]
    columns: List[Dict[str, Any]]
    sample_rows: List[Dict[str, Any]]
    null_rates: Dict[str, float]

def profile_file(file_path: Path, max_rows: int = 200) -> DatasetProfile:
    suffix = file_path.suffix.lower()

    if suffix in [".csv"]:
        df = pd.read_csv(file_path)
        fmt = "csv"
    elif suffix in [".json"]:
        # supports JSON array or JSONL
        try:
            df = pd.read_json(file_path)
        except ValueError:
            df = pd.read_json(file_path, lines=True)
        fmt = "json"
    else:
        raise ValueError(f"Unsupported file type: {suffix}. Use CSV or JSON for now.")

    df_small = df.head(max_rows)

    cols = []
    for c in df.columns:
        cols.append({
            "name": str(c),
            "dtype": str(df[c].dtype),
            "nullable": bool(df[c].isna().any()),
        })

    null_rates = {str(c): float(df_small[c].isna().mean()) for c in df_small.columns}

    sample_rows = df_small.fillna("").to_dict(orient="records")[:20]

    return DatasetProfile(
        filename=file_path.name,
        format=fmt,
        shape={"rows": int(df.shape[0]), "cols": int(df.shape[1])},
        columns=cols,
        sample_rows=sample_rows,
        null_rates=null_rates,
    )

def profile_to_text(profile: DatasetProfile) -> str:
    return json.dumps(asdict(profile), indent=2)