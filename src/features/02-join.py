from pathlib import Path

import pandas as pd

root_project_dir = Path(__file__).resolve().parent.parent.parent
base_intermediate_dir = f"{root_project_dir}/data/intermediate"
base_processed_dir = f"{root_project_dir}/data/processed"

forecasts_file_path = f"{base_intermediate_dir}/forecasts.csv"
statuses_file_path = f"{base_intermediate_dir}/statuses.csv"
output_file_path = f"{base_processed_dir}/forecasts_and_statuses.csv"

def format_statuses(status_df: pd.DataFrame) -> pd.DataFrame:
  status_map = {
    "M/LCC 2, Abnormal Conditions": "ACON", 
    "Min Gen, Min Gen Emergency": "MGEN",
    "OP4 Action 1, Power Caution": "OP41",
    "OP4 Action 2": "OP42",
    "OP4 Action 3": "OP43",
    "OP4 Action 4, Power Watch": "OP44", 
    "OP4 Action 5": "OP45",
  }

  status_df["Abnormal"] = True

  statuses = (status_df
              .replace(status_map)
              .pivot(index="Date", columns="Status", values="Abnormal")
              .fillna(False))
  statuses["Abnormal"] = True
  return statuses

if __name__ == "__main__":
  print("Reading intermediate files...")
  forecasts = pd.read_csv(forecasts_file_path)
  statuses = pd.read_csv(statuses_file_path)
  statuses = format_statuses(statuses)

  print("Joining data...")
  joined = forecasts.merge(statuses, on="Date", how="left")
  status_cols = ["ACON", "MGEN", "OP41", "OP42", "OP43", "OP44", "OP45", "Abnormal"]
  joined[status_cols] = joined[status_cols].fillna(False)

  print("Writing data...")
  Path(base_processed_dir).mkdir(exist_ok=True, parents=True)
  joined.to_csv(output_file_path)

  print("Done.")