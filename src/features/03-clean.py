from pathlib import Path

import pandas as pd

root_project_dir = Path(__file__).resolve().parent.parent.parent
base_intermediate_dir = f"{root_project_dir}/data/intermediate"
base_processed_dir = f"{root_project_dir}/data/processed"

input_file_path = f"{base_intermediate_dir}/forecasts_and_statuses.csv"
output_file_path = f"{base_processed_dir}/full_all_day_ahead.csv"

if __name__ == "__main__":
  print("Reading intermediate files...")
  input_data = pd.read_csv(input_file_path)

  print("Cleaning...")
  # Remove junk cols. AREG_ has mostly NA values.
  selected_cols = [col for col in input_data.columns 
                 if not col.startswith(("Unnamed", "PWH_", "PWG_", "CWWH_", "CWWG_", "CWE_", "AREG_"))]
  output_data = input_data[selected_cols]

  # Trim date
  output_data = output_data[output_data["Date"] < "2024-01-01"]

  print("Writing data...")
  Path(base_processed_dir).mkdir(exist_ok=True, parents=True)
  output_data.to_csv(output_file_path, index=False)

  print("Done.")
  
