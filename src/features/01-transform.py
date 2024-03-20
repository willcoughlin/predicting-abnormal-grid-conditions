"""This script transforms the downloaded reports from ISO-NE to intermediate datasets."""

import os
from io import StringIO
from pathlib import Path

import pandas as pd

root_project_dir = Path(__file__).resolve().parent.parent.parent
base_download_dir = f"{root_project_dir}/data/raw"
base_transformed_dir = f"{root_project_dir}/data/intermediate"

def transform_all_forecasts() -> pd.DataFrame:
  def format_7day_forecast_txt(file_path: str) -> pd.DataFrame:
    """Extracts CSV data from single forecast text file."""

    date_line_start = '"D","Date",'
    last_line_start = '"T"'

    data_lines = []

    skip_line = True
    with open(file_path) as f:
      for line in f:
        # Skip til start of data section
        if line.startswith(date_line_start):
          skip_line = False
        # Also skip if end of data section
        if skip_line or line.startswith(last_line_start):
          continue

        data_lines += [line]

    data_lines_joined = "".join(data_lines)
    df = pd.read_csv(StringIO(data_lines_joined))
    return df

  def transform_7day_forecast(df: pd.DataFrame) -> pd.DataFrame:
    """Transforms a single 7 day forecast from the downloaded report to wide format."""

    new_col_names = {
      "High Temperature - Boston": "HTB",
      "Dew Point - Boston": "DPB",
      "High Temperature - Hartford": "HTH",
      "Dew Point - Hartford": "DPH",
      "Total Capacity Supply Obligation (CSO)": "CSO",
      "Anticipated Cold Weather Outages": "ACWO",
      "Other Generation Outages": "OGO",
      "Anticipated De-List MW Offered": "ADMO",
      "Total Generation Available": "TGA",
      "Import at Time of Peak": "ITP",
      "Total Available Generation and Imports": "TAGI",
      "Projected Peak Load": "PPL",
      "Replacement Reserve Requirement": "RRR",
      "Required Reserve": "RR",
      "Required Reserve including Replacement": "RRIR", 
      "Total Load plus Required Reserve": "TLRR",
      "Projected Surplus/(Deficiency)": "PS",
      "Available Demand Response Resources": "ADRR",
      "Available Real-Time Emergency Generation": "AREG",
      "Power Watch": "PWH",
      "Power Warning": "PWG",
      "Cold Weather Watch": "CWWH",
      "Cold Weather Warning": "CWWG",
      "Cold Weather Event": "CWE"
    }

    df = (df.dropna()
          .drop(df.columns[0], axis=1)
          .set_index(df.columns[1])
          .transpose()
          .reset_index(names="Date")
          .rename_axis(None, axis=1)
          .rename(columns=new_col_names))
    
    # Parse date column as datetime object
    df["Date"] = pd.to_datetime(df["Date"])
    
    df_dict = df.to_dict()
    day_dfs = [pd.DataFrame({col if col == "Date" else f"{col}_{i+1}": [vals[i]] for col, vals in df_dict.items()})
              for i in range(len(df))]
    
    merged_df = day_dfs[0]
    for day_df in day_dfs[1:]:
      merged_df = merged_df.merge(day_df, on="Date", how="outer")

    return merged_df
  
  # Read all forecast files
  forecast_download_dir = f"{base_download_dir}/forecasts"
  file_paths = [f"{forecast_download_dir}/{file_name}" for file_name in os.listdir(forecast_download_dir)]
  # Transform first
  output = format_7day_forecast_txt(file_paths[0])
  output = transform_7day_forecast(output)
  output = output.set_index("Date")

  total_count = len(file_paths)
  print(f"Progress: 1/{total_count}", end="\r")
  i = 2

  # Transform and merge rest of files
  for file_path in file_paths[1:]:
    next_df = format_7day_forecast_txt(file_path)
    next_df = transform_7day_forecast(next_df)
    next_df = next_df.set_index("Date")
    output = pd.concat([output, next_df.iloc[-1:]]).combine_first(next_df)
    print(f"Progress: {i}/{total_count}", end="\r" if i < total_count else "\n")
    i += 1
    
  return output

def transform_historical_statuses() -> pd.DataFrame:
  """Reads all system status reports and outputs as a single dataframe indexed by date."""

  def transform_incident(row: pd.Series) -> dict[str, str]:
    start_date = row["Time In"].split(" ")[0]
    end_date = row["Time Out"].split(" ")[0]
    return {
      "Date": start_date,
      "Status": row["System Condition"],
      "StatusEnd": end_date
    }

  def transform_status_data(df: pd.DataFrame) -> pd.DataFrame:
    transformed = (df.apply(transform_incident, axis=1, result_type="expand")
                   .drop_duplicates()
                   .set_index("Date"))
    transformed.index = pd.to_datetime(transformed.index)
    return transformed

  status_download_dir = f"{base_download_dir}/statuses"
  file_paths = [f"{status_download_dir}/{file_name}" for file_name in os.listdir(status_download_dir) 
                if not file_name.startswith("2019")]  # No valid records reported for 2019

  # Transform first
  statuses = pd.read_csv(file_paths[0])
  statuses = transform_status_data(statuses)

  total_count = len(file_paths)
  print(f"Progress: 1/{total_count}", end="\r")
  i = 2

  for file_path in file_paths[1:]:
    next_df = pd.read_csv(file_path)
    next_df = transform_status_data(next_df)
    statuses = pd.concat([statuses, next_df])
    print(f"Progress: {i}/{total_count}", end="\r" if i < total_count else "\n")
    i += 1

  return statuses

if __name__ == "__main__":
  print("Transforming forecast data...")
  forecasts = transform_all_forecasts()
  
  print("Transforming historical statuses...")
  statuses = transform_historical_statuses()
  
  print("Writing transformed data...")
  Path(base_transformed_dir).mkdir(exist_ok=True)
  forecast_output_path = f"{base_transformed_dir}/forecasts.csv"
  forecasts.to_csv(forecast_output_path, index_label="Date")

  statuses_output_path = f"{base_transformed_dir}/statuses.csv"
  statuses.to_csv(statuses_output_path, index_label="Date")
  
  print("Done.")