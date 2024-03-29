from pathlib import Path

import pandas as pd
import numpy as np

from sklearn.model_selection import train_test_split
from imblearn.over_sampling import SMOTE

root_project_dir = Path(__file__).resolve().parent.parent.parent
base_processed_dir = f"{root_project_dir}/data/processed"
base_modeling_dir = f"{root_project_dir}/data/modeling"

input_file_path = f"{base_processed_dir}/full_all_day_ahead.csv"

output_1da_train_file_path = f"{base_modeling_dir}/data_1da_train.csv"
output_1da_test_file_path = f"{base_modeling_dir}/data_1da_test.csv"
output_reduced_train_file_path = f"{base_modeling_dir}/data_reduced_train.csv"
output_reduced_test_file_path = f"{base_modeling_dir}/data_reduced_test.csv"
output_expanded_train_file_path = f"{base_modeling_dir}/data_expanded_train.csv"
output_expanded_test_file_path = f"{base_modeling_dir}/data_expanded_test.csv"

if __name__ == "__main__":
  print("Reading processed file...")
  data_full = pd.read_csv(input_file_path)

  print("Preparing for modeling...")
  data_full = data_full.drop(columns=["RR_1", "Date"])  # No variance
  data_full = data_full.dropna()  # Drop first 5 rows that don't have full lagged sequence, plus 6 rows with missing values for 2023-09-17

  X_full = data_full.drop(columns="Abnormal")
  y = data_full.Abnormal

  # Split and oversample train data
  np.random.seed(500)
  X_full_train, X_full_test, y_train, y_test = train_test_split(X_full, y, train_size=0.7, stratify=y)
  smote = SMOTE()
  X_full_train, y_train = smote.fit_resample(X_full_train, y_train)

  # Base (1 day ahead)
  cols_1da = [c for c in data_full.columns if c.endswith("_1")]
  X_1da_train = X_full_train[cols_1da]
  X_1da_test = X_full_test[cols_1da]

  # Reduced (feature selected data)
  col_drop_subset = ["DPB_1", "DPH_1", "HTH_1", "TAGI_1", "RRIR_1", "TLRR_1", "OGO_1"]
  X_reduced_train = X_1da_train.drop(columns=col_drop_subset)
  X_reduced_test = X_1da_test.drop(columns=col_drop_subset)

  # Expanded (feature selected data for all days ahead)
  cols_expanded = [f"{col.split('_')[0]}_{i}" 
                  for col in X_reduced_train.columns 
                  for i in range(1, 7)]
  X_expanded_train = X_full_train[cols_expanded]
  X_expanded_test = X_full_test[cols_expanded]

  # Write files out
  df_1da_train = X_1da_train.assign(Abnormal=y_train)
  df_1da_test = X_1da_test.assign(Abnormal=y_test)

  df_reduced_train = X_reduced_train.assign(Abnormal=y_train)
  df_reduced_test = X_reduced_test.assign(Abnormal=y_test)

  df_expanded_train = X_expanded_train.assign(Abnormal=y_train)
  df_expanded_test = X_expanded_test.assign(Abnormal=y_test)

  print("Writing data...")
  Path(base_modeling_dir).mkdir(exist_ok=True, parents=True)

  df_1da_train.to_csv(output_1da_train_file_path, index=False)
  df_1da_test.to_csv(output_1da_test_file_path, index=False)

  df_reduced_train.to_csv(output_reduced_train_file_path, index=False)
  df_reduced_test.to_csv(output_reduced_test_file_path, index=False)

  df_expanded_train.to_csv(output_expanded_train_file_path, index=False)
  df_expanded_test.to_csv(output_expanded_test_file_path, index=False)

  print("Done.")