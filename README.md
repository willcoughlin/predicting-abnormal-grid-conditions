# Predicting Abnormal Grid Conditions

## Prerequisites

### [Optional] Create and activate virtual environment

Inside the project directory, create a new virtual environment with the name `env`.   
```sh 
python -m venv ./env
```

In your preferred shell, run the appropriate activate script. For example, Windows Powershell:
```
.\env\Scripts\Activate.ps1
```
   
### Install Packages

Install required dependencies from the `requirements.txt` file:
```sh
pip install -r ./requirements.txt
```

## [Optional] Make Dataset

The full dataset is included in this repository. If you wish to manually scrape and build it, these are the steps:

### Download Raw Data

1. Run the `01-scrape.py` script in `src/data`:
   ```sh
   python ./src/data/01-scrape.py
   ```
   This script may take around 10 minutes or longer, depending on your connection speed. This will output raw data files from ISO-NE to `data/raw`. You'll see the following output as the script progresses:

   ```
   Scraping forecast data...
   Getting content from web page...
   Extracting data file URLs...
   Downloading data files...
   Progress: 2555/2555
   Done scraping forecast data.
   Scraping system status data...
   Downloading content from web page...
   Progress: 7/7
   Done scraping system status data.
   ```

### Transform and Format Raw Data

2. Run the `01-transform.py` script in `src/features`:
   ```sh
   python ./src/features/01-transform.py
   ```
   
   Likewise, this script may take about 10 minutes or longer. The script will show its progress as it executes.

3. Run the `02-join.py` script in `src/features`:
   ```sh
   python ./src/features/02-join.py
   ```

4. Run the `03-clean.py` script in `src/features`:
     ```sh
   python ./src/features/03-clean.py
   ```

### Prepare Data for Modeling

1. Run the `04-modeling-prep.py` script in `src/features`:
     ```sh
   python ./src/features/04-modeling-prep.py

## Notebooks

EDA and modeling were performed in Jupyter Notebooks using the data prepared by the Python scripts. Important notebooks are listed below:

* `eda.ipynb` - Exploratory Data Analysis: correlation, clustering, etc.
* `logistic.ipynb` - Logistic Regression Model Building
* `svm.ipynb` - SVM Model Building
* `knn.ipynb` - KNN Model Building
* `ensemble.ipynb` - Ensemble Model Building
