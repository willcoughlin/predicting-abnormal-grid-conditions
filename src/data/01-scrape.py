"""This script retrieves all forecast and historical power system status reports from ISO-NE."""

import re
from itertools import groupby
from pathlib import Path
from io import StringIO

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
from bs4 import BeautifulSoup


base_url = "https://www.iso-ne.com"
root_project_dir = Path(__file__).resolve().parent.parent.parent
base_download_dir = f"{root_project_dir}/data/raw"

retry_strategy = Retry(total = 4, status_forcelist=[429, 500, 502, 503, 504])
adapter = HTTPAdapter(max_retries=retry_strategy)

def get_with_retry(url: str) -> requests.Response:
  session = requests.Session()
  session.mount("http://", adapter)
  session.mount("https://", adapter)
  return session.get(url)


def scrape_forecast_data():
  def get_forecast_csv_urls() -> list[str]:
    """Retrieves all forecast CSVs from web page"""
    
    url = f"{base_url}/markets-operations/system-forecast-status/seven-day-capacity-forecast-list?start=20170101&end=20231231"
    
    # page = requests.get(url)
    page = get_with_retry(url)
    soup = BeautifulSoup(page.content, "html.parser")
    table_el = soup.find(id="sdf-archived-list")
    table_cells = table_el.find_all("td", title="Click to download CSV of this report")
    urls = [cell.find("a").get("href") for cell in table_cells]
    return urls

  def filter_forecast_csv_urls(urls: list[str]) -> dict[str, str]:
    """Filters list of reports so that only the latest reports for each day are included."""

    filtered_urls = {}

    report_date_pattern = r"^\/transform\/csv\/sdf\?start=(\d{8})"
    for report_date, report_group in groupby(urls, lambda url: re.search(report_date_pattern, url).group(1)):
      report_group_urls = list(report_group)
      if len(report_group_urls) < 2:
        filtered_urls[report_date] = f"{base_url}{report_group_urls[0]}"
      else:
        report_version_pattern = r"&version=(\d{17})$"
        versions_timestamps = list(map(lambda url: re.search(report_version_pattern, url).group(1), report_group_urls))
        latest_version_idx = versions_timestamps.index(max(versions_timestamps))
        latest_version_url = report_group_urls[latest_version_idx]
        filtered_urls[report_date] = f"{base_url}{latest_version_url}"

    return filtered_urls

  def download_forecast_csvs(urls_by_date: dict[str, str]):
    """Retrieves report files from the web."""

    download_dir = f"{base_download_dir}/forecasts"
    Path(download_dir).mkdir(exist_ok=True, parents=True)
    
    total_count = len(urls_by_date)
    i = 1

    for date in urls_by_date:
      file_path = f"{download_dir}/{date}.txt"
      # file_response = requests.get(urls_by_date[date])
      file_response = requests.get(urls_by_date[date])
      with open(file_path, "wb") as f:
        f.write(file_response.content)
      
      print(f"Progress: {i}/{total_count}", end="\r" if i < total_count else "\n")
      i += 1

  print("Scraping forecast data...")
  print("Getting content from web page...")
  csv_urls = get_forecast_csv_urls()

  print("Extracting data file URLs...")
  filtered_csv_urls = filter_forecast_csv_urls(csv_urls)

  print("Downloading data files...")
  download_forecast_csvs(filtered_csv_urls)
  print("Done scraping forecast data.")

def scrape_system_status_data():
  def get_statuses_for_year(year: int) -> pd.DataFrame:
    url = f"{base_url}/markets-operations/system-forecast-status/current-system-status/power-system-status-year?year={year}"
    page = requests.get(url)
    soup = BeautifulSoup(page.content, "html.parser")
    table_el = soup.find(id="PowerSystemConditions-Table")
    table_el.find("tfoot").clear()
    table_df = pd.read_html(StringIO(str(table_el)))[0]
    return table_df

  def download_status_csvs():
    download_dir = f"{base_download_dir}/statuses"
    Path(download_dir).mkdir(exist_ok=True, parents=True)

    i = 1
    for year in range(2017, 2024):
      file_path = f"{download_dir}/{year}.csv"
      status_data = get_statuses_for_year(year)
      status_data.to_csv(file_path, index=False)
      print(f"Progress: {i}/7", end="\r" if i < 7 else "\n")
      i += 1

  print("Scraping system status data...")
  print("Downloading content from web page...")
  download_status_csvs()
  print("Done scraping system status data.")

if __name__ == "__main__":
  scrape_forecast_data()
  scrape_system_status_data()
  
