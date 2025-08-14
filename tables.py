import os
import time
import pandas as pd
import shutil
import subprocess
from dotenv import load_dotenv
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.edge.service import Service
from selenium.webdriver.edge.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from sqlalchemy import create_engine

# =========================================
# 1️⃣ Load DB credentials from .env
# =========================================
load_dotenv()
DB_USER = os.getenv('DB_USER', 'postgres')
DB_PASSWORD = os.getenv('DB_PASSWORD', '')
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_NAME = os.getenv('DB_NAME', 'sportdata')
DATABASE_URL = f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
engine = create_engine(DATABASE_URL)

# =========================================
# 2️⃣ Check Edge & EdgeDriver version match
# =========================================
def check_driver_version():
    edge_path = shutil.which("msedge") or r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe"
    try:
        edge_version = subprocess.check_output([edge_path, "--version"], text=True).strip()
    except Exception:
        edge_version = "Unknown"

    driver_path = shutil.which("msedgedriver")
    driver_version = "Unknown"
    if driver_path:
        try:
            driver_version = subprocess.check_output([driver_path, "--version"], text=True).strip()
        except Exception:
            pass

    print(f"Edge Version: {edge_version}")
    print(f"EdgeDriver Version: {driver_version}")
    if driver_version != "Unknown" and edge_version.split()[1].split('.')[0] != driver_version.split()[1].split('.')[0]:
        print("⚠️ Edge and EdgeDriver versions may not match. Download matching driver here: https://developer.microsoft.com/en-us/microsoft-edge/tools/webdriver/")

check_driver_version()

# =========================================
# 3️⃣ Set up Edge options (SSL ignore + stealth)
# =========================================
edge_driver_path = r'C:\Users\user\Downloads\edgedriver_win64\msedgedriver.exe'
options = Options()
options.use_chromium = True
options.add_argument('--ignore-certificate-errors')
options.add_argument('--ignore-ssl-errors')
options.add_argument("--disable-blink-features=AutomationControlled")
options.add_argument("--start-maximized")
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")

service = Service(executable_path=edge_driver_path)
driver = webdriver.Edge(service=service, options=options)

# =========================================
# 4️⃣ Scraper functions
# =========================================
def get_team_info():
    home_team, away_team, home_score, away_score = [], [], [], []
    matches = driver.find_elements(By.XPATH, '//div[contains(@class, "event__match")]')
    for match in matches:
        try:
            home = match.find_element(By.XPATH, './/div[contains(@class, "participant--home")]').text
            away = match.find_element(By.XPATH, './/div[contains(@class, "event__participant--away")]').text
            home_score_value = match.find_element(By.XPATH, './/div[contains(@class, "event__score--home")]').text
            away_score_value = match.find_element(By.XPATH, './/div[contains(@class, "event__score--away")]').text
            home_team.append(home)
            away_team.append(away)
            home_score.append(home_score_value)
            away_score.append(away_score_value)
        except Exception as e:
            print(f"Error extracting match data: {e}")
    return pd.DataFrame({
        'home_team': home_team,
        'away_team': away_team,
        'home_score': home_score,
        'away_score': away_score
    })

def get_tables():
    table_position, table_team = [], []
    tables = driver.find_elements(By.XPATH, '//div[contains(@class, "ui-table__body")]')
    for table in tables:
        try:
            position = table.find_elements(By.XPATH, './/div[contains(@class, "ui-table__row")]/div[contains(@class, "table__cell--rank")]')
            team_name = table.find_elements(By.XPATH, './/div[contains(@class, "ui-table__row")]/div[contains(@class, "table__cell--participant")]/a')
            table_position.extend([p.text for p in position])
            table_team.extend([t.text for t in team_name])
        except Exception as e:
            print(f"Error extracting table data: {e}")
    return pd.DataFrame({
        'position': table_position,
        'team_name': table_team
    })

# =========================================
# 5️⃣ League data structure
# =========================================
leagues = {
    "serbia": {
        "base_url": "https://www.flashscore.com/basketball/serbia/",
        "years": ['first-league/results/','first-league-2023-2024/results/','first-league-2022-2023/results/'],
        "tables": ['first-league-2023-2024/#/fPB68otI/table/overall','first-league-2022-2023/#/UgOzMRKQ/table/overall']
    },
    # Add other leagues here in same format...
}

# =========================================
# 6️⃣ Main scraping loop
# =========================================
start_time = time.time()
append_to_list = True
all_matches = []
all_tables = []

for league_name, league_data in leagues.items():
    base_url = league_data["base_url"]

    # Matches
    for year_path in league_data["years"]:
        try:
            driver.get(base_url + year_path)
            WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.XPATH, "//div[contains(@class, 'event__match')]")))
            df = get_team_info()
            df.to_sql(f"{league_name}_matches_{year_path.replace('/', '_')}", engine, index=False, if_exists='replace')
            if append_to_list:
                all_matches.append(df)
        except Exception as e:
            print(f"Error processing matches for {league_name} - {year_path}: {e}")

    # Tables
    for table_path in league_data["tables"]:
        try:
            driver.get(base_url + table_path)
            WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.XPATH, "//div[contains(@class, 'ui-table__body')]")))
            df_table = get_tables()
            df_table.to_sql(f"{league_name}_table_{table_path.replace('/', '_')}", engine, index=False, if_exists='replace')
            if append_to_list:
                all_tables.append(df_table)
        except Exception as e:
            print(f"Error processing table for {league_name} - {table_path}: {e}")

# Save combined tables
if append_to_list and all_matches:
    pd.concat(all_matches, ignore_index=True).to_sql('all_leagues_matches', engine, index=False, if_exists='replace')
if append_to_list and all_tables:
    pd.concat(all_tables, ignore_index=True).to_sql('all_leagues_tables', engine, index=False, if_exists='replace')

print(f"✅ Done in {time.time() - start_time:.2f} seconds")
driver.quit()
