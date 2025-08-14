import time
import pandas as pd
import logging
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from sqlalchemy import create_engine
from selenium.common.exceptions import TimeoutException
from tables import nba, lithuania_liga, serbia_data, italy_data, spain_data, greece_data, turkey_data, france_data, lithuania_data, poland_data, finland_data, italy_a, poland_liga

# PostgreSQL database connection parameters
DB_USER = 'postgres'
DB_PASSWORD = '16a9j63p'
DB_HOST = 'localhost'
DB_PORT = '5432'
DB_NAME = 'sportdata'

# Create connection URL for SQLAlchemy
DATABASE_URL = f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'

# Set up the SQLAlchemy engine
engine = create_engine(DATABASE_URL)

# Specify the path to your geckodriver if it's not in PATH
firefox_driver_path = r'C:\Users\user\Downloads\geckodriver.exe'

# Initialize the WebDriver options for Firefox (headless for background operation)
options = Options()
options.headless = True  # Run in the background without opening a browser window
service = Service(executable_path=firefox_driver_path)
driver = webdriver.Firefox(service=service, options=options)

# Set up logging
logging.basicConfig(level=logging.INFO)

# Function to extract team information from the webpage
def get_team_info(year_url):
    home_team = []
    away_team = []
    home_score = []
    away_score = []
    date = []

    # Wait for match elements to load
    try:
        WebDriverWait(driver, 20).until(
            EC.presence_of_all_elements_located((By.XPATH, '//div[contains(@class, "event__match")]'))
        )
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
                date.append(year_url)  # Add the year URL (or another identifier for the year)
            except Exception as e:
                logging.error(f"Error extracting match data: {e}")
                continue
    except Exception as e:
        logging.error(f"Error waiting for matches to load: {e}")

    return {
        'home_team': home_team,
        'away_team': away_team,
        'home_score': home_score,
        'away_score': away_score,
        'date': date
    }

# Function to scrape data for a specific club and its years
def scrape_club_data(club_url, years, wait_time=20):
    all_team_data = []

    logging.info(f"Scraping data for {club_url}")

    # Iterate through all years
    for year in years:
        logging.info(f"Scraping year {year} and table {year}")
        try:
            driver.get(f"{club_url}{year}")
        except TimeoutException:
            logging.error(f"Timeout while loading {club_url}{year}. Retrying...")
            time.sleep(3)  # Retry after a brief pause
            try:
                driver.get(f"{club_url}{year}")
            except TimeoutException:
                logging.error(f"Retry failed for {club_url}{year}. Skipping year.")
                continue  # Skip this year if it fails again

        # Wait dynamically before scraping each part
        WebDriverWait(driver, wait_time).until(
            EC.presence_of_element_located((By.XPATH, '//div[contains(@class, "event__match")]'))
        )

        # Get data for the specific year
        team_data = get_team_info(year)

        # Save data immediately for this year (to database)
        team_df = pd.DataFrame(team_data)
        
        # Check if the dataframe is not empty
        logging.info(f"Data for year {year} scraped. Shape: {team_df.shape}")
        
        # Insert into the database
        if not team_df.empty:
            insert_data_in_chunks(team_df, f'team_data_{year}')
        else:
            logging.warning(f"No data for year {year}.")

        all_team_data.append(team_data)  # Collect data for combining later

    return all_team_data

# Efficient insertion into PostgreSQL in chunks
def insert_data_in_chunks(df, table_name, chunk_size=500):
    try:
        for start in range(0, len(df), chunk_size):
            df.iloc[start:start+chunk_size].to_sql(table_name, engine, if_exists='append', index=False)
            logging.info(f"Inserted chunk {start} to {table_name}.")
    except Exception as e:
        logging.error(f"Error inserting data into {table_name}: {e}", exc_info=True)

# Specify dynamic wait time (in seconds)
wait_time = 2000  # Reasonable wait time

# Example: Enter the URL for a specific club and its corresponding years
club_url = 'https://www.flashscore.com.ng/basketball/usa/'
years = nba[club_url]['years']
all_team_data = scrape_club_data(club_url, years, wait_time)

# Combine the data from all years
combined_team_data = pd.DataFrame(all_team_data)

# Check combined data before inserting
logging.info(f"Combined data shape: {combined_team_data.shape}")

# Save the combined data to the database
if not combined_team_data.empty:
    insert_data_in_chunks(combined_team_data, 'combined_team_data')
else:
    logging.warning("No combined data to insert.")

# Close the browser
driver.quit()
