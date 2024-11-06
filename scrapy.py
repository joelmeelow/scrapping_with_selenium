import time
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.edge.service import Service
from selenium.webdriver.edge.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# Specify the path to your msedgedriver if it's not in PATH
edge_driver_path = r'C:\Users\user\Downloads\edgedriver_win64\msedgedriver.exe'

# Initialize the WebDriver options for Edge
options = Options()
options.use_chromium = True  # This is required for Edge to work properly

# Initialize the Edge WebDriver
service = Service(executable_path=edge_driver_path)
driver = webdriver.Edge(service=service, options=options)

# Start timer for loading the website
start_time = time.time()
site = 'https://www.flashscore.com/'

# Open the website
driver.get("https://www.flashscore.com/basketball/serbia/first-league/results/")

# Function to extract team information from the webpage
def get_team_info():
    home_team = []
    away_team = []
    home_score = []
    away_score = []

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
            continue

    team_info_dict = {
        'home_team': home_team,
        'away_team': away_team,
        'home_score': home_score,
        'away_score': away_score
    }
    return team_info_dict

def get_tables():
    table_position = []
    table_team = []

    tables = driver.find_elements(By.XPATH, '//div[contains(@class, "ui-table__body")]')

    for table in tables:
        try:
            position = table.find_elements(By.XPATH, './/div[contains(@class, "ui-table__row")]/div[contains(@class, "table__cell--rank")]')
            team_name = table.find_elements(By.XPATH, './/div[contains(@class, "ui-table__row")]/div[contains(@class, "table__cell--participant")]/a')

            table_position.extend([p.text for p in position])
            table_team.extend([t.text for t in team_name])
        except Exception as e:
            print(f"Error extracting table data: {e}")
            continue

    table_dict = {
        'position': table_position,
        'team_name': table_team
    }
    return table_dict

# Dummy lists for basketball clubs and years (replace with your actual lists)
list_of_basketball_clubs = ['club1', 'club2']  # Replace with your actual data
list_of_years = ['2023', '2024']  # Replace with your actual years
list_of_url = ['/2011/1', '/2014/2']

# Option to append DataFrames to the total_data list
append_to_list = True  # Set this to False if you don't want to keep the DataFrames in memory

# List to store DataFrames if needed
total_data = []

# Loop over basketball clubs and years to gather match data
for club in list_of_basketball_clubs:
    for year in list_of_years:
        try:
            driver.get(club + f'/{year}')  # Adjust URL if needed
            WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.XPATH, "//div[contains(@class, 'event__match')]")))
            team_info = get_team_info()

            # Create the DataFrame
            df = pd.DataFrame(team_info)

            # Create a filename based on the club and year
            filename = f"basketball_scores_{club}_{year}.csv"

            # Save the DataFrame to a CSV file
            df.to_csv(filename, index=False)

            # Optionally, append the DataFrame to the total_data list
            if append_to_list:
                total_data.append(df)
        except Exception as e:
            print(f"Error processing {club} for year {year}: {e}")

# Optionally, concatenate and save all data if you appended to the list
if append_to_list:
    if total_data:
        concatenated_df = pd.concat(total_data, ignore_index=True)
        concatenated_df.to_csv('all_basketball_scores.csv', index=False)

# Loop over basketball clubs and tables to gather data
total_data_table = []
for club in list_of_basketball_clubs:
    for table in list_of_url:
        try:
            driver.get(club + f'{table}')
            WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.XPATH, "//div[contains(@class, 'ui-table__body')]")))
            table_info = get_tables()

            # Create the DataFrame for table data
            df_table = pd.DataFrame(table_info)

            # Create a filename based on the club and table
            filename = f"basketball_table_{club}_{table.replace('/', '_')}.csv"

            # Save the DataFrame to a CSV file
            df_table.to_csv(filename, index=False)

            # Optionally, append the DataFrame to the total_data_table list
            if append_to_list:
                total_data_table.append(df_table)
        except Exception as e:
            print(f"Error processing table {table} for club {club}: {e}")

# Optionally, concatenate and save all table data if you appended to the list
if append_to_list and total_data_table:
    concatenated_df_table = pd.concat(total_data_table, ignore_index=True)
    concatenated_df_table.to_csv(f'basketball_tables.csv', index=False)

# Duration to keep the browser open (optional)
duration_to_keep_open = 60  # Adjust as needed (1 minute for testing)
time.sleep(duration_to_keep_open)

# Stop timer
end_time = time.time()
total_duration = end_time - start_time
print(f"Time taken to open the website and keep it open: {total_duration:.2f} seconds")

# Close the driver
driver.quit()
