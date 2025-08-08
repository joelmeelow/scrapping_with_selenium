import time
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from sqlalchemy import create_engine
import re





basketball_data = {
    'https://www.flashscore.com/basketball/serbia/': {
        'years': [
            'first-league/results/',
            'first-league-2023-2024/results/',
            'first-league-2022-2023/results/',
            'first-league-2021-2022/results/',
            'first-league-2020-2021/results/',
            'first-league-2019-2020/results/',
            'first-league-2018-2019/results/',
            'first-league-2017-2018/results/',
            'first-league-2016-2017/results/'
        ],
        'table_urls': [
            'first-league/#/h2WpL4E4/table/overall',
            'first-league-2023-2024/#/fPB68otI/table/overall',
            'first-league-2022-2023/#/UgOzMRKQ/table/overall',
            'first-league-2021-2022/#/WUJWbNld/table/overall',
            'first-league-2020-2021/#/hvLSa3Zk/table/overall',
            'first-league-2019-2020/#/A37knMV6/table/overall',
            'first-league-2018-2019/#/G8hfYmrK/table/overall',
            'first-league-2017-2018/#/Ao8OOsxl/table/overall',
            'first-league-2016-2017/#/fewZf0nn/table/overall'
        ]
    },
    'https://www.flashscore.com.ng/basketball/italy/': {
        'years': [
            'serie-a2/results/',
            'serie-a2-2023-2024/results/',
            'serie-a2-2022-2023/results/',
            'serie-a2-2021-2022/results/',
            'serie-a2-2020-2021/results/',
            'serie-a2-2019-2020/results/',
            'serie-a2-2018-2019/results/',
            'serie-a2-2017-2018/results/',
            'serie-a2-2016-2017/results/'
        ],
        'table_urls': [
            'serie-a2/#/hS3YM53f/table/overall',
            'serie-a2-2023-2024/#/CWzoUZer/table/overall',
            'serie-a2-2022-2023/#/I1Na127R/table/overall',
            'serie-a2-2021-2022/#/drRuq3oq/table/overall',
            'serie-a2-2020-2021/#/dEhXxdZ8/table/overall',
            'serie-a2-2019-2020/#/vZ3QJXBc/table/overall',
            'serie-a2-2018-2019/#/UsLx0enA/table/overall',
            'serie-a2-2017-2018/#/ry8a6zQS/table/overall',
            'serie-a2-2016-2017/#/2sdGbyVF/table/overall'
        ]
    },
    'https://www.flashscore.com.ng/basketball/italy/': {
        'years': [
            'lega-a/results/',
            'lega-a-2023-2024/results/',
            'lega-a-2022-2023/results/',
            'lega-a-2021-2022/results/',
            'lega-a-2020-2021/results/',
            'lega-a-2019-2020/results/',
            'lega-a-2018-2019/results/',
            'lega-a-2017-2018/results/',
            'lega-a-2016-2017/results/'
        ],
        'table_urls': [
            'lega-a/#/xhe1sf4F/table/overall',
            'lega-a-2023-2024/#/WUE5Rtzp/table/overall',
            'lega-a-2022-2023/#/QJGO7JEf/table/overall',
            'lega-a-2021-2022/#/xYTQssW8/table/overall',
            'lega-a-2020-2021/#/pnDe2WtN/table/overall',
            'lega-a-2019-2020/#/AslpZNCn/table/overall',
            'lega-a-2018-2019/#/rydC2Qu2/table/overall',
            'lega-a-2017-2018/#/nm9y71Co/table/overall',
            'lega-a-2016-2017/#/z7xXnAda/table/overall'
        ]
    },
    'https://www.flashscore.com.ng/basketball/spain/': {
        'years': [
            'acb/results/',
            'acb-2023-2024/results/',
            'acb-2022-2023/results/',
            'acb-2021-2022/results/',
            'acb-2020-2021/results/',
            'acb-2019-2020/results/',
            'acb-2018-2019/results/',
            'acb-2017-2018/results/',
            'acb-2016-2017/results/'
        ],
        'table_urls': [
            'acb/#/QanAHMDT/table/overall',
            'acb-2023-2024/#/jXKUPnBh/table/overall',
            'acb-2022-2023/#/I3QRD3S6/table/overall',
            'acb-2021-2022/#/pIPVLpd9/table/overall',
            'acb-2020-2021/#/pfuX9RA1/table/overall',
            'acb-2019-2020/#/4Q6gotoD/table/overall',
            'acb-2018-2019/#/nwKL1I6D/table/overall',
            'acb-2017-2018/#/h8lrdjuF/table/overall',
            'acb-2016-2017/#/IRyrrOSB/table/overall'
        ]
    },
    'https://www.flashscore.com/basketball/greece/': {
        'years': [
            'basket-league/results/',
            'basket-league-2023-2024/results/',
            'basket-league-2022-2023/results/',
            'basket-league-2021-2022/results/',
            'basket-league-2020-2021/results/',
            'basket-league-2019-2020/results/',
            'basket-league-2018-2019/results/',
            'basket-league-2017-2018/results/',
            'basket-league-2016-2017/results/'
        ],
        'table_urls': [
            'basket-league/#/Eyglx3l3/table/overall',
            'basket-league-2023-2024/#/b711xNaM/table/overall',
            'basket-league-2022-2023/#/bZJNU125/table/overall',
            'basket-league-2021-2022/#/KvQp8biG/table/overall',
            'basket-league-2020-2021/#/xELbWflN/table/overall',
            'basket-league-2019-2020/#/tnxx7NYj/table/overall',
            'basket-league-2018-2019/#/UD9QKWcm/table/overall',
            'basket-league-2017-2018/#/lACX5ki2/table/overall',
            'basket-league-2016-2017/#/byfLKcRb/table/overall'
        ]
    },
    'https://www.flashscore.com.ng/basketball/turkey/': {
        'years': [
            'super-lig/results/',
            'super-lig-2023-2024/results/',
            'super-lig-2022-2023/results/',
            'super-lig-2021-2022/results/',
            'super-lig-2020-2021/results/',
            'super-lig-2019-2020/results/',
            'super-lig-2018-2019/results/',
            'super-lig-2017-2018/results/',
            'super-lig-2016-2017/results/'
        ],
        'table_urls': [
            'super-lig/#/6u5XUQ4p/table/overall',
            'super-lig-2023-2024/#/GjpjDIgP/table/overall',
            'super-lig-2022-2023/#/2yWFem8E/table/overall',
            'super-lig-2021-2022/#/GrssBOiQ/table/overall',
            'super-lig-2020-2021/#/MXgO2hJD/table/overall',
            'super-lig-2019-2020/#/MJbubxPq/table/overall',
            'super-lig-2018-2019/#/Qu1RjeyI/table/overall',
            'super-lig-2017-2018/#/OlfLEN72/table/overall',
            'super-lig-2016-2017/#/YsjLMiYb/table/overall'
        ]
    },
     'https://www.flashscore.com.ng/basketball/france/': {
        'years': [
            'lnb/results/',
            'lnb-2023-2024/results/',
            'lnb-2022-2023/results/',
            'lnb-2021-2022/results/',
            'lnb-2020-2021/results/',
            'lnb-2019-2020/results/',
            'lnb-2018-2019/results/',
            'lnb-2017-2018/results/',
            'lnb-2016-2017/results/'
        ],
        'table_urls': [
            'lnb/#/EgA6k9Ud/table/overall',
            'lnb-2023-2024/#/SxdEuwf1/table/overall',
            'lnb-2022-2023/#/C0lXTBOs/table/overall',
            'lnb-2021-2022/#/ARilbuHE/table/overall',
            'lnb-2020-2021/#/Q9E4Xvpm/table/overall',
            'lnb-2019-2020/#/OCUbgPMj/table/overall',
            'lnb-2018-2019/#/jBQC3vx1/table/overall',
            'lnb-2017-2018/#/lCJPumFa/table/overall',
            'lnb-2016-2017/#/KEMc4X2Q/table/overall'
        ]
    },
    'https://www.flashscore.com.ng/basketball/lithuania/': {
        'years': [
            'lkl/results/',
            'lkl-2023-2024/results/',
            'lkl-2022-2023/results/',
            'lkl-2021-2022/results/',
            'lkl-2020-2021/results/',
            'lkl-2019-2020/results/',
            'lkl-2018-2019/results/',
            'lkl-2017-2018/results/',
            'lkl-2016-2017/results/'
        ],
        'table_urls': [
            'lkl/#/nVl1nqsl/table/overall',
            'lkl-2023-2024/#/jLfb8QaK/table/overall',
            'lkl-2022-2023/#/4E5F4v9e/table/overall',
            'lkl-2021-2022/#/A1dflzJB/table/overall',
            'lkl-2020-2021/#/GvcjkG35/table/overall',
            'lkl-2019-2020/#/KGZ4sy8C/table/overall',
            'lkl-2018-2019/#/OGj1ylTa/table/overall',
            'lkl-2017-2018/#/fFXbhyUC/table/overall',
            'lkl-2016-2017/#/lYzDotxG/table/overall'
        ]
    },
    'https://www.flashscore.com.ng/basketball/lithuania/': {
        'years': [
            'nkl/results/',
            'nkl-2023-2024/results/',
            'lkl-2022-2023/results/',
            'lkl-2021-2022/results/',
            'lkl-2020-2021/results/',
            'lkl-2019-2020/results/',
            'lkl-2018-2019/results/',
            'lkl-2017-2018/results/',
            'lkl-2016-2017/results/'
        ],
        'table_urls': [
            'nkl/#/x4BHpNB0/table/overall',
            'nkl-2023-2024/#/rJVY1Oxl/table/overall',
            'lkl-2022-2023/#/4E5F4v9e/table/overall',
            'lkl-2021-2022/#/A1dflzJB/table/overall',
            'lkl-2020-2021/#/GvcjkG35/table/overall',
            'lkl-2019-2020/#/KGZ4sy8C/table/overall',
            'lkl-2018-2019/#/OGj1ylTa/table/overall',
            'lkl-2017-2018/#/fFXbhyUC/table/overall',
            'lkl-2016-2017/#/lYzDotxG/table/overall'
        ]
    },
    'https://www.flashscore.com/basketball/poland/': {
        'years': [
            'basket-liga/results/',
            'basket-liga-2023-2024/results/',
            'basket-liga-2022-2023/results/',
            'basket-liga-2021-2022/results/',
            'basket-liga-2020-2021/results/',
            'basket-liga-2019-2020/results/',
            'basket-liga-2018-2019/results/',
            'basket-liga-2017-2018/results/',
            'basket-liga-2016-2017/results/'
        ],
        'table_urls': [
            'basket-liga/#/EP2hQswI/table/overall',
            'basket-liga-2023-2024/#/IN29bO3h/table/overall',
            'basket-liga-2022-2023/#/UakrnL50/table/overall',
            'basket-liga-2021-2022/#/IXAFoky2/table/overall',
            'basket-liga-2020-2021/#/nqowdzfM/table/overall',
            'basket-liga-2019-2020/#/7vZhtQhN/table/overall',
            'basket-liga-2018-2019/#/hVwsAqZm/table/overall',
            'basket-liga-2017-2018/#/yMvDQ7Hz/table/overall',
            'basket-liga-2016-2017/#/KZZe9fqg/table/overall'
        ]
    },
     'https://www.flashscore.com/basketball/poland/': {
        'years': [
            '1-liga/results/',
            '1-liga-2023-2024/results/',
            '1-liga-2022-2023/results/',
            '1-liga-2021-2022/results/',
            '1-liga-2020-2021/results/',
            '1-liga-2019-2020/results/',
            '1-liga-2018-2019/results/',
            '1-liga-2017-2018/results/',
            '1-liga-2016-2017/results/'
        ],
        'table_urls': [
            '1-liga/#/0t10OL7U/table/overall',
            '1-liga-2023-2024/#/tI9nu13U/table/overall',
            '1-liga-2022-2023/#/zgYMlNLs/table/overall',
            '1-liga-2021-2022/#/xbHA9hT8/table/overall',
            '1-liga-2020-2021/#/pp6VKih8/table/overall',
            '1-liga-2019-2020/#/WpfkHok9/table/overall',
            '1-liga-2018-2019/#/nHIMh9h9/table/overall',
            '1-liga-2017-2018/#/2F7XKeio/table/overall',
            '1-liga-2016-2017/#/IN3MFve5/table/overall'
        ]
    },
    'https://www.flashscore.com/basketball/finland/': {
        'years': [
            'korisliiga/results/',
            'korisliiga-2023-2024/results/',
            'korisliiga-2022-2023/results/',
            'korisliiga-2021-2022/results/',
            'korisliiga-2020-2021/results/',
            'korisliiga-2019-2020/results/',
            'korisliiga-2018-2019/results/',
            'korisliiga-2017-2018/results/',
            'korisliiga-2016-2017/results/'
        ],
        'table_urls': [
            'korisliiga/#/CYJuywPh/table/overall',
            'korisliiga-2023-2024/#/O6SNxpY2/table/overall',
            'korisliiga-2022-2023/#/pIQ0Zte3/table/overall',
            'korisliiga-2021-2022/#/OdWLhkIl/table/overall',
            'korisliiga-2020-2021/#/EuttBcYF/table/overall',
            'korisliiga-2019-2020/#/MkoGQq87/table/overall',
            'korisliiga-2018-2019/#/4E0lJKd3/table/overall',
            'korisliiga-2017-2018/#/IVuExJRi/table/overall',
            'korisliiga-2016-2017/#/lx119mnI/table/overall'
        ]
    }
}





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

# Initialize the WebDriver options for Firefox
options = Options()

# Initialize the Firefox WebDriver
service = Service(executable_path=firefox_driver_path)
driver = webdriver.Firefox(service=service, options=options)

# Start timer for loading the website
start_time = time.time()


# Function to extract team information from the webpage
def get_team_info(number):
    home_team = []
    away_team = []
    home_score = []
    away_score = []
    date = []
    count = 2024  # Year placeholder for current season
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
            date.append(count)
        except Exception as e:
            print(f"Error extracting match data: {e}")
            continue

    team_info_dict = {
        'home_team': home_team,
        'away_team': away_team,
        'home_score': home_score,
        'away_score': away_score,
        'date': date
    }
    count -= int(number)  # Decrease the year count
    return team_info_dict

# Function to extract table data
def get_tables():
    table_position = []
    table_team = []
    date = []
    number = 1
    count = 2024 
    tables = driver.find_elements(By.XPATH, '//div[contains(@class, "ui-table__body")]')

    for table in tables:
        try:
            position = table.find_elements(By.XPATH, './/div[contains(@class, "ui-table__row")]/div[contains(@class, "table__cell--rank")]')
            team_name = table.find_elements(By.XPATH, './/div[contains(@class, "ui-table__row")]/div[contains(@class, "table__cell--participant")]/a')

            table_position.extend([p.text for p in position])
            table_team.extend([t.text for t in team_name])
            date.append(count)
        except Exception as e:
            print(f"Error extracting table data: {e}")
            continue

    table_dict = {
        'position': table_position,
        'team_name': table_team,
        'date': date
    }
    count -= int(number)  # Decrease the year count
    return table_dict

# Option to append DataFrames to the total_data list
append_to_list = True  # Set this to False if you don't want to keep the DataFrames in memory

# List to store DataFrames if needed
total_data = []

# Loop over basketball clubs and years to gather match data
for club in list_of_basketball_clubs:
    country_name = re.search(r'https://www\.flashscore\.com/basketball/([a-zA-Z\-]+)/', club)
    if country_name:
        country_name = country_name.group(1)
    else:
        country_name = club
        continue

    for year in list_of_years_lithuana_nkl:
        try:
            driver.get(f"{club}{year}")
            WebDriverWait(driver, 60).until(EC.presence_of_element_located((By.XPATH, "//div[contains(@class, 'event__match')]")))

            team_info = get_team_info(1)

            # Create the DataFrame
            df = pd.DataFrame(team_info)

            # Save the DataFrame to PostgreSQL as a table
            table_name = f"basketball_scores_{country_name}_{year}"
            df.to_sql(table_name, engine, index=False, if_exists='replace')  # Replace existing table if needed

            # Optionally, append the DataFrame to the total_data list
            if append_to_list:
                total_data.append(df)
        except Exception as e:
            print(f"Error processing {country_name} for year {year}: {e}")
            continue

# Save the total_data combined DataFrame to PostgreSQL as a separate table
if append_to_list and total_data:
    concatenated_df = pd.concat(total_data, ignore_index=True)
    concatenated_df.to_sql('total_basketball_scores', engine, index=False, if_exists='replace')

# Loop over basketball clubs and tables to gather table data
total_data_table = []
for club in list_of_basketball_clubs:
    country_name = re.search(r'https://www\.flashscore\.com/basketball/([a-zA-Z\-]+)/', club)
    if country_name:
        country_name = country_name.group(1)
    else:
        country_name = club
        continue

    for table in table_url_lithuana_nkl:
        try:
            driver.get(f"{club}{table}")
            WebDriverWait(driver, 60).until(EC.presence_of_element_located((By.XPATH, "//div[contains(@class, 'ui-table__body')]")))

            table_info = get_tables()

            # Create the DataFrame for table data
            df_table = pd.DataFrame(table_info)

            # Save the DataFrame to PostgreSQL as a table
            table_name = f"basketball_table_{club}_{table.replace('/', '_')}"
            df_table.to_sql(table_name, engine, index=False, if_exists='replace')

            # Optionally, append the DataFrame to the total_data_table list
            if append_to_list:
                total_data_table.append(df_table)
        except Exception as e:
            print(f"Error processing table {table} for club {club}: {e}")
            continue

# Save the total table data combined DataFrame to PostgreSQL as a separate table
if append_to_list and total_data_table:
    concatenated_df_table = pd.concat(total_data_table, ignore_index=True)
    concatenated_df_table.to_sql('total_basketball_tables', engine, index=False, if_exists='replace')

# Duration to keep the browser open (optional)
duration_to_keep_open = 5000  # Adjust as needed (1 minute for testing)
time.sleep(duration_to_keep_open)

# Stop timer
end_time = time.time()
total_duration = end_time - start_time
print(f"Time taken to open the website and keep it open: {total_duration:.2f} seconds")

# Close the driver
driver.quit()
