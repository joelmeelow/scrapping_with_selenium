import time
from selenium import webdriver
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.firefox.options import Options

# Specify the path to your geckodriver if it's not in PATH
firefox_driver_path = r'C:\Users\user\Downloads\geckodriver.exe'

# Initialize the WebDriver options for Firefox
options = Options()
options.headless = False  # Set to True to run in the background without opening a browser window
service = Service(executable_path=firefox_driver_path)
driver = webdriver.Firefox(service=service, options=options)

# Open the target URL
driver.get("https://www.flashscore.com.ng/basketball/spain/acb/results/")

# Wait for 60 seconds to manually inspect the page
time.sleep(600)  # Adjust the time as needed

# Close the browser
driver.quit()
