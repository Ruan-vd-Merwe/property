from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
import csv
import time
import random

# Configure browser
options = Options()
options.add_argument("--window-size=1920,1080")
options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36")
driver = webdriver.Chrome(service=Service(), options=options)

property_urls = []

for page in range(1, 6):  # Adjust as needed
    print(f"Scraping page {page}")
    url = f"https://www.property24.com/for-sale/paarl/western-cape/344/p{page}"
    driver.get(url)
    time.sleep(random.uniform(2.5, 5.0))  # Wait to avoid blocking

    listings = driver.find_elements(By.CSS_SELECTOR, "div.p24_tileContainer a[href*='/for-sale/']")
    page_links = list(set([elem.get_attribute("href") for elem in listings]))
    print(f"  Found {len(page_links)} URLs")
    property_urls.extend(page_links)

# Remove duplicates
property_urls = list(set(property_urls))

# Save to CSV
with open("property_urls.csv", "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    for link in property_urls:
        writer.writerow([link])

print(f"✅ Saved {len(property_urls)} URLs to property_urls.csv")
driver.quit()
