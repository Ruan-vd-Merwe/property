from webdriver_manager.chrome import ChromeDriverManager
from selenium import webdriver

driver = webdriver.Chrome(ChromeDriverManager().install())
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from datetime import datetime
import csv
import time
import random

# Configure browser
options = Options()
options.add_argument("--window-size=1920,1080")
options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36")
driver = webdriver.Chrome(service=Service(), options=options)

# Go to first page to detect total number of pages
start_url = "https://www.property24.com/for-sale/paarl/western-cape/344"
driver.get(start_url)
time.sleep(random.uniform(3.0, 5.0))

try:
    pagination_links = driver.find_elements(By.CSS_SELECTOR, "ul.pagination a[data-pagenumber]")
    page_numbers = [int(link.get_attribute("data-pagenumber")) for link in pagination_links if link.get_attribute("data-pagenumber").isdigit()]
    max_page = max(page_numbers) if page_numbers else 1
except Exception as e:
    print("⚠️ Could not determine total pages. Defaulting to 1:", e)
    max_page = 1

print(f"🔍 Total pages found: {max_page}")

property_data = []

for page in range(1, max_page + 1):
    print(f"Scraping page {page}")
    url = f"https://www.property24.com/for-sale/paarl/western-cape/344/p{page}"
    driver.get(url)
    time.sleep(random.uniform(2.5, 5.0))

    listings = driver.find_elements(By.CSS_SELECTOR, "div.p24_tileContainer")
    for listing in listings:
        try:
            link_elem = listing.find_element(By.CSS_SELECTOR, "a[href*='/for-sale/']")
            price_elem = listing.find_element(By.CSS_SELECTOR, ".p24_price")

            try:
                size_elem = listing.find_element(By.CSS_SELECTOR, ".p24_size span").text.strip()
            except:
                size_elem = "N/A"

            location_elem = listing.find_element(By.CSS_SELECTOR, ".p24_location")

            features = listing.find_elements(By.CSS_SELECTOR, ".p24_featureDetails")
            bedrooms = bathrooms = parking = "N/A"

            for feature in features:
                title = feature.get_attribute("title")
                value = feature.find_element(By.TAG_NAME, "span").text.strip()
                if title == "Bedrooms":
                    bedrooms = value
                elif title == "Bathrooms":
                    bathrooms = value
                elif title == "Parking Spaces":
                    parking = value

            # ✅ Correct indentation here
            property_data.append({
                "url": link_elem.get_attribute("href"),
                "price": price_elem.text.strip(),
                "size": size_elem,
                "location": location_elem.text.strip() if location_elem else "N/A",
                "bedrooms": bedrooms,
                "bathrooms": bathrooms,
                "parking": parking,
                "scraped_at": datetime.now().isoformat()
            })

        except Exception as e:
            print(f"⚠️ Skipped one listing due to error: {e}")

# Remove duplicates
unique_data = {item["url"]: item for item in property_data}.values()

# Save to CSV
with open("property_urls.csv", "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=[
        "url", "price", "size", "location",
        "bedrooms", "bathrooms", "parking", "scraped_at"
    ])
    writer.writeheader()
    writer.writerows(unique_data)

print(f"✅ Done. Saved {len(unique_data)} unique properties.")
driver.quit()
