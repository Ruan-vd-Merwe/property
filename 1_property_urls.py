import time
import random
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from bs4 import BeautifulSoup
import snowflake.connector
from dotenv import load_dotenv
import os

# ----------------- SETUP SELENIUM -----------------
options = Options()
options.add_argument("--headless=new")
options.add_argument("--window-size=1920,1080")
options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36")
driver = webdriver.Chrome(service=Service(), options=options)

# ----------------- EXTRACT FUNCTION -----------------
def extract_property_data(soup, link):
    clean = lambda x: x.strip().lower() if isinstance(x, str) else None
    property_info = {"url": clean(link)}

    price_tag = soup.select_one(".p24_price")
    address_tag = soup.select_one(".js_displayMap.p24_address")
    title_tag = soup.select_one("h1.p24_title")

    property_info["price"] = clean(price_tag.get_text()) if price_tag else None
    property_info["suburb"] = clean(address_tag.get_text()) if address_tag else None
    property_info["title"] = clean(title_tag.get_text()) if title_tag else None

    return property_info

# ----------------- SCRAPE LISTING PAGES -----------------
property_urls = []

start_url = "https://www.property24.com/for-sale/paarl/western-cape/344"
driver.get(start_url)
time.sleep(random.uniform(2.5, 4.5))

try:
    pagination_links = driver.find_elements(By.CSS_SELECTOR, "ul.pagination a[data-pagenumber]")
    page_numbers = [int(link.get_attribute("data-pagenumber")) for link in pagination_links if link.get_attribute("data-pagenumber").isdigit()]
    max_page = max(page_numbers) if page_numbers else 1
except Exception as e:
    print("⚠️ Failed to get number of pages:", e)
    max_page = 1

print(f"📄 Total pages to scrape: {max_page}")

for page in range(1, max_page + 1):
    print(f"📄 Scraping page {page}")
    url = f"https://www.property24.com/for-sale/paarl/western-cape/344/p{page}"
    driver.get(url)
    time.sleep(random.uniform(2.5, 5.0))

    listings = driver.find_elements(By.CSS_SELECTOR, "div.p24_tileContainer a[href*='/for-sale/']")
    page_links = list(set([elem.get_attribute("href") for elem in listings]))
    print(f"  🔗 Found {len(page_links)} property links")

    for link in page_links:
        driver.get(link)
        time.sleep(random.uniform(1.5, 3.0))
        soup = BeautifulSoup(driver.page_source, "html.parser")
        prop_data = extract_property_data(soup, link)
        property_urls.append(prop_data)

# Deduplicate by URL
property_urls = list({prop['url']: prop for prop in property_urls}.values())
print(f"🔍 Total unique scraped properties: {len(property_urls)}")
driver.quit()

# ----------------- INSERT INTO SNOWFLAKE -----------------
print("Connecting to Snowflake...")
load_dotenv()

conn = snowflake.connector.connect(
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    database=os.getenv("SNOWFLAKE_DATABASE"),
    schema=os.getenv("SNOWFLAKE_SCHEMA")
)

cur = conn.cursor()

cur.execute("""
CREATE TABLE IF NOT EXISTS MASTER_PROPERTIES (
    URL STRING PRIMARY KEY,
    PRICE STRING,
    TITLE STRING,
    SUBURB STRING,
    LAST_UPDATED TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
)
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS SCRAPED_PROPERTIES (
    URL STRING,
    INSERT_DATE TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    PRICE STRING,
    TITLE STRING,
    SUBURB STRING
)
""")

# Fetch existing data
cur.execute("SELECT URL, PRICE FROM MASTER_PROPERTIES")
existing_properties = {row[0]: row[1] for row in cur.fetchall()}

# Insert scraped data into SCRAPED_PROPERTIES and upsert to MASTER_PROPERTIES
new_count = 0
updated_count = 0

for prop in property_urls:
    url = prop['url']
    price = prop['price']
    title = prop['title']
    suburb = prop['suburb']

    # Always insert into SCRAPED_PROPERTIES (history)
    cur.execute("""
        INSERT INTO SCRAPED_PROPERTIES (URL, PRICE, TITLE, SUBURB)
        VALUES (%s, %s, %s, %s)
    """, (url, price, title, suburb))

    # If new or price changed, upsert into MASTER_PROPERTIES
    if url not in existing_properties:
        new_count += 1
    elif existing_properties[url] != price:
        updated_count += 1
    else:
        continue  # Skip if nothing changed

    cur.execute(f"""
        MERGE INTO MASTER_PROPERTIES AS target
        USING (SELECT %s AS URL, %s AS PRICE, %s AS TITLE, %s AS SUBURB) AS source
        ON target.URL = source.URL
        WHEN MATCHED THEN
            UPDATE SET PRICE = source.PRICE, TITLE = source.TITLE, SUBURB = source.SUBURB, LAST_UPDATED = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN
            INSERT (URL, PRICE, TITLE, SUBURB, LAST_UPDATED)
            VALUES (source.URL, source.PRICE, source.TITLE, source.SUBURB, CURRENT_TIMESTAMP())
    """, (url, price, title, suburb))

print(f"✅ Insert complete: {new_count} new, {updated_count} updated.")

cur.close()
conn.close()
