import requests
from bs4 import BeautifulSoup
import os
import csv
import time
import random
from dotenv import load_dotenv
import snowflake.connector
import json


# ─── Load Environment Variables ──────────────────────────────────────────
load_dotenv()

SCRAPERAPI_KEY = os.getenv("SCRAPERAPI_KEY")
if not SCRAPERAPI_KEY:
    raise ValueError("❌ SCRAPERAPI_KEY not set in .env file.")

# ─── ScraperAPI Setup ────────────────────────────────────────────────────
BASE_SCRAPERAPI_URL = "https://api.scraperapi.com"

# ─── Snowflake Setup ─────────────────────────────────────────────────────
snowflake_conn_params = {
    'user': os.getenv('SNOWFLAKE_USER'),
    'password': os.getenv('SNOWFLAKE_PASSWORD'),
    'account': os.getenv('SNOWFLAKE_ACCOUNT'),
    'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE'),
    'database': os.getenv('SNOWFLAKE_DATABASE'),
    'schema': os.getenv('SNOWFLAKE_SCHEMA')
}

def connect_to_snowflake():
    try:
        return snowflake.connector.connect(**snowflake_conn_params)
    except Exception as e:
        print(f"❌ Snowflake connection error: {e}")
        return None

# ─── File Setup ──────────────────────────────────────────────────────────
failed_file = "failed_urls.csv"

# ─── Load Previously Scraped URLs ────────────────────────────────────────
scraped_links = set()
if os.path.exists("property_data.csv"):
    with open("property_data.csv", "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if "URL" in reader.fieldnames:
            scraped_links = set(row["URL"] for row in reader)

# ─── Load URLs from the correct file ─────────────────────────────────────
def get_urls_from_snowflake(conn, table_name="property_master", url_column="URL"):
    try:
        with conn.cursor() as cursor:
            cursor.execute(f"SELECT {url_column} FROM {table_name}")
            results = cursor.fetchall()
            return [row[0] for row in results if row[0]]
    except Exception as e:
        print(f"❌ Error fetching URLs from Snowflake: {e}")
        return []

# ─── Load URLs from Snowflake ────────────────────────────────────────────
conn = connect_to_snowflake()
all_links = get_urls_from_snowflake(conn)

links_to_scrape = [link for link in all_links if link not in scraped_links]
print(f"📌 {len(links_to_scrape)} new listings to scrape")

failed_links = []


# # ─── Extract Property Type ────────────────────────────────────────────────
# def extract_property_type(soup):
#     overview_keys = soup.select("div.p24_propertyOverviewKey")
#     for key in overview_keys:
#         if "Type of Property" in key.get_text(strip=True):
#             parent = key.find_parent()
#             info_divs = parent.find_all("div", class_="p24_info")
#             if info_divs:
#                 return info_divs[0].get_text(strip=True)
#     return None


# ─── Helper: Extract Property Data ───────────────────────────────────────
def extract_property_data(soup, link):
    property_info = {"URL": link}

    # Price
    price_tag = soup.select_one(".p24_price")
    if price_tag:
        property_info["Price"] = price_tag.get_text(strip=True)

    # Address
    address_tag = soup.select_one(".js_displayMap.p24_address")
    if address_tag:
        property_info["Address"] = address_tag.get_text(strip=True)

    # Title
    title_tag = soup.select_one("h1.p24_title")
    if title_tag:
        property_info["Title"] = title_tag.get_text(strip=True)

    # Property Overview (Bedrooms, Bathrooms, Rates, Levies, etc.)
    rows = soup.select(".p24_propertyOverviewRow")
    for row in rows:
        try:
            key_tag = row.select_one(".p24_propertyOverviewKey")
            value_tag = row.select_one(".p24_info")
            if key_tag and value_tag:
                key = key_tag.get_text(strip=True)
                value = value_tag.get_text(strip=True)
                property_info[key] = value
            # print(key_tag)
            # print(value_tag)
        except Exception:
            continue

            

     # Additional manually scraped value
    # property_info["Type of Property"] = extract_property_type(soup)

    # print(json.dumps(property_info, indent=2))

    return property_info

    # Property Type (special case)
    # for row in soup.select(".p24_section"):
    #     key_tag = row.select_one("h2")
    #     if key_tag and "Property Type" in key_tag.get_text():
    #         value_tag = row.select_one(".p24_info")
    #         if value_tag:
    #             property_info["Property Type"] = value_tag.get_text(strip=True)
    #             break

    # return property_info


def normalize_key(key):
    return key.lower().replace(" ", "_").replace("-", "_")

def insert_property_into_snowflake(property_info, connection, table_name="properties"):
    # List of predefined Snowflake columns
    snowflake_columns = [
        'LISTING_NUMBER', 'PETS_ALLOWED', 'OFFICE', 'ERF_SIZE', 'BATHROOMS', 'GARAGE', 'URL',
        'FACING', 'POOL', 'TYPE_OF_PROPERTY', 'ADDRESS', 'PRICE', 'FLOOR_SIZE', 'LEVIES',
        'BEDROOMS', 'RATES_AND_TAXES', 'PARKING', 'KITCHENS', 'GARDENS', 'DESCRIPTION', 
        'LIFESTYLE', 'RECEPTION_ROOMS', 'LISTING_DATE'
    ]

    # Normalize keys for Snowflake column compatibility
    normalized_data = {normalize_key(k): v for k, v in property_info.items()}
    
    # Ensure that all columns from the Snowflake table are present in the data
    # If a column is missing from property_info, set its value to 'na'
    full_data = {col: normalized_data.get(normalize_key(col), 'na') for col in snowflake_columns}
    
    # Debugging: Print the full_data to verify the normalized data
    print("Full Data for Insert:", full_data)

    columns = ", ".join([f'"{col}"' for col in full_data.keys()])  # Quoting column names for Snowflake compatibility
    placeholders = ", ".join(["%s"] * len(full_data))  # Using %s as placeholders for Snowflake
    values = list(full_data.values())  # Values to insert (including 'na' for missing values)

    # Debugging: Print the final SQL and values for comparison
    print("Insert SQL:", f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})")
    print("Values:", values)

    insert_sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"

    try:
        with connection.cursor() as cursor:
            cursor.execute(insert_sql, tuple(values))  # Ensure values are passed as a tuple (Snowflake)
        connection.commit()
        print("✅ Data inserted successfully.")
    except Exception as e:
        print("❌ Failed to insert data:", e)




# # ─── Insert into Snowflake ───────────────────────────────────────────────
# def insert_into_snowflake(data, conn):
#     try:
#         # Get existing column names from Snowflake
#         cursor = conn.cursor()
#         cursor.execute("DESC TABLE PROPERTY_DATA")
#         existing_columns = {row[0].upper() for row in cursor.fetchall()}
#         cursor.close()

#         # Filter the data to match only valid columns
#         valid_data = {k: v for k, v in data.items() if k.upper() in existing_columns}

#         if not valid_data:
#             raise Exception("No valid columns to insert.")

#         placeholders = ", ".join(["%s"] * len(valid_data))
#         columns = ', '.join(f'"{k}"' for k in valid_data.keys())
#         sql = f"INSERT INTO PROPERTY_DATA ({columns}) VALUES ({placeholders})"

#         cursor = conn.cursor()
#         cursor.execute(sql, list(valid_data.values()))
#         cursor.close()
#     except Exception as e:
#         raise Exception(f"❌ Failed to insert into Snowflake: {e}")
    



# ─── Scraping Loop ───────────────────────────────────────────────────────
conn = connect_to_snowflake()

for i, link in enumerate(links_to_scrape):
    success = False
    attempts = 0

    while not success and attempts < 2:
        attempts += 1
        print(f"[{i+1}/{len(links_to_scrape)}] Attempt {attempts}: {link}")
        try:
            params = {
                "api_key": SCRAPERAPI_KEY,
                "url": link
            }

            response = requests.get(BASE_SCRAPERAPI_URL, params=params, timeout=60)

            if response.status_code != 200:
                raise Exception(f"Status code {response.status_code}")

            soup = BeautifulSoup(response.text, "html.parser")
            property_info = extract_property_data(soup, link)

            insert_property_into_snowflake(property_info, conn)

            success = True
            time.sleep(random.uniform(3, 6))

        except Exception as e:
            print(f"⚠️ Failed attempt {attempts} for {link}: {e}")
            if attempts == 2:
                failed_links.append(link)


# ─── Save Failed Links ───────────────────────────────────────────────────
if failed_links:
    with open(failed_file, "a", encoding="utf-8") as f:
        for link in failed_links:
            f.write(link + "\n")
    print(f"❌ {len(failed_links)} failed links saved to {failed_file}")

if conn:
    conn.close()

print("✅ Done scraping.")
