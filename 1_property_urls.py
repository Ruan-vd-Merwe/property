import time
import random
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium_stealth import stealth
from bs4 import BeautifulSoup
from webdriver_manager.chrome import ChromeDriverManager
import tempfile
# from webdriver_manager.utils import ChromeType


# chrome_service = Service(ChromeDriverManager(chrome_type=ChromeType.CHROMIUM).install())
ChromeDriverManager().install()

chrome_options = Options()
options = [
    "--headless",
    "--disable-gpu",
    "--window-size=1920,1200",
    "--ignore-certificate-errors",
    "--disable-extensions",
    "--no-sandbox",
    "--disable-dev-shm-usage"
]
for option in options:
    chrome_options.add_argument(option)

driver = webdriver.Chrome(service=chrome_service, options=chrome_options)
# # ----------------- SETUP SELENIUM WITH STEALTH -----------------



# options = Options()
# # options.add_argument(r"--user-data-dir=C:\Users\RuanvanderMerwe\Documents\Code\Python_Training\prop24-scrape\git_streamlit_prop24\property-1\chrome_user_data")

# options.add_argument("--disable-blink-features=AutomationControlled")
# options.add_argument("--window-size=1920,1080")
# options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36")

# # Optional: headless for CI environments
# # options.add_argument("--headless=new")

# # # Automatically download and use correct chromedriver
# service = Service(ChromeDriverManager().install())
# driver = webdriver.Chrome(service=service, options=options)

# # options = Options()
# # options.add_argument(r"--user-data-dir=C:\Users\RuanvanderMerwe\Documents\Code\Python_Training\prop24-scrape\git_streamlit_prop24\property-1\chrome_user_data")
# # options.add_argument("--disable-blink-features=AutomationControlled")
# # options.add_argument("--window-size=1920,1080")
# # options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36")

# # Uncomment this if you want a truly unique session each time
# # options.add_argument(f"--user-data-dir={tempfile.mkdtemp()}")

# # Uncomment this if you want headless browsing
# # Optional for headless operation:
# # options.add_argument("--headless=new")

# # service = Service(executable_path=r"C:\Users\RuanvanderMerwe\.wdm\drivers\chromedriver\win64\135.0.7049.114\chromedriver-win32\chromedriver.exe")
# # driver = webdriver.Chrome(service=service, options=options)


# stealth(driver,
#     languages=["en-US", "en"],
#     vendor="Google Inc.",
#     platform="Win32",
#     webgl_vendor="Intel Inc.",
#     renderer="Intel Iris OpenGL Engine",
#     fix_hairline=True,
# )




# Setup Chrome options
# options = Options()
# options.add_argument("--disable-blink-features=AutomationControlled")
# options.add_argument("--window-size=1920,1080")
# options.add_argument("--headless=new")  # Headless is required in CI
# # options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36")
# # options.add_argument(f"--user-data-dir={tempfile.mkdtemp()}")  # Use a temporary profile to avoid conflicts

# # Setup driver service
# service = Service(ChromeDriverManager().install())
# driver = webdriver.Chrome(service=service, options=options)

# # Stealth settings
# stealth(driver,
#     languages=["en-US", "en"],
#     vendor="Google Inc.",
#     platform="Win32",
#     webgl_vendor="Intel Inc.",
#     renderer="Intel Iris OpenGL Engine",
#     fix_hairline=True,
# )

wait = WebDriverWait(driver, 10)


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

# Wait until pagination is visible or timeout
try:
    wait.until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, "ul.pagination a[data-pagenumber]")))
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

    try:
        wait.until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, "div.p24_tileContainer a[href*='/for-sale/']")))
        listings = driver.find_elements(By.CSS_SELECTOR, "div.p24_tileContainer a[href*='/for-sale/']")
        page_links = list(set([elem.get_attribute("href") for elem in listings if elem.get_attribute("href")]))
        print(f"  🔗 Found {len(page_links)} property links")
    except Exception as e:
        print(f"⚠️ Failed to load listings on page {page}: {e}")
        page_links = []

    for link in page_links:
        try:
            driver.get(link)
            wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, ".p24_price")))
            soup = BeautifulSoup(driver.page_source, "html.parser")
            prop_data = extract_property_data(soup, link)
            property_urls.append(prop_data)
            time.sleep(random.uniform(1.2, 2.0))  # mild delay between requests
        except Exception as e:
            print(f"❌ Failed to scrape {link}: {e}")

# Deduplicate by URL
property_urls = list({prop['url']: prop for prop in property_urls}.values())
print(f"🔍 Total unique scraped properties: {len(property_urls)}")

driver.quit()
