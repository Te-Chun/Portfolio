# Import required modules
import csv
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup

# To access eBay's web page using the Chrome WebDriver and configure the User-Agent
url = "https://www.ebay.com/sch/i.html?_from=R40&_trksid=p4432023.m570.l1313&_nkw=laptop&_sacat=0"
user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"

# Set the User-Agent string
options = webdriver.ChromeOptions()
options.add_argument(f"user-agent={user_agent}")

# Launch the Chrome WebDriver with the configured options
driver = webdriver.Chrome(options=options)
driver.get(url)

try:
    # Create a CSV file
    with open('project.csv', 'w', newline='', encoding='utf-8-sig') as csvfile:
        # Defines the column name of the CSV file
        fieldnames = ["Product_Name", "Product_Price", "Place_of_shipment", "Review", "Shipping_Cost"]
        writer = csv.writer(csvfile)
        # Write the header row of the CSV file
        writer.writerow(fieldnames)
        
        # Loop through each page (total 50 pages, starting from page 2)
        for i in range(50):
            # Wait for all the product titles to be present
            WebDriverWait(driver, 10).until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, ".s-item__title")))
            
            # Scroll the website to the bottom of the page
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            
            # Wait again for all the product titles after scrolling
            WebDriverWait(driver, 10).until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, ".s-item__title")))
            
            # Use BeautifulSoup to parse the current page source
            soup = BeautifulSoup(driver.page_source, 'html.parser')

            # Find the required elements
            product_items = soup.find_all("div", class_="s-item__info clearfix")
            for item in product_items:
                try:
                    product_name = item.find("div", class_="s-item__title").text.strip()
                    product_price = item.find("span", class_="s-item__price").text.strip()
                    place_of_shipment = item.find("span", class_="s-item__location s-item__itemLocation").text.strip()
                    review = item.find("span", class_="s-item__seller-info-text").text.strip()
                    shipping_cost = item.find("span", class_="s-item__shipping s-item__logisticsCost").text.strip()

                    # Write item details to CSV file
                    writer.writerow([product_name, product_price, place_of_shipment, review, shipping_cost])
                except AttributeError:
                    continue
            
            if i < 49:
                button_text = str(i + 2)
                button = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.LINK_TEXT, button_text)))
                button.click()            
finally:
    driver.quit()
