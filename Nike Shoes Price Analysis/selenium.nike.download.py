#Load Required Modules
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import time
import csv

try:
    #use Chrome() in webdriver object, Chrome() can browse the URL
    driver = webdriver.Chrome() 
    driver.get("https://www.nike.com/tw/w/shoes-y7ok")
    
    #wait for the web page to load
    #until at least one element with the class attribute "product-card__link-overlay" appears on the page
    WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CLASS_NAME, "product-card__link-overlay")))
    
    #create a CSV file
    with open('NIKE.Sports.and.Leisure.csv', 'w', newline='', encoding='utf-8-sig') as csvfile:

        #defines the column name of the CSV file[column1, column2...columnN]
        fieldnames = ['Product_Name', 'Product_Price','Number_of_Color']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        
        #write the header row of the CSV file
        writer.writeheader()

        #get the height of the current page
        prev_page_height = driver.execute_script("return document.body.scrollHeight")

        for i in range(20):

            #use find_element() method to locate the <body> tag in HTML
            #use send_keys() method to scroll to the bottom of the page
            driver.find_element(By.TAG_NAME, 'body').send_keys(Keys.END)

            #wait for some time to let the page load
            time.sleep(8)
            
            #use execute_script() method to conduct "return document.body.scrollHeight"
            #it can get the current page height after scrolling the page
            curr_page_height = driver.execute_script("return document.body.scrollHeight")

            #check if page height remains the same after scrolling
            #stop scrolling if curr_page_height = prev_page_height
            if curr_page_height == prev_page_height:
                break  
            
            #update the previous page height
            prev_page_height = curr_page_height

            #parse the updated page source with BeautifulSoup
            soup = BeautifulSoup(driver.page_source, 'html.parser')

            #find all product names, product prices and number of color
            product_names = soup.find_all("a", class_="product-card__link-overlay")
            product_prices = soup.find_all("div", class_="product-price tw__styling is--current-price css-11s12ax")
            number_of_colors = soup.find_all("div", class_="product-card__product-count")

            #write product_name, product_price and number_of_color to CSV file
            for product_name, product_price, number_of_color in zip(product_names, product_prices, number_of_colors):
                writer.writerow({'Product_Name': product_name.text, 'Product_Price': product_price.text, 'Number_of_Color': number_of_color.text })
finally:
    driver.quit()
