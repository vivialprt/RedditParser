from bs4 import BeautifulSoup
from selenium import webdriver


if __name__ == '__main__':
    driver = webdriver.Firefox()
    driver.get('https://www.reddit.com/top/?t=month')
    resp = driver.execute_script('return document.documentElement.outerHTML')
    soup = BeautifulSoup(resp, 'html.parser')
    posts = soup.find('div', class_='rpBJOHq2PR60pnwJlUyP0').contents
    with open('out.html', 'w') as f:
        f.write(soup.find('div', class_='rpBJOHq2PR60pnwJlUyP0').prettify())
    driver.quit()
