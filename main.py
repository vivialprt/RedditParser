from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.keys import Keys


def get_post_url(post):
    for a in post.find_all('a'):
        link = a.get('href')
        if link.startswith('https'):
            return link
    return None


def get_post_username(post):
    for a in post.find_all('a'):
        link = a.get('href')
        if link.startswith('/user/'):
            return link.split('/')[-2]
    return None


def get_user_info(username, driver):
    link = 'https://www.reddit.com/user/' + username
    # open tab
    driver.find_element_by_tag_name('body').send_keys(Keys.CONTROL + 't')

    # Load a page
    driver.get(link)
    # resp = driver.execute_script('return document.documentElement.outerHTML')
    print(driver.current_url)

    # close the tab
    driver.find_element_by_tag_name('body').send_keys(Keys.CONTROL + 'w')


if __name__ == '__main__':
    options = webdriver.FirefoxOptions()
    options.add_argument('--headless')
    driver = webdriver.Firefox(options=options)
    driver.get('https://www.reddit.com/top/?t=month')
    resp = driver.execute_script('return document.documentElement.outerHTML')
    soup = BeautifulSoup(resp, 'html.parser')
    posts = soup.find('div', class_='rpBJOHq2PR60pnwJlUyP0').contents
    urls = [get_post_url(post) for post in posts]
    usernames = [get_post_username(post) for post in posts]
    get_user_info(usernames[0], driver)
    driver.quit()
