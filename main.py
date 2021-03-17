from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
import datetime
import time


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


def get_post_category(post):
    for a in post.find_all('a'):
        link = a.get('href')
        if link.startswith('/r/'):
            return link.split('/')[-2]
    return None


def get_user_info(username, driver):
    link = 'https://www.reddit.com/user/' + username
    # open tab
    driver.find_element_by_tag_name('body').send_keys(Keys.CONTROL + 't')

    # Load a page
    driver.get(link)
    driver.execute_script('return document.documentElement.outerHTML')

    karma_span = driver.find_element_by_id(
        'profile--id-card--highlight-tooltip--karma'
    )
    hover = ActionChains(driver).move_to_element(karma_span)
    hover.perform()
    time.sleep(2)
    soup = BeautifulSoup(driver.page_source, 'html.parser')

    user_karma = int(soup.find(
        'span', id='profile--id-card--highlight-tooltip--karma'
    ).text.replace(',', ''))

    user_cakeday_string = soup.find(
        'span', id='profile--id-card--highlight-tooltip--cakeday'
    ).text
    user_cakeday = datetime.datetime.strptime(user_cakeday_string, '%B %d, %Y')

    karma_extended = soup.find(
        class_='_3uK2I0hi3JFTKnMUFHD2Pd'
    ).text.split('\n')
    post_karma = int(karma_extended[0].split()[0].replace(',', ''))
    comment_karma = int(karma_extended[1].split()[0].replace(',', ''))

    # close the tab
    driver.find_element_by_tag_name('body').send_keys(Keys.CONTROL + 'w')
    return user_karma, user_cakeday, post_karma, comment_karma


def get_comments_number(post):
    comment_number_div = post.find('span', class_='FHCV02u6Cp2zYL0fhQPsO')
    if comment_number_div is None:
        return None
    else:
        comment_number_raw = comment_number_div.text.split()[0]
    if 'k' in comment_number_raw:
        return int(float(comment_number_raw.split('k')[0]) * 1000)
    return int(comment_number_raw)


def get_votes_number(post):
    votes_number_div = post.find('div', class_='_1rZYMD_4xY3gRcSS3p8ODO')
    if votes_number_div is None:
        return None
    if 'k' in votes_number_div.text:
        return int(votes_number_div.text.split('k')[0]) * 1000
    return int(votes_number_div.text)


def get_post_date(post):
    post_date_a = post.find('a', class_='_3jOxDPIQ0KaOWpzvSQo-1s')
    now = datetime.datetime.now()
    if post_date_a is None:
        return None
    else:
        count, units = post_date_a.text.split()[:2]
        count = int(count)
    if units == 'hours':
        return now - datetime.timedelta(hours=count)
    else:
        return now - datetime.timedelta(days=count)


if __name__ == '__main__':
    options = webdriver.FirefoxOptions()
    options.add_argument('--headless')
    driver = webdriver.Firefox(options=options)
    driver.get('https://www.reddit.com/top/?t=month')
    resp = driver.execute_script('return document.documentElement.outerHTML')
    soup = BeautifulSoup(resp, 'html.parser')
    posts = soup.find('div', class_='rpBJOHq2PR60pnwJlUyP0').contents
    votes_nums = [get_votes_number(post) for post in posts]
    comments_nums = [get_comments_number(post) for post in posts]
    urls = [get_post_url(post) for post in posts]
    usernames = [get_post_username(post) for post in posts]
    dates = [get_post_date(post) for post in posts]
    categories = [get_post_category(post) for post in posts]
    print(get_user_info(usernames[0], driver))
    driver.quit()
