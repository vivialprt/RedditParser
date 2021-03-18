from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
import datetime
import time
import pandas as pd


class RedditParser:
    """
    Performs parsing from reddit
    and compiles all data to pandas DataFrame.
    """
    def __init__(self, link: str, num_posts: int = 10):
        self.link = link
        self.num_posts = num_posts

        options = webdriver.FirefoxOptions()
        options.add_argument('--headless')
        self.driver = webdriver.Firefox(options=options)
        self.driver.get(self.link)
        self.driver.execute_script('return document.documentElement.outerHTML')

    def __del__(self):
        self.driver.quit()

    def get_posts_data(self):
        posts = []
        seen_posts = 0
        seen_urls = []
        while True:
            soup = BeautifulSoup(self.driver.page_source, 'html.parser')
            current_seen_posts = soup.find(
                'div', class_='rpBJOHq2PR60pnwJlUyP0'
            ).contents[seen_posts:]
            for post in current_seen_posts:
                seen_posts += 1
                data = {}
                data['url'] = self._get_post_url(post)
                if data['url'] is None or data['url'] in seen_urls:
                    continue
                data['username'] = self._get_post_username(post)
                if data['username'] is None:
                    continue
                user_data = self._get_user_info(data['username'])
                if None in user_data:
                    continue
                data['user_karma'] = user_data[0]
                data['user_cakeday'] = user_data[1]
                data['post_karma'] = user_data[2]
                data['comment_karma'] = user_data[3]
                data['post_category'] = self._get_post_category(post)
                data['comments_number'] = self._get_comments_number(post)
                data['votes_number'] = self._get_votes_number(post)
                data['post_date'] = self._get_post_date(post)

                seen_urls.append(data['url'])
                posts.append(data)
                if len(posts) == self.num_posts:
                    print(len(posts), f'({seen_posts})')
                    break
                print(len(posts), f'({seen_posts})')
            if len(posts) >= self.num_posts:
                break
            self.driver.find_element_by_tag_name('body').send_keys(Keys.END)
        return pd.DataFrame(posts)

    def _get_post_url(self, post):
        for a in post.find_all('a'):
            link = a.get('href')
            if link.startswith('https'):
                return link
        return None

    def _get_post_username(self, post):
        for a in post.find_all('a'):
            link = a.get('href')
            if link.startswith('/user/'):
                return link.split('/')[-2]
        return None

    def _get_post_category(self, post):
        for a in post.find_all('a'):
            link = a.get('href')
            if link.startswith('/r/'):
                return link.split('/')[-2]
        return None

    def _get_comments_number(self, post):
        comment_number_div = post.find('span', class_='FHCV02u6Cp2zYL0fhQPsO')
        if comment_number_div is None:
            return None
        else:
            comment_number_raw = comment_number_div.text.split()[0]
        if 'k' in comment_number_raw:
            return int(float(comment_number_raw.split('k')[0]) * 1000)
        return int(comment_number_raw)

    def _get_votes_number(self, post):
        votes_number_div = post.find('div', class_='_1rZYMD_4xY3gRcSS3p8ODO')
        if votes_number_div is None:
            return None
        if 'k' in votes_number_div.text:
            return int(votes_number_div.text.split('k')[0]) * 1000
        return int(votes_number_div.text)

    def _get_post_date(self, post):
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

    def _get_user_info(self, username):
        if username is None:
            return None, None, None, None
        link = 'https://www.reddit.com/user/' + username
        # open tab
        self.driver.execute_script("window.open('');")
        self.driver.switch_to.window(self.driver.window_handles[1])
        self.driver.get(link)

        # Load a page
        self.driver.execute_script('return document.documentElement.outerHTML')
        time.sleep(2)

        try:
            karma_span = self.driver.find_element_by_id(
                'profile--id-card--highlight-tooltip--karma'
            )
        except NoSuchElementException:
            self.driver.close()
            self.driver.switch_to.window(self.driver.window_handles[0])
            return None, None, None, None

        hover = ActionChains(self.driver).move_to_element(karma_span)
        hover.perform()
        soup = BeautifulSoup(self.driver.page_source, 'html.parser')

        user_karma = int(soup.find(
            'span', id='profile--id-card--highlight-tooltip--karma'
        ).text.replace(',', ''))

        user_cakeday_string = soup.find(
            'span', id='profile--id-card--highlight-tooltip--cakeday'
        ).text
        user_cakeday = datetime.datetime.strptime(
            user_cakeday_string, '%B %d, %Y'
        )

        while soup.find(class_='_3uK2I0hi3JFTKnMUFHD2Pd') is None:
            time.sleep(1)
            soup = BeautifulSoup(self.driver.page_source, 'html.parser')

        karma_extended = soup.find(
            class_='_3uK2I0hi3JFTKnMUFHD2Pd'
        ).text.split('\n')
        post_karma = int(karma_extended[0].split()[0].replace(',', ''))
        comment_karma = int(karma_extended[1].split()[0].replace(',', ''))

        # close the tab
        self.driver.close()
        self.driver.switch_to.window(self.driver.window_handles[0])
        return user_karma, user_cakeday, post_karma, comment_karma
