import datetime
import time
from typing import Optional, List, Tuple

from bs4 import BeautifulSoup
from bs4.element import Tag
from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
import pandas as pd


class RedditParser:
    """
    Performs parsing from reddit
    and compiles all data to pandas DataFrame.
    """

    POSTS_BLOCK_CLASS = 'rpBJOHq2PR60pnwJlUyP0'
    POST_URL_CLASS = 'SQnoC3ObvgnGjWt90zD9Z'
    USER_URL_CLASS = '_2tbHP6ZydRpjI44J3syuqC'
    POST_CATEGORY_CLASS = '_3ryJoIoycVkA88fy40qNJc'
    COMMENT_NUMBER_CLASS = 'FHCV02u6Cp2zYL0fhQPsO'
    VOTES_NUMBER_CLASS = '_1rZYMD_4xY3gRcSS3p8ODO'
    POST_DATE_CLASS = '_3jOxDPIQ0KaOWpzvSQo-1s'
    USER_BASE_LINK = 'https://www.reddit.com/user/'
    KARMA_SPAN_ID = 'profile--id-card--highlight-tooltip--karma'
    CAKEDAY_SPAN_ID = 'profile--id-card--highlight-tooltip--cakeday'
    KARMA_POPUP_CLASS = '_3uK2I0hi3JFTKnMUFHD2Pd'

    def __init__(self, link: str):
        self.link = link

        options = webdriver.FirefoxOptions()
        options.add_argument('--headless')
        self.driver = webdriver.Firefox(options=options)
        self.driver.get(self.link)
        self.driver.execute_script('return document.documentElement.outerHTML')

    def __del__(self):
        self.driver.quit()

    def get_posts_data(
        self, num_posts: int, verbose: bool = False
    ) -> pd.DataFrame:
        """
        Parses scecified ammount of posts.
        Skips posts of deleted users and 18+ users.
        """
        posts = []
        seen_posts = 0
        seen_urls = []

        while True:
            soup = BeautifulSoup(self.driver.page_source, 'html.parser')
            current_seen_posts = soup.find(
                'div', class_=self.POSTS_BLOCK_CLASS
            ).contents[seen_posts:]
            for post in current_seen_posts:
                seen_posts += 1
                post_data = self._get_post_data(post, seen_urls)
                if post_data is None:
                    continue

                seen_urls.append(post_data['url'])
                posts.append(post_data)

                if verbose:
                    print(
                        f'Parsed posts: {len(posts)} of {num_posts}',
                        f'Seen posts: {seen_posts}'
                    )
                if len(posts) >= num_posts:
                    break
            if len(posts) >= num_posts:
                break
            # scroll to the end of page to load more posts
            self.driver.find_element_by_tag_name('body').send_keys(Keys.END)
        return pd.DataFrame(posts)

    def _get_post_data(
        self, post: Tag, seen_urls: List[str]
    ) -> Optional[dict]:
        data = {}
        data['url'] = self._get_post_url(post)
        if data['url'] is None or data['url'] in seen_urls:
            # not a post or seen post
            return None
        data['username'] = self._get_post_username(post)
        if data['username'] is None:
            # deleted user
            return None
        user_data = self._get_user_info(data['username'])
        if user_data is None:
            # 18+ page
            return None
        data['user_karma'] = user_data[0]
        data['user_cakeday'] = user_data[1]
        data['post_karma'] = user_data[2]
        data['comment_karma'] = user_data[3]
        data['post_category'] = self._get_post_category(post)
        data['comments_number'] = self._get_comments_number(post)
        data['votes_number'] = self._get_votes_number(post)
        data['post_date'] = self._get_post_date(post)
        return data

    def _get_post_url(self, post: Tag) -> Optional[str]:
        post_url_a = post.find('a', class_=self.POST_URL_CLASS)
        if post_url_a is None:  # not a post
            return None
        return post_url_a.get('href')

    def _get_post_username(self, post: Tag) -> Optional[str]:
        user_url_a = post.find('a', class_=self.POST_CATEGORY_CLASS)
        if user_url_a is None:  # not a post
            return None
        return user_url_a.get('href').split('/')[-2]

    def _get_post_category(self, post: Tag) -> Optional[str]:
        post_category_a = post.find('a', class_=self.POST_CATEGORY_CLASS)
        if post_category_a is None:  # not a post
            return None
        return post_category_a.get('href').split('/')[-2]

    def _get_comments_number(self, post: Tag) -> Optional[int]:
        comment_number_div = post.find(
            'span', class_=self.COMMENT_NUMBER_CLASS
        )
        if comment_number_div is None:  # not a post
            return None
        comment_number_raw = comment_number_div.text.split()[0]
        if 'k' in comment_number_raw:  # comments specified in thousands
            return int(float(comment_number_raw[:-1]) * 1000)
        return int(comment_number_raw)

    def _get_votes_number(self, post: Tag) -> Optional[int]:
        votes_number_div = post.find('div', class_=self.VOTES_NUMBER_CLASS)
        if votes_number_div is None:  # not a post
            return None
        if 'k' in votes_number_div.text:  # votes specified in thousands
            return int(votes_number_div.text[:-1]) * 1000
        return int(votes_number_div.text)

    def _get_post_date(self, post: Tag) -> Optional[str]:
        post_date_a = post.find('a', class_=self.POST_DATE_CLASS)
        if post_date_a is None:  # not a post
            return None
        count, units = post_date_a.text.split()[:2]
        count = int(count)
        now = datetime.datetime.now()
        if units == 'hours':
            post_date = now - datetime.timedelta(hours=count)
        else:
            post_date = now - datetime.timedelta(days=count)
        return post_date.strftime('%d-%m-%y')

    def _get_user_info(
        self, username: str
    ) -> Optional[Tuple[int, str, int, int]]:
        link = self.USER_BASE_LINK + username

        # open tab
        self.driver.execute_script("window.open('');")
        self.driver.switch_to.window(self.driver.window_handles[1])

        # load a page
        self.driver.get(link)
        self.driver.execute_script('return document.documentElement.outerHTML')
        time.sleep(2)

        # hover mouse to karma to show karma details
        try:
            karma_span = self.driver.find_element_by_id(self.KARMA_SPAN_ID)
        except NoSuchElementException:  # 18+ page
            self.driver.close()
            self.driver.switch_to.window(self.driver.window_handles[0])
            return None
        hover = ActionChains(self.driver).move_to_element(karma_span)
        hover.perform()
        soup = BeautifulSoup(self.driver.page_source, 'html.parser')

        # get user karma
        user_karma = int(soup.find(
            'span', id=self.KARMA_SPAN_ID
        ).text.replace(',', ''))

        # get user cakeday
        user_cakeday_string = soup.find('span', id=self.CAKEDAY_SPAN_ID).text
        user_cakeday = datetime.datetime.strptime(
            user_cakeday_string, '%B %d, %Y'
        ).strftime('%d-%m-%y')

        # wait until karma details popup appears
        while True:
            karma_popup = soup.find(class_=self.KARMA_POPUP_CLASS)
            if karma_popup is not None:
                break
            time.sleep(1)
            soup = BeautifulSoup(self.driver.page_source, 'html.parser')

        # get post and comment karma
        post_karma_str, comment_karma_str = karma_popup.text.split('\n')[:2]
        post_karma = int(post_karma_str.split()[0].replace(',', ''))
        comment_karma = int(comment_karma_str.split()[0].replace(',', ''))

        # close the tab
        self.driver.close()
        self.driver.switch_to.window(self.driver.window_handles[0])
        return user_karma, user_cakeday, post_karma, comment_karma
