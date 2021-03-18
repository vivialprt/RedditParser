import time
from parser import RedditParser


if __name__ == '__main__':

    start = time.time()
    parser = RedditParser('https://www.reddit.com/top/?t=month', 50)
    posts = parser.get_posts_data()
    print(f'Elapsed time: {time.time() - start:.3f}s')
    print(posts)
