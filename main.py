import time

from parser import RedditParser


if __name__ == '__main__':

    start = time.time()
    parser = RedditParser('https://www.reddit.com/top/?t=month')
    posts = parser.get_posts_data(10, True)
    print(f'Elapsed time: {time.time() - start:.3f}s')
    posts.to_csv('out.csv', sep=';', index=False)
