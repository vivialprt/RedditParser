import time

from parser import RedditParser


if __name__ == '__main__':

    output_file = f"reddit-{time.strftime('%Y%m%d%H%M')}.csv"  # noqa: E228
    start = time.time()
    parser = RedditParser('https://www.reddit.com/top/?t=month')
    posts = parser.get_posts_data(100, verbose=True)
    print(f'Elapsed time: {time.time() - start:.3f}s')
    posts.to_csv(output_file, sep=';', index=False)
