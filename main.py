from parser import RedditParser


if __name__ == '__main__':

    parser = RedditParser('https://www.reddit.com/top/?t=month', 50)
    posts = parser.get_posts_data()
    [print(post) for post in posts]
