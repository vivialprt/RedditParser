import psycopg2 as pg


def create_users(cursor):
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS users (
            id integer PRIMARY KEY,
            total_karma integer,
            cake_day date,
            post_karma integer,
            comment_karma integer
        );
    ''')


def create_posts(cursor):
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS posts (
            uuid char(32),
            url varchar(100),
            user_id integer,
            post_date date,
            comments integer,
            votes integer,
            category varchar(100),

            PRIMARY KEY (uuid),
            FOREIGN KEY (user_id)
                REFERENCES users (id)
        );
    ''')


if __name__ == '__main__':
    conn = pg.connect(
        dbname='redditdb',
        user='',
        password='',
        host='localhost'
    )
    cursor = conn.cursor()

    create_users(cursor)
    create_posts(cursor)
    conn.commit()

    cursor.close()
    conn.close()
