import psycopg2 as pg


def create_users(cursor):
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS users (
            id serial PRIMARY KEY,
            name varchar(100),
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


def load_data(cursor, filename):
    with open(filename) as f:
        next(f)  # skip header
        cursor.execute('''
            CREATE TEMPORARY TABLE t (
                uuid char(32) PRIMARY KEY,
                url varchar(100),
                username varchar(100),
                user_karma integer,
                cake_day date,
                post_karma integer,
                comment_karma integer,
                post_date date,
                comments integer,
                votes integer,
                category varchar(100)
            );
        ''')
        cursor.copy_from(f, 't', sep=';')

        cursor.execute('''
            INSERT INTO users (
                name,
                total_karma,
                cake_day,
                post_karma,
                comment_karma
            )
            SELECT
                username,
                user_karma,
                cake_day,
                post_karma,
                comment_karma
            FROM t;
        ''')

        cursor.execute('''
            DELETE FROM users
            WHERE id NOT IN (
                SELECT MIN(id) as id
                FROM users
                GROUP BY name
            )
        ''')

        cursor.execute('''
            INSERT INTO posts (
                uuid,
                url,
                user_id,
                post_date,
                comments,
                votes,
                category
            )
            SELECT
                t.uuid,
                t.url,
                u.id,
                post_date,
                comments,
                votes,
                category
            FROM t
            JOIN users as u
            ON t.username = u.name;
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

    load_data(cursor, 'reddit-202103191932.csv')
    conn.commit()

    cursor.close()
    conn.close()
