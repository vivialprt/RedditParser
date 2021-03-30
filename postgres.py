import psycopg2 as pg
import uuid


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


def get_all_data(cursor):
    cursor.execute('''
        SELECT * FROM posts;
    ''')
    post_result = cursor.fetchall()
    if len(post_result) == 0:
        raise RuntimeError('No posts!')
    data = []
    for row in post_result:
        post = {}
        post['post_uuid'] = row[0]
        post['url'] = row[1]
        post['user_id'] = row[2]
        post['post_date'] = row[3].strftime('%d-%m-%y')
        post['comments_number'] = row[4]
        post['votes_number'] = row[5]
        post['post_category'] = row[6]

        cursor.execute('''
            SELECT * FROM users
            WHERE id = %s;
        ''', [post['user_id']])
        user_result = cursor.fetchone()
        if user_result is None:
            raise RuntimeError('No such user. How is it possible?')
        post['username'] = user_result[1]
        post['user_karma'] = user_result[2]
        post['user_cakeday'] = user_result[3].strftime('%d-%m-%y')
        post['post_karma'] = user_result[4]
        post['comment_karma'] = user_result[5]
        del post['user_id']
        data.append(post)
    return data


def get_data_by_uuid(cursor, uuid):
    cursor.execute('''
        SELECT * FROM posts
        WHERE uuid = %s;
    ''', [uuid])
    post_result = cursor.fetchone()
    if post_result is None:
        raise RuntimeError('No such post')
    data = {}
    data['post_uuid'] = post_result[0]
    data['url'] = post_result[1]
    data['user_id'] = post_result[2]
    data['post_date'] = post_result[3].strftime('%d-%m-%y')
    data['comments_number'] = post_result[4]
    data['votes_number'] = post_result[5]
    data['post_category'] = post_result[6]

    cursor.execute('''
        SELECT * FROM users
        WHERE id = %s;
    ''', [data['user_id']])
    user_result = cursor.fetchone()
    if user_result is None:
        raise RuntimeError('No such user. How is it possible?')
    data['username'] = user_result[1]
    data['user_karma'] = user_result[2]
    data['user_cakeday'] = user_result[3].strftime('%d-%m-%y')
    data['post_karma'] = user_result[4]
    data['comment_karma'] = user_result[5]
    del data['user_id']
    return data


def insert_data(cursor, data):
    cursor.execute('''
        SELECT id FROM users
        WHERE name = %(username)s;
    ''', data)
    result = cursor.fetchall()
    if len(result) == 0:  # no such user
        cursor.execute('''
            INSERT INTO users (
                name,
                total_karma,
                cake_day,
                post_karma,
                comment_karma
            )
            VALUES (
                %(username)s,
                %(user_karma)s,
                %(user_cakeday)s,
                %(post_karma)s,
                %(comment_karma)s
            );
        ''', data)
        cursor.execute('''
            SELECT id FROM users
            WHERE name = %(username)s;
        ''')
        data['user_id'] = cursor.fetchall()[0][0]
    else:
        data['user_id'] = result[0][0]

    data['post_uuid'] = uuid.uuid1().hex
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
        VALUES (
            %(post_uuid)s,
            %(url)s,
            %(user_id)s,
            %(post_date)s,
            %(comments_number)s,
            %(votes_number)s,
            %(post_category)s
        );
    ''', data)


if __name__ == '__main__':
    conn = pg.connect(
        dbname='redditdb',
        user='',
        password='',
        host='localhost'
    )
    cursor = conn.cursor()

    data = {
        'url': '/r/memes/comments/m1w02p/same_energy/',
        'username': 'swat_08',
        'user_karma': 259334,
        'user_cakeday': '20-09-20',
        'post_karma': 218415,
        'comment_karma': 6223,
        'post_date': '10-03-21',
        'comments_number': 976,
        'votes_number': 153000,
        'post_category': 'memes',
    }
    insert_data(cursor, data)
    conn.commit()

    retrived = get_all_data(cursor)

    cursor.close()
    conn.close()
