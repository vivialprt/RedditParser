import psycopg2 as pg
from psycopg2 import sql
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
    return data['post_uuid']


def translate_keys(to, data):
    # json, db
    pairs = [
        ('url', 'url'),
        ('post_date', 'post_date'),
        ('comments_number', 'comments'),
        ('votes_number', 'votes'),
        ('post_category', 'category'),
        ('username', 'name'),
        ('user_karma', 'total_karma'),
        ('user_cakeday', 'cake_day'),
        ('post_karma', 'post_karma'),
        ('comment_karma', 'comment_karma')
    ]
    idx = 1 if to == 'db' else 0
    return {
        pair[idx]: v for pair in pairs
        if (v := data.get(pair[idx ^ 1])) is not None
    }


def update_data(cursor, uuid, data):
    cursor.execute(
        'SELECT user_id FROM posts WHERE uuid = %s;', [uuid]
    )
    if cursor.fetchone() is None:
        raise RuntimeError('No such post.')

    post_keys = [
        'url',
        'post_date',
        'comments_number',
        'votes_number',
        'post_category',
    ]
    user_keys = [
        'username',
        'user_karma',
        'user_cakeday',
        'post_karma',
        'comment_karma'
    ]
    post_data = translate_keys(
        to='db', data={k: data[k] for k in data.keys() if k in post_keys}
    )
    user_data = translate_keys(
        to='db', data={k: data[k] for k in data.keys() if k in user_keys}
    )

    if post_data:
        sql_query = sql.SQL(
            'UPDATE posts SET {data} WHERE uuid = {id}'
        ).format(
            data=sql.SQL(', ').join(sql.Composed([
                sql.Identifier(k),
                sql.SQL(" = "),
                sql.Placeholder(k)
            ]) for k in post_data.keys()),
            id=sql.Placeholder('id')
        )
        post_data.update(id=uuid)
        cursor.execute(sql_query, post_data)

    if user_data:
        cursor.execute(
            'SELECT user_id FROM posts WHERE uuid = %s;', [uuid]
        )
        user_id = cursor.fetchone()[0]
        sql_query = sql.SQL(
            'UPDATE users SET {data} WHERE id = {id}'
        ).format(
            data=sql.SQL(', ').join(sql.Composed([
                sql.Identifier(k),
                sql.SQL(" = "),
                sql.Placeholder(k)
            ]) for k in user_data.keys()),
            id=sql.Placeholder('id')
        )
        user_data.update(id=user_id)
        cursor.execute(sql_query, user_data)


def delete_data(cursor, uuid):
    cursor.execute(
        'DELETE FROM posts WHERE uuid = %s', [uuid]
    )


def connect_to_redditdb(user, password):
    return pg.connect(
        dbname='redditdb',
        user=user,
        password=password,
        host='localhost'
    )


if __name__ == '__main__':
    conn = pg.connect(
        dbname='redditdb',
        user='',
        password='',
        host='localhost'
    )
    cursor = conn.cursor()

    data = {
        'user_cakeday': '20-09-20',
        'post_karma': 218415,
        'votes_number': 153000,
        'post_category': 'memes',
    }
    update_data(cursor, 'ba7796c888d011eb877701c07849de8f', data)
    conn.commit()

    delete_data(cursor, '849b822697a811eba80d4167c3822297')
    conn.commit()

    cursor.close()
    conn.close()
