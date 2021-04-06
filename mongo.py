import pymongo
import pymongo.errors as mongoerrors
import pandas as pd
import datetime
import uuid


POSTS_COLUMNS = [
    'post_uuid',
    'url',
    'post_date',
    'comments_number',
    'votes_number',
    'post_category',
]
USERS_COLUMNS = [
    'username',
    'user_karma',
    'user_cakeday',
    'post_karma',
    'comment_karma',
]


def load_data(db, filename):
    df = pd.read_csv(filename, sep=';')
    posts = df[POSTS_COLUMNS].copy()
    users = df[USERS_COLUMNS].copy()
    users.drop_duplicates('username', ignore_index=True, inplace=True)
    posts['user_id'] = df.username.apply(
        lambda username: users.loc[users.username == username].index[0].item()
    )
    posts['post_date'] = posts.post_date.apply(
        lambda date: datetime.datetime.strptime(date, '%d-%m-%y')
    )
    posts = posts.rename(columns={'post_uuid': '_id'})
    users['_id'] = users.index
    users['user_cakeday'] = users.user_cakeday.apply(
        lambda date: datetime.datetime.strptime(date, '%d-%m-%y')
    )
    try:
        db.posts.insert_many(posts.to_dict(orient='records'))
    except mongoerrors.BulkWriteError as e:
        print(e)
    try:
        db.users.insert_many(users.to_dict(orient='records'))
    except mongoerrors.BulkWriteError as e:
        print(e)


def get_all_data(db):
    posts = pd.DataFrame(db.posts.find())
    users = pd.DataFrame(db.users.find())
    users = users.rename(columns={'_id': 'user_id'})
    result = pd.merge(posts, users, on='user_id')
    result = result.rename(columns={'_id': 'post_uuid'})
    return result.drop(['user_id'], axis=1)


def get_data_by_uuid(db, uuid):
    post = db.posts.find_one({'_id': uuid})
    if post is None:
        raise RuntimeError('No such post')
    user = db.users.find_one({'_id': post['user_id']})
    if user is None:
        raise RuntimeError('No such user')
    del user['_id']
    del post['user_id']
    post['post_uuid'] = post['_id']
    del post['_id']
    post['post_date'] = post['post_date'].strftime('%d-%m-%y')
    user['user_cakeday'] = user['user_cakeday'].strftime('%d-%m-%y')
    return {**post, **user}


def insert_data(db, data):
    # get uuid
    # create post
    user = db.users.find_one({'username': data['username']})
    if user is None:
        user_data = {col: data[col] for col in USERS_COLUMNS}
        user_data['_id'] = db.users.count()
        user_data['user_cakeday'] = datetime.datetime.strptime(
            user_data['user_cakeday'], '%d-%m-%y'
        )
        db.users.insert_one(user_data)
        user_id = user_data['_id']
    else:
        user_id = user['_id']
    post_data = {col: data[col] for col in POSTS_COLUMNS if col != 'post_uuid'}
    post_data['_id'] = uuid.uuid1().hex
    post_data['user_id'] = user_id
    post_data['post_date'] = datetime.datetime.strptime(
        post_data['post_date'], '%d-%m-%y'
    )
    db.posts.insert_one(post_data)
    return post_data['_id']


if __name__ == '__main__':
    client = pymongo.MongoClient('localhost', 27017)
    db = client['redditdb']
    data = {
        'url': '/r/memes/comments/m1w02p/same_energy/',
        'username': 'heheboi',
        'user_karma': 259334,
        'user_cakeday': '20-09-20',
        'post_karma': 218415,
        'comment_karma': 6223,
        'post_date': '10-03-21',
        'comments_number': 976,
        'votes_number': 153000,
        'post_category': 'memes',
    }
    inserted_uuid = insert_data(db, data)
    print(inserted_uuid)
