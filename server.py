from flask import Flask, jsonify, request, abort
from flask import g
from flask.views import MethodView
import postgres as pg


app = Flask(__name__)


def get_connection():
    if not hasattr(g, 'conn'):
        g.conn = pg.connect_to_redditdb('ivan2', 'qweqwe')
    return g.conn


@app.teardown_appcontext
def close_db_connection(error):
    if hasattr(g, 'conn'):
        g.conn.close()


class PostAPI(MethodView):

    def __init__(self):
        self.data_keys = set([
            'url',
            'post_date',
            'comments_number',
            'votes_number',
            'post_category',
            'username',
            'user_karma',
            'user_cakeday',
            'post_karma',
            'comment_karma'
        ])
        self.cursor = get_connection().cursor()
        super().__init__()

    def __del__(self):
        self.cursor.close()

    def get(self, post_id):
        if post_id is None:
            return jsonify(pg.get_all_data(self.cursor))
        else:
            try:
                result = pg.get_data_by_uuid(self.cursor, post_id)
            except RuntimeError:
                abort(404)
            return jsonify(result)

    def post(self):
        if not request.json:
            abort(400, description='No JSON specified.')
        data = request.json
        if set(data.keys()) != self.data_keys:
            abort(400, description='Insufficient data.')
        index = pg.insert_data(self.cursor, data)
        get_connection().commit()
        return jsonify({'post_uuid': index}), 201

    def delete(self, post_id):
        pg.delete_data(self.cursor, post_id)
        get_connection().commit()
        return {}, 200

    def put(self, post_id):
        if not request.json:
            abort(400, description='No JSON specified.')
        data = request.json
        try:
            pg.update_data(self.cursor, post_id, data)
        except RuntimeError:
            abort(404, description='No such post.')
        get_connection().commit()
        return {}, 200


post_view = PostAPI.as_view('post_api')
app.add_url_rule(
    '/posts/', defaults={'post_id': None},
    view_func=post_view, methods=['GET']
)
app.add_url_rule('/posts/', view_func=post_view, methods=['POST'])
app.add_url_rule(
    '/posts/<post_id>', view_func=post_view,
    methods=['GET', 'PUT', 'DELETE']
)


if __name__ == '__main__':
    app.run()
