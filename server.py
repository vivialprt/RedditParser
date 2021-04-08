from flask import Flask, jsonify, request, abort
from flask import g
from flask.views import MethodView
import mongo


app = Flask(__name__)


def get_connection():
    if not hasattr(g, 'conn'):
        g.conn = mongo.connect_to_redditdb()
    return g.conn


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
        self.db = get_connection()
        super().__init__()

    def get(self, post_id):
        if post_id is None:
            return jsonify(mongo.get_all_data(self.db))
        else:
            try:
                result = mongo.get_data_by_uuid(self.db, post_id)
            except RuntimeError:
                abort(404)
            return jsonify(result)

    def post(self):
        if not request.json:
            abort(400, description='No JSON specified.')
        data = request.json
        if set(data.keys()) != self.data_keys:
            abort(400, description='Insufficient data.')
        index = mongo.insert_data(self.db, data)
        return jsonify({'post_uuid': index}), 201

    def delete(self, post_id):
        mongo.delete_data(self.db, post_id)
        return {}, 200

    def put(self, post_id):
        if not request.json:
            abort(400, description='No JSON specified.')
        data = request.json
        try:
            mongo.update_data(self.db, post_id, data)
        except RuntimeError:
            abort(404, description='No such post.')
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
