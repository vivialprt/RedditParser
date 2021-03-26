from flask import Flask, jsonify, request, abort, Response
from flask.views import MethodView
import pandas as pd


app = Flask(__name__)


class PostAPI(MethodView):

    def __init__(self):
        self.filename = 'reddit-202103191932.csv'
        self.data = pd.read_csv(self.filename, sep=';')
        self.data_keys = set(self.data.columns)
        super().__init__()

    def get(self, post_id):
        if post_id is None:
            return Response(
                response=self.data.to_json(orient='records'),
                status=200,
                mimetype='application/json'
            )
        else:
            result = self.data.loc[self.data.post_uuid == post_id].iloc[0]
            if result.empty:
                abort(404)
            return Response(
                response=result.to_json(),
                status=200,
                mimetype='application/json'
            )

    def post(self):
        if not request.json:
            abort(400, description='No JSON specified.')
        data = request.json
        if set(data.keys()) != self.data_keys:
            abort(400, description='Insufficient data.')
        post_id = data['post_uuid']
        if post_id in self.data['post_uuid'].values:
            abort(400, description='Post with such ID is already exist.')

        self.data = self.data.append(data, ignore_index=True)
        self.data.to_csv(self.filename, sep=';', index=False)
        index = int(self.data.loc[self.data.post_uuid == post_id].index[0])

        return jsonify({data['post_uuid']: index}), 201

    def delete(self, post_id):
        if post_id not in self.data['post_uuid'].values:
            return {}, 200

        index = self.data.loc[self.data.post_uuid == post_id].index[0]
        self.data.drop(index=index, inplace=True)
        self.data.to_csv(self.filename, sep=';', index=False)
        return {}, 200

    def put(self, post_id):
        if not request.json:
            abort(400, description='No JSON specified.')
        data = request.json
        if post_id not in self.data['post_uuid'].values:
            abort(404, description='Post with such ID does not exist.')

        index = self.data.loc[self.data.post_uuid == post_id].index[0]
        for key in data:
            self.data[key][index] = data[key]
        self.data.to_csv(self.filename, sep=';', index=False)
        return Response(
                response=self.data.iloc[index].to_json(),
                status=200,
                mimetype='application/json'
        )


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
