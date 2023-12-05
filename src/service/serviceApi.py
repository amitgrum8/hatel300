from flask import Flask, jsonify, request
from flask.views import MethodView
from src.KafkaMircoService.kafkaHandler import KafkaHandler

app = Flask(__name__)


class InitAPI(MethodView):
    def __init__(self):
        self.kafkaHandler = KafkaHandler()


class GetDFAPI(MethodView):
    def post(self):
        data = request.get_json()
        file_name = data.get('nameOfFile')
        df = read_from_lake(file_name)
        result = df.to_json(orient='records')
        return jsonify(result), 200


class CreateTopicAPI(MethodView):
    def post(self):
        data = request.get_json()
        topic_name = data.get('topic_name')
        res = create_topic(topic_name)
        return jsonify(res), 200


app.add_url_rule('/init', view_func=InitAPI.as_view('init_api'))
app.add_url_rule('/', view_func=GetDFAPI.as_view('get_df_api'))
app.add_url_rule('/create_topic', view_func=CreateTopicAPI.as_view('create_topic_api'))

if __name__ == '__main__':
    app.run(debug=True)
