from flask import Flask, jsonify, request
from src.KafkaMircoService.kafkaHandler import create_topic
from src.preProcessMicroService.preProcessAndUploadToKafka import start_pipline
from src.dalteLakeMicorService.daltelLake import read_from_lake

app = Flask(__name__)


@app.route('/start', methods=['POST'])
def get_all_dfs_api():
    start_pipline()


@app.route('/get_df', methods=['POST'])
def get_df():
    data = request.get_json()
    file_name = data.get('nameOfFile')
    df = read_from_lake(file_name)
    result = df.to_json(orient='records')

    return jsonify(result)


@app.route('/create_topic', methods=['POST'])
def get_df():
    data = request.get_json()
    topic_name = data.get('topic_name')
    res = create_topic(topic_name)
    result = jsonify(res)

    return jsonify(result)


if __name__ == '__main__':
    app.run(debug=True)
