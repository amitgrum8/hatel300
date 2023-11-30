from flask import Flask, jsonify, request
from src.preProcessMicroService.preProcessHandler import start_pipline
from src.dalteLakeMicorService.daltelLakeHandler import read_from_lake
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


app.run(debug=True)

