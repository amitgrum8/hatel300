from flask import Flask, jsonify, request
from flask.views import MethodView
from src.KafkaMircoService.kafkaHandler import KafkaHandler
from src.preProcessMicroService.preProcessAndUploadToKafka import PreProcessAndUploadToKafka
from src.dbMicroService.PostgresInsertion import PostgresInsertionService
import os
import threading

app = Flask(__name__)
kafka_handler = KafkaHandler()

kafka_server = os.getenv("KAFKA_SERVER", "localhost:9092")
kafka_topic = os.getenv("KAFKA_TOPIC", "processed_data_one")
preprocessor = PreProcessAndUploadToKafka(kafka_server, kafka_topic)
postgres_service = PostgresInsertionService(kafka_handler)


class StartPipelineAPI(MethodView):
    def post(self):
        pipeline_thread = threading.Thread(target=preprocessor.start_pipeline)
        pipeline_thread.start()
        return jsonify({"message": "Pipeline started"}), 202


class ConsumeAndInsertAPI(MethodView):
    def post(self):
        data = request.get_json()
        topic = data.get('topic')
        consume_thread = threading.Thread(target=postgres_service.consume_and_insert, args=(topic,))
        consume_thread.start()
        return jsonify({"message": "Consumption and insertion started"}), 202


# Add endpoints to the application
app.add_url_rule('/start_pipeline', view_func=StartPipelineAPI.as_view('start_pipeline_api'))
app.add_url_rule('/consume_insert', view_func=ConsumeAndInsertAPI.as_view('consume_insert_api'))

if __name__ == '__main__':
    app.run(debug=True)
