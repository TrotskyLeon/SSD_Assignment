#necessary to run in Google Colab, which i used
!pip install flask_restful 
!pip install kafka-python flask flask_cors

kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic INFERENCE
kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic EMAIL

#Below is the real app.py
from flask import Flask
from flask_restful import reqparse, abort, Api, Resource
from flask_cors import flask_cors
from kafka import KafkaConsumer, KafkaProducer
import pickle
import numpy as np
import json
from model import NLPModel

app = Flask(__name__)
api = Api(app)

TOPIC_NAME = "INFERENCE"
KAFKA_SERVER = "localhost:9092"

producer = KafkaProducer(bootstrap_servers = KAFKA_SERVER, api_version = (0, 11, 15))

model = NLPModel()

clf_path = 'lib/models/SentimentClassifier.pkl'
with open(clf_path, 'rb') as f:
    model.clf = pickle.load(f)

vec_path = 'lib/models/TFIDFVectorizer.pkl'
with open(vec_path, 'rb') as f:
    model.vectorizer = pickle.load(f)

parser = reqparse.RequestParser()
parser.add_argument('query')

class PredictSentiment(Resource):
    def get(self):
        args = parser.parse_args()
        user_query = args['query']

        uq_vectorized = model.vectorizer_transform(
            np.array([user_query]))
        prediction = model.predict(uq_vectorized)
        pred_proba = model.predict_proba(uq_vectorized)

        if prediction == 0:
            pred_text = 'Negative'
        else:
            pred_text = 'Positive'
            

        confidence = round(pred_proba[0], 3)

        output = {'prediction': pred_text, 'confidence': confidence}
        
        return output

api.add_resource(PredictSentiment, '/')

@app.route('/kafka/pushToConsumers', methods=['POST'])
def kafkaProducer():
    req = request.get_json()
    json_payload = str.encode(json_dumps(req))
    producer.send(TOPIC_NAME, json_payload)
    producer.flush()
    print("Data sent to consumer")
    return jsonify({"message": "The prediction will also be sent to your email",
        "status": "Pass"})

if __name__ == '__main__':
    app.run(debug=True)