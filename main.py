import json
from pymongo import MongoClient, DESCENDING
from kafka import KafkaConsumer
from pymongo.errors import DuplicateKeyError


if __name__ == '__main__':
    client = MongoClient("localhost", 27017)
    db = client.vinvehicle_batch
    db.batchSimple.create_index([('VinVehicle', 1), ('Timestamp', 1)], unique=True)
    consumer = KafkaConsumer(
        'batch',
        bootstrap_servers='localhost:9092',
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        auto_offset_reset='earliest'
    )
    count = 1
    for msg in consumer:
        print(f"fetch message {count}")
        doc = msg.value
        doc['Position'] = {
            "lon": doc["Position.lon"],
            "lat": doc["Position.lat"],
            "altitude": doc["Position.altitude"],
            "heading": doc["Position.heading"],
            "speed": doc["Position.speed"],
            "satellites": doc["Position.satellites"]
        }
        del doc["Position.lon"]
        del doc["Position.lat"]
        del doc["Position.altitude"]
        del doc["Position.heading"]
        del doc["Position.speed"]
        del doc["Position.satellites"]

        try:
            res = db.batchSimple.insert_one(doc)
            print(f"inserted {res.inserted_id} into batchSimple")
        except DuplicateKeyError as dke:
            print(dke)

        msg_complex = msg.value
        found = False
        for doc in db.batchComplex.find({'VinVehicle': doc['VinVehicle']}).sort([('_id', DESCENDING)]):
            found = True
            lastOdometer = doc['Odometer']
            lastLifeConsumption = doc['LifeConsumption']
            msg_complex['DeltaOdometer'] = lastOdometer - doc['Odometer']
            msg_complex['DeltaLifeConsumption'] = lastLifeConsumption - doc['LifeConsumption']
            break

        if not found:
            msg_complex['DeltaOdometer'] = doc['Odometer']
            msg_complex['DeltaLifeConsumption'] = doc['LifeConsumption']

        res = db.batchComplex.insert_one(msg_complex)
        print(f"inserted {res.inserted_id} into batchComplex")
        count = count + 1

