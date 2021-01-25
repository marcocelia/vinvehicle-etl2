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
        m_simple = msg.value
        m_simple['Position'] = {
            "lon": m_simple["Position.lon"],
            "lat": m_simple["Position.lat"],
            "altitude": m_simple["Position.altitude"],
            "heading": m_simple["Position.heading"],
            "speed": m_simple["Position.speed"],
            "satellites": m_simple["Position.satellites"]
        }
        del m_simple["Position.lon"]
        del m_simple["Position.lat"]
        del m_simple["Position.altitude"]
        del m_simple["Position.heading"]
        del m_simple["Position.speed"]
        del m_simple["Position.satellites"]

        try:
            res = db.batchSimple.insert_one(m_simple)
            print(f"inserted {res.inserted_id} into batchSimple")
        except DuplicateKeyError as dke:
            print(dke)
            continue

        m_complex = msg.value
        if db.batchComplex.count_documents({'VinVehicle': m_complex['VinVehicle']}) > 0:
            for doc in db.batchComplex.find({'VinVehicle': m_complex['VinVehicle']}).sort([('_id', DESCENDING)]).limit(1):
                lastOdometer = doc['Odometer']
                lastLifeConsumption = doc['LifeConsumption']
                m_complex['DeltaOdometer'] = lastOdometer - m_complex['Odometer']
                m_complex['DeltaLifeConsumption'] = lastLifeConsumption - m_complex['LifeConsumption']
        else:
            m_complex['DeltaOdometer'] = m_complex['Odometer']
            m_complex['DeltaLifeConsumption'] = m_complex['LifeConsumption']

        res = db.batchComplex.insert_one(m_complex)
        print(f"inserted {res.inserted_id} into batchComplex")
