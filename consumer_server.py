import logging
from kafka import KafkaConsumer
import time

def run_kafka_consumer(consumer_obj):
    logger = logging.getLogger(__name__)
    consumer_obj.subscribe(["police-department-calls-for-service"])
    for msg in consumer:
        try:
            print(msg.value)
        except Exception as e:
            logger.debug(e) 
    return
            

if __name__ == "__main__":
    logger = logging.getLogger(__name__)
    consumer = KafkaConsumer(bootstrap_servers="localhost:9092",
                             auto_offset_reset='earliest',
                             group_id='kafka-python-consumer-#1')
    try:
        run_kafka_consumer(consumer)
    except KeyboardInterrupt:
        consumer.close()
        logger.info("consumer exited")
        time.sleep(5)
    
    