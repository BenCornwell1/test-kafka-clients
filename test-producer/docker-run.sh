#!/bin/bash

docker run --name producer \
    --network confluent-local_default \
    bencornwell/test-producer \
    java -jar /app/test-producer.jar \
        test-topic \
        null \
        null \
        null \
        broker:9092 \
        null \
        100 \
        insecure
