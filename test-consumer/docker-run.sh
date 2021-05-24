#!/bin/bash

docker run --name consumer \
    --network confluent-local_default \
    bencornwell/test-consumer \
    java -jar /app/test-consumer.jar \
        test-topic \
        null \
        null \
        null \
        null \
        broker:9092 \
        messages.log \
        insecure
