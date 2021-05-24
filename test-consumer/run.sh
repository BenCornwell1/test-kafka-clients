#!/bin/bash

java -jar target/test-consumer-1.0-SNAPSHOT-shaded.jar \
    test-topic \
    user \
    password \
    confluent.jks \
    password \
    kafka.bentest5-403315c8b53cfaaf40d7fd4ee4d91267-0000.eu-gb.containers.appdomain.cloud:443 \
    dest-messages.log \
    confluent

