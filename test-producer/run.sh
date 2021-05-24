#!/bin/bash

java -jar target/test-producer-1.0-SNAPSHOT-shaded.jar \
    test-topic \
    user \
    password \
    confluent.jks \
    kafka.bentest5-403315c8b53cfaaf40d7fd4ee4d91267-0000.eu-gb.containers.appdomain.cloud:443 \
    password \
    100 \
    confluent 

