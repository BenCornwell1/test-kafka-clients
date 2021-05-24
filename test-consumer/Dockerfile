FROM openjdk:latest
 
COPY target/test-consumer-1.0-SNAPSHOT-shaded.jar /app/test-consumer.jar

# set the startup command to execute the jar
CMD ["java", "-jar", "/app/test-consumer.jar"]