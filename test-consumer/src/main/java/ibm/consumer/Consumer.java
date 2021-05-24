package ibm.consumer;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.Thread.UncaughtExceptionHandler;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;


/**
 * Test class to consume PNR records from Event Streams or Confluent
 *
 */
public class Consumer 
{

    private static String TOPIC_NAME;
    private static String USERNAME;
    private static String PASSWORD;
    private static String TRUST_STORE;
    private static String BOOTSTRAP_SERVERS;
    private static String TRUST_PASS;
    private static String FILENAME;
    private static ClusterType CLUSTER_TYPE;

    private static int POLL_DURATION_SECS = 5;

	private static KafkaConsumer<String, String> consumer;
    private static BufferedWriter outputFile;
    	
	private static final Logger log = Logger.getLogger(Consumer.class);

    public enum ClusterType {
        CONFLUENT,
        EVENTSTREAMS,
        INSECURE
    }

    static {
    	
    	log.info("PID = " + ProcessHandle.current().pid());
    	
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                log.log(Level.INFO, "Shutdown received.");
                cleanup();
            }
        });
        
        Thread.setDefaultUncaughtExceptionHandler(new UncaughtExceptionHandler() {
            @Override
            public void uncaughtException(Thread t, Throwable e) {
                log.log(Level.ERROR, "Uncaught Exception on " + t.getName() + " : " + e, e);
                cleanup();
            }
        });
    }
    
    public static void main( String[] args )
    {	
        TOPIC_NAME= args[0];
        USERNAME = args[1];
        PASSWORD = args[2];
        TRUST_STORE = args[3];
        TRUST_PASS = args[4];
        BOOTSTRAP_SERVERS = args[5];
        FILENAME = args[6];


        switch (args[7]) {
            case "eventstreams":
                CLUSTER_TYPE = ClusterType.EVENTSTREAMS;
                break;
            case "confluent":
                CLUSTER_TYPE = ClusterType.CONFLUENT;
                break;
            case "insecure":
                CLUSTER_TYPE = ClusterType.INSECURE;
                break;
        }

    	// Connect producer
    	try {
    		consumer = getConsumer();

    	} catch (Exception e) {
    		log.error("Exception during consumer creation", e);
    	}
    	
    	//Start receiving events
    	try {

    		while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(POLL_DURATION_SECS));
                log.info(records.count() + " records received");
                for (ConsumerRecord<String, String> cr: records) {
                    log.info(cr.key() + ":" + cr.value());
                    writeMessageToFile(FILENAME, cr.key(), cr.value());
                }
    		}
    	} catch (Exception e) {
    		log.error("Error consuming messages", e);
    	}
    	
    }


    private static void writeMessageToFile(String filename, String key, String message) throws IOException {

        if (outputFile == null) {
            outputFile = new BufferedWriter(new FileWriter(filename, true));
        }
        outputFile.write(key + ":" + message);
        outputFile.newLine();
        outputFile.flush();
    }

    private static KafkaConsumer<String, String> getConsumer()  {
    	
    	Properties props = getConsumerConnectionProperties();
        KafkaConsumer<String, String> kc = new KafkaConsumer<String, String>(props);
        kc.subscribe(Arrays.asList(TOPIC_NAME));

        return kc;
    }
    
    private static Properties getConsumerConnectionProperties() {
    	
    	Properties properties = new Properties();
    	
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, "migration-consumer");    	
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "migration-consumer-group");

    	properties.putAll(getCommonConnectionProperties(CLUSTER_TYPE));
    	
    	return properties;
    }

    
    static final Properties getCommonConnectionProperties(ClusterType type) {
        Properties props = new Properties();

        switch(type) {
            case CONFLUENT:
                props.put("bootstrap.servers", BOOTSTRAP_SERVERS);
                props.put("security.protocol", "SASL_SSL");
                props.put("sasl.mechanism", "PLAIN");
                props.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"" + USERNAME + "\" password=\"" + PASSWORD + "\";");
                props.put("ssl.truststore.location", TRUST_STORE);
                props.put("ssl.truststore.password", TRUST_PASS);
                break;
            case EVENTSTREAMS:
                props.put("bootstrap.servers", BOOTSTRAP_SERVERS);
                props.put("security.protocol", "SASL_SSL");
                props.put("sasl.mechanism", "SCRAM-SHA-512");
                props.put("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"" + USERNAME + "\" password=\"" + PASSWORD + "\";");
                props.put("ssl.truststore.location", TRUST_STORE);
                props.put("ssl.truststore.password", TRUST_PASS);
                break;
            case INSECURE:
                props.put("bootstrap.servers", BOOTSTRAP_SERVERS);
                props.put("security.protocol", "PLAINTEXT");
                break;
        }

        return props;
    }
    /*
    static final Properties getCommonConnectionProperties() {
        Properties props = new Properties();
        props.put("bootstrap.servers", BOOTSTRAP_SERVERS);
        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.mechanism", "SCRAM-SHA-512");
        props.put("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"" + USERNAME + "\" password=\"" + PASSWORD + "\";");
        props.put("ssl.truststore.location", TRUST_STORE);
        props.put("ssl.truststore.password", TRUST_PASS);

        return props;
    }
    */
    
    private static void cleanup() {
    	consumer.close();
    	log.info("Consumer closed");
    }
    
}

