package ibm.producer;

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;


/**
 * Test class to produce PNR records to Event Streams
 *
 */
public class Producer 
{
    private static String TOPIC_NAME;
    private static String USERNAME;
    private static String PASSWORD;
    private static String TRUST_STORE;
    private static String BOOTSTRAP_SERVERS;
    private static String TRUST_PASS;
    private static int PRODUCER_DELAY;
    private static int MESSAGE_WRAP = 999999;
    private static ClusterType CLUSTER_TYPE;

	private static KafkaProducer<String, String> producer;
	
	private static final Logger log = Logger.getLogger(Producer.class);

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
        BOOTSTRAP_SERVERS = args[4];
        TRUST_PASS = args[5];
    
        PRODUCER_DELAY = Integer.valueOf(args[6]);

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
        log.info("Creating topic");
        // Create topics just in case
    	createTopics();
    	
    	// Connect producer
    	try {
            log.info("Getting producer");
    		producer = getProducer();

    	} catch (Exception e) {
    		log.error("Exception during topic creation", e);
    	}
    	
    	//Start sending events
    	try {

            int i = 0;
            log.info("Starting sending events");
    		while (true) {
                Date d = new Date();
                String testKey = "test";
                String testMessage = "Message index " + i;

                ProducerRecord<String, String> pr = new ProducerRecord<String,String>(TOPIC_NAME, null, d.getTime(), testKey, testMessage);

                producer.send(pr);
                log.info("Record " + i + " sent");
    			Thread.sleep(PRODUCER_DELAY);
                if (i < MESSAGE_WRAP) {
                    i++;
                } else {
                    i = 0;
                }

    		}
    	} catch (Exception e) {
    		log.error("Error producing messages", e);
    	}
    	
    }

    private static KafkaProducer<String, String> getProducer()  {
    	
    	Properties props = getProducerConnectionProperties();
        KafkaProducer<String, String> kp = new KafkaProducer<String, String>(props);
        
        return kp;
    }
    
    
    public static void createTopics() {

        Properties props = getAdminConnectionProperties();

        try (AdminClient admin = AdminClient.create(props)) {
        
        	NewTopic nt = new NewTopic(TOPIC_NAME, 1, (short)3);
        	Collection<NewTopic> topics = new ArrayList<NewTopic>();
        	topics.add(nt);
        	CreateTopicsResult ctr = admin.createTopics(topics);
        	ctr.all().get(10, TimeUnit.SECONDS);
        	log.info("Created topic " + TOPIC_NAME);
        } catch (ExecutionException e) {
        	if (e.getCause() instanceof TopicExistsException) {
        		log.info("Topic " + TOPIC_NAME + " exists");
        	} else {
            	log.error(e);
            }
        } catch (Exception e) {
        	log.error(e);
        }
 
    }
    
    private static Properties getProducerConnectionProperties() {
    	
    	Properties properties = new Properties();
    	
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, TOPIC_NAME + "-producer");    	
    	properties.put(ProducerConfig.LINGER_MS_CONFIG, 500);
    	properties.put(ProducerConfig.BATCH_SIZE_CONFIG, 65535);

    	properties.putAll(getCommonConnectionProperties(CLUSTER_TYPE));
    	
    	return properties;
    }
    
    private static Properties getAdminConnectionProperties() {
        Properties props = new Properties();
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "producer-admin");
        props.put(AdminClientConfig.CLIENT_DNS_LOOKUP_CONFIG, "use_all_dns_ips");
        props.putAll(getCommonConnectionProperties(CLUSTER_TYPE));
        return props;
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

    private static void cleanup() {
    	producer.close();
    	log.info("Producer closed");
    }
    
}

