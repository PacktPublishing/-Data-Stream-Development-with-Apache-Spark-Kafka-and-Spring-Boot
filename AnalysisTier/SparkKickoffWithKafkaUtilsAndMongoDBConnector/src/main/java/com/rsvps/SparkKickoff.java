package com.rsvps;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.mongodb.spark.MongoSpark;
import org.apache.spark.streaming.kafka010.KafkaUtils;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkKickoff {

    private static final String APPLICATION_NAME = "Spark Kickoff With Kafka And MongoDB";
    private static final String HADOOP_HOME_DIR_VALUE = "C:/winutils";
    private static final String RUN_LOCAL_WITH_AVAILABLE_CORES = "local[*]";
    
    private static final Map<String, Object> KAFKA_CONSUMER_PROPERTIES;
	
    private static final String KAFKA_BROKERS = "localhost:9092";
    private static final String KAFKA_OFFSET_RESET_TYPE = "latest";
    private static final String KAFKA_GROUP = "meetupGroup";
    private static final String KAFKA_TOPIC = "meetupTopic";
	
    static {
        Map<String, Object> kafkaProperties = new HashMap<>();
        kafkaProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKERS);
        kafkaProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        kafkaProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        kafkaProperties.put(ConsumerConfig.GROUP_ID_CONFIG, KAFKA_GROUP);
        kafkaProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, KAFKA_OFFSET_RESET_TYPE);
        kafkaProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);

        KAFKA_CONSUMER_PROPERTIES = Collections.unmodifiableMap(kafkaProperties);
    }
	
    private static final String MONGODB_OUTPUT_URI = "mongodb://localhost/meetupDB.rsvps";

    public static void main(String[] args) throws InterruptedException {

        System.setProperty("hadoop.home.dir", HADOOP_HOME_DIR_VALUE);

        final SparkConf conf = new SparkConf()
                .setMaster(RUN_LOCAL_WITH_AVAILABLE_CORES)
                .setAppName(APPLICATION_NAME)
                .set("spark.mongodb.output.uri", MONGODB_OUTPUT_URI);

        JavaSparkContext sparkContext = new JavaSparkContext(conf);

        // business logic                      

        sparkContext.stop();
        sparkContext.close();
    }
}
