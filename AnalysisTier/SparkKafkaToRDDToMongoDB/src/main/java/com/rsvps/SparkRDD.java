package com.rsvps;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.mongodb.spark.MongoSpark;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.spark.streaming.kafka010.OffsetRange;
import org.bson.Document;

public class SparkRDD {

    private static final String APPLICATION_NAME = "Spark RDD";
    private static final String HADOOP_HOME_DIR_VALUE = "C:/winutils";
    private static final String RUN_LOCAL_WITH_AVAILABLE_CORES = "local[*]";

    private static final Map<String, Object> KAFKA_CONSUMER_PROPERTIES;
	
    private static final String KAFKA_BROKERS = "localhost:9092";
    private static final String KAFKA_GROUP = "meetupGroup";
    private static final String KAFKA_TOPIC = "meetupTopic";
    private static final OffsetRange[] offsetRange = 
        // topic, partition, inclusive starting offset, exclusive ending offset
        { OffsetRange.create(KAFKA_TOPIC, 0, 1, 100) };

    static {
        Map<String, Object> kafkaProperties = new HashMap<>();
        kafkaProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKERS);
        kafkaProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        kafkaProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        kafkaProperties.put(ConsumerConfig.GROUP_ID_CONFIG, KAFKA_GROUP);
       
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

        JavaRDD<ConsumerRecord<String, String>> rdd = 
	    KafkaUtils.createRDD(sparkContext, KAFKA_CONSUMER_PROPERTIES,
                offsetRange, LocationStrategies.PreferConsistent());
       
        MongoSpark.save(rdd.map(r -> Document.parse(r.value())));

        sparkContext.stop();
        sparkContext.close();
    }
}
