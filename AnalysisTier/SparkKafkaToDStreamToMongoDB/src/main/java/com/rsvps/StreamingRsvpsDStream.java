package com.rsvps;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.mongodb.spark.MongoSpark;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.CanCommitOffsets;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.HasOffsetRanges;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.spark.streaming.kafka010.OffsetRange;
import org.bson.Document;

public class StreamingRsvpsDStream {

    private static final String APPLICATION_NAME = "Streaming Rsvps DStream";
    private static final String HADOOP_HOME_DIR_VALUE = "C:/winutils";    
    private static final String RUN_LOCAL_WITH_AVAILABLE_CORES = "local[*]";    
    private static final int BATCH_DURATION_INTERVAL_MS = 5000;	

    private static final Map<String, Object> KAFKA_CONSUMER_PROPERTIES;    
    
    private static final String KAFKA_BROKERS = "localhost:9092";
    private static final String KAFKA_OFFSET_RESET_TYPE = "latest";
    private static final String KAFKA_GROUP = "meetupGroup";
    private static final String KAFKA_TOPIC = "meetupTopic";
    private static final Collection<String> TOPICS = 
            Collections.unmodifiableList(Arrays.asList(KAFKA_TOPIC));
			
    static {
        Map<String, Object> kafkaProperties = new HashMap<>();
        kafkaProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKERS);
        kafkaProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        kafkaProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        kafkaProperties.put(ConsumerConfig.GROUP_ID_CONFIG, KAFKA_GROUP);
        kafkaProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, KAFKA_OFFSET_RESET_TYPE);
        kafkaProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        KAFKA_CONSUMER_PROPERTIES = Collections.unmodifiableMap(kafkaProperties);
    }
	
    private static final String MONGODB_OUTPUT_URI = "mongodb://localhost/meetupDB.rsvpsguests";
        
    public static void main(String[] args) throws InterruptedException {

        System.setProperty("hadoop.home.dir", HADOOP_HOME_DIR_VALUE);

        final SparkConf conf = new SparkConf()
                .setMaster(RUN_LOCAL_WITH_AVAILABLE_CORES)
                .setAppName(APPLICATION_NAME)
                .set("spark.mongodb.output.uri", MONGODB_OUTPUT_URI);

        final JavaStreamingContext streamingContext
                = new JavaStreamingContext(conf, new Duration(BATCH_DURATION_INTERVAL_MS));

        final JavaInputDStream<ConsumerRecord<String, String>> meetupStream =
                KafkaUtils.createDirectStream(
                        streamingContext,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<String, String>Subscribe(TOPICS, KAFKA_CONSUMER_PROPERTIES)
                );
                
        // transformations, streaming algorithms, etc
        JavaDStream<ConsumerRecord<String, String>> rsvpsWithGuestsStream =
                meetupStream.filter(f -> !f.value().contains("\"guests\":0"));

        rsvpsWithGuestsStream.foreachRDD((JavaRDD<ConsumerRecord<String, String>> r) -> {        
            MongoSpark.save(
                    r.map(
                        e -> Document.parse(e.value())
                    )
            );            
        });
        
        // some time later, after outputs have completed
        meetupStream.foreachRDD((JavaRDD<ConsumerRecord<String, String>> meetupRDD) -> {        
            OffsetRange[] offsetRanges = ((HasOffsetRanges) meetupRDD.rdd()).offsetRanges();            

            ((CanCommitOffsets) meetupStream.inputDStream())
                .commitAsync(offsetRanges, new MeetupOffsetCommitCallback());
        });

        streamingContext.start();
        streamingContext.awaitTermination();    
    }
}

final class MeetupOffsetCommitCallback implements OffsetCommitCallback {

    private static final Logger log = Logger.getLogger(MeetupOffsetCommitCallback.class.getName());

    @Override
    public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
        log.info("---------------------------------------------------");
        log.log(Level.INFO, "{0} | {1}", new Object[]{offsets, exception});
        log.info("---------------------------------------------------");
    }
}
