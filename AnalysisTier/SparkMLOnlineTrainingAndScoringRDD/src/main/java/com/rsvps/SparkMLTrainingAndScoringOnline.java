package com.rsvps;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.classification.StreamingLogisticRegressionWithSGD;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.CanCommitOffsets;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.HasOffsetRanges;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.spark.streaming.kafka010.OffsetRange;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import scala.Tuple2;

public class SparkMLTrainingAndScoringOnline {

        private static final String HADOOP_HOME_DIR_VALUE = "C:/winutils";
        
        private static final String RUN_LOCAL_WITH_AVAILABLE_CORES = "local[*]";
        private static final String APPLICATION_NAME = "Training and Scoring Online";
        private static final String CASE_SENSITIVE = "false";

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
       
        public static void main(String[] args) throws InterruptedException {

                System.setProperty("hadoop.home.dir", HADOOP_HOME_DIR_VALUE);

                final SparkConf conf = new SparkConf()
                    .setMaster(RUN_LOCAL_WITH_AVAILABLE_CORES)
                    .setAppName(APPLICATION_NAME)
                    .set("spark.sql.caseSensitive", CASE_SENSITIVE);                               

                JavaStreamingContext streamingContext = new JavaStreamingContext(conf,
                    new Duration(BATCH_DURATION_INTERVAL_MS));
                
                JavaInputDStream<ConsumerRecord<String, String>> meetupStream = 
                    KafkaUtils.createDirectStream(
                                streamingContext, 
				LocationStrategies.PreferConsistent(),
                                ConsumerStrategies.<String, String>Subscribe(TOPICS, KAFKA_CONSUMER_PROPERTIES)
                    );

                JavaDStream<String> meetupStreamValues = 
		    meetupStream.map(v -> {                     
                        return v.value();
                    });

                // Prepare the training data as strings of type: (y,[x1,x2,x3,...,xn])
                // Where n is the number of features, y is a binary label, 
                // and n must be the same for train and test.
                // e.g. "(response, [group_lat, group_long])";
                JavaDStream<String> trainData = meetupStreamValues.map(e -> {
                        
                        JSONParser jsonParser = new JSONParser();
                        JSONObject json = (JSONObject)jsonParser.parse(e);

                        String result = "(" 
                            + (String.valueOf(json.get("response")).equals("yes") ? "1.0,[":"0.0,[") 
                            + ((JSONObject)json.get("group")).get("group_lat") + "," 
                            + ((JSONObject)json.get("group")).get("group_lon")
                            + "])";
                        
                        return result;
                });

                trainData.print();

                JavaDStream<LabeledPoint> labeledPoints = trainData.map(LabeledPoint::parse);
        
                StreamingLogisticRegressionWithSGD streamingLogisticRegressionWithSGD 
			= new StreamingLogisticRegressionWithSGD()
                            .setInitialWeights(Vectors.zeros(2));

                streamingLogisticRegressionWithSGD.trainOn(labeledPoints);

                JavaPairDStream<Double, Vector> values = 
			labeledPoints.mapToPair(f -> new Tuple2<>(f.label(), f.features()));

                streamingLogisticRegressionWithSGD.predictOnValues(values).print();

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
                log.log(Level.INFO, "{0} | {1}", new Object[] { offsets, exception });
                log.info("---------------------------------------------------");
        }
}
