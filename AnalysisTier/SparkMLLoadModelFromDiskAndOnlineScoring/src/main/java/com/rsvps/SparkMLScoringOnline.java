package com.rsvps;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.from_json;
import static org.apache.spark.sql.types.DataTypes.DoubleType;
import static org.apache.spark.sql.types.DataTypes.LongType;
import static org.apache.spark.sql.types.DataTypes.StringType;

import org.apache.spark.SparkConf;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

public class SparkMLScoringOnline {
		
    private static final String HADOOP_HOME_DIR_VALUE = "C:/winutils";

    private static final String RUN_LOCAL_WITH_AVAILABLE_CORES = "local[*]";
    private static final String APPLICATION_NAME = "Spark ML Scoring Online";
    private static final String CASE_SENSITIVE = "false";
		
    private static final String MODEL_FOLDER_PATH 
	    = "D:\\streaming\\AnalysisTier\\ml\\model";
    private static final String RESULT_FOLDER_PATH 
	    = "D:\\streaming\\AnalysisTier\\ml\\results";
	
    private static final String KAFKA_FORMAT = "kafka";
    private static final String KAFKA_BROKERS = "localhost:9092";
    private static final String KAFKA_TOPIC = "meetupTopic";
	
    private static final String JSON_FORMAT = "json";
    private static final String CHECKPOINT_LOCATION = "D://rsvpml";
    private static final String QUERY_INTERVAL_SECONDS = "30 seconds";

    public static void main(String[] args) throws InterruptedException, StreamingQueryException {
   
        System.setProperty("hadoop.home.dir", HADOOP_HOME_DIR_VALUE);

        // * the schema can be written on disk, and read from disk
        // * the schema is not mandatory to be complete, it can contain only the needed fields    
        StructType RSVP_SCHEMA = new StructType()                                
                .add("event",
                        new StructType()
                                .add("event_id", StringType, true)
                                .add("event_name", StringType, true)
                                .add("event_url", StringType, true)
                                .add("time", LongType, true))
                .add("group",
                        new StructType()
                                .add("group_city", StringType, true)
                                .add("group_country", StringType, true)
                                .add("group_id", LongType, true)
                                .add("group_lat", DoubleType, true)
                                .add("group_lon", DoubleType, true)
                                .add("group_name", StringType, true)
                                .add("group_state", StringType, true)
                                .add("group_topics", DataTypes.createArrayType(
                                        new StructType()
                                                .add("topicName", StringType, true)
                                                .add("urlkey", StringType, true)), true)
                                .add("group_urlname", StringType, true))
                .add("guests", LongType, true)
                .add("member",
                        new StructType()
                                .add("member_id", LongType, true)
                                .add("member_name", StringType, true)                                
                                .add("photo", StringType, true))
                .add("mtime", LongType, true)
                .add("response", StringType, true)
                .add("rsvp_id", LongType, true)
                .add("venue",
                        new StructType()
                                .add("lat", DoubleType, true)
                                .add("lon", DoubleType, true)
                                .add("venue_id", LongType, true)
                                .add("venue_name", StringType, true))
                .add("visibility", StringType, true);

        final SparkConf conf = new SparkConf()
                .setMaster(RUN_LOCAL_WITH_AVAILABLE_CORES)
                .setAppName(APPLICATION_NAME)
                .set("spark.sql.caseSensitive", CASE_SENSITIVE);

        SparkSession spark = SparkSession
                .builder()
                .config(conf)
                .getOrCreate();

        PipelineModel pipelineModel = PipelineModel.load(MODEL_FOLDER_PATH);
       
        Dataset<Row> meetupStream = spark.readStream()
                .format(KAFKA_FORMAT)
                .option("kafka.bootstrap.servers", KAFKA_BROKERS)
                .option("subscribe", KAFKA_TOPIC)
                .load();

        Dataset<Row> gatheredDF = meetupStream.select(
		    (from_json(col("value").cast("string"), RSVP_SCHEMA))
			        .alias("rsvp"))
			.alias("meetup")
            .select("meetup.*");
				
        Dataset<Row> filteredDF = gatheredDF.filter(e -> !e.anyNull());  
		
        Dataset<Row> preparedDF = filteredDF.select(
		        col("rsvp.group.group_city"),
		        col("rsvp.group.group_lat"), col("rsvp.group.group_lon"), 
				col("rsvp.response")
		);
				                
        preparedDF.printSchema();
     
        Dataset<Row> predictionDF = pipelineModel.transform(preparedDF);
        
        StreamingQuery query = predictionDF.writeStream()                
                .format(JSON_FORMAT)
                .option("path", RESULT_FOLDER_PATH)
                .option("checkpointLocation", CHECKPOINT_LOCATION)
                .trigger(Trigger.ProcessingTime(QUERY_INTERVAL_SECONDS))
                .option("truncate", false)
                .start();

        query.awaitTermination();
    }
}
