package com.rsvps;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.when;
import static org.apache.spark.sql.types.DataTypes.DoubleType;
import static org.apache.spark.sql.types.DataTypes.LongType;
import static org.apache.spark.sql.types.DataTypes.StringType;

import java.io.IOException;
import java.util.logging.Logger;

import org.apache.spark.SparkConf;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

public class SparkMLTrainingOffline {

    private static final Logger logger = Logger.getLogger(SparkMLTrainingOffline.class.getName());
	
    private static final String HADOOP_HOME_DIR_VALUE = "C:/winutils";

    private static final String RUN_LOCAL_WITH_AVAILABLE_CORES = "local[*]";
    private static final String APPLICATION_NAME = "Spark ML Training Offline";
    private static final String CASE_SENSITIVE = "false";
		
    private static final String JSON_FORMAT = "json";
    private static final String RSVPS_FOLDER_PATH = ".\\src\\main\\resources\\rsvpdata\\";
    private static final String MODEL_FOLDER_PATH = "D:\\streaming\\AnalysisTier\\ml\\model";

    public static void main(String[] args) 
	    throws InterruptedException, StreamingQueryException, IOException {
        
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
       
        // Gathering Data        
        Dataset<Row> gatheredDF = spark.read()
                .format(JSON_FORMAT)
                .schema(RSVP_SCHEMA)
                .load(RSVPS_FOLDER_PATH);
                              
        // Data Preparation        
        Dataset<Row> selectedDF = gatheredDF.select(col("group.group_city"), 
		    col("group.group_lat"), col("group.group_lon"), col("response"));       
			
        Dataset<Row> filteredDF = selectedDF.filter(e -> !e.anyNull());     
		
        Dataset<Row> yesnoDF = filteredDF.withColumn("response", 
		    when(col("response").equalTo("yes"), "1").otherwise("0"));        
			
        Dataset<Row> preparedDF = yesnoDF.withColumn("responseNum", 
		    col("response").cast("double")).drop("response")
			    .withColumnRenamed("responseNum", "label");
        
        VectorAssembler vectorAssembler = new VectorAssembler()
                .setInputCols(new String[]{"group_lat", "group_lon"})
                .setOutputCol("features");      
                   
        // Choosing a Model        
        LogisticRegression logisticRegression = new LogisticRegression()
                .setMaxIter(10)
                .setRegParam(0.00001)
                .setElasticNetParam(0.1)               
                .setThreshold(0.1);

        Pipeline pipeline = new Pipeline()
                .setStages(new PipelineStage[]{
				      vectorAssembler, logisticRegression
				});
         
        // Training the data               		
        Dataset<Row>[] splitDF = preparedDF.randomSplit(new double[]{0.8, 0.2});
        Dataset<Row> trainDF = splitDF[0];
        Dataset<Row> evaluationDF = splitDF[1];                
                       
        PipelineModel pipelineModel = pipeline.fit(trainDF);        		
        
        // Evaluation
        Dataset<Row> predictionsDF = pipelineModel.transform(evaluationDF);
        
	predictionsDF.show(false);

        BinaryClassificationEvaluator 
            binaryClassificationEvaluator = new BinaryClassificationEvaluator();
		
        String metricName = binaryClassificationEvaluator.getMetricName();
        boolean largerBetter = binaryClassificationEvaluator.isLargerBetter();
	double evalValue = binaryClassificationEvaluator.evaluate(predictionsDF);                                  

        logger.info(() -> "\n\nBinary Classification Evaluator:\n\nMetric name: " +
            metricName + "\nIs larger better?: " + largerBetter + "\nValue: " + evalValue);
 
        // Save the model on disk
        pipelineModel.save(MODEL_FOLDER_PATH);        
    }
}
