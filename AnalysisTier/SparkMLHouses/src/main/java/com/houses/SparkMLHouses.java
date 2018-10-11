package com.houses;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.types.DataTypes.FloatType;
import static org.apache.spark.sql.types.DataTypes.LongType;
import static org.apache.spark.sql.types.DataTypes.StringType;

import java.util.logging.Logger;

import org.apache.spark.SparkConf;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.feature.Imputer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.StructType;

public class SparkMLHouses {

        private static final Logger logger = Logger.getLogger(SparkMLHouses.class.getName());

        private static final String HADOOP_HOME_DIR_VALUE = "C:/winutils";

        private static final String RUN_LOCAL_WITH_AVAILABLE_CORES = "local[*]";
        private static final String APPLICATION_NAME = "Spark ML Houses";
        private static final String CASE_SENSITIVE = "false";

        private static final String HOUSES_FILE_PATH = ".\\src\\main\\resources\\houses.json";

        public static void main(String[] args) throws InterruptedException, StreamingQueryException {

                System.setProperty("hadoop.home.dir", HADOOP_HOME_DIR_VALUE);

                // * the schema can be written on disk, and read from disk
                // * the schema is not mandatory to be complete, it can contain only the needed fields    
                StructType HOUSES_SCHEMA = 
                       new StructType()
                           .add("House", LongType, true)
                           .add("Taxes", LongType, true)
                           .add("Bedrooms", LongType, true)
                           .add("Baths", FloatType, true)
                           .add("Quadrant", LongType, true)
                           .add("NW", StringType, true)
                           .add("Price($)", LongType, false)
                           .add("Size(sqft)", LongType, false)
                           .add("lot", LongType, true);

                final SparkConf conf = new SparkConf()
                    .setMaster(RUN_LOCAL_WITH_AVAILABLE_CORES)
                    .setAppName(APPLICATION_NAME)
                    .set("spark.sql.caseSensitive", CASE_SENSITIVE);

                SparkSession sparkSession = SparkSession.builder()
                    .config(conf)
                    .getOrCreate();

                Dataset<Row> housesDF = sparkSession.read()
                     .schema(HOUSES_SCHEMA)
                     .json(HOUSES_FILE_PATH);
             
                // Gathering Data				
                Dataset<Row> gatheredDF = housesDF.select(col("Taxes"), 
                    col("Bedrooms"), col("Baths"),
                    col("Size(sqft)"), col("Price($)"));
                
                // Data Preparation  
                Dataset<Row> labelDF = gatheredDF.withColumnRenamed("Price($)", "label");
                
                Imputer imputer = new Imputer()
                    // .setMissingValue(1.0d)
                    .setInputCols(new String[] { "Baths" })
                    .setOutputCols(new String[] { "~Baths~" });

                VectorAssembler assembler = new VectorAssembler()
                    .setInputCols(new String[] { "Taxes", "Bedrooms", "~Baths~", "Size(sqft)" })
                    .setOutputCol("features");
                
                // Choosing a Model               
                LinearRegression linearRegression = new LinearRegression();
                linearRegression.setMaxIter(1000);

                Pipeline pipeline = new Pipeline()
                                .setStages(new PipelineStage[] {
                                    imputer, assembler, linearRegression 
                                });

                // Training The Data
                Dataset<Row>[] splitDF = labelDF.randomSplit(new double[] { 0.8, 0.2 });

                Dataset<Row> trainDF = splitDF[0];
                Dataset<Row> evaluationDF = splitDF[1];

                PipelineModel pipelineModel = pipeline.fit(trainDF);
                
                // Evaluation 
                Dataset<Row> predictionsDF = pipelineModel.transform(evaluationDF);

                predictionsDF.show(false);

                Dataset<Row> forEvaluationDF = predictionsDF.select(col("label"), 
                    col("prediction"));

                RegressionEvaluator evaluteR2 = new RegressionEvaluator().setMetricName("r2");
                RegressionEvaluator evaluteRMSE = new RegressionEvaluator().setMetricName("rmse");

                double r2 = evaluteR2.evaluate(forEvaluationDF);
                double rmse = evaluteRMSE.evaluate(forEvaluationDF);

                logger.info("---------------------------");
                logger.info("R2 =" + r2);
                logger.info("RMSE =" + rmse);
                logger.info("---------------------------");
        }
}
