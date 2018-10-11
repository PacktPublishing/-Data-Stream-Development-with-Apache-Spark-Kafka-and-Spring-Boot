package com.graphx;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.graphx.Edge;
import org.apache.spark.graphx.Graph;
import org.apache.spark.storage.StorageLevel;

import scala.Tuple2;
import scala.reflect.ClassTag;

public class SparkGraphXKickoff {
    
    private static final String HADOOP_HOME_DIR_VALUE = "C:/winutils";

    private static final String RUN_LOCAL_WITH_AVAILABLE_CORES = "local[*]";
    private static final String APPLICATION_NAME = "Spark GraphX Kickoff";        

    private static final String VERTICES_FOLDER_PATH = 
        "D:\\streaming\\AnalysisTier\\graphx\\vertices";
    private static final String EDGES_FOLDER_PATH = 
        "D:\\streaming\\AnalysisTier\\graphx\\edges";

    public static void main(String[] args) throws InterruptedException {

        System.setProperty("hadoop.home.dir", HADOOP_HOME_DIR_VALUE);

        final SparkConf conf = new SparkConf()
            .setMaster(RUN_LOCAL_WITH_AVAILABLE_CORES)
            .setAppName(APPLICATION_NAME);

        JavaSparkContext javaSparkContext = new JavaSparkContext(conf);        
        
        List<Tuple2<Object, String>> listOfVertex = new ArrayList<>();
        listOfVertex.add(new Tuple2<>(1l, "James"));
        listOfVertex.add(new Tuple2<>(2l, "Andy"));
        listOfVertex.add(new Tuple2<>(3l, "Ed"));
        listOfVertex.add(new Tuple2<>(4l, "Roger"));
        listOfVertex.add(new Tuple2<>(5l, "Tony"));

        List<Edge<String>> listOfEdge = new ArrayList<>();
        listOfEdge.add(new Edge<>(2, 1, "Friend"));
        listOfEdge.add(new Edge<>(3, 1, "Friend"));
        listOfEdge.add(new Edge<>(3, 2, "Colleague"));    
        listOfEdge.add(new Edge<>(3, 5, "Partner"));
        listOfEdge.add(new Edge<>(4, 3, "Boss"));        
        listOfEdge.add(new Edge<>(5, 2, "Partner"));       
    
        JavaRDD<Tuple2<Object, String>> vertexRDD = javaSparkContext.parallelize(listOfVertex);
        JavaRDD<Edge<String>> edgeRDD = javaSparkContext.parallelize(listOfEdge);

        ClassTag<String> stringTag = scala.reflect.ClassTag$.MODULE$.apply(String.class);
		
        Graph<String, String> graph = Graph.apply(
            vertexRDD.rdd(), 
            edgeRDD.rdd(), 
            "", 
            StorageLevel.MEMORY_ONLY(), 
			StorageLevel.MEMORY_ONLY(), 
			stringTag, 
			stringTag
            );    

        //apply specific algorithms, such as PageRank

        graph.vertices()
            .saveAsTextFile(VERTICES_FOLDER_PATH);        
			 
        graph.edges()
	    .saveAsTextFile(EDGES_FOLDER_PATH);        

        javaSparkContext.close();
    }
}
