package in.dreamlab.Euler;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import java.io.*;
import java.util.*;
import scala.Tuple2;
import java.lang.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.io.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import java.util.Arrays;
import java.util.Map.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import in.dreamlab.Euler.SourcePartition;
import in.dreamlab.Euler.VertexValues;

public class TestEdgeCount {
  static Logger LOGGER = LoggerFactory.getLogger(TestEdgeCount.class);
  public static void main(String[] args) {
    SparkConf conf = new SparkConf().setAppName("Phase1Test1");
    JavaSparkContext sc = new JavaSparkContext(conf);
      
    //Input and Output filenames are given as userargs
    String inputPath1 = args[0]; //Input the dataset graph adjacency list
    String inputPath2 = args[1]; //Input the RDD from hdfs which is flat file of format- (PartId, long[] path)
    String outputPath = args[2]; //Write the output RDD

    JavaRDD<String> rdd1 = sc.textFile(inputPath1); //read the graph adjacency list as input
    JavaRDD<String> rdd2 = sc.textFile(inputPath2); //read the paths from previous code.
    
    //Create edge list of local edges from dataset
    JavaPairRDD<Long, Long> rddLocalEdgeList = rdd1.flatMapToPair(new PairFlatMapFunction<String, Long, Long>(){
      @Override
      public Iterator<Tuple2<Long, Long>> call(String s) {
        String [] records = s.split("@");
        String [] adjList = records[1].split("&");
        long key = Long.parseLong(adjList[0]);
        String [] neighbours = adjList[1].split("#");
        String [] localNeighbours = neighbours[0].split(",");
        List<Tuple2<Long, Long>> edges = new ArrayList<>();
        for (String n : localNeighbours) {
          edges.add(new Tuple2(key, Long.parseLong(n)));
        }
        return edges.iterator();
      }
    });
    
    //Create edge list from vertices visited in the path
    JavaPairRDD<Long, Long> rddPathEdgeListForward = rdd2.flatMapToPair(new PairFlatMapFunction<String, Long, Long>(){
      @Override
      public Iterator<Tuple2<Long, Long>> call(String x) {
        List<Tuple2<Long, Long>> edgesOfPath = new ArrayList<>();
        String[] record = x.split(",", 2);
        StringBuilder sb = new StringBuilder(record[1]);
        sb.deleteCharAt(sb.indexOf("["));
        sb.deleteCharAt(sb.indexOf("]"));
        sb.deleteCharAt(sb.indexOf(")"));
        String path = sb.toString();
        String[] vertices = path.split(",");
        for( int i = 0; i< vertices.length-1; i++) {
          edgesOfPath.add(new Tuple2(Long.parseLong(vertices[i].trim()), Long.parseLong(vertices[i+1].trim())));
        }
        return edgesOfPath.iterator();
      }
    });
    
    //Reverse the forward edges 
    JavaPairRDD<Long, Long> rddPathEdgeListBackward = rddPathEdgeListForward.mapToPair(x-> new Tuple2(x._2(), x._1()));
    JavaPairRDD<Long, Long> rddPathEdgeListForwardNew = rddPathEdgeListForward.distinct(); //Remove any edges which occur more than once
    
    //Sanity check that each edge which is present, is present exactly once.
    if(rddPathEdgeListForwardNew.count() != rddPathEdgeListForward.count()) {
      LOGGER.info("Some edge occurs twice!");
      return;
    }

    rddPathEdgeListBackward = rddPathEdgeListBackward.distinct();    
    rddLocalEdgeList = rddLocalEdgeList.subtract(rddPathEdgeListForward);
    rddLocalEdgeList = rddLocalEdgeList.subtract(rddPathEdgeListBackward);
    
    //Sanity check that each each that is present exactly once and no less
    if(rddLocalEdgeList.count() == 0) {
      LOGGER.info("Every edge visited exactly once.");
    }
    sc.stop();
  }
}