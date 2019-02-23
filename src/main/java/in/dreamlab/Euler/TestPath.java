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
import org.apache.spark.api.java.Optional;
import java.util.Arrays;
import java.util.Map.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import in.dreamlab.Euler.SourcePartition;
import in.dreamlab.Euler.VertexValues;

public class TestPath {
  static Logger LOGGER = LoggerFactory.getLogger(TestPath.class);
  public static void main(String[] args) {
    SparkConf conf = new SparkConf().setAppName("Phase1Test2");
    JavaSparkContext sc = new JavaSparkContext(conf);
    
    String inputPath1 = args[0]; //Input the dataset graph adjacency list
    String inputPath2 = args[1]; //Input the RDD from hdfs which is flat file of format- (PartId, long[] path)
    String outputPath = args[2]; //Write the output RDD
    
    JavaRDD<String> rdd1 = sc.textFile(inputPath1); //read the graph adjacency list as input
    JavaRDD<String> rddPaths = sc.textFile(inputPath2); //read in the paths calculated by Phase 1 algorithm

    JavaPairRDD<Long, String> rdd2 = rdd1.mapToPair(new PairFunction<String, Long, String>(){
      @Override
      public Tuple2<Long, String> call(String s) {
        String [] records = s.split("@");
        Long key = Long.valueOf(records[0]);
        String value = records[1];
        return new Tuple2(key,value);
      }
    });

    JavaPairRDD<Long, Iterable<String>> rdd3 = rdd2.groupByKey();
      
    JavaPairRDD<Long, SourcePartition> rdd4 = rdd3.mapToPair(new PairFunction<Tuple2<Long, Iterable<String>>, Long, SourcePartition>(){
      @Override
      public Tuple2<Long, SourcePartition> call(Tuple2<Long, Iterable<String>> x) {
          SourcePartition pX = new SourcePartition(); //Create a new object for storing subgraph info corresponding to this partitionID 
          pX.setPartitionId(x._1()); //Partition ID is the key again
          Iterator it = x._2().iterator();
          while(it.hasNext()) {
            pX.setAdjacencyList((String)it.next()); //Enter each adjacency list and create entries in the map
          }
          return new Tuple2(x._1(),pX);
      }
    });

    int numPartitions = (int)rdd4.count(); //Find the count of the RDD 
    JavaPairRDD<Long, SourcePartition> rdd5 = rdd4.repartition(numPartitions); //Repartition the RDD to have same partitions as number of subgraphs
      
    //Create an RDD of paths per partition cleaned to suit required format
    JavaPairRDD<Long, String> rddPaths2 = rddPaths.mapToPair(new PairFunction<String, Long, String>(){
      @Override
      public Tuple2<Long, String> call(String y) {
        String[] record = y.split(",", 2);
        StringBuilder sb = new StringBuilder(record[0]);
        StringBuilder sb2 = new StringBuilder(record[1]);
        sb.deleteCharAt(sb.indexOf("("));
        String partId = sb.toString();
        Long key = Long.valueOf(partId.trim());
          
        sb2.deleteCharAt(sb2.indexOf("["));
        sb2.deleteCharAt(sb2.indexOf("]"));
        sb2.deleteCharAt(sb2.indexOf(")"));
        String path = sb2.toString();
        String[] vertices = path.split(",");
        for( int i = 0; i< vertices.length; i++) {
          vertices[i] = vertices[i].trim();
        }
        
        String str = String.join(",",vertices);
        return new Tuple2(key, str);  
      }
    });
    
    JavaPairRDD<Long, Iterable<String>> rddPaths3 = rddPaths2.groupByKey();
    
    JavaPairRDD<Long, Tuple2<SourcePartition, Optional<Iterable<String>>>> rddJoined = rdd5.leftOuterJoin(rddPaths3, numPartitions);

    JavaPairRDD<Long, String> rddFinal = rddJoined.mapToPair(new PairFunction<Tuple2<Long, Tuple2<SourcePartition, Optional<Iterable<String>>>>, Long, String>(){
      @Override
      public Tuple2<Long, String> call(Tuple2<Long, Tuple2<SourcePartition, Optional<Iterable<String>>>> z) {
        Iterator<String> it = null;
        
        //assign iterator if there are local paths in the subgraph
        if(z._2()._2().isPresent()) {
          it = z._2()._2().get().iterator();
        }
        else {
          return new Tuple2(z._1(), "No local paths");
        }

        while(it.hasNext()) {
          String[] pathVertices = it.next().split(",");
          for(int i = 0; i< pathVertices.length-1; i++) {
            if(z._2()._1().getVertexMap().containsKey(Long.parseLong(pathVertices[i]))) {
              long vertex = Long.parseLong(pathVertices[i+1]);
              boolean present = false;
              if (z._2()._1().getVertexMap().get(Long.parseLong(pathVertices[i])).getLocalNeighbourList().contains(vertex)) {
                present = true;
              }

              if(present == false) {
                System.out.println("Path is wrong.");
                return new Tuple2(z._1(), "Path is wrong.");
              } else {
                continue;
              }
              
            } 
          } 
        }
        
        return new Tuple2(z._1(), "Paths are valid");
      }
    });
      
    rddFinal.saveAsTextFile(outputPath);
    sc.stop();
  }
}