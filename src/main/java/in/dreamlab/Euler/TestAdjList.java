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

public class TestAdjList 
{
  static Logger LOGGER = LoggerFactory.getLogger(TestAdjList.class);
    public static void main( String[] args )
    {
      SparkConf conf = new SparkConf().setAppName("Phase1Test0");
      JavaSparkContext sc = new JavaSparkContext(conf);
      
      //Input and Output filenames are given as userargs
      String inputPath = args[0];
      String outputPath1 = args[1];
      String outputPath2 = args[2];

      JavaRDD<String> rdd1 = sc.textFile(inputPath); //read the graph adjacency list as input

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
            
      //get the adjacency list of neighbours
      JavaPairRDD<Long, LinkedList<Long>> rdd6 = rdd5.flatMapToPair(new PairFlatMapFunction<Tuple2<Long, SourcePartition>, Long, LinkedList<Long>>(){
        @Override
        public Iterator<Tuple2<Long, LinkedList<Long>>> call(Tuple2<Long, SourcePartition> t) {
          SourcePartition processPart = new SourcePartition();
          processPart = t._2(); //Object of the partition
          Map<Long, VertexValues> vertexTable = new HashMap<>(processPart.getVertexMap()); //local copy of the subgraph          
          List<Tuple2<Long, LinkedList<Long>>> e = new ArrayList<>(vertexTable.size());
          for(Map.Entry<Long, VertexValues> entry : vertexTable.entrySet()) {
            long key = entry.getKey();
            e.add(new Tuple2(entry.getKey(), entry.getValue().getLocalNeighbourList()));            
          }
          return e.iterator();
        }
      });

      JavaPairRDD<Long, Long> rdd7 = rdd6.flatMapToPair(new PairFlatMapFunction<Tuple2<Long, LinkedList<Long>>, Long, Long>(){
        @Override
        public Iterator<Tuple2<Long, Long>> call(Tuple2<Long, LinkedList<Long>> x) {
          List<Tuple2<Long, Long>> edges = new ArrayList<>();
          while(!x._2().isEmpty()) {
            edges.add(new Tuple2(x._1(), x._2().poll()));
          }
          return edges.iterator();
        }
      });

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

      JavaPairRDD<Long, Long> rdd8 = rdd7.intersection(rddLocalEdgeList);
      long newCount = rdd8.count();
      long oldCount = rdd7.count();
      LOGGER.info("Counts are " + newCount + " and " + oldCount);
      rdd8.saveAsTextFile(outputPath1);
      rdd7.saveAsTextFile(outputPath2);
      sc.stop();
    }
}
