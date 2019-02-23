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
import java.util.Map.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import in.dreamlab.Euler.SourcePartition;
import in.dreamlab.Euler.VertexValues;

public class SubgraphEulerTour 
{
  static Logger LOGGER = LoggerFactory.getLogger(SubgraphEulerTour.class);

      public static long addToLocalPath(Map<Long, VertexValues> vertexTable, VertexValues currentVertexObject, long currentVertexID, List<Long> path) {
        while(!currentVertexObject.getLocalNeighbourList().isEmpty()) {
          long nextVertexID = currentVertexObject.getLocalNeighbourList().remove();
          path.add(nextVertexID);
          currentVertexObject = vertexTable.get(nextVertexID);
          currentVertexObject.getLocalNeighbourList().remove(currentVertexID);  // FIXME: Costly. Can we replace by Map?
          currentVertexID = nextVertexID;
        }

        return currentVertexID;
      }

    public static void main( String[] args )
    {
      SparkConf conf = new SparkConf().setAppName("Phase1");
      JavaSparkContext sc = new JavaSparkContext(conf);
      
      //Input and Output filenames are given as userargs
      String inputPath = args[0];
      String outputPath = args[1];


      //read the graph adjacency list as input
      JavaRDD<String> rdd1 = sc.textFile(inputPath); 

      //Read each line of string input and convert to (K,V) pair with partitionID as key(K) and the rest of the string as value(V)
      JavaPairRDD<Long, String> rdd2 = rdd1.mapToPair(new PairFunction<String, Long, String>(){
        @Override
        public Tuple2<Long, String> call(String s) {
          String [] records = s.split("@");
          Long key = Long.valueOf(records[0]);
          String value = records[1];
          return new Tuple2(key,value);
        }
      });

      //Group all the records for a particular partitionID into the same RDD record
      JavaPairRDD<Long, Iterable<String>> rdd3 = rdd2.groupByKey();
      
      //Create objects per partition which stores all information regarding the vertices and edges in that partition
      JavaPairRDD<Long, SourcePartition> rdd4 = rdd3.mapToPair(new PairFunction<Tuple2<Long, Iterable<String>>, Long, SourcePartition>(){
      @Override
      public Tuple2<Long, SourcePartition> call(Tuple2<Long, Iterable<String>> x) {
          SourcePartition pX = new SourcePartition(); //Create a new object for storing subgraph info corresponding to this partitionID 
          pX.setPartitionId(x._1()); //Partition ID is the key again
          Iterator<String> it = x._2().iterator();
          while(it.hasNext()) {
            pX.setAdjacencyList((String)it.next()); //Enter each adjacency list and create entries in the map
          }
          return new Tuple2(x._1(),pX);
      }
      });

      int numPartitions = (int)rdd4.count(); //Find the count of the RDD 
      JavaPairRDD<Long, SourcePartition> rdd5 = rdd4.repartition(numPartitions); //Repartition the RDD to have same partitions as number of subgraphs
      
      //Return a flat map of paths traversed in each partition with partition id as the key
      JavaPairRDD<Long, List<Long>> rdd6 = rdd5.flatMapToPair(new PairFlatMapFunction<Tuple2<Long, SourcePartition>, Long, List<Long>>(){
        @Override
        public Iterator<Tuple2<Long, List<Long>>> call (Tuple2<Long, SourcePartition> t) {
        SourcePartition processPartition = new SourcePartition();
        processPartition = t._2(); //Object of the partition
        LOGGER.info("Vertex Map for PartitionId " + processPartition.getPartitionId()  + " and Vertex map is " + processPartition.getVertexMap() + " and list of OBs is " + processPartition.getOBList() + " and list of EBs is " + processPartition.getEBList()); //method of the partition class which returns the subgraph information
        
        Map<Long, VertexValues> vertexTable = new HashMap<>(processPartition.getVertexMap()); //local copy of the subgraph
        List<Tuple2<Long, List<Long>>> p = new ArrayList<>(); //The list per partition which holds all paths in this partition

        //Process the OB paths in the following loop-

        while (!processPartition.getOBList().isEmpty()) {
          long currentVertexID = processPartition.getOBList().remove();
          List<Long> path = new ArrayList<Long>();
          LOGGER.info("Source Vertex for OB Path is " + currentVertexID);
          path.add(currentVertexID);
          VertexValues currentVertexObject = vertexTable.get(currentVertexID);
          
          //Add this while loop to a method and call with currentVertexObject as a parameter     
          long vertex = addToLocalPath(vertexTable ,currentVertexObject, currentVertexID, path);
/*
          while(!currentVertexObject.getLocalNeighbourList().isEmpty()) {
            long nextVertexID = currentVertexObject.getLocalNeighbourList().remove();
            path.add(nextVertexID);
            currentVertexObject = vertexTable.get(nextVertexID);
            currentVertexObject.getLocalNeighbourList().remove(currentVertexID);  // FIXME: Costly. Can we replace by Map?
            currentVertexID = nextVertexID;
          }
*/
          assert currentVertexObject.getLocalNeighbourList().isEmpty() : "Current vertex has some local unvisited neighbour";

          processPartition.getOBList().remove(vertex);
          p.add(new Tuple2<Long, List<Long>>(t._1(), path));
          LOGGER.info("path is" + path + "in partition " + t._1());
        }

        assert processPartition.getOBList().isEmpty() : "There are OB vertices left to process!";

        while (!processPartition.getEBList().isEmpty()) {
          long currentVertexID = processPartition.getEBList().remove();
          List<Long> path = new ArrayList<Long>();
          LOGGER.info("Source Vertex for EB cycle is " + currentVertexID);
          path.add(currentVertexID);
          VertexValues currentVertexObject = vertexTable.get(currentVertexID);
          long vertex = addToLocalPath(vertexTable ,currentVertexObject, currentVertexID, path);
/*          
          while(!currentVertexObject.getLocalNeighbourList().isEmpty()) {
            long nextVertexID = currentVertexObject.getLocalNeighbourList().remove();
            path.add(nextVertexID);
            currentVertexObject = vertexTable.get(nextVertexID);
            currentVertexObject.getLocalNeighbourList().remove(currentVertexID);
            currentVertexID = nextVertexID;
          }
*/
          assert currentVertexObject.getLocalNeighbourList().isEmpty() : "Current vertex has some local unvisited neighbour";
          
          p.add(new Tuple2<Long, List<Long>>(t._1(), path));
          LOGGER.info("path is" + path + "in partition " + t._1());
        }
        
        assert processPartition.getEBList().isEmpty() : "There are EB vertices left to process!";

        for(Map.Entry<Long, VertexValues> entry : vertexTable.entrySet()) {
          List<Long> path = null;
          if (entry.getValue().getType() == 0 && !entry.getValue().getLocalNeighbourList().isEmpty()) {
            path = new ArrayList<Long>();
            LOGGER.info("Source vertex for this internal path is " + entry.getKey());
            path.add(entry.getKey()); //Add this current vertex to the current inner loop
            VertexValues currentVertexObject = entry.getValue(); //Current vertex is now current of internal vertex cycle
            long currentVertexID = entry.getKey(); //this is parent for next vertex on list
            long vertex = addToLocalPath(vertexTable ,currentVertexObject, currentVertexID, path);
/*            
            while(!currentVertexObject.getLocalNeighbourList().isEmpty()) {
              long nextVertexID = current.getLocalNeighbourList().remove(); //find first vertex to visit
              path.add(next);
              currentVertexObject = vertexTable.get(nextVertexID);
              currentVertexObject.getLocalNeighbourList().remove(currentVertexID);
              currentVertexID = nextVertexID;
            }
*/
            assert currentVertexObject.getLocalNeighbourList().isEmpty() : "Current vertex has some local unvisited neighbour";

          }
          if (path != null) {
            p.add(new Tuple2<Long, List<Long>>(t._1(), path));
            LOGGER.info("path is" + path + "in partition " + t._1());            
          }
        }

      LOGGER.info("After computation, Vertex Map for PartitionId " + processPartition.getPartitionId()  + " and Vertex map is " + processPartition.getVertexMap() + " and list of OBs is " + processPartition.getOBList() + " and list of EBs is " + processPartition.getEBList()); //method of the partition class which returns the subgraph information

      return p.iterator();
        }
      });

      rdd6.saveAsTextFile(outputPath);    
      sc.stop();
    }
}
