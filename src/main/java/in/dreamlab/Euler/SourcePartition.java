package in.dreamlab.Euler;

import java.io.*;
import java.util.*;
import scala.Tuple2;
import java.lang.*;
import in.dreamlab.Euler.VertexValues;

public class SourcePartition implements Serializable{
  
  //PartitionId and Subgraph Hashmap is declared
  //VertexId : (LinkedList of local neighbours, LinkedList of remote neighbours, local neighbour count, remote neighbour count, type)

  private long partitionId; //Store the partition id 
  Map<Long, VertexValues> vertexTable;  //Hashmap with Key = vertexID and Value= <local adjlist, remote adjlist, type>
  private LinkedList<Long> obList; //List of OB Vertices in this partition
  private LinkedList<Long> ebList; //List of EB Vertices in this partition
  
  //Constructor initializes all the data members of the class
  public SourcePartition() {
    vertexTable = new HashMap<>();
    obList = new LinkedList<>();
    ebList = new LinkedList<>();
  }

  //Method to set the partition Id for the partition
  public void setPartitionId(long pID) {
    this.partitionId = pID;
  }
  
  //Method to return the partition Id for this partition
  public long getPartitionId() {
    return this.partitionId;
  }

 //Method to add the adjacency list per vertex to the hashmap
  public void setAdjacencyList(String record) {
    VertexValues vertexTuple = new VertexValues();
//    int lCount = 0; //Count of local neighbours for vertex; 
//    int rCount = 0; //Count of remote neighbours for vertex; 
    byte typ = 0;  //type of the vertex; OB = 1, EB = -1, Internal = 0
    String [] localNs = null;  //Find all local neighbours
    String [] remoteNs = null; //Find all remote neighbours

    String [] recordReader = record.split("&"); //split vertex from adjacency lists
    long vId = Long.parseLong(recordReader[0]); //Convert the vertexID to a long
    String [] adjLists = recordReader[1].split("#"); //split local and remote adjacency list

    //find local and remote degree for the vertex and create a string array from values read from file
    if(adjLists.length == 1) {                     
//      lCount = adjLists[0].split(",").length;
//      rCount = 0;                                //Only local neighbour present
      localNs = adjLists[0].split(",");
      remoteNs= new String[0];
    } else {
      if (!adjLists[0].equals("")) {              
 //       lCount = adjLists[0].split(",").length;
        localNs = adjLists[0].split(",");
      } else {
//        lCount = 0;                              //Only remote neighbour present
        localNs= new String[0];      
      }
//      rCount = adjLists[1].split(",").length;
      remoteNs = adjLists[1].split(",");
    }
    
    //Find type of current vertex
    if(localNs.length % 2 == 0) {
      if(localNs.length > 0 && remoteNs.length == 0) {
        typ = 0; //Internal vertex
      } else if (remoteNs.length > 0) {
        typ = -1; //EB Vertex
        ebList.add(vId);
      }
    } else if(localNs.length % 2 == 1) {
      typ = 1; //OB Vertexx
      obList.add(vId);
    }

    vertexTuple.setLocalNeighbourList(localNs);     //Convert all local neighbours to Long and store in array list
    vertexTuple.setRemoteNeighbourList(remoteNs);  //Convert all remote neighbours to Long and store in array list   
    vertexTuple.setType(typ);                   //type of the vertex
    //VertexTuple.pathId.add(vId); //Every vertex has it's own id as path id currently.
    this.vertexTable.put(vId, vertexTuple);  
  }
 
//Return the Subgraph Map for this partitionID
 public Map<Long, VertexValues> getVertexMap() {
    return this.vertexTable;
 }
 //Return the list of OBs for this partition
 public LinkedList<Long> getOBList() {
   return this.obList;
   }
//Return the list of EBs for this partition
 public LinkedList<Long> getEBList() {
   return this.ebList;
   }
}