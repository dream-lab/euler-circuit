package in.dreamlab.Euler;

import java.io.*;
import java.util.*;
import java.lang.*;

public class VertexValues implements Serializable {
  private LinkedList<Long> localNeighbourList; //store all local neighbours, if any
  private LinkedList<Long> remoteNeighbourList; //store all remote neighbours, if any
  private Byte type; //Store type of vertex- OB, EB, Internal new Byte();

//public LinkedList<Long> pathId = new LinkedList<>(); //List of Path Ids that this vertex belongs to
 
 //Constructor initializes all the data members of the class
  public VertexValues() {
    localNeighbourList = new LinkedList<>();
    remoteNeighbourList = new LinkedList<>();
    type = 0;
  }
  
  //Methods to access and update the data members of the class-
  
  //Set the local Neighbour list from the input dataset
  public void setLocalNeighbourList(String[] localNs) {
    for(int i=0;i<localNs.length;i++) {
      this.localNeighbourList.add(Long.parseLong(localNs[i]));
    }
  }
  
  //return the local neighbour list
  public LinkedList<Long> getLocalNeighbourList() {
    return this.localNeighbourList;
  }
  
  //set the remote neighbour list from input dataset
  public void setRemoteNeighbourList(String[] remoteNs) {
    for(int i=0;i<remoteNs.length;i++) {
      this.remoteNeighbourList.add(Long.parseLong(remoteNs[i]));
    }
  }
  
  //return the remote neighbour list
  public LinkedList<Long> getRemoteNeighbourList() {
    return this.remoteNeighbourList;
  }
  
  //set the type of this vertex- OB/EB/Internal
  public void setType(byte typ) {
    this.type = typ;
  }
  
  //return the type of the vertex
  public byte getType() {
    return this.type;
  }
}
