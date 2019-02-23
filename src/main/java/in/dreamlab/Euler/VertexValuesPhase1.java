package in.dreamlab.Euler;

import java.io.*;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.list.linked.TLongLinkedList;
import gnu.trove.map.hash.TByteObjectHashMap;


public class VertexValuesPhase1 implements Serializable { // extend from GraphUtils.PartitionedAdjacencyList ?
    public static final byte TYPE_OB = 1;
    public static final byte TYPE_EB = -1;
    public static final byte TYPE_INTERNAL = 0;
    public static final TLongArrayList EMPTY_LOCAL_LIST = new TLongArrayList(0);

    // This will have the list of unvisited local neighbors.
    // We use Long.MAX_VALUE to indicate a visited vertex that has not been removed yet for efficient removal ops.
    // NOTE: The local list is drained over time and hence its contents will be modified!!!
    private TLongArrayList localNeighbourList;

    // store all remote neighbours, if any // YS: TODO: make
    // part id as byte and remote neighbors as elastic list. key
    // is the partition ID and value the remote vertices in that
    // partition?
    // FIXME: The remote neighbors are used later. Do NOT modify!!
    private TByteObjectHashMap<TLongArrayList> remoteNeighbourMap;

    // Store type of vertex- OB, EB, Internal new Byte();
    private byte type;

    // List of Path Ids that this vertex belongs to
    public TLongLinkedList pathIds;


    private VertexValuesPhase1() {

    }


    // Constructor initializes all the data members of the class
    public VertexValuesPhase1(TLongArrayList local, TByteObjectHashMap<TLongArrayList> remote, Byte _type) {
	localNeighbourList = local != null ? local : EMPTY_LOCAL_LIST;
	remoteNeighbourMap = remote;
	type = _type;
	pathIds = new TLongLinkedList();
    }

    // Methods to access and update the data members of the class-


    // return the local neighbour list
    public TLongArrayList getUnvisitedLocalNeighbourList() {
	// FIXME: Check if callees handle empty local list if returning EMPTY_LOCAL_LIST
	return this.localNeighbourList;
    }


    // return the remote neighbour list
    public TByteObjectHashMap<TLongArrayList> getRemoteNeighbourMap() {
	return this.remoteNeighbourMap;
    }


    // add a path id to this vertex
    public void appendPathId(long pathID) {
	pathIds.add(pathID);
    }


    // return the list of paths this vertex belongs to
    public TLongLinkedList getPathId() {
	return this.pathIds;
    }


    // return the type of the vertex
    public byte getType() {
	return this.type;
    }
}

