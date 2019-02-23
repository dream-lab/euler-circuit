package in.dreamlab.Euler;

import java.io.*;

import gnu.trove.iterator.TByteObjectIterator;
import gnu.trove.list.TByteList;
import gnu.trove.list.array.TByteArrayList;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.map.hash.TByteObjectHashMap;
import in.dream_lab.graph.euler.GraphUtils;
import in.dream_lab.graph.euler.GraphUtils.PartitionedAdjacencyMapEntry;


/**
 * 
 * @author siddharthj
 * @author simmhan
 *
 */
@SuppressWarnings("serial")
public class AdjListObject implements Serializable {

    private long vertexId;
    private TLongArrayList localNeighbourList;
    // partId, remote vId list
    // TODO: Have changed part ID to byte. Verify cascading effect...
    private TByteObjectHashMap<TLongArrayList> remoteNeighbourMap; // PECS does not hold. Use fixed type for value of
								   // map.

    private static final TByteObjectHashMap<TLongArrayList> EMPTY_MAP = new TByteObjectHashMap<TLongArrayList>(0);


    /**
     * Parses a string with source VId, list of local sink VId, list of pairs of remote part and sink VId.
     * FIXME: SJ to change over to binary representation of input instead of string
     * 
     * @param vList
     */
    public AdjListObject(String vList) {
	// set the values from the string obtained to create this object
	String[] vSplit = vList.split("&");
	this.vertexId = Long.parseLong(vSplit[0]); // get vertex ID

	String[] adjLists = vSplit[1].split("#"); // split local and remote adjacency list
	String[] localNs = null;  // Find all local neighbours
	String[] remoteNs = null; // Find all remote neighbours


	if (adjLists.length == 1) {
	    localNs = adjLists[0].split(","); // internal vertex
	    remoteNs = new String[0];
	} else {
	    if (!adjLists[0].equals("")) {
		localNs = adjLists[0].split(","); // extract local neighbours if present
	    } else {
		localNs = new String[0];  // no local neighbours present
	    }
	    remoteNs = adjLists[1].split(",");
	}

	localNeighbourList = new TLongArrayList(localNs.length); // capacity = localNs.length
	for (int i = 0; i < localNs.length; i++) {
	    this.localNeighbourList.add(Long.parseLong(localNs[i]));
	}

	remoteNeighbourMap = remoteNs.length == 0 ? EMPTY_MAP : new TByteObjectHashMap<TLongArrayList>(); // capacity =
	// numParts
	for (int i = 0; i < remoteNs.length; i++) {
	    String[] rSplit = remoteNs[i].split(":");
	    long vID = Long.parseLong(rSplit[0]);
	    byte remotePartID = Byte.parseByte(rSplit[1]);
	    TLongArrayList remoteAdj = remoteNeighbourMap.get(remotePartID);
	    if (remoteAdj == null) {
		remoteAdj = new TLongArrayList(); // capacity = remoteNs.length / numParts
		remoteNeighbourMap.put(remotePartID, remoteAdj);
	    }
	    remoteAdj.add(vID);
	}
    }


    /**
     * Construct adj obj from binary input
     * 
     * @param srcVID
     * @param localNeighbourList
     * @param remoteAdjList
     */
    public AdjListObject(PartitionedAdjacencyMapEntry entry) {
	assert entry.hasValues() : "entry did not have populated values";
	vertexId = entry.sourceVID; // get vertex ID
	localNeighbourList = entry.localAdjacencyList; // capacity = localNs.length
	remoteNeighbourMap = entry.remoteAdjacencyMap.size() == 0 ? EMPTY_MAP : entry.remoteAdjacencyMap;

    }


    public long getVid() {
	return this.vertexId;
    }


    public TByteObjectHashMap<TLongArrayList> getRemoteAdjMap() {
	return this.remoteNeighbourMap;
    }


    public TLongArrayList getLocalAdjList() {
	return localNeighbourList;
    }


    public void writePartitionedAdjacencyList(byte partID, OutputStream os) throws IOException {
	TByteList rpid = new TByteArrayList();
	TLongArrayList remoteAdjacencyList = new TLongArrayList();
	TByteObjectIterator<TLongArrayList> remoteIter = remoteNeighbourMap.iterator();
	while (remoteIter.hasNext()) {
	    remoteIter.advance();
	    byte rpart = remoteIter.key();
	    TLongArrayList rlist = remoteIter.value();
	    for (int i = 0; i < rlist.size(); i++) {
		rpid.add(rpart);
		remoteAdjacencyList.add(rlist.get(i));
	    }
	}

	GraphUtils.writePartitionedAdjacencyList(partID, vertexId, localNeighbourList, rpid, remoteAdjacencyList, os);
    }
}

