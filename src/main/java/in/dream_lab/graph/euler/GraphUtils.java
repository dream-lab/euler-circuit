package in.dream_lab.graph.euler;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import gnu.trove.iterator.TByteObjectIterator;
import gnu.trove.iterator.TLongIterator;
import gnu.trove.iterator.TLongObjectIterator;
import gnu.trove.list.TByteList;
import gnu.trove.list.TLongList;
import gnu.trove.list.array.TByteArrayList;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.map.TByteObjectMap;
import gnu.trove.map.TLongObjectMap;
import gnu.trove.map.hash.TByteObjectHashMap;
import gnu.trove.map.hash.TLongObjectHashMap;


/**
 * Various graph I/O and data structure utilities using a binary format and arrays
 * 
 * @author simmhan
 *
 */
public class GraphUtils {

    ////////////////////////////////////////////////////
    // UTILITY METHODS FOR PARTITIONED BINARY GRAPH
    // TO BE CONVERTED TO/FROM ADJ LIST AND ADJ ARRAY
    ////////////////////////////////////////////////////

    /**
     * Writes a single adjacency list entry to output stream in binary format.
     * 
     * Entries are of the form:
     * [partID(1b)][sourceVID(8b)][localDegree(4b)][localVID_1(8b)]...[localVID_l(8b)][remoteDegree(4b)][remotePID+remoteVID_1(8b)]...[remotePID+remoteVID_r(8b)]
     * 
     * where local and remote degrees are 4 byte integers, source and local/remote sink vertex IDs are 8 byte longs, and
     * partition ID is a byte.
     * 
     * Each entry's length in bytes is 4+8+8*(l+r).
     * 
     * NOTE: the remote partition ID for a remote vertex ID is set as the MSB byte for the remote vertex ID.
     * So only 7 LSB bytes of the remote VID are useful for representing the remote VID.
     * The MSB byte has to be extracted and zeroed out from the remoteVID before use.
     * 
     * @param v
     * @param n
     * @param os
     * @throws IOException
     */
    public static void writePartitionedAdjacencyList(byte pid, long v, TLongList ln, TByteList rpid,
	    TLongArrayList remoteAdjacencyList, OutputStream os) throws IOException {

	assert rpid != null && ln != null && remoteAdjacencyList != null && os != null : "input params were null";
	assert rpid.size() == remoteAdjacencyList
		.size() : "remote sink vertices count did not match remote partition ID count. Size of remote part ID: "
			+ rpid.size() + "; size of remoteAdjacencyList: " + remoteAdjacencyList.size();

	DataOutputStream out = new DataOutputStream(os);
	// write prefix
	out.writeByte(pid); // 1b partition ID
	out.writeLong(v); // source VID

	// write local
	int localCount = ln.size();
	out.writeInt(localCount); // local sink degree
	for (int l = 0; l < localCount; l++) {
	    out.writeLong(ln.get(l)); // local sink ID
	}

	// write remote
	int remoteCount = remoteAdjacencyList.size();
	out.writeInt(remoteCount); // local sink degree
	for (int r = 0; r < remoteCount; r++) {
	    long rv = rpid.get(r); // get remote PID
	    assert rv != pid : "source and remote sink part IDs were the same for source VID " + v + " and remote VID "
		    + remoteAdjacencyList.get(r) + " with part ID " + pid;
	    rv = (rv << 56) | remoteAdjacencyList.get(r); // shift remote PID to MSB byte and OR with remote VID
	    out.writeLong(rv); // remote pid + sink ID
	}
    }


    public static void writePartitionedAdjacencyListText(byte pid, long v, TLongList ln,
	    TByteObjectHashMap<TLongArrayList> remAdj, BufferedWriter os) throws IOException {


	assert remAdj != null && ln != null && os != null : "input params were null";

	// write prefix
	os.write(Byte.toString(pid)); // 1b partition ID
	os.write(':');
	os.write(Long.toString(v)); // source VID
	os.write(' ');

	// write local
	int localCount = ln.size();
	for (int l = 0; l < localCount; l++) {
	    os.write(Long.toString(ln.get(l))); // local sink ID
	    os.write(l == (localCount - 1) ? ' ' : ',');
	}

	// write remote
	TByteObjectIterator<TLongArrayList> remIter = remAdj.iterator();
	while (remIter.hasNext()) {
	    remIter.advance();

	    byte rp = remIter.key(); // get remote PID
	    TLongArrayList rvals = remIter.value();
	    assert rp != pid : "source and remote sink part IDs were the same for source VID " + v
		    + " and remote VID of size " + rvals.size() + " with part ID " + pid;
	    os.write(Byte.toString(rp)); // 1b partition ID
	    os.write(':');
	    int rlen = rvals.size();
	    for (int i = 0; i < rlen; i++) {
		long rv = rvals.get(i);
		os.write(Long.toString(rv)); // source VID
		os.write(i == (rlen - 1) ? ' ' : ',');
	    }
	    // os.write(';');
	}
	os.write('\n');
    }


    /**
     * Helper class to return the the Partitioned Adjacency List entry.
     * This is reusable in that the same instance of the class can be used across multiple calls.
     * 
     * @author simmhan
     *
     */
    public static class PartitionedAdjacencyListEntry {
	public byte partitionID;
	public long sourceVID;
	public TLongArrayList localAdjacencyList;
	public TByteList remotePartitionList;
	public TLongArrayList remoteAdjacencyList;
	private boolean hasValues;


	public PartitionedAdjacencyListEntry() {
	    hasValues = false;
	}


	public boolean hasValues() {
	    return this.hasValues;
	}


	public void reset() {
	    hasValues = false;
	    localAdjacencyList = null;
	    remotePartitionList = null;
	    remoteAdjacencyList = null;
	}


	public void set(byte partitionID, long sourceVID, TLongArrayList localAdjacencyList,
		TByteList remotePartitionList, TLongArrayList remoteAdjacencyList) {
	    this.partitionID = partitionID;
	    this.sourceVID = sourceVID;
	    this.localAdjacencyList = localAdjacencyList;
	    this.remoteAdjacencyList = remoteAdjacencyList;
	    this.remotePartitionList = remotePartitionList;
	    hasValues = true;
	}
    }


    /**
     * Read a partitioned input binary adjacency list.
     * Returns the partition id of the source vertex, the source vertex, list of local vertices, list of remote vertex
     * partitions, and list of remote vertex IDs.
     * 
     * Entry is the class in which the return values have to be passed back.
     * User should call has values before they access the output values.
     * 
     * Returns false if we reached the EOF.
     * 
     * @param is
     * @throws IOException
     */
    public static boolean readPartitionedAdjacencyEntry(InputStream is, PartitionedAdjacencyListEntry entry)
	    throws IOException {

	try {
	    DataInputStream in = new DataInputStream(is);

	    // read prefix
	    byte partID = in.readByte();
	    long srcVID = in.readLong();

	    // read local
	    int localCount = in.readInt(); // NOTE: only 31 bits have singed value
	    assert localCount >= 0 : "adjacency list entry's local vertex count was < 0";
	    assert localCount <= Integer.MAX_VALUE : "adjacency list entry's local vertex count was > Integer.MAX_VALUE. Array overflow!";

	    TLongArrayList localAdjList = new TLongArrayList(localCount);
	    for (int i = 0; i < localCount; i++) {
		localAdjList.add(in.readLong());
	    }

	    // read remote
	    int remoteCount = in.readInt(); // NOTE: only 31 bits have singed value
					    // //Integer.toUnsignedLong(in.readInt());
	    assert remoteCount >= 0 : "adjacency list entry's remote vertex count was < 0";
	    assert remoteCount <= Integer.MAX_VALUE : "adjacency list entry's remote vertex count was > Integer.MAX_VALUE. Array overflow!";

	    final long RVID_MASK = 0x00FFFFFFFFFFFFFFL; // set 14 MSB hex bytes (56 MSB)
	    TLongArrayList remoteAdjList = new TLongArrayList(remoteCount);
	    TByteList remotePartList = new TByteArrayList(remoteCount);
	    for (int i = 0; i < remoteCount; i++) {
		long rv = in.readLong();
		long rvid = rv & RVID_MASK; // get the lower 56 bits as remote VID
		byte rpid = (byte) (rv >>> 56); // use the upper 8 bits as partition ID
		assert rpid != partID : "source and remote sink part IDs were the same for source VID " + srcVID
			+ " and remote VID " + rvid + " with part ID " + partID;

		remoteAdjList.add(rvid);
		remotePartList.add(rpid);
	    }

	    entry.set(partID, srcVID, localAdjList, remotePartList, remoteAdjList);

	    return true;
	} catch (EOFException e) {
	    return false; // return NULL if read was unsuccessful due to EOF
	}
    }


    /**
     * Helper class to return the the Partitioned Adjacency List entry.
     * This is reusable in that the same instance of the class can be used across multiple calls.
     * 
     * @author simmhan
     *
     */
    public static class PartitionedAdjacencyMapEntry extends PartitionedAdjacencyMapValue {
	public long sourceVID;

	protected boolean hasValues;


	public PartitionedAdjacencyMapEntry() {
	    hasValues = false;
	}


	public boolean hasValues() {
	    return this.hasValues;
	}


	public void reset() {
	    hasValues = false;
	    localAdjacencyList = null;
	    remoteAdjacencyMap = null;
	}


	public void set(byte partitionID, long sourceVID, TLongArrayList localAdjacencyList,
		TByteObjectHashMap<TLongArrayList> remoteAdjacencyMap) {
	    this.partitionID = partitionID;
	    this.sourceVID = sourceVID;
	    this.localAdjacencyList = localAdjacencyList;
	    this.remoteAdjacencyMap = remoteAdjacencyMap;
	    hasValues = true;
	}
    }

    /**
     * Class to store the partitioned adj entry for a source vertex.
     * Contains the src part id, local vertices, and a map from a remote part ID to remote vertices
     * 
     * @author simmhan
     *
     */
    public static class PartitionedAdjacencyMapValue {
	public byte partitionID;
	public TLongArrayList localAdjacencyList;
	public TByteObjectHashMap<TLongArrayList> remoteAdjacencyMap;


	public static PartitionedAdjacencyMapValue copyFrom(PartitionedAdjacencyMapValue o) {
	    PartitionedAdjacencyMapValue e = new PartitionedAdjacencyMapValue();
	    e.set(o.partitionID, o.localAdjacencyList, o.remoteAdjacencyMap);
	    return e;
	}


	public void set(byte partitionID, TLongArrayList localAdjacencyList,
		TByteObjectHashMap<TLongArrayList> remoteAdjacencyMap) {
	    this.partitionID = partitionID;
	    this.localAdjacencyList = localAdjacencyList;
	    this.remoteAdjacencyMap = remoteAdjacencyMap;
	}
    }


    /**
     * Read a partitioned input binary adjacency list.
     * Returns the partition id of the source vertex, the source vertex, list of local vertices,
     * map from remote partition ID to remote vertex IDs in that partition.
     * 
     * Entry is the class in which the return values have to be passed back.
     * User should call has values before they access the output values.
     * 
     * @param is
     * @param entry
     * @throws IOException
     */
    public static boolean readPartitionedAdjacencyMap(InputStream is, PartitionedAdjacencyMapEntry entry)
	    throws IOException {
	try {
	    DataInputStream in = new DataInputStream(is);
	    return readPartitionedAdjacencyMap(in, entry);

	} catch (EOFException e) {
	    return false; // return NULL if read was unsuccessful due to EOF
	}
    }


    /**
     * Read a partitioned input binary adjacency list.
     * Returns the partition id of the source vertex, the source vertex, list of local vertices,
     * map from remote partition ID to remote vertex IDs in that partition.
     * 
     * Entry is the class in which the return values have to be passed back.
     * User should call has values before they access the output values.
     * 
     * @param is
     * @param entry
     * @throws IOException
     */
    public static boolean readPartitionedAdjacencyMap(DataInputStream in, PartitionedAdjacencyMapEntry entry)
	    throws IOException {
	try {

	    // read prefix
	    byte partID = in.readByte();
	    long srcVID = in.readLong();

	    // read local
	    int localCount = in.readInt(); // NOTE: only 31 bits have singed value
	    assert localCount >= 0 : "adjacency list entry's local vertex count was < 0";
	    assert localCount <= Integer.MAX_VALUE : "adjacency list entry's local vertex count was > Integer.MAX_VALUE. Array overflow!";

	    TLongArrayList localAdjList = new TLongArrayList(localCount);
	    for (int i = 0; i < localCount; i++) {
		localAdjList.add(in.readLong());
	    }

	    // read remote
	    int remoteCount = in.readInt(); // NOTE: only 31 bits have singed value
					    // //Integer.toUnsignedLong(in.readInt());
	    assert remoteCount >= 0 : "adjacency list entry's remote vertex count was < 0";
	    assert remoteCount <= Integer.MAX_VALUE : "adjacency list entry's remote vertex count was > Integer.MAX_VALUE. Array overflow!";

	    final long RVID_MASK = 0xFFFFFFFFFFFFFFL; // set 14 MSB hex bytes (56 MSB)
	    TByteObjectHashMap<TLongArrayList> remoteAdjMap = new TByteObjectHashMap<TLongArrayList>(remoteCount);
	    for (int i = 0; i < remoteCount; i++) {
		long rv = in.readLong();
		long rvid = rv & RVID_MASK; // get the lower 56 bits as remote VID
		byte rpid = (byte) (rv >>> 56); // use the upper 8 bits as partition ID
		assert rpid != partID : "source and remote sink part IDs were the same for source VID " + srcVID
			+ " and remote VID " + rvid + " with part ID " + partID;
		TLongArrayList remoteList = remoteAdjMap.get(rpid);
		if (remoteList == null) {
		    remoteList = new TLongArrayList();
		    remoteAdjMap.put(rpid, remoteList);
		}
		remoteList.add(rvid);
	    }

	    entry.set(partID, srcVID, localAdjList, remoteAdjMap);
	    return true;
	} catch (EOFException e) {
	    // // tempdel
	    // System.out.printf("readPartitionedAdjacencyMap caught EOFException,TimestampMS,%d%n",
	    // System.currentTimeMillis(), e.getMessage());
	    return false; // return NULL if read was unsuccessful due to EOF
	}
    }


    /**
     * Read a partitioned adj list binary file into a partitioned map
     * 
     * @param is
     * @return
     * @throws IOException
     */
    public static TLongObjectMap<PartitionedAdjacencyMapValue> readPartitionedAdjacencyMap(InputStream is)
	    throws IOException {
	TLongObjectMap<PartitionedAdjacencyMapValue> partAdjMap = new TLongObjectHashMap<PartitionedAdjacencyMapValue>();

	// create a list of Adj Objects
	PartitionedAdjacencyMapEntry entry = new PartitionedAdjacencyMapEntry();
	while (GraphUtils.readPartitionedAdjacencyMap(is, entry)) {
	    long srcVID = entry.sourceVID;
	    PartitionedAdjacencyMapValue partAdjList = PartitionedAdjacencyMapValue.copyFrom(entry);
	    partAdjMap.put(srcVID, partAdjList);
	    entry.reset();
	}

	return partAdjMap;
    }


    /**
     * Converts a partitioned adj map into a one where remote vertices are converted to local vertices and all
     * partitioning info is removed.
     * NOTE: if moveAndDelete is true, the content is MOVED and the old values in the partAdjMap will be lost. This
     * conserves memory if the input map is not required.
     * 
     * @param is
     * @throws IOException
     */
    public static TLongObjectHashMap<TLongArrayList> flattenPartitionedAdjList(
	    TLongObjectMap<PartitionedAdjacencyMapValue> partAdjMap, boolean moveAndDelete) throws IOException {

	// create a flattened map with same size as input
	TLongObjectHashMap<TLongArrayList> adjMap = new TLongObjectHashMap<>(partAdjMap.size());

	// iterate thru each entry. Merge remote neighbors with local.
	TLongObjectIterator<PartitionedAdjacencyMapValue> partAdjMapIter = partAdjMap.iterator();
	while (partAdjMapIter.hasNext()) {
	    partAdjMapIter.advance();
	    long srcVID = partAdjMapIter.key();
	    PartitionedAdjacencyMapValue partAdjList = partAdjMapIter.value();
	    if (moveAndDelete) partAdjMapIter.remove(); // remove the entry from the map

	    // Increase capacity of local list to accomodate remote neighbors
	    TLongArrayList localAdjList = partAdjList.localAdjacencyList;
	    TByteObjectMap<TLongArrayList> remoteAdjMap = partAdjList.remoteAdjacencyMap;

	    int capacity = localAdjList.size();
	    TByteObjectIterator<TLongArrayList> remoteAdjMapIter = remoteAdjMap.iterator();
	    while (remoteAdjMapIter.hasNext()) {
		remoteAdjMapIter.advance();
		TLongArrayList remoteAdjList = remoteAdjMapIter.value();
		assert remoteAdjList != null
			&& remoteAdjList.size() > 0 : "Found a partition with NULL or empty remote adj list";
		capacity += remoteAdjList.size();
	    }
	    localAdjList.ensureCapacity(capacity);

	    // append remote vertices to local list
	    remoteAdjMapIter = remoteAdjMap.iterator();
	    while (remoteAdjMapIter.hasNext()) {
		remoteAdjMapIter.advance();
		TLongArrayList remoteAdjList = remoteAdjMapIter.value();
		capacity += remoteAdjList.size();
		localAdjList.addAll(remoteAdjList);
	    }

	    // add entry in output map
	    adjMap.put(srcVID, localAdjList);
	}

	return adjMap;
    }


    /**
     * Return a metagraph constructed from the partitioned graph.
     * Maps from a source part ID (meta vertex) to a list of neighboring sink part IDs, with the weights for each giving
     * the remote edge cuts.
     * 
     * @param partAdjMap
     * @return
     */
    public static Map<Byte, Map<Byte, Long>> buildMetaGraph(TLongObjectMap<PartitionedAdjacencyMapValue> partAdjMap,
	    boolean verifyRemote, boolean verifyLocal, boolean verifyMetGraphSymmetry) {


	TLongObjectIterator<PartitionedAdjacencyMapValue> partAdjMapIter = partAdjMap.iterator();
	Map<Byte, Map<Byte, Long>> metaGraph = new HashMap<Byte, Map<Byte, Long>>();

	long vc = 0, startTime = System.currentTimeMillis();
	System.out.printf(
		"%d,buildMetaGraph Starts,Verification flags are verifyRemote,%b,verifyLocal,%b,verifyMetGraphSymmetry,%b%n",
		startTime, verifyRemote, verifyLocal, verifyMetGraphSymmetry);

	// iterate through each source vertex
	// add new meta vertex entry for the source vertex's partition, if not present
	// for each remote partition from this source vertex
	// - add a neighbor meta vertex entry for a remote vertex (in a remote partition--verify!), if not present
	// -- ASSERT that remote vertices do not have the local part ID
	// - increment count of edge from local to remote partition
	while (partAdjMapIter.hasNext()) {
	    partAdjMapIter.advance();
	    long srcVID = partAdjMapIter.key();
	    PartitionedAdjacencyMapValue adjMapValue = partAdjMapIter.value();

	    byte srcPartID = adjMapValue.partitionID;
	    Map<Byte, Long> metaAdjList = metaGraph.get(srcPartID);
	    if (metaAdjList == null) { // add entry for source part, if not present
		metaAdjList = new HashMap<Byte, Long>();
		metaGraph.put(srcPartID, metaAdjList);
	    }

	    TByteObjectMap<TLongArrayList> remoteAdjMap = adjMapValue.remoteAdjacencyMap;
	    TByteObjectIterator<TLongArrayList> remoteIter = remoteAdjMap.iterator();
	    while (remoteIter.hasNext()) {
		remoteIter.advance();
		byte remotePartID = remoteIter.key();
		TLongArrayList remoteAdjList = remoteIter.value();
		assert remotePartID != srcPartID : "source partition ID and remote sink partition IDs were the same! srcVID="
			+ srcVID + " with Part ID=" + srcPartID;
		Long remotePartWeight = metaAdjList.get(remotePartID);
		if (remotePartWeight == null) remotePartWeight = 0L;
		remotePartWeight += remoteAdjList.size();
		metaAdjList.put(remotePartID, remotePartWeight);

		// check if each remote vertex is present with a partition ID that matches the remote part ID
		// check if a symmetric reverse edge is present from the remote vertex to this vertex
		if (verifyRemote) for (int i = 0; i < remoteAdjList.size(); i++) {
		    Byte actualRemotePartID = null;
		    long remoteVID = remoteAdjList.get(i);
		    PartitionedAdjacencyMapValue remoteObj = partAdjMap.get(remoteVID);

		    assert (partAdjMap.containsKey(remoteVID)
			    && (actualRemotePartID = remoteObj.partitionID) == remotePartID) : "Source vertex " + srcVID
				    + " thinks the remote sink vertex " + remoteVID + " is in remote part ID "
				    + remotePartID + " but it is present in " + actualRemotePartID;

		    assert remoteObj.remoteAdjacencyMap.containsKey(srcPartID)
			    && remoteObj.remoteAdjacencyMap.get(srcPartID).contains(srcVID) : "Source vertex " + srcVID
				    + " has a remote vertex sink " + remoteVID + " in remote part ID " + remotePartID
				    + ", but the remote sink does not have a remote neighbor back to src ID in src parition "
				    + srcPartID;
		}
	    }

	    // verify local adj map
	    if (verifyLocal) {
		TLongArrayList localAdjList = adjMapValue.localAdjacencyList;
		for (int i = 0; i < localAdjList.size(); i++) {
		    Byte actualLocalPartID = null;
		    assert (partAdjMap.containsKey(localAdjList.get(i)) && (actualLocalPartID = partAdjMap
			    .get(localAdjList.get(i)).partitionID) == srcPartID) : "Source vertex " + srcVID
				    + " thinks the local sink vertex " + localAdjList.get(i) + " is in local part ID "
				    + srcPartID + " but it is present in " + actualLocalPartID;
		    assert partAdjMap.get(localAdjList.get(i)).localAdjacencyList.contains(srcVID) : "Source vertex "
			    + srcVID + " has a local sink vertex " + localAdjList.get(i) + " is in local part ID "
			    + srcPartID + " but the sink does not have a local edge back to the source VID";
		}
	    }

	    vc++;

	    if (vc % 1000000 == 0) {
		long deltaTime = (System.currentTimeMillis() - startTime);
		System.out.printf("Done,VertexCount,%d,DurationMS,%d,CummVertexPerSec,%d%n", vc, deltaTime,
			(long) ((double) vc / (deltaTime / 1000)));
	    }
	}

	// Logging
	// for (Entry<Byte, Map<Byte, Long>> metaAdjEntry : metaGraph.entrySet()) {
	// byte srcPartID = metaAdjEntry.getKey();
	// System.out.printf("%d [", srcPartID);
	// Map<Byte, Long> metaAdjList = metaAdjEntry.getValue();
	// for (Entry<Byte, Long> edgeEntry : metaAdjList.entrySet()) {
	// System.out.printf(" [%d,%d]", edgeEntry.getKey(), edgeEntry.getValue());
	// }
	// System.out.printf(" ]%n");
	// }

	// ASSERT that the metagraph is symmetric
	if (verifyMetGraphSymmetry) for (Entry<Byte, Map<Byte, Long>> metaAdjEntry : metaGraph.entrySet()) {
	    byte srcPartID = metaAdjEntry.getKey();
	    Map<Byte, Long> metaAdjList = metaAdjEntry.getValue();
	    for (Entry<Byte, Long> edgeEntry : metaAdjList.entrySet()) {
		Byte sinkPartID = edgeEntry.getKey();
		Long sinkWeight = edgeEntry.getValue();
		assert metaGraph.get(sinkPartID) != null && metaGraph.get(sinkPartID).containsKey(
			srcPartID) : "Meta graph was not symmetric. Source Part ID " + srcPartID + " with Sink Part ID "
				+ sinkPartID + " and edge weight " + sinkWeight + " did not have a reverse entry";
		assert metaGraph.get(sinkPartID).get(srcPartID)
			.equals(sinkWeight) : "Meta graph was not symmetric. Source Part ID " + srcPartID
				+ " with Sink Part ID " + sinkPartID + " and edge weight " + sinkWeight
				+ " has a different reverse edge weight " + metaGraph.get(sinkPartID).get(srcPartID);
	    }
	}

	return metaGraph;
    }


    ////////////////////////////////////////////////////
    // UTILITY METHODS FOR SINGLE BINARY GRAPH
    // TO BE CONVERTED TO/FROM ADJ LIST AND ADJ ARRAY
    // BINARY FILE IS OF THE FORM
    // [degreeCount+1 (4b)][source (8b)][sink1 (8b)][sink2]...[sink_d (8b)]
    ////////////////////////////////////////////////////

    /**
     * Writes a single adjacency list entry to output stream in binary format.
     * 
     * Entries are of the form:
     * [degreeCount+1 (4b)][source (8b)][sink1 (8b)][sink2][sink_d (8b)]
     * 
     * where degree is a 4 byte integer, and source/sinks are 8 byte longs.
     * Each entry's length in bytes is 4+8(degree+1)
     * 
     * @param v
     * @param n
     * @param os
     * @throws IOException
     */
    public static void writeAdjacencyList(long v, List<Long> n, OutputStream os) throws IOException {

	assert n != null && os != null : "input params were null";

	DataOutputStream out = new DataOutputStream(os);
	out.writeInt(n.size() + 1); // SOURCE vid + DEGREE count
	out.writeLong(v);
	for (Long w : n) {
	    out.writeLong(w);
	}
    }


    /**
     * Same as above, but for Trove list
     * 
     * @param v
     * @param n
     * @param os
     * @throws IOException
     */
    public static void writeAdjacencyList(long v, TLongList n, OutputStream os) throws IOException {

	assert n != null && os != null : "input params were null";

	DataOutputStream out = new DataOutputStream(os);
	out.writeInt(n.size() + 1); // SOURCE vid + DEGREE count
	out.writeLong(v);
	for (int i = 0; i < n.size(); i++)
	    out.writeLong(n.get(i));

    }


    /**
     * Writes all vertices in the given map of source vertex to list of sink vertices to output stream in binary format
     * 
     * @param adjListMap
     * @param os
     * @throws IOException
     */
    public static void writeAdjacencyList(Map<Long, List<Long>> adjListMap, OutputStream os) throws IOException {

	assert adjListMap != null && os != null : "input params were null";

	DataOutputStream out = new DataOutputStream(os);
	for (Entry<Long, List<Long>> vn : adjListMap.entrySet()) {
	    out.writeInt(vn.getValue().size() + 1);
	    out.writeLong(vn.getKey());
	    for (Long w : vn.getValue()) {
		out.writeLong(w);
	    }
	}
    }


    /**
     * Same as above, but for Trove list
     * 
     * @param adjListMap
     * @param os
     * @throws IOException
     */
    public static void writeAdjacencyList(TLongObjectHashMap<TLongArrayList> adjListMap, OutputStream os)
	    throws IOException {

	assert adjListMap != null && os != null : "input params were null";

	DataOutputStream out = new DataOutputStream(os);
	TLongObjectIterator<TLongArrayList> adjListIter = adjListMap.iterator();
	while (adjListIter.hasNext()) {
	    adjListIter.advance();
	    TLongList v = adjListIter.value();
	    int vs = v.size();
	    out.writeInt(vs + 1);
	    out.writeLong(adjListIter.key());
	    for (int i = 0; i < vs; i++)
		out.writeLong(v.get(i));
	}
    }


    /**
     * Writes adj list to binary file in a canonical format.
     * Source vertices are sorted in increasing order.
     * Adj vertices for each source are sorted in increasing order.
     * 
     * @param adjListMap
     * @param os
     * @throws IOException
     */
    public static void writeAdjacencyListCanonical(TLongObjectMap<TLongArrayList> adjListMap, OutputStream os)
	    throws IOException {

	assert adjListMap != null && os != null : "input params were null";

	// sort the source vertices
	long[] keys = adjListMap.keys();
	Arrays.sort(keys);

	DataOutputStream out = new DataOutputStream(os);
	for (int i = 0; i < keys.length; i++) {
	    TLongList v = adjListMap.get(keys[i]);
	    int vs = v.size();
	    out.writeInt(vs + 1); // write degree+1
	    out.writeLong(keys[i]); // write source
	    if (vs != 0) { // skip if adj list empty
		// sort adj values
		long[] adjArr = v.toArray();
		Arrays.sort(adjArr);
		// write sorted adj list
		for (int j = 0; j < vs; j++)
		    out.writeLong(adjArr[j]);
	    }
	}
    }


    /**
     * Write the file to a character separated value file. The character can be passed as input.
     * 
     * @param adjMap
     * @param bw
     * @throws IOException
     */
    private static void writeAdjacencyListCsv(TLongObjectHashMap<TLongArrayList> adjListMap, char separator,
	    BufferedWriter bw) throws IOException {
	writeAdjacencyListCsv(adjListMap, separator, separator, bw);
    }


    /**
     * Write the file to a character separated value file. The character can be passed as input.
     * 
     * @param adjMap
     * @param bw
     * @throws IOException
     */
    private static void writeAdjacencyListCsv(TLongObjectHashMap<TLongArrayList> adjListMap, char srcSeparator,
	    char sinkSeparator, BufferedWriter bw) throws IOException {
	assert adjListMap != null && bw != null : "input params were null";

	// sort the source vertices
	TLongObjectIterator<TLongArrayList> adjListMapIter = adjListMap.iterator();
	while (adjListMapIter.hasNext()) {
	    adjListMapIter.advance();
	    // get and write source vertex id
	    long v = adjListMapIter.key();
	    bw.write(Long.toString(v));

	    // iterate and write each sink vid
	    TLongArrayList adj = adjListMapIter.value();
	    for (int i = 0; i < adj.size(); i++) {
		bw.write(i == 0 ? srcSeparator : sinkSeparator);
		bw.write(Long.toString(adj.get(i)));
	    }

	    // terminate with newline
	    bw.write('\n');
	}
    }


    /**
     * Write the file to a character separated value file. The character can be passed as input.
     * 
     * @param adjMap
     * @param bw
     * @throws IOException
     */
    private static void writeAdjacencyListBlogel(TLongObjectHashMap<TLongArrayList> adjListMap, char srcSeparator,
	    char sinkSeparator, BufferedWriter bw) throws IOException {
	assert adjListMap != null && bw != null : "input params were null";

	// sort the source vertices
	TLongObjectIterator<TLongArrayList> adjListMapIter = adjListMap.iterator();
	while (adjListMapIter.hasNext()) {
	    adjListMapIter.advance();
	    // get and write source vertex id
	    long v = adjListMapIter.key();
	    bw.write(Long.toString(v));

	    // iterate and write each sink vid
	    TLongArrayList adj = adjListMapIter.value();
	    bw.write(srcSeparator);
	    bw.write(Long.toString(adj.size()));
	    for (int i = 0; i < adj.size(); i++) {
		bw.write(sinkSeparator);
		bw.write(Long.toString(adj.get(i)));
	    }

	    // terminate with newline
	    bw.write('\n');
	}
    }


    /**
     * Writes a single adjacency array entry to output stream in binary format, prefixing the length.
     * The input array is of the form:
     * [source (8b)][sink1 (8b)][sink2]...[sink_d (8b)]
     * 
     * @param vn
     * @param os
     * @throws IOException
     */
    public static void writeAdjacencyArray(long[] vn, OutputStream os) throws IOException {

	assert vn != null && os != null : "input params were null";
	assert vn.length >= 1 : "source vertex was empty";

	DataOutputStream out = new DataOutputStream(os);
	out.writeInt(vn.length);
	for (Long w : vn) {
	    out.writeLong(w);
	}
    }


    /**
     * Writes a list of adjacency arrays to output stream in binary format
     * 
     * @param vn
     * @param os
     * @throws IOException
     */
    public static void writeAdjacencyArray(ElasticList<long[]> adjList, OutputStream os) throws IOException {

	assert adjList != null && os != null : "input params were null";

	long startTime = System.currentTimeMillis();

	// logging
	int vcount = 0;
	long lastTime = System.currentTimeMillis();

	DataOutputStream out = new DataOutputStream(os);
	for (long[] vn : adjList) {
	    assert vn.length >= 1 : "source vertex was empty";
	    out.writeInt(vn.length);
	    for (Long w : vn) {
		out.writeLong(w);
	    }

	    // logging
	    if (++vcount % 1000000 == 0) {
		System.out.printf("writeAdjacencyArray,VerticesWritten,%d,TimeTakenMS=%d%n", vcount,
			(System.currentTimeMillis() - lastTime));
		lastTime = System.currentTimeMillis();
	    }
	}
	System.out.printf("writeAdjacencyArray,SrcVerticesWritten,%d,TimeTakenMS=%d%n", adjList.size(),
		(System.currentTimeMillis() - startTime));

    }


    /**
     * Writes a concatenated adjacency array with multiple entries but all with same degree, to output stream in binary
     * format
     * 
     * @param vn
     * @param degree
     * @param os
     * @throws IOException
     */
    public static void writeConcatAdjacencyArray(long[] vn, int degree, OutputStream os) throws IOException {

	assert vn != null && os != null : "input params were null";
	assert vn.length >= 1 && degree >= 0 : "array was empty or degree was negative";
	assert vn.length % (degree + 1) == 0 : "concat array did not have entries that are a multiple of degree+1";
	long startTime = System.currentTimeMillis();

	DataOutputStream out = new DataOutputStream(os);
	int i = 0;
	while (i < vn.length) {
	    out.writeInt(degree + 1); // we have degree+1 entries in a row
	    for (int j = 0; j < (degree + 1); j++) { // write source and all sinks
		out.writeLong(vn[i]);
		i++;
	    }
	}
	System.out.printf("writeConcatAdjacencyArray,SrcVerticesWritten,%d,TimeTakenMS=%d%n", vn.length / (degree + 1),
		(System.currentTimeMillis() - startTime));
    }


    /**
     * Writes a concatenated adjacency array with multiple entries but all with same degree, to output stream in binary
     * format.
     * This additionally appends one sink vertex present in the addedN array to each source vertex. This makes the
     * effective number of sinks of the graph as degree+1.
     * 
     * @param vn
     * @param addedN
     * @param degree
     *            Original number of sink vertices in vn array
     * @param os
     * @throws IOException
     */
    public static void writeConcatInterleaveAdjacencyArray(long[] vn, long[] addedN, int degree, OutputStream os)
	    throws IOException {

	assert vn != null && os != null : "input params were null";
	assert vn.length >= 1 && degree >= 0 : "array was empty or degree was negative";
	assert vn.length % (degree + 1) == 0 : "concat array did not have entries that are a multiple of degree+1";
	assert addedN.length == vn.length
		/ (degree + 1) : "length of new sink to be added and length of source vertices do not match";
	long startTime = System.currentTimeMillis();

	// System.out.printf("vn.length,%d,addedN.length,%d,degree,%d%n", vn.length, addedN.length, degree); // tempdel
	DataOutputStream out = new DataOutputStream(os);
	int i = 0, vc = 0, ec = 0;
	while (i < vn.length) {
	    out.writeInt(degree + 2); // write source and one more than the degree number of sink
	    for (int j = 0; j < (degree + 1); j++) { // write source and all sinks
		out.writeLong(vn[i]);
		i++;
	    }
	    out.writeLong(addedN[vc]); // include added sink suffix
	    vc++;
	    ec += (degree + 1); // degree + addedN
	}
	System.out.printf(
		"writeConcatInterleaveAdjacencyArray,SrcVerticesWritten,%d,Degree,%d,EdgesWritten,%d,TimeTakenMS=%d%n",
		addedN.length, degree, ec, (System.currentTimeMillis() - startTime));
    }


    /**
     * reads and inserts a single adjacency list from input into a map, with key as source vertex and value as adjacency
     * list of sink vertices.
     * 
     * @param adjListMap
     * @param is
     * @return
     * @throws IOException
     */
    public static boolean readEntryIntoAdjacencyMap(Map<Long, List<Long>> adjListMap, InputStream is)
	    throws IOException {

	assert adjListMap != null && is != null : "input params were null";

	try {
	    DataInputStream in = new DataInputStream(is);
	    int count = in.readInt();

	    assert count >= 1 : "adjacency list file entry count was < 1";

	    long v = in.readLong();
	    List<Long> n = new ArrayList<>(count - 1);
	    for (int i = 1; i < count; i++) {
		n.add(in.readLong());
	    }
	    adjListMap.put(v, n);
	    return true;
	} catch (EOFException e) {
	    return false; // return NULL if read was unsuccessful due to EOF
	}

    }


    /**
     * reads and inserts all available adjacency list entries from input into a map, with key as source vertex and value
     * as adjacency list of sink vertices.
     * 
     * @param adjListMap
     * @param is
     * @return
     * @throws IOException
     */
    public static TLongObjectHashMap<TLongArrayList> readAllIntoAdjacencyTMap(InputStream is, int initCapacity)
	    throws IOException {

	assert is != null : "input params were null";
	TLongObjectHashMap<TLongArrayList> adjListMap = new TLongObjectHashMap<TLongArrayList>(
		initCapacity > 0 ? initCapacity : 10 * 1000 * 1000);
	DataInputStream in = new DataInputStream(is);

	// logging
	int vcount = 0, ecount = 0;
	long lastTime = System.currentTimeMillis();
	try {

	    while (true) {
		int count = in.readInt();

		assert count >= 1 : "adjacency list file entry count was < 1;" + count;

		long v = in.readLong();
		TLongArrayList n = new TLongArrayList(count - 1);
		for (int i = 1; i < count; i++) {
		    n.add(in.readLong());
		}
		adjListMap.put(v, n);

		// logging
		ecount += (count - 1);
		if (++vcount % 1000000 == 0) {
		    System.out.printf("readAllIntoAdjacencyTMap,VerticesLoaded,%d,EdgesLoaded,%d,TimeTakenMS=%d%n",
			    vcount, ecount, (System.currentTimeMillis() - lastTime));
		    lastTime = System.currentTimeMillis();
		}
	    }
	} catch (EOFException e) {
	    return adjListMap;
	} finally {
	    System.out.printf("readAllIntoAdjacencyTMap,VerticesLoaded,%d,EdgesLoaded,%d,TimeTakenMS=%d%n", vcount,
		    ecount, (System.currentTimeMillis() - lastTime));
	}

    }


    /**
     * returns a map of the degree distribution of the input graph
     * 
     * @param adjMap
     */
    public static Map<Integer, Long> printDegreeDistribution(TLongObjectHashMap<TLongArrayList> adjMap) {
	Map<Integer, Long> degMap = new HashMap<>(5000);

	TLongObjectIterator<TLongArrayList> adjIter = adjMap.iterator();
	while (adjIter.hasNext()) {
	    adjIter.advance();
	    int deg = adjIter.value().size();
	    Long count = degMap.get(deg);
	    if (count == null) count = 0L;
	    count++;
	    degMap.put(deg, count);
	}

	return degMap;
    }


    /**
     * reads and returns a single adjacency array from input
     * 
     * @param is
     * @return
     * @throws IOException
     */
    public static long[] readAsAdjacencyArray(InputStream is) throws IOException {

	assert is != null : "input params were null";
	try {
	    DataInputStream in = new DataInputStream(is);
	    int count;
	    count = in.readInt();

	    assert count >= 1 : "adjacency list file entry count was < 1";

	    long[] vn = new long[count];
	    for (int i = 0; i < count; i++) {
		vn[i] = in.readLong();
	    }
	    return vn;
	} catch (EOFException e) {
	    return null; // return NULL if read was unsuccessful due to EOF
	}
    }


    /**
     * Loads a binary graph into adjacency arrays, and returns a list of all the arrays.
     * 
     * @param vertices
     * @param adjListMap
     * @param degree
     * @return
     * @throws IOException
     */
    public static ElasticList<long[]> loadBinGraphAsArray(InputStream is, boolean skipOddDegree) throws IOException {

	// step of 1000, init capacity 1000
	ElasticList<long[]> adjList = new ElasticList<long[]>(n -> new long[n][], 10000, 1000);
	long[] adj;
	// logging
	int vcount = 0, ecount = 0;
	long lastTime = System.currentTimeMillis();

	while ((adj = readAsAdjacencyArray(is)) != null) {
	    if (skipOddDegree && (adj.length - 1) % 2 == 1) continue;
	    adjList.add(adj);

	    // logging
	    ecount += (adj.length - 1);
	    if (++vcount % 1000000 == 0) {
		System.out.printf("loadBinGraphAsArray,VerticesLoaded,%d,EdgesLoaded,%d,TimeTakenMS=%d%n", vcount,
			ecount, (System.currentTimeMillis() - lastTime));
		lastTime = System.currentTimeMillis();
	    }
	}

	System.out.printf("Done loadBinGraphAsArray,VerticesLoaded,%d,EdgesLoaded,%d,TimeTakenMS=%d%n", vcount, ecount,
		(System.currentTimeMillis() - lastTime));

	return adjList;
    }


    /**
     * Loads a binary graph into adjacency arrays, and groups them by their degree into a map, which has key as degree
     * and value as a list of adj arrays.
     * 
     * @param vertices
     * @param adjListMap
     * @param degree
     * @return
     * @throws IOException
     */
    public static Map<Integer, ElasticList<long[]>> loadBinGraphAsArrayByDegree(InputStream is, boolean skipEvenDegree,
	    int initCapacity) throws IOException {

	long startTime = System.currentTimeMillis(), lastTime = startTime;
	Map<Integer, ElasticList<long[]>> adjMap = new HashMap<>(initCapacity <= 0 ? 1000 : initCapacity);

	ElasticList<long[]> adjList;
	long[] adj;
	int vcount = 0, dcount = 0, ecount = 0;

	while ((adj = readAsAdjacencyArray(is)) != null) {
	    int degree = adj.length - 1; // skip source vertex
	    if (skipEvenDegree && degree % 2 == 0) continue;
	    adjList = adjMap.get(degree);
	    if (adjList == null) {
		adjList = new ElasticList<long[]>(n -> new long[n][], 1000, 100);
		adjMap.put(degree, adjList);
		dcount++;
	    }
	    adjList.add(adj);
	    vcount++;
	    ecount += degree;
	    if (vcount % 1000000 == 0) {
		System.out.printf("loadBinGraphAsArrayByDegree,VerticesLoaded,%d,DegreeBuckets,%d,TimeTakenMS=%d%n",
			vcount, dcount, (System.currentTimeMillis() - lastTime));
		lastTime = System.currentTimeMillis();
	    }
	}

	System.out.printf(
		"loadBinGraphAsArrayByDegree,skipEvenDegree,%b,TotalVertexLoaded,%d,TotalEdgesLoaded,%d,TotalDegreeBuckets,%d,TimeTakenMS=%d%n",
		skipEvenDegree, vcount, ecount, dcount, (System.currentTimeMillis() - startTime));
	return adjMap;
    }


    /**
     * Given a list with items containing adjacency list arrays, this returns a single array that has all thr adj arrays
     * concatenated with each other. All arrays have to be of the same size, which is given by degree+1.
     * 
     * @param adjList
     * @param degree
     * @return
     */
    public static long[] concatAdjacencyArrays(ElasticList<long[]> adjList, int degree) {
	int count = (int) (adjList.size() * (degree + 1));

	long[] foldedArr = new long[count];

	// iterate thru elastic list to array copy adj arr to folded arr
	int offset = 0;
	for (long[] adj : adjList) {
	    System.arraycopy(adj, 0, foldedArr, offset, adj.length);
	    offset += adj.length;
	}

	return foldedArr;
    }


    /**
     * Loads a graph present in binary format into a map with key as source vertex and value as a list of sink vertices
     * 
     * @param vertices
     * @param adjListMap
     * @param degree
     * @return
     * @throws IOException
     */
    public static Map<Long, List<Long>> loadBinGraphAsMap(InputStream is, int initCapacity) throws IOException {

	Map<Long, List<Long>> adjListMap = new HashMap<>(initCapacity <= 0 ? 1000 : initCapacity);
	while (readEntryIntoAdjacencyMap(adjListMap, is)) {}
	return adjListMap;
    }


    /**
     * Converts Siddharth's custom CSV-ish file to std binary format used in this Util.
     * Method can be removed after Siddharth changes his Spark code to generate binary format.
     * 
     * @param br
     * @param os
     * @throws NumberFormatException
     * @throws IOException
     */
    public static void transformGraphCsvToBin(BufferedReader br, OutputStream os)
	    throws NumberFormatException, IOException {

	String vertexAdjList;
	int lineCount = 0, logCount = 0;
	long startTime = System.currentTimeMillis();
	long intervalStartTime = startTime;

	// Read the adjacency list file from disk
	while ((vertexAdjList = br.readLine()) != null) {
	    String[] splits = vertexAdjList.split(":");
	    long vId = Long.parseLong(splits[0]);
	    String[] adjStr = splits[1].split(", ");
	    // int degree = adjStr.length;
	    // if (degree % 2 == 0) continue; // skip odd degree

	    // create neighbors and add to adjacency list
	    List<Long> neighbours = Stream.of(adjStr).map(Long::parseLong).collect(Collectors.toList());

	    writeAdjacencyList(vId, neighbours, os);

	    // logging
	    lineCount++;
	    logCount++;
	    if (logCount == 1000000) {
		long intervalEndTime = System.currentTimeMillis();
		System.out.printf("transformGraphCsvToBin,VertexXfredMillions,%d,TimeTakenMS,%d%n",
			(lineCount / 1000000), (intervalEndTime - intervalStartTime));
		intervalStartTime = intervalEndTime;
		logCount = 0;
	    }
	}
	System.out.printf("transformGraphCsvToBin,TotalVertexXfred,%d,TimeTakenMS=%d%n", lineCount,
		(System.currentTimeMillis() - startTime));
    }


    public static void main(String args[]) throws IOException {
	if (args[0].equals("ConvertCvsToBin")) {
	    try (BufferedReader br = Files.newBufferedReader(Paths.get(args[1]));
		    OutputStream os = Files.newOutputStream(Paths.get(args[2]), StandardOpenOption.CREATE)) {
		GraphUtils.transformGraphCsvToBin(br, os);
	    }
	} else if (args[0].equals("FilterInEvenDegree")) {
	    try (BufferedInputStream is = new BufferedInputStream(
		    Files.newInputStream(Paths.get(args[1]), StandardOpenOption.READ));
		    OutputStream os = new BufferedOutputStream(
			    Files.newOutputStream(Paths.get(args[2]), StandardOpenOption.CREATE))) {

		// read the vertices from binary file
		System.out.printf("%d,loadBinGraphAsArray Start,skipOdd%n", System.currentTimeMillis());
		ElasticList<long[]> adjArrList = GraphUtils.loadBinGraphAsArray(is, true); // skip odd
		System.out.printf("%d,loadBinGraphAsArray End,added vertices,%d%n", System.currentTimeMillis(),
			adjArrList.size());

		System.out.printf("%d,writeAdjacencyArray Start%n", System.currentTimeMillis());
		GraphUtils.writeAdjacencyArray(adjArrList, os);
		System.out.printf("%d,writeAdjacencyArray End%n", System.currentTimeMillis());

	    }
	} else if (args[0].equals("FlattenPartitionedAdjList")) {
	    // Usage: GraphUtils FlattenPartitionedAdjList <part bin file> <write canonical>
	    // Writes to <part bin file>.unparted.bin
	    //
	    boolean writeCanonical = args.length >= 2 ? Boolean.parseBoolean(args[2]) : false;
	    try (BufferedInputStream is = new BufferedInputStream(
		    Files.newInputStream(Paths.get(args[1]), StandardOpenOption.READ));
		    OutputStream os = new BufferedOutputStream(
			    Files.newOutputStream(Paths.get(args[1] + ".unparted.bin"), StandardOpenOption.CREATE))) {

		// load the partitioned binary file
		System.out.printf("%d,readPartitionedAdjacencyMap Start%n", System.currentTimeMillis());
		TLongObjectMap<PartitionedAdjacencyMapValue> partAdjMap = readPartitionedAdjacencyMap(is);
		System.out.printf("%d,readPartitionedAdjacencyMap End,loaded vertices,%d%n", System.currentTimeMillis(),
			partAdjMap.size());

		// flatten the partitioned binary file
		System.out.printf("%d,flattenPartitionedAdjList Start%n", System.currentTimeMillis());
		TLongObjectHashMap<TLongArrayList> adjMap = flattenPartitionedAdjList(partAdjMap, true);
		System.out.printf("%d,flattenPartitionedAdjList End%n", System.currentTimeMillis());

		// write to binary output, optionally in canonical order
		System.out.printf("%d,writeAdjacencyList Start,Canonical,%b%n", System.currentTimeMillis(),
			writeCanonical);
		if (writeCanonical) {
		    writeAdjacencyListCanonical(adjMap, os);
		} else {
		    writeAdjacencyList(adjMap, os);
		}
		System.out.printf("%d,writeAdjacencyList End%n", System.currentTimeMillis());

	    }
	} else if (args[0].equals("WriteAdjacencyListCanonical")) {
	    // Usage: GraphUtils WriteAdjacencyListCanonical <bin file>
	    // Writes to <bin file>.canonical.bin
	    //
	    try (BufferedInputStream is = new BufferedInputStream(
		    Files.newInputStream(Paths.get(args[1]), StandardOpenOption.READ));
		    OutputStream os = new BufferedOutputStream(
			    Files.newOutputStream(Paths.get(args[1] + ".canonical.bin"), StandardOpenOption.CREATE))) {

		// load the partitioned binary file
		System.out.printf("%d,readAllIntoAdjacencyTMap Start%n", System.currentTimeMillis());
		TLongObjectHashMap<TLongArrayList> adjMap = readAllIntoAdjacencyTMap(is, -1);
		System.out.printf("%d,readAllIntoAdjacencyTMap End,loaded vertices,%d%n", System.currentTimeMillis(),
			adjMap.size());

		System.out.printf("%d,writeAdjacencyListCanonical Start%n", System.currentTimeMillis());
		writeAdjacencyListCanonical(adjMap, os);
		System.out.printf("%d,writeAdjacencyListCanonical End%n", System.currentTimeMillis());

	    }
	} else if (args[0].equals("BuildMetaGraph")) {
	    try (BufferedInputStream is = new BufferedInputStream(
		    Files.newInputStream(Paths.get(args[1]), StandardOpenOption.READ))) {

		// load the partitioned binary file
		System.out.printf("%d,readPartitionedAdjacencyMap Start%n", System.currentTimeMillis());
		TLongObjectMap<PartitionedAdjacencyMapValue> partAdjMap = readPartitionedAdjacencyMap(is);
		System.out.printf("%d,readPartitionedAdjacencyMap End,loaded vertices,%d%n", System.currentTimeMillis(),
			partAdjMap.size());

		System.out.printf("%d,buildMetaGraph Start%n", System.currentTimeMillis());
		Map<Byte, Map<Byte, Long>> metaGraph = buildMetaGraph(partAdjMap, true, true, true);
		System.out.printf("%d,buildMetaGraph End%n", System.currentTimeMillis());

		for (Entry<Byte, Map<Byte, Long>> metaAdjEntry : metaGraph.entrySet()) {
		    byte srcPartID = metaAdjEntry.getKey();
		    System.out.printf("%d [", srcPartID);
		    Map<Byte, Long> metaAdjList = metaAdjEntry.getValue();
		    for (Entry<Byte, Long> edgeEntry : metaAdjList.entrySet()) {
			System.out.printf(" [%d,%d]", edgeEntry.getKey(), edgeEntry.getValue());
		    }
		    System.out.printf(" ]%n");
		}
	    }
	} else if (args[0].equals("WriteAdjacencyListCsv")) {
	    // Usage: GraphUtils WriteAdjacencyListCsv SRC_SEPARATOR SINK_SEPARATOR <csv file>
	    // Writes to character separated value adjacency file using provided SEPARATOR char
	    //
	    char srcSeparator = args[2].charAt(0);
	    char sinkSeparator = args[3].charAt(0);
	    try (BufferedInputStream is = new BufferedInputStream(
		    Files.newInputStream(Paths.get(args[1]), StandardOpenOption.READ));
		    BufferedWriter bw = Files.newBufferedWriter(Paths.get(args[4]), StandardOpenOption.CREATE)) {

		// load the partitioned binary file
		System.out.printf("%d,readAllIntoAdjacencyTMap Start%n", System.currentTimeMillis());
		TLongObjectHashMap<TLongArrayList> adjMap = readAllIntoAdjacencyTMap(is, -1);
		System.out.printf("%d,readAllIntoAdjacencyTMap End,loaded vertices,%d%n", System.currentTimeMillis(),
			adjMap.size());

		System.out.printf("%d,writeAdjacencyListCsv Start, SEPARATOR=[%c][%c], Writing to,%s%n",
			System.currentTimeMillis(), srcSeparator, sinkSeparator, (args[4]));
		writeAdjacencyListCsv(adjMap, srcSeparator, sinkSeparator, bw);
		System.out.printf("%d,writeAdjacencyListCsv End%n", System.currentTimeMillis());

	    }
	} else if (args[0].equals("WriteAdjacencyListBlogel")) {
	    // Usage: GraphUtils WriteAdjacencyListCsv SRC_SEPARATOR SINK_SEPARATOR <csv file>
	    // Writes to character separated value adjacency file using provided SEPARATOR char
	    //
	    char srcSeparator = '\t';
	    char sinkSeparator = ' ';
	    try (BufferedInputStream is = new BufferedInputStream(
		    Files.newInputStream(Paths.get(args[1]), StandardOpenOption.READ));
		    BufferedWriter bw = Files.newBufferedWriter(Paths.get(args[2]), StandardOpenOption.CREATE)) {

		// load the partitioned binary file
		System.out.printf("%d,readAllIntoAdjacencyTMap Start%n", System.currentTimeMillis());
		TLongObjectHashMap<TLongArrayList> adjMap = readAllIntoAdjacencyTMap(is, -1);
		System.out.printf("%d,readAllIntoAdjacencyTMap End,loaded vertices,%d%n", System.currentTimeMillis(),
			adjMap.size());

		System.out.printf("%d,writeAdjacencyListCsv Start, SEPARATOR=[%c][%c], Writing to,%s%n",
			System.currentTimeMillis(), srcSeparator, sinkSeparator, (args[2]));
		writeAdjacencyListBlogel(adjMap, srcSeparator, sinkSeparator, bw);
		System.out.printf("%d,writeAdjacencyListCsv End%n", System.currentTimeMillis());

	    }
	} else if (args[0].equals("Degrees")) {
	    // Usage: GraphUtils Degrees <binary file> <output csv file>
	    // writes <degree,freq> in sorted order of degrees to out file
	    //
	    try (BufferedInputStream is = new BufferedInputStream(
		    Files.newInputStream(Paths.get(args[1]), StandardOpenOption.READ));
		    BufferedWriter bw = Files.newBufferedWriter(Paths.get(args[2]), StandardOpenOption.CREATE)) {

		// load the partitioned binary file
		System.out.printf("%d,readAllIntoAdjacencyTMap Start%n", System.currentTimeMillis());
		TLongObjectHashMap<TLongArrayList> adjMap = readAllIntoAdjacencyTMap(is, -1);
		System.out.printf("%d,readAllIntoAdjacencyTMap End,loaded vertices,%d%n", System.currentTimeMillis(),
			adjMap.size());

		System.out.printf("%d,printDegreeDistribution Start, Writing to,%s%n", System.currentTimeMillis(),
			(args[2]));
		Map<Integer, Long> map = printDegreeDistribution(adjMap);
		List<Integer> keys = new ArrayList<Integer>(map.keySet());
		Collections.sort(keys);

		for (Integer key : keys) {
		    bw.write(key.toString());
		    bw.write(",");
		    bw.write(map.get(key).toString());
		    bw.write("\n");
		}
		System.out.printf("%d,printDegreeDistribution End%n", System.currentTimeMillis());

	    }
	} else if (args[0].equals("PartBinToText")) {
	    try (BufferedInputStream is = new BufferedInputStream(
		    Files.newInputStream(Paths.get(args[1]), StandardOpenOption.READ));
		    BufferedWriter bw = Files.newBufferedWriter(Paths.get(args[2]), StandardOpenOption.CREATE)) {

		// load the partitioned binary file
		System.out.printf("%d,readPartitionedAdjacencyMap Start%n", System.currentTimeMillis());
		TLongObjectMap<PartitionedAdjacencyMapValue> partAdjMap = readPartitionedAdjacencyMap(is);
		System.out.printf("%d,readPartitionedAdjacencyMap End,loaded vertices,%d%n", System.currentTimeMillis(),
			partAdjMap.size());

		System.out.printf("%d,writePartitionedAdjacencyListText Start%n", System.currentTimeMillis());
		TLongObjectIterator<PartitionedAdjacencyMapValue> partAdjIter = partAdjMap.iterator();
		while (partAdjIter.hasNext()) {
		    partAdjIter.advance();
		    long vid = partAdjIter.key();
		    PartitionedAdjacencyMapValue adj = partAdjIter.value();
		    writePartitionedAdjacencyListText(adj.partitionID, vid, adj.localAdjacencyList,
			    adj.remoteAdjacencyMap, bw);
		}
		System.out.printf("%d,writePartitionedAdjacencyListText End%n", System.currentTimeMillis());

	    }
	} else System.out.println("invalid input: " + args[0]);
    }


}
