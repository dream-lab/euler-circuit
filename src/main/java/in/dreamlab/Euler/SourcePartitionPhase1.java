package in.dreamlab.Euler;

import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gnu.trove.iterator.TByteObjectIterator;
import gnu.trove.iterator.TLongIterator;
import gnu.trove.iterator.TLongObjectIterator;
import gnu.trove.list.TByteList;
import gnu.trove.list.array.TByteArrayList;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.map.TByteObjectMap;
import gnu.trove.map.TLongObjectMap;
import gnu.trove.map.hash.TByteObjectHashMap;
import gnu.trove.map.hash.TLongObjectHashMap;
import in.dream_lab.graph.euler.GraphUtils;


@SuppressWarnings("serial")
public class SourcePartitionPhase1 implements Serializable {

    static Logger LOGGER = LoggerFactory.getLogger(SourcePartitionPhase1.class);

    private byte partitionId; // Store the partition id as byte
    private TLongObjectHashMap<VertexValuesPhase1> vertexTable;  // Hashmap with Key = vertexID and Value= <local
							         // adjlist, remote
    // adjlist, type>
    private TLongArrayList obList; // List of OB Vertices in this partition // YS: TODO: switch to elastic list?
    private TLongArrayList ebList; // List of EB Vertices in this partition
    private TLongArrayList intList; // List of Internal vertices in this partition
    // private Map<Byte, Long> metaEdgeWeightMap; // meta edge weight per partition from this

    private boolean finalized = false; // have we added all adj list items?
    private static final int VERTEX_CAPACITY = 1 * 1000 * 1000;


    // Constructor initializes all the data members of the class
    public SourcePartitionPhase1() {
	this(15, 15, 15); // create init array with 15 vertices of each class
    }


    public SourcePartitionPhase1(int o, int e, int i) {
	vertexTable = new TLongObjectHashMap<VertexValuesPhase1>(VERTEX_CAPACITY);
	intList = new TLongArrayList(i);
	// YS TODO: we don't know capacity a priori. ArrayList or LinkedList?
	obList = new TLongArrayList(o);
	ebList = new TLongArrayList(e);
    }


    // Method to add the adjacency list per vertex to the hashmap
    // public void addAdjacencyList(AdjListObject adjList) {
    public void addAdjacencyList(long vertexId, TLongArrayList localAdjList,
	    TByteObjectHashMap<TLongArrayList> remoteAdjMap) {

	if (finalized) throw new RuntimeException("addAdjacencyList after the partition has been finalized");

	// change to iterator to avoid array copy
	int remoteListSize = 0;
	TByteObjectIterator<TLongArrayList> remoteAdjMapIter = remoteAdjMap.iterator();
	while (remoteAdjMapIter.hasNext()) {
	    remoteAdjMapIter.advance();
	    remoteListSize += remoteAdjMapIter.value().size();
	}

	// FIXME: We should populate on the receiving of the RDD since data is duplicated and shipped over the network!!
	// Add a "delayedAdd" method
	byte typ;  // type of the vertex; OB = 1, EB = -1, Internal = 0
	if (remoteListSize == 0) {
	    assert localAdjList.size() % 2 == 0 : "internal vertex did NOT have an EVEN number of local vertices "
		    + vertexId;
	    typ = VertexValuesPhase1.TYPE_INTERNAL; // internal vertex
	    intList.add(vertexId);
	} else if (remoteListSize % 2 == 1) {
	    assert localAdjList.size() % 2 == 1 : "OB vertex did NOT have an ODD  number of local vertices " + vertexId;
	    typ = VertexValuesPhase1.TYPE_OB; // OB
	    obList.add(vertexId);
	} else {
	    // TODO: In phase 2, we may have EB's with no local vertices. Can we skip adding them to EB list?
	    // OPTIMIZE!!
	    assert localAdjList == null
		    || localAdjList.size() % 2 == 0 : "EB vertex did NOT have an EVEN number of local vertices "
			    + vertexId;
	    typ = VertexValuesPhase1.TYPE_EB; // EB
	    ebList.add(vertexId);
	}


	VertexValuesPhase1 vertexTuple = new VertexValuesPhase1(localAdjList, remoteAdjMap, typ);
	this.vertexTable.put(vertexId, vertexTuple);
    }


    public void verifyLocalNeighbors() {
	System.out.println("SourcePartitionPhase1.verifyLocalNeighbors. Sanity check verification starts for part " + partitionId);
	TLongObjectIterator<VertexValuesPhase1> vmIter = vertexTable.iterator();
	boolean foundMismatch = false;
	while (vmIter.hasNext()) {
	    vmIter.advance();
	    long entryKey = vmIter.key();
	    VertexValuesPhase1 entryVal = vmIter.value();
	    // check if every local neighbor has a corresponding entry in vertex map
	    TLongArrayList locals = entryVal.getUnvisitedLocalNeighbourList();
	    TLongIterator localsIter = locals.iterator();

	    // tempdel
	    // if (60682545L == entryKey)
	    // System.out.printf("Source VID=%d, Local AdjList=%s%n", entryKey, locals.toString());
	    // if (7055424L == entryKey)
	    // System.out.printf("Source VID=%d, Local AdjList=%s%n", entryKey, locals.toString());
	    // if (16060820L == entryKey)
	    // System.out.printf("Source VID=%d, Local AdjList=%s%n", entryKey, locals.toString());

	    while (localsIter.hasNext()) {
		long sink = localsIter.next();
		if (!vertexTable.containsKey(sink)) {
		    // ASSERT ERROR
		    System.out.printf("SourcePartitionPhase1.verifyLocalNeighbors Error: Source vertex %d has a sink vertex %d that does not have a key entry in the vertex map in part %d%n",
			    entryKey, sink, partitionId);
		    foundMismatch = true;
		}
	    }
	}

	if (!foundMismatch) System.out.println("SourcePartitionPhase1.verifyLocalNeighbors. Sanity check verification of every sink vertex having a key entry in the vertex map was successful in part "
			+ partitionId);
    }


    /**
     * Called after all adjacency list items have been added.
     * 
     */
    public void finalizeAdjacencyList() {
	// TODO
	// tempdel: Do sanity check that vertex map contains an entry for every local adj vertex
	if (SubgraphEulerTourPhase1.DEEP_ASSERT) verifyLocalNeighbors();

	// trim the array lists as all loading of adj objs are done
	obList.trimToSize(); // TODO: why are we not finalizing the intList?
	ebList.trimToSize();
	intList.trimToSize();
	finalized = true;

	// TODO: ASSERT: If this is the LAST level of phase 2 when all have been merged into 1 partition, the number of
	// OB and
	// EB must be 0
	LOGGER.info("SourcePartitionPhase1.finalize,obCount," + obList.size() + ",ebCount," + ebList.size()
		+ ",intCount," + intList.size());
    }


    public TLongObjectHashMap<VertexValuesPhase1> getVertexMap() {
	return this.vertexTable;
    }


    // Return the list of OBs for this partition
    public TLongArrayList getOBList() {
	return this.obList;
    }


    // Return the list of EBs for this partition
    public TLongArrayList getEBList() {
	return this.ebList;
    }


    // Return list of internal vertices
    public TLongArrayList getIntList() {
	return intList;
    }


    // Method to set the partition Id for the partition
    public void setPartitionId(byte pID) {
	this.partitionId = pID;
    }


    // Method to return the partition Id for this partition
    public Byte getPartitionId() {
	return this.partitionId;
    }


    ////////////////////////////////////////////////////////
    // STATIC HELPER UTILITIES
    ////////////////////////////////////////////////////////
    // FIXME: Memory hungry in this method
    public static SourcePartitionPhase1 createFromPhase1FinalObjects(Phase1FinalObject part1Obj,
	    Phase1FinalObject part2Obj, List<TuplePair<Byte, Byte>> partPairs) {


	long startTime = System.currentTimeMillis();
	byte part1Id = part1Obj.getPartitionId();
	// if part2 is null, we have just a single partition in this stage
	Byte part2Id = part2Obj != null ? part2Obj.getPartitionId() : null;

	System.out.printf("createFromPhase1FinalObjects.call Start,PartID,%d,Part2ID,%s,TimeStampMS,%d%n", part1Id,
		(part2Id == null ? "NULL" : Byte.toString(part2Id)), startTime);

	assert (part2Id == null || part1Id > part2Id);

	SourcePartitionPhase1 nextPartObj = null;

	if (part2Obj != null) {
	    // get an estimate of the capacity
	    int vcount = part1Obj.getOBEBCount();
	    vcount += (part2Id != null ? part2Obj.getOBEBCount() : 0);

	    LOGGER.info("Merging 2 Phase1FinalObjects from partitions " + part1Id + " and " + part2Id
		    + " with Total SrcSink VCOUNT=" + vcount + " into new partition ID " + part1Id);

	    nextPartObj = new SourcePartitionPhase1(vcount / 3, vcount / 3, vcount / 3);
	    nextPartObj.setPartitionId(part1Id);
	}

	// Update the part IDs of the remote vertices with their merged part ID
	// return the updated part1Obj without any oher change back to user.
	// Set SourcePartitionPhase1 return as NULL

	// All remote vertices of part1 that are in part2 (and vice versa) are the new local vertices
	// merge in part1obj into next obj
	createLocalAdjList(part1Obj, part2Id, nextPartObj, partPairs, // tempdel
		part2Obj);

	// So same for part 2, if NOT NULL
	if (part2Id != null) {
	    createLocalAdjList(part2Obj, part1Id, nextPartObj, partPairs, // tempdel
		    part1Obj);

	    // trim list sizes
	    nextPartObj.finalizeAdjacencyList();

	    long endTime = System.currentTimeMillis();
	    System.out.printf("createFromPhase1FinalObjects.call Done SrcPart,PartID,%d,TotalTimeMS,%d,TimestampMS,%d%n", part1Id,
		    (endTime - startTime), endTime);

	    return nextPartObj;
	} else {
	    long endTime = System.currentTimeMillis();
	    System.out.printf("createFromPhase1FinalObjects.call Done FinalObj,PartID,%d,TotalTimeMS,%d,TimestampMS,%d%n",
		    part1Id, (endTime - startTime), endTime);
	    return null;
	}
    }


    static void createLocalAdjList(Phase1FinalObject partObj, Byte mergedPartId, SourcePartitionPhase1 nextPartObj,
	    List<TuplePair<Byte, Byte>> partPairs, // tempdel
	    Phase1FinalObject tmpMergedPartObj) {
	// Populate local and remote neighbors for source and sink vertices of the given partObj
	// into nextPartObj, when merging with otherPartId
	TLongObjectHashMap<PathSinkSource> partSrcSinkMap = partObj.getPathMap();
	TLongObjectIterator<PathSinkSource> part1SrcSinkMapIter = partSrcSinkMap.iterator();
	byte myPartId = partObj.getPartitionId();

	LOGGER.info("createLocalAdjList,PartID," + myPartId + ",OtherPartID," + mergedPartId + ",partSrcSinkMapCount,"
		+ partSrcSinkMap.size() + ",pivotVertexMapCount," + partObj.getPivotVertexMap().size());

	// iterate thru vertices in part 1 to add to local set
	long mergeSourcePartsDuration = 0, mergeSourcePartsStart;
	while (part1SrcSinkMapIter.hasNext()) {
	    part1SrcSinkMapIter.advance();
	    PathSinkSource srcSinkObj = part1SrcSinkMapIter.value();

	    byte assertResult;
	    assert (assertResult = srcSinkObj.verifySelfPartNotRemote(
		    myPartId)) == 0 : "1) the remote neighbors contain *this* partition as key for my part id "
			    + myPartId + ". Check result: " + assertResult;

	    // Reassign remote part IDs for OTHER remote parts that are being merged.
	    // for each part IDs partX and partY that are NOT part1 or part2, AND where we are merging partX into partY,
	    // call par1Obj.mergeParts(partX,partY) and par2Obj.mergeParts(partX,partY). This will ensure that the
	    // remote partition IDs point to the new part IDs for the other parts being merged as well.

	    mergeSourcePartsStart = System.currentTimeMillis();
	    for (TuplePair<Byte, Byte> pair : partPairs) {
		assert pair._1() != null && pair._2() != null : "either of the pairs were null";
		if (pair._1() != myPartId && pair._2() != myPartId) {
		    // merge the other remote parts
		    srcSinkObj.mergeSourceParts(pair._2(), pair._1()); // from part, to part
		    if (srcSinkObj.hasSink()) srcSinkObj.mergeSinkParts(pair._2(), pair._1());
		}
	    }
	    mergeSourcePartsDuration += (System.currentTimeMillis() - mergeSourcePartsStart);

	    assert (assertResult = srcSinkObj.verifySelfPartNotRemote(
		    myPartId)) == 0 : "2) the remote neighbors contain *this* partition as key for my part id "
			    + myPartId + ". Check result: " + assertResult;

	    // nothing to transfer since this is a single final obeject without any merge required or phase 1 required
	    if (nextPartObj == null) continue;


	    /////////////////////////////////////////////////////////
	    // This part is relevance only if we are merging and transfering to sourcepart obj for doing Phase 1 tour
	    // convert local edges of source vertex
	    {
		// get adj list details for the source vertex
		long srcVID = srcSinkObj.getSource();
		TLongArrayList srcLocalNeighbors = null;
		// if part2 is null, we have just a single partition in this stage
		// So there will be no new local vertices.
		if (mergedPartId != null) {
		    srcLocalNeighbors = srcSinkObj.getAndRemoveSourceNeighbor(mergedPartId);
		    // TODO: verify if the merged partition has ALL of these vertices in them as a source/sink
		    // tempdel
		    if (SubgraphEulerTourPhase1.DEEP_ASSERT) if (srcLocalNeighbors != null) {
			for (int i = 0; i < srcLocalNeighbors.size(); i++) {
			    if (!tmpMergedPartObj.containsAsSourceOrSink(srcLocalNeighbors.get(i))) {
				System.out.println("createLocalAdjList Error: Could not find a source/sink pair in merged part "+ mergedPartId + " for a local vertex " + srcLocalNeighbors.get(i)
						+ " (that neighbors an source vertex " + srcVID
						+ "), in the other part " + myPartId);
			    }
			}
		    }
		}

		// If srcLocalNeighbors is null and its an EB, it has no local edges.
		// If srcLocalNeighbors is null and its and OB, it has only 1 local edge inherited from its parth to the
		// other EB vertex.

		// ADD A LOCAL EDGE BETWEEN SRC AND SINK IF srcSinkObj.hasSink()
		if (srcSinkObj.hasSink()) {
		    if (srcLocalNeighbors == null)
			srcLocalNeighbors = new TLongArrayList(new long[] { srcSinkObj.getSink() });
		    else srcLocalNeighbors.add(srcSinkObj.getSink());
		}

		TByteObjectHashMap<TLongArrayList> srcRemoteNeighbors = srcSinkObj.getRemainingSourceNeighbors();
		// add to source partition obj
		// this should also mark them as OB/EB
		nextPartObj.addAdjacencyList(srcVID, srcLocalNeighbors, srcRemoteNeighbors);
		// LOGGER.info("nextPartObj.addAdjacencyList,srcVID," + srcVID + ",srcLocalNeighborsCount,"
		// + (srcLocalNeighbors != null ? srcLocalNeighbors.size() : "NULL") + ",srcRemoteNeighborsCount,"
		// + (srcRemoteNeighbors != null ? srcRemoteNeighbors.size() : "NULL"));

		assert (assertResult = srcSinkObj.verifySelfPartNotRemote(
			myPartId)) == 0 : "3) the remote neighbors contain *this* partition as key for my part id "
				+ myPartId + ". Check result: " + assertResult;
	    }

	    // convert local edges of sink vertex, if present
	    if (srcSinkObj.hasSink()) { // True for an OB. False for an EB.
		// Same code as above for sink
		// get adj list details for the sink vertex
		long sinkVID = srcSinkObj.getSink();
		TLongArrayList sinkLocalNeighbors = null;

		// if part2 is null, we have just a single partition in this stage
		// So there will be no new local vertices.
		if (mergedPartId != null) {
		    sinkLocalNeighbors = srcSinkObj.getAndRemoveSinkNeighbor(mergedPartId);

		    // TODO: verify if the merged partition has ALL of these vertices in them as a source/sink
		    // tempdel
		    if (SubgraphEulerTourPhase1.DEEP_ASSERT) if (sinkLocalNeighbors != null) {
			for (int i = 0; i < sinkLocalNeighbors.size(); i++) {
			    if (!tmpMergedPartObj.containsAsSourceOrSink(sinkLocalNeighbors.get(i))) {
				System.out.println("createLocalAdjList Error: Could not find a source/sink pair in merged part "
						+ mergedPartId + " for a local vertex " + sinkLocalNeighbors.get(i)
						+ " (that neighbors an OB sink vertex " + sinkVID
						+ "), in the other part " + myPartId);
			    }
			}
		    }
		}

		// ADD A LOCAL EDGE BETWEEN SRC AND SINK
		if (sinkLocalNeighbors == null)
		    sinkLocalNeighbors = new TLongArrayList(new long[] { srcSinkObj.getSource() });
		else sinkLocalNeighbors.add(srcSinkObj.getSource());

		TByteObjectHashMap<TLongArrayList> sinkRemoteNeighbors = srcSinkObj.getRemainingSinkNeighbors();
		// add to source partition obj
		// this should also mark them as OB/EB
		nextPartObj.addAdjacencyList(sinkVID, sinkLocalNeighbors, sinkRemoteNeighbors);

		// LOGGER.info("nextPartObj.addAdjacencyList,sinkVID," + sinkVID + ",sinkLocalNeighborsCount,"
		// + (sinkLocalNeighbors != null ? sinkLocalNeighbors.size() : "NULL")
		// + ",sinkRemoteNeighborsCount,"
		// + (sinkRemoteNeighbors != null ? sinkRemoteNeighbors.size() : "NULL"));
		assert (assertResult = srcSinkObj.verifySelfPartNotRemote(
			myPartId)) == 0 : "4) the remote neighbors contain *this* partition as key for my part id "
				+ myPartId + ". Check result: " + assertResult;
	    }

	    assert (assertResult = srcSinkObj.verifySelfPartNotRemote(
		    mergedPartId)) == 0 : "5) the remote neighbors contain *other* partition as key; my part id "
			    + myPartId + "; merged part id " + mergedPartId + "; Check result: " + assertResult;

	    // garbage collect
	    // remove appears cheap since auto-compaction is disabled by the iterator
	    // TODO: Do we get memory benefits?
	    part1SrcSinkMapIter.remove();
	    /////////////////////////////////////////////////////////
	}
	System.out.printf("createLocalAdjList.call Done,PartID,%d,TotalMergeSourcePartsTimeMS,%d,TimestampMS,%d%n",
		myPartId, mergeSourcePartsDuration, System.currentTimeMillis());
    }


    /**
     * Utility to build metagraph
     * 
     * @param vertexTable
     * @return
     */
    public static Map<Byte, Long> buildMetaEdgeMap(TLongObjectHashMap<VertexValuesPhase1> vertexTable) {

	Map<Byte, Long> metaEdgeWeightMap = new HashMap<>(); // capacity = numParts
	TLongObjectIterator<VertexValuesPhase1> vmIter = vertexTable.iterator();
	while (vmIter.hasNext()) {
	    vmIter.advance();
	    long vertex = vmIter.key();
	    TByteObjectMap<TLongArrayList> remNeighbors = vertexTable.get(vertex).getRemoteNeighbourMap();
	    TByteObjectIterator<TLongArrayList> remoteIter = remNeighbors.iterator();
	    while (remoteIter.hasNext()) {
		remoteIter.advance();
		Byte partId = remoteIter.key();
		Long partWeight = metaEdgeWeightMap.get(partId);
		if (partWeight == null) {
		    metaEdgeWeightMap.put(partId, 0L);
		    partWeight = 0L;
		}
		partWeight += remoteIter.value().size();
		metaEdgeWeightMap.put(partId, partWeight);
	    }
	}

	return metaEdgeWeightMap;
    }


    public void writePartitionedAdjacencyList(OutputStream os) throws IOException {
	TLongObjectIterator<VertexValuesPhase1> vIter = vertexTable.iterator();
	Byte partID = getPartitionId();
	while (vIter.hasNext()) {
	    vIter.advance();
	    long vertexId = vIter.key();
	    VertexValuesPhase1 vobj = vIter.value();
	    TLongArrayList localNeighbourList = vobj.getUnvisitedLocalNeighbourList();
	    TByteObjectHashMap<TLongArrayList> remoteNeighbourMap = vobj.getRemoteNeighbourMap();
	    TByteObjectIterator<TLongArrayList> remoteIter = remoteNeighbourMap.iterator();
	    TLongArrayList remoteAdjacencyList = new TLongArrayList();
	    TByteList rpid = new TByteArrayList();
	    while (remoteIter.hasNext()) {
		remoteIter.advance();
		byte rpart = remoteIter.key();
		TLongArrayList rlist = remoteIter.value();
		for (int i = 0; i < rlist.size(); i++) {
		    rpid.add(rpart);
		    remoteAdjacencyList.add(rlist.get(i));
		}
	    }
	    GraphUtils.writePartitionedAdjacencyList(partID, vertexId, localNeighbourList, rpid, remoteAdjacencyList,
		    os);
	}
    }

}


