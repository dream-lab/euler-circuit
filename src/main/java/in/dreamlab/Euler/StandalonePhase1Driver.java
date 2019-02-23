package in.dreamlab.Euler;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import in.dream_lab.graph.euler.GraphUtils;
import in.dream_lab.graph.euler.GraphUtils.PartitionedAdjacencyMapEntry;
import in.dreamlab.Euler.SubgraphEulerTourPhase1.Phase1Tour;


public class StandalonePhase1Driver {

    public static void main(String[] args) throws IOException {

	final boolean IS_FILE_BINARY = true;
	long startTime, endTime;
	List<TuplePair<Byte, AdjListObject>> adjObjList = new ArrayList<>();


	////////////////////////////////////////////////////////
	// Transform input binary file to Adj Obj
	/*-
	// INPUT FORMAT:
	// 1b   8b	4b		l*8b		4b		r*8b
	// pid	srcVID	No. Local	Sink VIDs	No. Remote	PartID+Remote VIDs
	//		Sink VID (l)			Sink VID (r)
	*/
	//////////////////////////////////////////////////////////////////////////////////
	if (IS_FILE_BINARY) {
	    startTime = System.currentTimeMillis();
	    try (BufferedInputStream is = new BufferedInputStream(Files.newInputStream(Paths.get(args[0])))) {
		// create a list of Adj Objects
		PartitionedAdjacencyMapEntry entry = new PartitionedAdjacencyMapEntry();
		while (GraphUtils.readPartitionedAdjacencyMap(is, entry)) {
		    AdjListObject adjList = new AdjListObject(entry);
		    // return an Pair RDD where key is part ID and value if AdjList object for a vertex
		    adjObjList.add(new TuplePair<Byte, AdjListObject>(entry.partitionID, adjList));
		    entry.reset();
		}
	    } catch (Exception ex) {
		ex.printStackTrace();
	    }
	    endTime = System.currentTimeMillis();
	    System.out.printf("BinaryFile to AdjObj,TimeMS=%d%n", (endTime - startTime));
	} else {
	    ////////////////////////////////////////////////////////
	    // Transform input string file to Adj Obj
	    ////////////////////////////////////////////////////////
	    startTime = System.currentTimeMillis();
	    try (BufferedReader br = Files.newBufferedReader(Paths.get(args[0]))) {
		// create a list of Adj Objects, one per line
		String s;
		while ((s = br.readLine()) != null) {
		    try {
			String[] records = s.split("@");
			byte partId = Integer.valueOf(records[0]).byteValue();
			String adjListStr = records[1];
			AdjListObject adjList = new AdjListObject(adjListStr);
			// return an Pair RDD where key is part ID and value if AdjList object for a vertex
			adjObjList.add(new TuplePair<Byte, AdjListObject>(partId, adjList));
		    } catch (Exception ex) {
			ex.printStackTrace();
		    }
		}
	    }
	    endTime = System.currentTimeMillis();
	    System.out.printf("Text File to AdjObj,TimeMS=%d%n", (endTime - startTime));
	}

	////////////////////////////////////////////////////////
	// Transform Adj Obj List to Source Partition
	////////////////////////////////////////////////////////
	startTime = System.currentTimeMillis();
	Iterator<TuplePair<Byte, AdjListObject>> partAdjListIter = adjObjList.iterator();
	SourcePartitionPhase1 partPhase1Obj = new SourcePartitionPhase1();

	// since all vertices (values) in this spark partition belong to the same graph partition,
	// populate the details for the graph partition object just once from the first vertex
	byte initPartId = -1;
	if (partAdjListIter.hasNext()) {
	    TuplePair<Byte, AdjListObject> partAdjList = partAdjListIter.next();
	    initPartId = partAdjList._1();
	    partPhase1Obj.setPartitionId(initPartId);
	    AdjListObject adjList = partAdjList._2();
	    partPhase1Obj.addAdjacencyList(adjList.getVid(), adjList.getLocalAdjList(), adjList.getRemoteAdjMap());
	    // pX.setMetaEdgeMap();
	}
	// add the rest of the adj list items in the spark partition to the graph partition
	while (partAdjListIter.hasNext()) {
	    TuplePair<Byte, AdjListObject> partAdjList = partAdjListIter.next();
	    // mapParition must guarantee that all items are from same partition
	    assert initPartId == partAdjList._1() : "Found a different graph partition in this spark partition!";
	    AdjListObject adjList = partAdjList._2();
	    partPhase1Obj.addAdjacencyList(adjList.getVid(), adjList.getLocalAdjList(), adjList.getRemoteAdjMap());
	}

	// indicate to part object that all vertex adj lists have been added
	partPhase1Obj.finalizeAdjacencyList();

	// build adjacency list with edge weights for this partition which is a metavertex in the
	// metagraph
	Map<Byte, Long> metaEdgeMap = SourcePartitionPhase1.buildMetaEdgeMap(partPhase1Obj.getVertexMap());

	endTime = System.currentTimeMillis();
	System.out.printf("AdjObj to Ph1Obj,TimeMS=%d%n", (endTime - startTime));

	////////////////////////////////////////////////////////
	// Transform Source Partition to Phase1 Output
	////////////////////////////////////////////////////////
	startTime = System.currentTimeMillis();
	byte partitionId = partPhase1Obj.getPartitionId();
	SourcePartitionPhase1 partition = partPhase1Obj; // Object of the partition
	TuplePair<Byte, Phase1FinalObject> output = Phase1Tour.doPhase1Stuff(partitionId, partition);
	endTime = System.currentTimeMillis();
	System.out.printf("Ph1Obj Tour,TimeMS=%d%n", (endTime - startTime));

    }

}

