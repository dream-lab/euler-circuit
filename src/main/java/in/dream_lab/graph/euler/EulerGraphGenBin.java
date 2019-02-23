package in.dream_lab.graph.euler;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Map.Entry;


/**
 * Utility to convert a non-eulerian graph to an eulerian graph
 * 
 * @author simmhan
 *
 */
public class EulerGraphGenBin {

    static final boolean GREEDY_MATCH = true;


    /**
     * The method takes an adjacency list with vertices having an odd degree. It greedily adds a new edge between pairs
     * of vertices to make their degree even, while ensuring we do not add an edge between vertices that already have an
     * edge existing between them.
     * 
     * @param localDegree
     *            This is the degree of the input vertices
     * @param vertexCount
     *            This is the number of vertices present in the bucket
     * @param adjList
     *            Input array has vertexCount*(1+degree) items. First item is the source vertex
     *            ID, while the rest of the degree items are its adjacent sink vertex IDs.
     *            NOTE: The vertex IDs should be non-negative long values.
     * @param stragglerMap
     * 
     * @return Output array contains vertexCount number of items, where for each vertex, it
     *         has the new adjacent vertex that has been added for it.
     *         It returns a -1 as item value if it cannot find a match for the vertex.
     */
    public static long[] matchBucket(int localDegree, Map<Integer, BucketEntry> bucketMap,
	    List<StragglerEntry> stragglerMap) {

	// local bucket constants
	final BucketEntry localBucket = bucketMap.get(localDegree);
	final int localVertexCount = localBucket.vertexCount;
	final long[] localAdjList = localBucket.inlineAdjList;
	final int localIncrement = localDegree + 1;
	final int localAdjListlength = localAdjList.length;


	// sanity check
	assert localDegree % 2 == 1 : "degree has to be odd";
	assert localVertexCount > 0 : "vertex count has to be non-empty";
	assert localAdjList.length == (localVertexCount * (localDegree
		+ 1)) : "adjacency list does not have correct number of items for given degree and count";


	// generic variables: set to either straggler or local bucket's values
	long source = Long.MIN_VALUE;
	long[] outputEdges = null;
	int outputIndex = Integer.MIN_VALUE;
	int nextGreedySinkIndex = 0, nextGreedySinkOutputIndex = 0;

	// local bucket variables
	long[] localOutputEdges = new long[localVertexCount]; // local bucket's output edges
	Arrays.fill(localOutputEdges, -1); // init all fields to -1 to indicate that its match has not been found
	int localSrcIndex = 0; // index in adjList for source vertex for which a new edge has to be added
	int localOutputIndex = 0; // index in output array for source vertex to which a newly added edge has to be set
	// int tempdelLocalNoMatchCount = 0;

	// straggler variables
	ListIterator<StragglerEntry> stragglerEntryIter = null; // iterate over all straggler entries
	StragglerEntry stragglerEntry = null; // currently active straggler entry
	BucketEntry stragglerBucket = null; // bucket for currently active straggler entry
	ListIterator<Integer> stragglerSrcIter = null; // iterator over source vertex indices for currently active
						       // straggler entry
	// int stragglerMatchCount = 0;
	boolean doneStragglers = true; // are we still processing stragglers?


	////////////////////////////////////////////
	// Initialize stragglers, if present
	if (stragglerMap.size() > 0) {
	    stragglerEntryIter = stragglerMap.listIterator();
	    stragglerEntry = stragglerEntryIter.next();
	    stragglerSrcIter = stragglerEntry.vertexIndexList.listIterator();
	    assert stragglerSrcIter.hasNext() : "found a straggler entry with no vertices";
	    stragglerBucket = bucketMap.get(stragglerEntry.degree);
	    doneStragglers = false;
	}

	do {
	    ////////////////////////////////////////////
	    // Find the next vertex that has not been processed yet
	    ////////////////////////////////////////////


	    if (!doneStragglers) {
		////////////////////////////////////////////
		// Find a source vertex in input stragglers

		if (!stragglerSrcIter.hasNext()) { // done with vertices in this entry. move to next straggler entry
		    if (stragglerEntryIter.hasNext()) { // more entries present?
			stragglerEntry = stragglerEntryIter.next();
			stragglerSrcIter = stragglerEntry.vertexIndexList.listIterator();
			assert stragglerSrcIter.hasNext() : "found a straggler entry with no vertices";
			stragglerBucket = bucketMap.get(stragglerEntry.degree);
		    } else {
			// no more entries present. all done for stragglers.
			doneStragglers = true;

			stragglerEntryIter = null;
			stragglerEntry = null;
			stragglerSrcIter = null;
			stragglerBucket = null;

			continue; // start search for local source vertices
		    }
		}

		// found straggler. set variables to point to straggler source vertex.
		int stagglerVertexIndex = stragglerSrcIter.next();

		source = stragglerBucket.inlineAdjList[stagglerVertexIndex * (stragglerEntry.degree + 1)];
		outputEdges = stragglerBucket.addedEdges;
		outputIndex = stagglerVertexIndex;


	    } else {
		////////////////////////////////////////////
		// Find a local source vertex in this bucket
		while (localOutputIndex < localVertexCount && localOutputEdges[localOutputIndex] != -1) {
		    localSrcIndex += localIncrement;
		    localOutputIndex++;
		}

		assert (localOutputIndex == localSrcIndex / localIncrement) && (localSrcIndex
			% localIncrement == 0) : "mismatch between local source adjList index and output index";

		if (localSrcIndex >= (localAdjListlength - localIncrement)) {
		    // all done, even if only 1 vertex is remaining since we cannot find a match for the last odd vertex
		    assert localOutputIndex >= (localVertexCount - 1);
		    break;
		}

		// found local. set variables to point to local source vertex.
		source = localAdjList[localSrcIndex];
		outputEdges = localOutputEdges;
		outputIndex = localOutputIndex;

		// relevant for greedy match. select next entry after source.
		nextGreedySinkIndex = localSrcIndex + localIncrement;
		nextGreedySinkOutputIndex = localOutputIndex + 1;
	    }


	    ////////////////////////////////////////////
	    // process vertex at location srcIndex to find a matching sink vertex to add an edge to

	    // locate next available sink index
	    int sinkIndex;
	    int sinkOutputIndex;
	    if (GREEDY_MATCH) {
		// Here, we assume that all vertices before the source vertex have been matched. This need NOT be the
		// case, as we see in the NOTE below. This is a greedy assumption for efficiency to avoid searching all
		// vertices in this adjacency list, and only search forward.
		sinkIndex = nextGreedySinkIndex;
		sinkOutputIndex = nextGreedySinkOutputIndex;

		// auto-increment here for straggler. will be overwritten for local.
		nextGreedySinkIndex = sinkIndex + localIncrement;
		nextGreedySinkOutputIndex = sinkOutputIndex + 1;

	    } else {
		// This will scan all sink vertices from start. Costly, but deterministic.
		sinkIndex = 0;
		sinkOutputIndex = 0;
	    }

	    do {
		while (sinkOutputIndex < localVertexCount && localOutputEdges[sinkOutputIndex] != -1
			&& (!doneStragglers || sinkOutputIndex != localOutputIndex)) {
		    // if doing local, we should not match own source vertex as sink vertex. This can happen if scanning
		    // for sink from starting point in non-greedy
		    sinkIndex += localIncrement;
		    sinkOutputIndex++;
		}

		assert (sinkOutputIndex == sinkIndex / localIncrement)
			&& (sinkIndex % localIncrement == 0) : "mismatch between sink adjList index and output index";

		if (sinkOutputIndex >= localVertexCount) break; // could not find a matching sink. quit for this source.

		long sink = localAdjList[sinkIndex]; // FIXME: when is localAdjList.length == 0?

		// test if sink index is not present in adjList for source vertex
		boolean hasDuplicateEdge = false;
		for (int offset = 1; offset <= localDegree; offset++) {
		    if (localAdjList[sinkIndex + offset] == source) {
			hasDuplicateEdge = true;
			break;
		    }
		}

		// sink is not a match. try next sink.
		if (hasDuplicateEdge) {
		    sinkIndex += localIncrement;
		    sinkOutputIndex++;
		    // if (sinkOutputIndex >= localVertexCount) { // TEMPDEL
		    // tempdelLocalNoMatchCount++;
		    // }
		    continue;
		} else { // sink is a match. add edge to source and sink.
		    outputEdges[outputIndex] = sink; // this out array may be local or staggler
		    localOutputEdges[sinkOutputIndex] = source; // this out array will only be local since sink is local
		    if (!doneStragglers) {
			// stragglerMatchCount++;
			// remove straggler vertex ID from linked list in straggler entry
			stragglerSrcIter.remove();
			if (!stragglerSrcIter.hasNext()) {
			    // remove straggler entry since this was the last vertex in that entry
			    stragglerEntryIter.remove();
			}
		    }
		    break;
		}

	    } while (sinkOutputIndex < localVertexCount); // repeat until we find sink or overflow

	    // NOTE: if sinkOutputIndex >= localVertexCount, then we were not able to find a match for the source
	    // vertex. Its output item will remain -1;
	    // TODO: create a straggler entry for this bucket and add vertex to it!!
	    //
	    // NOTE: Since we always look for a sink in indexes after the source index, it means that it is possible to
	    // miss out on matching a source vertex with another unmatched sink vertex that appears before it in the
	    // adjList.
	    //
	    // TODO: Should we retry match of vertices without a match in the local bucket again using full scan? May
	    // not get much improvement. May need to do this for last bucket.


	    ////////////////////////////////////////////
	    // Increment to next source vertex
	    localSrcIndex += localIncrement;
	    localOutputIndex++;

	    assert (localOutputIndex == localSrcIndex / localIncrement)
		    && (localSrcIndex % localIncrement == 0) : "mismatch between source adjList index and output index";

	} while (true);


	// System.out.printf("matchBucket:: degree=%d,StragglersMatched=%d%n", localDegree, stragglerMatchCount);
	return localOutputEdges;
    }


    static void saveGraphInlineAdj(OutputStream os, Map<Integer, BucketEntry> bucketMap, boolean includeAdded)
	    throws IOException {
	// iterate thru degree buckets
	for (Entry<Integer, BucketEntry> entry : bucketMap.entrySet()) {
	    int degree = entry.getKey();
	    BucketEntry bucket = entry.getValue();
	    if (includeAdded)
		GraphUtils.writeConcatInterleaveAdjacencyArray(bucket.inlineAdjList, bucket.addedEdges, degree, os);
	    else GraphUtils.writeConcatAdjacencyArray(bucket.inlineAdjList, degree, os);
	}
    }


    static int countStragglers(List<StragglerEntry> stragglerEntryList) {
	int noMatchCount = 0;
	for (StragglerEntry entry : stragglerEntryList) {
	    noMatchCount += entry.vertexIndexList.size();
	}
	return noMatchCount;
    }

    static class BucketEntry {
	BucketEntry(int vertexCount, long[] inlineAdjList) {
	    this.vertexCount = vertexCount;
	    this.inlineAdjList = inlineAdjList;
	}

	final int vertexCount;
	final long[] inlineAdjList;
	long[] addedEdges;
    }

    static class StragglerEntry {
	StragglerEntry(int degree, List<Integer> vertexIndexList) {
	    this.degree = degree;
	    this.vertexIndexList = vertexIndexList;
	}

	int degree; // bucker degree for the stragglers
	List<Integer> vertexIndexList; // index of the vertex for which we could not find a match earlier
    }


    /**
     * EulerGraphGenBin <odd degree input adj list binary file> <output eulerian adj list file>
     * 
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
	String inFName = args[0];
	String outFName = args[1];
	////////////////////////////////////////////
	// Load file

	// get optional count of ALL degrees
	// int degreeCount = args.length > 1 ? Integer.parseInt(args[1]) : -1;
	int degreeCount = -1;

	// input binary graph file to load
	Map<Integer, ElasticList<long[]>> adjArrOddDegreeMap;
	try (InputStream is = new BufferedInputStream(
		Files.newInputStream(Paths.get(inFName), StandardOpenOption.READ))) {

	    System.out.printf("%d,Reading file %s%n", System.currentTimeMillis(), inFName);

	    // read the ODD degree vertices of the adjList binary file, grouped by degree
	    adjArrOddDegreeMap = GraphUtils.loadBinGraphAsArrayByDegree(is, true, degreeCount);
	}

	int totalVertexCount = 0;

	////////////////////////////////////////////
	// Populate inline adj list
	long[] adjListInline = null;
	Map<Integer, BucketEntry> bucketMap = new HashMap<>(adjArrOddDegreeMap.size());
	long startTime = System.currentTimeMillis();
	int option = -1;

	//
	// OPTION 1: using no incremental removal. Fails at 4GB/OK at 5GB heap for 50M vertices (29M odd vertices).
	//
	// option = 1;
	// for (Entry<Integer, ElasticList<long[]>> entry : adjArrOddDegreeMap.entrySet()) {
	// // start common
	// int degreeId = entry.getKey();
	// ElasticList<long[]> adjArrList = entry.getValue();
	// int adjArrListSize = (int) adjArrList.size();
	// adjListInline = GraphUtils.concatAdjacencyArrays(adjArrList, degreeId);
	// bucketMap.put(degreeId, new BucketEntry(adjArrListSize, adjListInline));
	// totalVertexCount += (adjArrListSize * degreeId);
	// // end common
	// }

	//
	// OPTION 2: using entry iterator and implicit remove. Fails at 2.5GB/OK at 3GB heap for 50M vertices (29M odd
	// vertices). BEST OF THE LOT!
	//
	option = 2;
	Entry<Integer, ElasticList<long[]>> entry;
	for (Iterator<Entry<Integer, ElasticList<long[]>>> entries = adjArrOddDegreeMap.entrySet().iterator(); entries
		.hasNext();) {
	    entry = entries.next();
	    // start common
	    int degreeId = entry.getKey();
	    ElasticList<long[]> adjArrList = entry.getValue();
	    int adjArrListSize = (int) adjArrList.size();
	    adjListInline = GraphUtils.concatAdjacencyArrays(adjArrList, degreeId);
	    bucketMap.put(degreeId, new BucketEntry(adjArrListSize, adjListInline));
	    totalVertexCount += (adjArrListSize * degreeId);
	    // end common
	    entries.remove();
	}

	//
	// OPTION 3: using entryset and explicit remove. Fails at 4GB/OK at 5GB heap for 50M vertices (29M odd
	// vertices).
	//
	// option = 3;
	// Set<Entry<Integer, ElasticList<long[]>>> entries = adjArrOddDegreeMap.entrySet();
	// for (Entry<Integer, ElasticList<long[]>> entry : entries) {
	// // start common
	// int degreeId = entry.getKey();
	// ElasticList<long[]> adjArrList = entry.getValue();
	// int adjArrListSize = (int) adjArrList.size();
	// adjListInline = GraphUtils.concatAdjacencyArrays(adjArrList, degreeId);
	// bucketMap.put(degreeId, new BucketEntry(adjArrListSize, adjListInline));
	// totalVertexCount += (adjArrListSize * degreeId);
	// // end common
	// entries.remove(degreeId);
	// }
	System.out.printf("ConvertToInline,OPTION,%d,TimeTakenMS,%d%n", option,
		(System.currentTimeMillis() - startTime));
	adjArrOddDegreeMap = null;


	////////////////////////////////////////////
	// process matches
	Integer[] sortedBuckets = bucketMap.keySet().toArray(new Integer[0]);
	Arrays.sort(sortedBuckets, Comparator.reverseOrder());


	List<StragglerEntry> stragglerEntryList = new LinkedList<>();

	int noMatchSum = 0, newMatchSum = 0;
	int newPendingMatchCount = 0, oldPendingMatchCount = 0;
	System.out.println(
		"matchBucket,degree,vertexCount,LocalMatch,LocalNoMatch,StragglerMatch,StragglerPending,TimeTakenMS");

	startTime = System.currentTimeMillis();
	for (Integer bucketId : sortedBuckets) {
	    // match source vertices with sink
	    long intervalStartTime = System.currentTimeMillis();
	    BucketEntry bucketEntry = bucketMap.get(bucketId);
	    bucketEntry.addedEdges = matchBucket(bucketId, bucketMap, stragglerEntryList);

	    // collect straggler vertices with no match
	    int newNoMatchCount = 0;
	    List<Integer> stagglerVertexIndexList = new LinkedList<>();
	    for (int i = 0; i < bucketEntry.addedEdges.length; i++) {
		if (bucketEntry.addedEdges[i] == -1) {
		    stagglerVertexIndexList.add(i);
		    newNoMatchCount++;
		}
	    }
	    if (newNoMatchCount > 0) {
		stragglerEntryList.add(new StragglerEntry(bucketId, stagglerVertexIndexList));
		noMatchSum += newNoMatchCount;
	    }
	    long intervalEndTime = System.currentTimeMillis();

	    // logging
	    newPendingMatchCount = countStragglers(stragglerEntryList);
	    int newStragglersMatchCount = oldPendingMatchCount + newNoMatchCount - newPendingMatchCount;
	    newMatchSum += newStragglersMatchCount;
	    oldPendingMatchCount = newPendingMatchCount;

	    System.out.printf("matchBucket,%d,%d,%d,%d,%d,%d,%d%n", bucketId, bucketEntry.vertexCount,
		    (bucketEntry.vertexCount - newNoMatchCount), newNoMatchCount, newStragglersMatchCount,
		    newPendingMatchCount, (intervalEndTime - intervalStartTime));
	    // System.out.printf("matchBucket,%d,%d,%d,%d,%d,%d%d%n", bucketId, bucketEntry.vertexCount,
	    // (bucketEntry.vertexCount - newNoMatchCount), newNoMatchCount, -1, -1, (endTime - startTime));

	    // TEMPDEL
	    // try (BufferedWriter bw = Files.newBufferedWriter(Paths.get("adjListInline.log." + bucketId),
	    // Charset.forName("UTF-8"), StandardOpenOption.CREATE)) {
	    // // bw.write(Arrays.deepToString(Arrays.asList(adjListInline).toArray()));
	    // saveGraphInlineAdj(bw, bucketMap, false);
	    // }
	}
	long endTime = System.currentTimeMillis();
	System.out.printf("TotalStragglerNoMatch=%d,TotalStragglerMatch=%d,TotalVertices=%d,TotalMatchComputeTime=%d%n",
		noMatchSum, newMatchSum, totalVertexCount, (endTime - startTime));

	startTime = System.currentTimeMillis();
	try (OutputStream os = new BufferedOutputStream(Files.newOutputStream(Paths.get(outFName)))) {
	    // bw.write(Arrays.deepToString(Arrays.asList(adjListInline).toArray()));
	    saveGraphInlineAdj(os, bucketMap, true);
	}
	System.out.printf("Writing file %s%n", outFName);
	System.out.printf("saveGraphInlineAdj,TimeTakenMS=%d%n", (System.currentTimeMillis() - startTime));


    }


}

