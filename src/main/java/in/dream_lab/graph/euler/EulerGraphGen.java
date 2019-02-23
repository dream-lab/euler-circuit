package in.dream_lab.graph.euler;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;


/**
 * Utility to convert a non-eulerian graph to an eulerian graph
 * 
 * @author simmhan
 *
 */
public class EulerGraphGen {

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

		long sink = localAdjList[sinkIndex];

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


    /**
     * 
     * @param vertices
     * @param adjListMap
     * @param degree
     * @return
     */
    public static long[] toInlineAdjacencyList(Collection<Long> vertices,
	    Map<Long, ? extends Collection<Long>> adjListMap, int degree) {

	final long[] adjList = new long[(degree + 1) * vertices.size()];

	int vIndex = -1;
	for (long vertex : vertices) {
	    adjList[++vIndex] = vertex;
	    Collection<Long> neighbors = adjListMap.get(vertex);

	    assert neighbors != null : "could not find matching adj list for vertex";
	    assert neighbors.size() == degree : "mismatch between input adjacency list degree and expected degree";

	    for (long sink : neighbors) {
		adjList[++vIndex] = sink;
	    }
	}

	return adjList;
    }


    /**
     * 
     * @param br
     * @param degreeDistMap
     * @param adjList
     * @throws NumberFormatException
     * @throws IOException
     */
    public static void loadGraph(BufferedReader br, Map<Integer, Set<Long>> degreeDistMap, Map<Long, Set<Long>> adjList)
	    throws NumberFormatException, IOException {

	String vertexAdjList;
	int lineCount = 0, logCount = 0;
	long startTime = System.currentTimeMillis();
	long intervalStartTime = startTime;

	// Read the adjacency list file from disk and populate 2 HashMaps- adjList,
	// degreeDistMap for only odd degree vertices
	while ((vertexAdjList = br.readLine()) != null) {
	    String[] splits = vertexAdjList.split(":");
	    long vId = Long.parseLong(splits[0]);
	    String[] adjStr = splits[1].split(", ");
	    int degree = adjStr.length;
	    if (degree % 2 == 0) continue; // skip odd degree

	    // create neighbors and add to adjacency list
	    List<Long> neighbours = Stream.of(adjStr).map(Long::parseLong).collect(Collectors.toList());
	    // int degree = neighbours.size();
	    adjList.put(vId, new HashSet<>(neighbours));

	    // add vertex to degree bucket
	    Set<Long> adjListEntry = degreeDistMap.get(degree);
	    if (adjListEntry == null) {
		adjListEntry = new HashSet<>();
		degreeDistMap.put(degree, adjListEntry);
	    }
	    adjListEntry.add(vId);

	    // logging
	    lineCount++;
	    logCount++;
	    if (logCount == 1000000) {
		long intervalEndTime = System.currentTimeMillis();
		System.out.printf("VertexAddedMillions=%d,TimeTakenMS=%d%n", (lineCount / 1000000),
			(intervalEndTime - intervalStartTime));
		intervalStartTime = intervalEndTime;
		logCount = 0;
	    }
	}
	br.close();
	System.out.printf("TotalVertexAdded=%d,TimeTakenMS=%d%n", lineCount, (System.currentTimeMillis() - startTime));
    }


    static void saveGraphAdj(BufferedWriter bw, Map<Long, ? extends Collection<Long>> adjListMap) throws IOException {

	int lineCount = 0, logCount = 0;
	long startTime = System.currentTimeMillis();

	final Long[] longConverter = new Long[0];
	Long[] sortedAdjVertices = adjListMap.keySet().toArray(longConverter);
	Arrays.sort(sortedAdjVertices);

	System.out.printf("saveGraphAdj,VertexSortTimeTakenMS=%d%n", (System.currentTimeMillis() - startTime));

	long intervalStartTime = System.currentTimeMillis();
	for (long vertex : sortedAdjVertices) {
	    Collection<Long> neighbors = adjListMap.get(vertex);
	    Long[] sortedNeighbors = neighbors.toArray(longConverter);
	    Arrays.sort(sortedNeighbors);
	    StringBuilder buf = new StringBuilder();
	    buf.append(vertex);
	    for (long neighbor : sortedNeighbors) {
		buf.append(',').append(neighbor);
	    }
	    buf.append('\n');
	    bw.write(buf.toString());

	    // logging
	    lineCount++;
	    logCount++;
	    if (logCount == 1000000) {
		long intervalEndTime = System.currentTimeMillis();
		System.out.printf("saveGraphAdj,VertexWriteMillions=%d,TimeTakenMS=%d%n", (lineCount / 1000000),
			(intervalEndTime - intervalStartTime));
		intervalStartTime = intervalEndTime;
		logCount = 0;
	    }
	}
	System.out.printf("saveGraphAdj,TotalVertexWritten=%d,TimeTakenMS=%d%n", lineCount,
		(System.currentTimeMillis() - startTime));
    }


    static void saveGraphInlineAdj(BufferedWriter bw, Map<Integer, BucketEntry> bucketMap, boolean includeAdded)
	    throws IOException {
	Map<Long, List<Long>> adjListMap = new HashMap<>();
	long startTime = System.currentTimeMillis();
	// iterate thru degree buckets
	for (Entry<Integer, BucketEntry> entry : bucketMap.entrySet()) {
	    int degree = entry.getKey();
	    BucketEntry bucket = entry.getValue();
	    // iterate thru all vertices in a degree bucket
	    int vindex = 0;
	    for (int i = 0; i < bucket.vertexCount; i++) {
		// add vertex
		long vertex = bucket.inlineAdjList[vindex];
		// add neighbors
		List<Long> neighbors = new ArrayList<>(degree + 1);
		vindex++;
		for (int j = 0; j < degree; j++) {
		    neighbors.add(bucket.inlineAdjList[vindex]);
		    vindex++;
		}
		// add newly inserted sink vertex
		if (includeAdded) {
		    neighbors.add(bucket.addedEdges[i]);
		    if (bucket.addedEdges[i] == -1) {
			System.err.printf("ERROR: found -1 as added sink vertex ID for vertex %d, degree %d%n", vertex,
				degree);
		    }
		}
		// add vertex adj list
		adjListMap.put(vertex, neighbors);
	    }
	}
	System.out.printf("saveGraphInlineAdj,TimeTakenMS=%d%n", (System.currentTimeMillis() - startTime));

	// write adj list to file
	saveGraphAdj(bw, adjListMap);
    }


    public static void main(String[] args) throws Exception {

	////////////////////////////////////////////
	// Load file
	Map<Long, Set<Long>> adjListMap = new HashMap<>(args.length > 1 ? Integer.parseInt(args[1]) : 1000);
	Map<Integer, Set<Long>> degreeDistMap = new HashMap<>(args.length > 2 ? Integer.parseInt(args[2]) : 1000);

	BufferedReader br = Files.newBufferedReader(Paths.get(args[0])); // to read the adjList file

	// TODO: Load more efficiently directly into InlineAdjacencyList and BucketMap
	loadGraph(br, degreeDistMap, adjListMap);
	int totalVertexCount = adjListMap.size();

	// try (BufferedWriter bw = Files.newBufferedWriter(Paths.get(args[0] + "-orig.csv"), Charset.forName("UTF-8"),
	// StandardOpenOption.CREATE)) {
	// // bw.write(Arrays.deepToString(adjList.entrySet().toArray()));
	// saveGraphAdj(bw, adjListMap);
	// }


	////////////////////////////////////////////
	// Populate inline adj list
	long[] adjListInline = null;
	Map<Integer, BucketEntry> bucketMap = new HashMap<>(degreeDistMap.size());
	long startTime = System.currentTimeMillis();
	for (Integer bucketId : degreeDistMap.keySet()) {
	    adjListInline = toInlineAdjacencyList(degreeDistMap.get(bucketId), adjListMap, bucketId);
	    bucketMap.put(bucketId, new BucketEntry(degreeDistMap.get(bucketId).size(), adjListInline));
	}
	System.out.printf("ConvertToInlineTimeTakenMS=%d%n", (System.currentTimeMillis() - startTime));
	adjListMap = null;
	degreeDistMap = null;

	// try (BufferedWriter bw = Files.newBufferedWriter(Paths.get("adjListInline.log"), Charset.forName("UTF-8"),
	// StandardOpenOption.CREATE)) {
	// // bw.write(Arrays.deepToString(Arrays.asList(adjListInline).toArray()));
	// saveGraphInlineAdj(bw, bucketMap, false);
	// }


	////////////////////////////////////////////
	// process matches
	Integer[] sortedBuckets = bucketMap.keySet().toArray(new Integer[1]);
	Arrays.sort(sortedBuckets, Comparator.reverseOrder());
	// Arrays.sort(sortedBuckets);


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

	try (BufferedWriter bw = Files.newBufferedWriter(Paths.get(args[0] + "-euler.csv"), Charset.forName("UTF-8"),
		StandardOpenOption.CREATE)) {
	    // bw.write(Arrays.deepToString(Arrays.asList(adjListInline).toArray()));
	    saveGraphInlineAdj(bw, bucketMap, true);
	}

    }


    private static int countStragglers(List<StragglerEntry> stragglerEntryList) {
	int noMatchCount = 0;
	for (StragglerEntry entry : stragglerEntryList) {
	    noMatchCount += entry.vertexIndexList.size();
	}
	return noMatchCount;
    }

    private static class BucketEntry {
	BucketEntry(int vertexCount, long[] inlineAdjList) {
	    this.vertexCount = vertexCount;
	    this.inlineAdjList = inlineAdjList;
	}

	final int vertexCount;
	final long[] inlineAdjList;
	long[] addedEdges;
    }

    private static class StragglerEntry {
	StragglerEntry(int degree, List<Integer> vertexIndexList) {
	    this.degree = degree;
	    this.vertexIndexList = vertexIndexList;
	}

	int degree; // bucker degree for the stragglers
	List<Integer> vertexIndexList; // index of the vertex for which we could not find a match earlier
    }
}

