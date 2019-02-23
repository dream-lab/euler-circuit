package in.dream_lab.graph.euler;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import gnu.trove.iterator.TLongObjectIterator;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.map.hash.TLongObjectHashMap;


public class EulerTour {

    // Use MSB bit of neighbor to indicate that this edge is visited
    static final long VISITED_MASK = 0x01 << 63;


    public static void eulerTour(TLongObjectHashMap<TLongArrayList> adjMap, OutputStream os) throws IOException {

	assert adjMap != null && adjMap.size() > 0 : "adjMap was empty";

	long startTime = System.currentTimeMillis();

	// iterate through map to find the number of edges. also pick a source vertex.
	long srcVID = 0;
	int maxDegree = -1;
	int edgeCount = 0;
	TLongObjectIterator<TLongArrayList> adjIter = adjMap.iterator();
	while (adjIter.hasNext()) {
	    adjIter.advance();
	    int adjSize = adjIter.value().size();
	    assert adjSize % 2 == 0 : "Found odd degree " + adjSize + " for vertex " + adjIter.key();
	    if (adjSize > maxDegree) {
		srcVID = adjIter.key(); // prefer source vertex with high degree
		maxDegree = adjSize;
	    }
	    edgeCount += adjSize;
	}

	assert maxDegree > 0 : "could not find a source vertex with degree > 0!";

	// init output path array
	// FIXME: limited to signed Integer (~2B)
	// To avoid memory pressure, we are storing incremental paths and writing to disk once full
	final int PATH_CAPACITY = ((edgeCount + 1) < (50 * 1000 * 1000)) ? (edgeCount + 1) : (50 * 1000 * 1000);
	TLongArrayList paths = new TLongArrayList(PATH_CAPACITY);

	// start tour


	// how many neighbors need to have been removed before we reclaim space?
	// NOTE: this involved memcopy of remaining items in arraylist
	// final int TRIM_THRESHOLD = 10 * 1000 * 1000;
	final int LOG_THRESHOLD = 1 * 1000 * 1000;

	// add source to paths
	long currVID = srcVID, prevVID = 0, nextVID = 0;
	boolean firstVisit = true;
	long pathCount = 0;
	paths.add(currVID);

	long writeTime = 0;
	long srcTime = System.currentTimeMillis() - startTime;
	System.out.printf("eulerTour,Starting at vid,%d,AtTime,%d,VCount,%d,ECount,%d%n", currVID,
		System.currentTimeMillis(), adjMap.size(), edgeCount);
	while (true) {
	    // get neighbors of current vertex we are visiting
	    // FIXME: Replace TLongArrayList with long[] for neighbors?
	    TLongArrayList neighbors = adjMap.get(currVID);
	    int neighborsSize = 0;
	    int neighborsIndex = 0;
	    if (neighbors != null && (neighborsSize = neighbors.size()) > 0) {
		// assert neighbors != null : "neighbor list was NULL for vertex " + currVID;
		// int neighborsSize = neighbors.size();
		// assert neighborsSize > 0 : "neighbor list was empty for vertex " + currVID;

		// mark the prevVID from the neighbor list of currVID as visited edge
		if (firstVisit) firstVisit = false; // no prevVID exists for source vertex
		else for (int i = 0; i < neighborsSize; i++) {
		    if (neighbors.get(i) == prevVID) {
			neighbors.set(i, prevVID | VISITED_MASK);
			break;
		    }
		}

		// find next unvisited neighbor. start at end of list.
		// start testing from last neighbor as its easier to remove the trail entries of arraylist
		neighborsIndex = neighborsSize - 1;
		do {
		    nextVID = neighbors.get(neighborsIndex);
		    if ((nextVID & VISITED_MASK) != 0) neighbors.removeAt(neighborsIndex); // remove visited edges
		    else break; // found unvisited
		    neighborsIndex--;
		} while (neighborsIndex >= 0);
	    }

	    // all visited for current vertex. no more out edges to traverse. tour done.
	    if (neighbors == null || neighborsSize == 0 || neighborsIndex < 0) {
		if (pathCount < (edgeCount / 2)) {
		    // tour should end where we started
		    assert currVID == srcVID : "One tour done, but the start and edge vertices were different!";

		    // TODO: add new path file? Some marker?
		    long writeStartTime = System.currentTimeMillis();
		    writePaths(paths, os, PATH_CAPACITY);
		    writeTime += (System.currentTimeMillis() - writeStartTime);

		    // if we still have edges to traverse, find next source vertex for new tour
		    Long nextSource = findNextSource(adjMap);
		    assert nextSource != null : "could not find the next source path";
		    currVID = srcVID = nextSource;
		    prevVID = nextVID = 0;
		    System.out.printf("eulerTour,REStarting at vid,%d,PathCount,%d,AtTime,%d%n", currVID, pathCount,
			    System.currentTimeMillis());
		    firstVisit = true;
		    paths.add(currVID);
		    continue;
		} else {
		    // else, we're done
		    long writeStartTime = System.currentTimeMillis();
		    writePaths(paths, os, PATH_CAPACITY);
		    writeTime += (System.currentTimeMillis() - writeStartTime);
		    long totTime = (System.currentTimeMillis() - startTime);
		    System.out.printf(
			    "eulerTour,AllDone,PathCount,%d,SrcFindDuration,%d,WriteDuration,%d,LoopDuration,%d,TotalDuration,%d%n",
			    pathCount, srcTime, writeTime, (totTime - srcTime - writeTime), totTime);
		    assert currVID == srcVID : "Tour done, but the start and edge vertices were different!";
		    assert pathCount == edgeCount
			    / 2 : "Tour done, but the vertex path length did not match edge count+1! pathCount="
				    + pathCount + "; edgeCount+1=" + (edgeCount / 2);
		    return;
		}
	    }

	    assert (nextVID & VISITED_MASK) == 0 : "More neighbors present but loop returned a visited neighbor "
		    + nextVID + " for currVID " + currVID;
	    assert (neighbors.size()
		    - 1) == neighborsIndex : "vertex being added to path is not the last vertex in neighbors; neighbor size="
			    + (neighbors.size() - 1) + "; remove index=" + (neighborsIndex);

	    // visit edge to nextVID
	    // add edge to path
	    prevVID = currVID;
	    currVID = nextVID;
	    paths.add(currVID);
	    pathCount++; // increment path count only on edge addition

	    // mark edge as visited by removing the neighbor. This should be the last item.
	    neighbors.removeAt(neighborsIndex);

	    if (pathCount % PATH_CAPACITY == 10) writePaths(paths, os, PATH_CAPACITY);


	    // Do trimtosize if the neighbor is empty
	    if (neighborsIndex == 0) neighbors.trimToSize();


	    // Log
	    if (pathCount % LOG_THRESHOLD == 0) {
		long cummTime = System.currentTimeMillis() - startTime;
		System.out.printf("eulerTour,PathCountM,%d,CummTimeMS,%d,EdgesTraversedPerSec,%d,%n",
			(pathCount / 1000000), cummTime, (int) (((double) pathCount / cummTime) * 1000));
	    }
	}
    }


    static Long findNextSource(TLongObjectHashMap<TLongArrayList> adjMap) {
	// find vertices with neighbors != empty (has unvisited vertices)
	// while looking, also clean up entries whith empty neighbors
	adjMap.tempDisableAutoCompaction(); // wait till we're done with pruning
	int pruneCount = 0;
	TLongObjectIterator<TLongArrayList> adjIter = adjMap.iterator();
	Long srcVID = null;
	while (adjIter.hasNext()) {
	    adjIter.advance();
	    int adjSize = adjIter.value().size();
	    // check if possible source vertex has neighbors
	    if (adjSize > 0) {
		// check if possible source vertex has unvisited neighbors
		srcVID = adjIter.key();
		TLongArrayList neighbors = adjMap.get(srcVID);
		int neighborsSize = neighbors.size();
		int neighborsIndex = neighborsSize - 1;
		do {
		    long nextVID = neighbors.get(neighborsIndex);
		    if ((nextVID & VISITED_MASK) != 0) neighbors.removeAt(neighborsIndex); // remove visited edges
		    else break; // found unvisited
		    neighborsIndex--;
		} while (neighborsIndex >= 0);

		if (neighborsIndex >= 0) break; // found valid source vertex
	    }
	    // all neighbors were visited/removed!
	    adjIter.remove();
	    pruneCount++;
	}

	// run compacter only if we get enough benefit
	System.out.printf("findNextSource,nextSource,%d,prunedCount,%d%n", srcVID, pruneCount);
	adjMap.reenableAutoCompaction(pruneCount > 1000000 ? true : false);

	return srcVID;
    }


    private static void writePaths(TLongArrayList paths, OutputStream os, int PATH_CAPACITY) throws IOException {
	// write incremental path to file
	long startTime = System.currentTimeMillis();
	DataOutputStream out = new DataOutputStream(os);
	int pathLength = paths.size();
	long lastTime = System.currentTimeMillis(); // logging
	for (int i = 0; i < pathLength; i++) {
	    out.writeLong(paths.get(i));

	    // logging
	    if (i > 50000000 && i % 50000000 == 0) {
		System.out.printf("writePaths,VerticesWritten,%d,TimeTakenMS=%d%n", i,
			(System.currentTimeMillis() - lastTime));
		lastTime = System.currentTimeMillis();
	    }

	}
	System.out.printf("writePaths,WrittenPaths,%d,TimeTakenMS=%d%n", pathLength,
		(System.currentTimeMillis() - startTime));

	out.flush();
	// clear paths
	paths.clear(PATH_CAPACITY);
    }


    /**
     * USAGE:
     * EulerTour  <input adj bin eulerian file>   <output paths bin fine>  <vertex count estimate>
     * 
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
	String inputFName = args[0];
	String outputFName = args[1];
	int vcount = args.length >= 3 ? Integer.parseInt(args[2]) : -1;

	// input binary graph file to load
	long startTime = System.currentTimeMillis();
	TLongObjectHashMap<TLongArrayList> adjMap;
	try (InputStream is = new BufferedInputStream(
		Files.newInputStream(Paths.get(inputFName), StandardOpenOption.READ))) {

	    // read the vertices of the adjList binary file into map
	    // key is source, value is list of neighbors.
	    adjMap = GraphUtils.readAllIntoAdjacencyTMap(is, vcount);
	}
	System.out.printf("LoadAdjMap,TimeTakenMS=%d%n", (System.currentTimeMillis() - startTime));

	// find Euler tour
	startTime = System.currentTimeMillis();
	try (OutputStream os = new BufferedOutputStream(Files.newOutputStream(Paths.get(outputFName)))) {
	    eulerTour(adjMap, os);
	    System.out.printf("FindTour,TimeTakenMS=%d%n", (System.currentTimeMillis() - startTime));
	}
    }

}
