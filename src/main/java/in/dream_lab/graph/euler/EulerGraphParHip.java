package in.dream_lab.graph.euler;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import gnu.trove.iterator.TLongLongIterator;
import gnu.trove.list.TByteList;
import gnu.trove.list.array.TByteArrayList;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.map.hash.TLongByteHashMap;
import gnu.trove.map.hash.TLongLongHashMap;


public class EulerGraphParHip {

    static final long START_VID_METIS = 1;

    static final byte PMAP_METIS = 1;
    static final byte PMAP_BLOGEL = 2;


    /**
     * Returns a mapping from existing absolute VID to relative VID starting from 0
     * 
     * @param adjArrList
     * @return
     */
    public static Long remapVertices(ElasticList<long[]> adjArrList, TLongLongHashMap vmap) {
	long startTime = System.currentTimeMillis();
	System.out.printf("Starting remapVertices,StartTime,%d%n", startTime);
	long relVID = START_VID_METIS; // starting VID. NOTE: According to METIS document, this must start at 1
	long edgeCount = 0;
	for (long[] adjArr : adjArrList) {
	    long absVID = adjArr[0];
	    assert !vmap.containsKey(absVID) : "source VID was already mapped to relative VID before!";
	    vmap.put(absVID, relVID);
	    relVID++;
	    edgeCount += (adjArr.length - 1); // skip source VID count
	}

	System.out.printf("Done remapVertices,LastRelVIDAssigned,%d,EdgeCount,%d,EndTime,%d,DurationMS,%d%n",
		relVID - 1, edgeCount, System.currentTimeMillis(), System.currentTimeMillis() - startTime);

	return edgeCount;
    }


    /**
     * Writes the vertex map from old VID to new relaitve VID to file
     * Each mapping is a pair of 8byte longs.
     * Total size = 2*8*vcount
     * 
     * @param vmap
     * @param os
     * @throws IOException
     */
    private static void writeVertexMap(TLongLongHashMap vmap, OutputStream os) throws IOException {
	long startTime = System.currentTimeMillis();
	System.out.printf("Starting writeVertexMap,StartTime,%d%n", startTime);
	DataOutputStream out = new DataOutputStream(os);
	TLongLongIterator vmapIter = vmap.iterator();
	while (vmapIter.hasNext()) {
	    vmapIter.advance();
	    out.writeLong(vmapIter.key());
	    out.writeLong(vmapIter.value());
	}
	System.out.printf("Done writeVertexMap,EndTime,%d,DurationMS,%d%n", System.currentTimeMillis(),
		System.currentTimeMillis() - startTime);
    }


    /**
     * reads the vertex map binary into map object, pairs of 8b long
     * Returns mapping from absolute VID to relative VID
     * 
     * @param vmap
     * @param os
     * @throws IOException
     */
    private static TLongLongHashMap readVertexMap(InputStream is, int vcountEstimate) throws IOException {
	long startTime = System.currentTimeMillis();
	System.out.printf("Starting readVertexMap,StartTime,%d,ExpectedVertices,%d%n", startTime, vcountEstimate);
	TLongLongHashMap vmap = new TLongLongHashMap(vcountEstimate <= 0 ? (1 * 1000 * 1000) : vcountEstimate);
	int vc = 0;
	try {
	    DataInputStream in = new DataInputStream(is);
	    while (true) {
		long absVID = in.readLong();
		long relVID = in.readLong();
		vmap.put(absVID, relVID); // fwd map
		vc++;
	    }

	} catch (EOFException e) {
	    return vmap;
	} finally {
	    System.out.printf("Done readVertexMap,VerticesRead,%d,EndTime,%d,DurationMS,%d%n", vc,
		    System.currentTimeMillis(), System.currentTimeMillis() - startTime);
	}
    }


    /**
     * reads the partition map text file into map object.
     * Implicit mapping from vid to part: the partition ID for vertex v is present in line v+1
     * 
     * @param vmap
     * @param os
     * @throws IOException
     */
    private static TLongByteHashMap readParHipPartitionMap(BufferedReader br, int vcountEstimate) throws IOException {
	long startTime = System.currentTimeMillis();
	System.out.printf("Starting readParHipPartitionMap,StartTime,%d,ExpectedVertices,%d%n", startTime,
		vcountEstimate);
	TLongByteHashMap pmap = new TLongByteHashMap(vcountEstimate <= 0 ? (10 * 1000 * 1000) : vcountEstimate);
	long vID = START_VID_METIS;
	try {
	    String partStr;
	    while ((partStr = br.readLine()) != null) {
		byte part = Byte.parseByte(partStr);
		pmap.put(vID, part);
		vID++;
	    }

	} finally {
	    System.out.printf("Done readParHipPartitionMap,VerticesRead,%d,EndTime,%d,DurationMS,%d%n", vID,
		    System.currentTimeMillis(), System.currentTimeMillis() - startTime);
	}
	return pmap;
    }


    /**
     * reads the partition map text file into map object.
     * Explicit mapping from VID to part.
     * [src1] [??] [pid]\t[sink1] [sink2] ...
     * 
     * @param vmap
     * @param os
     * @throws IOException
     */
    private static TLongByteHashMap readBlogelPartitionMap(BufferedReader br, int vcountEstimate) throws IOException {
	long startTime = System.currentTimeMillis();
	System.out.printf("Starting readBlogelPartitionMap,StartTime,%d,ExpectedVertices,%d%n", startTime,
		vcountEstimate);
	TLongByteHashMap pmap = new TLongByteHashMap(vcountEstimate <= 0 ? (1 * 1000 * 1000) : vcountEstimate);

	vcountEstimate = 0;
	try {
	    String partStr;
	    while ((partStr = br.readLine()) != null) {
		String[] parts = partStr.split(" ");
		long sourceVID = Long.parseLong(parts[0]);
		byte partID = Byte.parseByte(parts[1]);
		pmap.put(sourceVID, partID);
		vcountEstimate++;
	    }

	} finally {
	    System.out.printf("Done readBlogelPartitionMap,VerticesRead,%d,EndTime,%d,DurationMS,%d%n", vcountEstimate,
		    System.currentTimeMillis(), System.currentTimeMillis() - startTime);
	}
	return pmap;
    }


    /**
     * This updates the adj list mapping with the new vertex IDs.
     * Optionally, it writes the output adj list to disk in native binary format.
     * Optionally, it returns the udpated relative adj list object back.
     * NOTE: Both returning the object and writing to file cannot be false as otherwise there is no side-effect of
     * calling this method.
     * NOTE: the input adjArrList should have the source vertices in ascending order of VID, starting from 0
     * NOTE: setting returnRelList will mean that two copies of the adj list will have to be in memory at one instant
     * 
     * @param adjArrList
     * @param vmap
     * @param returnRelList
     * @param skipWriteRelAdjList
     * @param os
     * @return
     * @throws IOException
     */
    public static ElasticList<long[]> remapAdjListAndWrite(ElasticList<long[]> adjArrList, TLongLongHashMap vmap,
	    boolean returnRelList, boolean generateRelAdjList, OutputStream os) throws IOException {

	long startTime = System.currentTimeMillis();
	System.out.printf("Starting remapAdjListAndWrite,StartTime,%d,returnRelList,%b,generateRelAdjList,%b%n",
		startTime, returnRelList, generateRelAdjList);

	assert returnRelList
		|| generateRelAdjList : "We have set to NOT writing output relative adj list to file and are NOT returning the list. Updates will not be visible!";

	// if we need to return the new adjList, then cannot write incrementally
	int WRITE_THRESHOLD = 10 * 1000 * 1000;

	ElasticList<long[]> relAdjList = new ElasticList<long[]>(n -> new long[n][], WRITE_THRESHOLD, 1);
	int bufCount = 0, totCount = 0;

	// iterate thru all vertices, and their neighbors
	long sanityVID = START_VID_METIS; // NOTE: relative VID starts at 1
	for (long[] adjArr : adjArrList) {
	    // create new adj list with relative IDs for source and sink VIDs
	    int arrSize = adjArr.length;
	    long[] relAdjArr = new long[arrSize];
	    for (int i = 0; i < arrSize; i++) {
		relAdjArr[i] = vmap.get(adjArr[i]);
	    }
	    assert relAdjArr[0] == sanityVID++ : "the order of the relative VID was not in acsending order in the adj list";
	    relAdjList.add(relAdjArr);
	    bufCount++;
	    totCount++;

	    if (!returnRelList && generateRelAdjList && bufCount == WRITE_THRESHOLD) {
		// if we reach write threshold, incrementally write to output file
		GraphUtils.writeAdjacencyArray(relAdjList, os);
		relAdjList = new ElasticList<long[]>(n -> new long[n][], WRITE_THRESHOLD, 1);
		bufCount = 0;
	    }

	    if (totCount % 1000000 == 0) {
		System.out.printf("Progress remapAdjListAndWrite.remap,VerticesMapped,%d,CurrentTime,%d%n", totCount,
			System.currentTimeMillis());
	    }
	}
	System.out.printf("Done remapAdjListAndWrite.remap,VerticesMapped,%d,CurrentTime,%d%n", totCount,
		System.currentTimeMillis());


	// finally, write remaining
	if (generateRelAdjList) {
	    GraphUtils.writeAdjacencyArray(relAdjList, os);
	    System.out.printf("Done remapAdjListAndWrite.writeAdjacencyArray,VerticesMapped,%d,CurrentTime,%d%n",
		    totCount, System.currentTimeMillis());
	}

	return returnRelList ? relAdjList : null;
    }


    /**
     * Writes adj list with relative vid starting from 0, and with adj list given in sorted order of source vertices to
     * parhip binary format.
     * 
     * @param relAdjArrList
     * @param os
     * @throws IOException
     */
    private static void writeToParHipBin(ElasticList<long[]> relAdjArrList, long edgeCount, OutputStream os)
	    throws IOException {
	// Assert that input source VIDs are in ascending order
	//
	// PARHIP FORMAT (all entries are 8byte longs)
	// [ver][VCount][ECount][V1AdjOffsetBytes][V2AdjOffsetBytes]..[VvcountAdjOffsetBytes][EOFOffsetBytes]
	// [V1Sink1][V1Sink2]...
	// [V2Sink1][V2Sink2]...
	// ...
	// [VvcountSink1]...
	// <EOF>
	// See
	// https://github.com/schulzchristian/KaHIP/blob/b2d96cdc44b7f1a9e5e891e04d197f6a89df1a7a/parallel/parallel_src/lib/io/parallel_graph_io.cpp
	// Method writeGraphSequentiallyBinary, line 483
	long startTime = System.currentTimeMillis();
	System.out.printf("Starting writeToParHipBin,StartTime,%d%n", startTime);
	DataOutputStream out = new DataOutputStream(os);


	// write header
	final long VERSION = 3;
	out.writeLong(VERSION);

	// write number of vertices
	long vertexCount = relAdjArrList.size();
	out.writeLong(vertexCount);

	// write the number of edges
	// take as input to avoid scanning once more, and to avoid in memory header construction
	out.writeLong(edgeCount);


	// write offsets
	long offset = 8 * (3 + vertexCount + 1); // header + vertex offset count + EOF offset
	long sanityVID = START_VID_METIS;
	for (long[] adjArr : relAdjArrList) {
	    out.writeLong(offset); // write offset for V_i
	    long degree = adjArr.length - 1; // subtract 1 to skip source vertex
	    offset += degree * 8; // increase offset by the number of neighbors of V_i
	    assert adjArr[0] == sanityVID++ : "the order of the relative VID was not in acsending order in the adj list";
	}
	out.writeLong(offset); // write offset for EOF as well

	// start writing actual adj list
	for (long[] adjArr : relAdjArrList) {
	    // start at 1 to skip source vertex
	    for (int i = 1; i < adjArr.length; i++) {
		out.writeLong(adjArr[i]); // write neighbor
	    }
	}

	System.out.printf(
		"Done writeToParHipBin,VertexCount,%d,EdgeCount,%d,ExpectedFileSize,%d,EndTime,%d,DurationMS,%d%n",
		vertexCount, edgeCount, offset, System.currentTimeMillis(), System.currentTimeMillis() - startTime);
    }


    /**
     * Writes adj list with relative vid starting from START_VID_METIS, and with adj list given in sorted order of
     * source vertices to metis/parhip graph text format (unweighted vertices and edges).
     * 
     * @param relAdjArrList
     * @param os
     * @throws IOException
     */
    private static void writeToParHipText(ElasticList<long[]> relAdjArrList, long edgeCount, PrintStream ps)
	    throws IOException {
	//
	// METIS FORMAT (all entries are 8byte longs)
	// [VCount] [ECount]
	// [V1Sink1] [V1Sink2]...
	// [V2Sink1] [V2Sink2]...
	// ...
	// [VvcountSink1] ...
	// <EOF>
	//
	long startTime = System.currentTimeMillis();
	System.out.printf("Starting writeToParHipText,StartTime,%d%n", startTime);

	// write header
	// write number of vertices and edges
	// NOTE: taking edgeCount as input to avoid scanning once more, and to avoid in memory header construction
	long vertexCount = relAdjArrList.size();
	assert edgeCount % 2 == 0 : "expect bi-directed edge count to be even";

	ps.printf("%d %d%n", vertexCount, edgeCount / 2);

	// start writing actual adj list
	long sanityVID = START_VID_METIS;
	for (long[] adjArr : relAdjArrList) {
	    // start at 1 to skip source vertex
	    // Assert that input source VIDs are in ascending order
	    assert adjArr[0] == sanityVID++ : "Expected VID in ascending orde. Found mismatch.";

	    StringBuilder buf = new StringBuilder();
	    for (int i = 1; i < adjArr.length; i++) {
		buf.append(adjArr[i]); // write neighbor
		if (i < adjArr.length - 1) buf.append(' '); // write space for all but last line
	    }
	    ps.println(buf.toString()); // write buffer to output
	}

	ps.flush();
	System.out.printf("Done writeToParHipText,VertexCount,%d,EdgeCount,%d,EndTime,%d,DurationMS,%d%n", vertexCount,
		edgeCount, System.currentTimeMillis(), System.currentTimeMillis() - startTime);
    }


    /**
     * Converts input adj list binary to relative vertex mapping file, optionally parhip adj list binary, and optionally
     * relative adj list binary
     * 
     * @param is
     * @param generateParhip
     * @param skipWriteRelAdjList
     * @param osVmap
     * @param osPhip
     * @param osRadj
     * @throws IOException
     */
    public static void convertAdjListToParHip(InputStream is, boolean generateParhip, boolean isParhipBinary,
	    boolean generateRelAdjList, OutputStream osVmap, OutputStream osPhip, OutputStream osRadj)
	    throws IOException {

	System.out.printf("Starting convertAdjListToParHip,%d%n", System.currentTimeMillis());
	// read the vertex adjList binary file
	ElasticList<long[]> adjArrList = GraphUtils.loadBinGraphAsArray(is, false);


	// create mapping from each vertex to relative VID
	// this also returns an edge count that is used in future steps
	TLongLongHashMap vmap = new TLongLongHashMap((int) adjArrList.size());
	long edgeCount = remapVertices(adjArrList, vmap);

	// write mapping to file
	writeVertexMap(vmap, osVmap);
	System.out.printf("Done convertAdjListToParHip.writeVertexMap,%d%n", System.currentTimeMillis());

	// remap adjacency list using new VIDs. write to file.
	ElasticList<long[]> relAdjArrList = null;
	if (!generateParhip && !generateRelAdjList) {
	    System.out.printf(
		    "convertAdjListToParHip,Skipping remapAdjListAndWrite since generateParhip=%b and generateRelAdjList=%b%n",
		    generateParhip, generateRelAdjList);
	    return;
	}

	relAdjArrList = remapAdjListAndWrite(adjArrList, vmap, generateParhip, generateRelAdjList, osRadj);
	System.out.printf("Done convertAdjListToParHip.remapAdjListAndWrite,%d%n", System.currentTimeMillis());

	// write to parhip format
	if (generateParhip && isParhipBinary) {
	    writeToParHipBin(relAdjArrList, edgeCount, osPhip);
	    System.out.printf("Done convertAdjListToParHip.writeToParHipBin,%d%n", System.currentTimeMillis());
	}
	if (generateParhip && !isParhipBinary) {
	    try (PrintStream psPhip = new PrintStream(osPhip)) {
		writeToParHipText(relAdjArrList, edgeCount, psPhip);
		System.out.printf("Done convertAdjListToParHip.writeToParHipText,%d%n", System.currentTimeMillis());
	    }
	}
    }


    /**
     * INPUT:
     * A vertex map file from old VID to new relative VID.
     * An adj list binary file using old VIDs.
     * A parhip partition output file where n entries are present, 1 per line corresponding to n vertices, with a value
     * of 0..k-1, for k partitions.
     * 
     * OUTPUT:
     * A binary partition file using old VIDs.
     * This will also separate local and remote vertices, and the partition in which a remote vertex is present
     * 
     * @throws IOException
     */
    public static void convertPartitionerMapToAdjList(InputStream adjIs, InputStream vmapIs, BufferedReader pmapBr,
	    OutputStream[] os, int vcountEstimate, byte pmapFormat) throws IOException {

	TLongLongHashMap vmap = null;
	if (vmapIs != null) // skip using a vmap file, and use abs vertex addressing
	    vmap = readVertexMap(vmapIs, vcountEstimate); // abs VID to rel VID
	TLongByteHashMap pmap = pmapFormat == PMAP_METIS ? readParHipPartitionMap(pmapBr, vcountEstimate)
		: (pmapFormat == PMAP_BLOGEL ? readBlogelPartitionMap(pmapBr, vcountEstimate) : null);

	ElasticList<long[]> adjArrList = GraphUtils.loadBinGraphAsArray(adjIs, false);

	assert pmap != null : "Unknown pmap format. Pmap was null. Found type " + pmapFormat;
	assert vmap == null || vmap.size() == pmap.size() : "vertex map and partition maps have different sizes";
	assert vmap == null || vmap.size() == adjArrList.size() : "vertex map and adj list have different sizes";
	assert pmap.size() == adjArrList.size() : "partition map and adj list have different sizes";

	// FIXME: we do a vmap lookup twice for each vertex when we decide on its pmap, once as a source and once as a
	// sink. If we construct the pmap from abs VID to part (instead of rel VID to part), we can avoid a lookup
	//
	for (long[] adjArr : adjArrList) { // iterate thru each vertex in adj list based on abs VID
	    // get relative vertex ID from vid
	    long srcVID = adjArr[0];
	    assert vmap == null || vmap.containsKey(srcVID) : "Relative ID for Abs srcVID was not present in VMap, "
		    + srcVID;
	    long relSrcVID = vmap != null ? vmap.get(srcVID) : srcVID; // skip using relative?
	    // get part ID for relVID
	    assert pmap.containsKey(relSrcVID) : "srcVID " + srcVID + " with relSrcVID " + relSrcVID
		    + " was not present in PMap";
	    byte srcPart = pmap.get(relSrcVID); // get part ID for rel src VID

	    // get the degree of each vertex
	    int degree = adjArr.length - 1;

	    // (over)allocate arrays for local and remote vertices, and remote vertex partitions
	    TLongArrayList localAdjacencyList = new TLongArrayList(degree);
	    TByteList remotePartitionList = new TByteArrayList(degree);
	    TLongArrayList remoteAdjacencyList = new TLongArrayList(degree);
	    for (int i = 0; i < degree; i++) {
		long sinkVID = adjArr[i + 1]; // skip src vertex index
		assert vmap == null || vmap.containsKey(sinkVID) : "Relative sink ID for abs sinkVID " + sinkVID
			+ " (neighbor of abs srcVID " + srcVID + ") was not present in VMap";
		long relSinkVID = vmap != null ? vmap.get(sinkVID) : sinkVID;
		// get part ID for sink relVID
		assert pmap.containsKey(relSinkVID) : "relSinkVID " + relSinkVID + " for abs sinkVID " + sinkVID
			+ " (neighbor of abs srcVID " + srcVID + ") was not present in PMap";
		byte sinkPart = pmap.get(relSinkVID);
		// is sink local or remote?
		if (sinkPart == srcPart) { // local neighbor
		    // add to local adj list
		    localAdjacencyList.add(sinkVID);
		} else { // remote neighbor
		    // add to remote adj list and remote part ID
		    remoteAdjacencyList.add(sinkVID);
		    remotePartitionList.add(sinkPart);
		}
	    }


	    if (os.length == 1) {
		// write partitioned ad list to file
		GraphUtils.writePartitionedAdjacencyList(srcPart, srcVID, localAdjacencyList, remotePartitionList,
			remoteAdjacencyList, os[0]);
	    } else {
		// TODO: write to separate partition files by creating that many os? Avoids shuffle from read adj to
		// create source obj
		assert srcPart < os.length : "found a partition ID " + srcPart
			+ " that was greater than the number of streams " + os.length;
		// spread the list across part files. Ideally, the number of part files equals number of parts
		GraphUtils.writePartitionedAdjacencyList(srcPart, srcVID, localAdjacencyList, remotePartitionList,
			remoteAdjacencyList, os[srcPart % os.length]);
	    }

	}
    }


    /*-
     * EulerGraphParHip  A2P   <input adj list euler graph binary>   <output file prefix X>   [<generate parhip?> | true]  [<generate parhip as binary> | false]   [<generate rel adj list binary?> | false]
     * 		Writes X.vmap (X.phip | X.metis) X..radj.bin files
     * 
     * EulerGraphParHip  P2A   <input adj list euler graph binary>  <input vmap | "false" is use abs src id>  <input part map>  <output partitioned adj list binary file> <vertex count> <out file split count>
     * 		Reads X.bin, X.vmap, X.pmap
     * 
     * FIXME: CODE TO BE TESTED
     * 
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
	String op = args[0]; // A2P or P2A

	switch (op) {
	case "A2P": {

	    // A2P: eulerian binary adj graph
	    String inputFName = args[1];

	    // A2P: X.vmap, X.phip, X.radj.bin
	    String outputFName = args[2];

	    boolean generateParhip = args.length >= 4 ? Boolean.parseBoolean(args[3]) : true;
	    boolean isParhipBinary = args.length >= 5 ? Boolean.parseBoolean(args[4]) : false;
	    boolean generateRelAdjList = args.length >= 6 ? Boolean.parseBoolean(args[5]) : false;

	    String vmapFName = outputFName + ".vmap";
	    String phipFName = outputFName + (isParhipBinary ? ".phip" : ".metis");
	    String radjFName = outputFName + ".radj.bin";
	    try (InputStream is = new BufferedInputStream(
		    Files.newInputStream(Paths.get(inputFName), StandardOpenOption.READ));
		    OutputStream osVmap = new BufferedOutputStream(Files.newOutputStream(Paths.get(vmapFName)));
		    OutputStream osPhip = new BufferedOutputStream(Files.newOutputStream(Paths.get(phipFName)));
		    // null pointer ex?
		    OutputStream osRadj = generateRelAdjList
			    ? new BufferedOutputStream(Files.newOutputStream(Paths.get(radjFName)))
			    : null;) {

		System.out.printf("Reading file %s%n", inputFName);

		convertAdjListToParHip(is, generateParhip, isParhipBinary, generateRelAdjList, osVmap, osPhip, osRadj);

		if (generateRelAdjList)
		    System.out.printf("Writing files %s, %s, %s%n", vmapFName, phipFName, radjFName);
		else System.out.printf("Writing files %s, %s%n", vmapFName, phipFName);
	    }
	}
	    break;

	case "P2A": {
	    // P2A: X.bin X.vmap X.pmap 1|2 Y.parted.bin <vcount_est> <num_splits>
	    String adjFName = args[1];
	    String vmapFName = args[2];
	    boolean useAbsoluteVID = "false".equals(vmapFName);
	    String pmapFName = args[3];
	    byte pmapFormat = Byte.parseByte(args[4]);

	    // P2A: partitioned binary adj graph
	    String outputFName = args[5];

	    int vcount = args.length >= 7 ? Integer.parseInt(args[6]) : -1;
	    int partSplits = args.length >= 8 ? Integer.parseInt(args[7]) : 1; // split the output by parts?

	    OutputStream[] os;
	    if (partSplits == 1) {
		os = new OutputStream[] { new BufferedOutputStream(Files.newOutputStream(Paths.get(outputFName))) };
	    } else {
		os = new OutputStream[partSplits];
		for (int i = 0; i < partSplits; i++) {
		    os[i] = new BufferedOutputStream(Files.newOutputStream(Paths.get(outputFName + "." + (i + 1))));
		}
	    }

	    try (InputStream vmapIs = useAbsoluteVID ? null // if vmap file name is false, use absolute addressing
		    : new BufferedInputStream(Files.newInputStream(Paths.get(vmapFName), StandardOpenOption.READ));
		    InputStream adjIs = new BufferedInputStream(
			    Files.newInputStream(Paths.get(adjFName), StandardOpenOption.READ));
		    BufferedReader pmapBr = Files.newBufferedReader(Paths.get(pmapFName));) {

		System.out.printf("Reading files %s, %s, %s, with pmap format %d, with out split count %d%n", vmapFName,
			pmapFName, adjFName, pmapFormat, partSplits);

		convertPartitionerMapToAdjList(adjIs, vmapIs, pmapBr, os, vcount, pmapFormat);

		System.out.printf("Writing file %s%n", outputFName);
	    }

	    for (int i = 0; i < partSplits; i++) {
		os[i].close();
	    }
	}
	    break;
	}
    }


}
