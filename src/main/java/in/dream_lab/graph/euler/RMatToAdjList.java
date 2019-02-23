package in.dream_lab.graph.euler;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import gnu.trove.iterator.TLongObjectIterator;
import gnu.trove.list.TLongList;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.map.TLongObjectMap;
import gnu.trove.map.hash.TLongObjectHashMap;


public class RMatToAdjList {

    /**
     * Given an undirected RMAT tab-separated edge list file, this generates a bi-directed adjacency list from it and
     * writes to two separate binary files, one with the adj list for odd degree and the other for even degree.
     * 
     * See RMatToEulerGraph
     * 
     * @param rmatReader
     * @param osOdd
     * @param osEven
     * @throws NumberFormatException
     * @throws IOException
     */
    public static void convertRMATtoBinAdjListOddEven(BufferedReader rmatReader, OutputStream osOdd,
	    OutputStream osEven) throws NumberFormatException, IOException {
	// 2) Convert directed edge list to bi-directed binary adjacency list
	TLongObjectMap<TLongList> biDirAdjMap = new TLongObjectHashMap<TLongList>();
	String t;
	long recount = 0;
	while ((t = rmatReader.readLine()) != null) {
	    recount++;
	    String[] edgePair = t.split("\t");
	    assert edgePair.length == 2 : "found an input TSV edge list without exactly 2 entries after split, line="
		    + recount + ", item=[" + t + "]";
	    long srcVID = Long.parseLong(edgePair[0]);
	    long sinkVID = Long.parseLong(edgePair[1]);

	    // add src entry to map key, and sink as its neighbor
	    {
		TLongList adjArrListSrc;
		adjArrListSrc = biDirAdjMap.get(srcVID);
		if (adjArrListSrc == null) {
		    adjArrListSrc = new TLongArrayList();
		    biDirAdjMap.put(srcVID, adjArrListSrc);
		}
		adjArrListSrc.add(sinkVID);
	    }
	    // add sink entry to map key, and source as its neighbor...bidirected
	    {
		TLongList adjArrListSink;
		adjArrListSink = biDirAdjMap.get(sinkVID);
		if (adjArrListSink == null) {
		    adjArrListSink = new TLongArrayList();
		    biDirAdjMap.put(sinkVID, adjArrListSink);
		}
		adjArrListSink.add(srcVID);
	    }
	    if (recount % 5000000 == 0) {
		System.out.printf("convertRMATtoBinAdjListOddEven,RMAT Reading EdgeCount,%d%n", recount);
	    }
	}
	System.out.printf("convertRMATtoBinAdjListOddEven done,RMAT Read EdgeCount,%d%n", recount);

	// 4) Split into odd and even degree vertices
	// write 2 binary files
	// GraphUtils.writeAdjacencyList(adjListMap, os);
	DataOutputStream outOdd = new DataOutputStream(osOdd);
	DataOutputStream outEven = new DataOutputStream(osEven);
	DataOutputStream out;
	TLongObjectIterator<TLongList> adjListIter = biDirAdjMap.iterator();
	int oddVCount = 0, evenVCount = 0;
	int oddECount = 0, evenECount = 0;
	while (adjListIter.hasNext()) {
	    adjListIter.advance();
	    TLongList v = adjListIter.value();
	    int degree = v.size();
	    // System.out.printf("writing,%d,%d%n", v.get(0), degree);
	    out = degree % 2 == 0 ? outEven : outOdd;
	    out.writeInt(degree + 1); // write size
	    out.writeLong(adjListIter.key()); // write source

	    for (int i = 0; i < degree; i++)
		out.writeLong(v.get(i)); // write neighbors

	    oddVCount += degree % 2 == 0 ? 0 : 1;
	    evenVCount += degree % 2 == 0 ? 1 : 0;
	    oddECount += degree % 2 == 0 ? 0 : degree;
	    evenECount += degree % 2 == 0 ? degree : 0;

	    if ((oddVCount + evenVCount) % 1000000 == 0) {
		System.out.printf(
			"convertRMATtoBinAdjListOddEven at,oddVCount,%d,oddECount,%d,evenVCount,%d,evenECount,%d,totVCount,%d,totECount,%d%n",
			oddVCount, oddECount, evenVCount, evenECount, oddVCount + evenVCount, oddECount + evenECount);
	    }
	}

	System.out.printf(
		"convertRMATtoBinAdjListOddEven done,oddVCount,%d,oddECount,%d,evenVCount,%d,evenECount,%d,totVCount,%d,totECount,%d%n",
		oddVCount, oddECount, evenVCount, evenECount, oddVCount + evenVCount, oddECount + evenECount);
    }


    /**
     * RMatToAdjList <rmat edge list tsv> <odd degree adj list binary> <even degree adj list binary>
     * 
     * @param args
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {
	String inputFName = args[0]; // RMat file
	String outputOddFName = args[1]; // output binary odd adj file name
	String outputEvenFName = args[2]; // output binary even adj file name

	// open reader to the RMat TSV file
	try (BufferedReader bw = Files.newBufferedReader(Paths.get(inputFName));
		OutputStream osOdd = new BufferedOutputStream(Files.newOutputStream(Paths.get(outputOddFName)));
		OutputStream osEven = new BufferedOutputStream(Files.newOutputStream(Paths.get(outputEvenFName)))) {

	    long startTime = System.currentTimeMillis();
	    System.out.printf("%d,Reading file %s%n", startTime, inputFName);

	    convertRMATtoBinAdjListOddEven(bw, osOdd, osEven);

	    System.out.printf("%d,Writing files %s and %s took %d secs%n", System.currentTimeMillis(), outputOddFName,
		    outputEvenFName, System.currentTimeMillis() - startTime);

	}


    }

}
