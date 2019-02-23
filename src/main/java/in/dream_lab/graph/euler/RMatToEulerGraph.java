package in.dream_lab.graph.euler;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SerializableWritable;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import gnu.trove.list.TLongList;
import gnu.trove.list.array.TLongArrayList;
import in.dream_lab.graph.euler.EulerGraphGenBin.BucketEntry;
import in.dream_lab.graph.euler.EulerGraphGenBin.StragglerEntry;
import scala.Tuple2;


public class RMatToEulerGraph {

    /**
     * Generate RMAT graph in SPARK
     * ---
     * Spark Driver Code for converting RMat edge file to eulerian graph.
     * 1) Convert directed edge list to bi-directed edge list
     * 2) Convert bi-directed edge list to adjacency list
     * 3) Split into odd and even degree vertices
     * 4) Call eulerian code to add missing edges to odd degree vertices
     * 5) union even degree vertices to updated odd degree vertices
     * 6) i) Write to native binary format
     * ii) Write to ParHIP binary format; write mapping file from native VID to parhip VID starting at 0
     * ---
     * call parhip/metis
     * ---
     * 1) Convert parhip/metis output to native partitioned binary graph
     * 
     * @param args
     * @throws IOException
     * @throws ClassNotFoundException
     */
    @SuppressWarnings("serial")
    public static void main(String[] args) throws IOException, ClassNotFoundException {
	SparkConf conf = new SparkConf().setAppName("RMatToParHip");
	JavaSparkContext sc = new JavaSparkContext(conf);
	//
	// Handle java.lang.RuntimeException: java.io.NotSerializableException:
	// org.apache.hadoop.io.ArrayPrimitiveWritable
	// https://stackoverflow.com/questions/29876681/hadoop-writables-notserializableexception-with-apache-spark-api
	//
	// new SparkConf().registerKryoClasses(new Class<?>[] {
	// Class.forName("org.apache.hadoop.io.ArrayPrimitiveWritable") });

	// Input and Output filenames are given as userargs
	String inputPath = args[0]; // to read input adjacency list
	String outputPath = args[1]; // to save output in HDFS
	int partitions = args.length >= 3 ? Integer.parseInt(args[2]) : 16;

	// send the HDFS filesystem info to partitions to directly write to remote FS
	Configuration configuration = JavaSparkContext.toSparkContext(sc).hadoopConfiguration();
	Broadcast<SerializableWritable<Configuration>> hdfsConf = sc
		.broadcast(new SerializableWritable<Configuration>(configuration));
	Broadcast<String> outputPathBC = sc.broadcast(outputPath);


	///////////////////////////////
	// 1) Read the edge list as text input. Create bi-directed entry from undirected.
	JavaRDD<String> rdd1 = sc.textFile(inputPath);
	JavaRDD<String> rdd1x = rdd1.repartition(partitions); // FIXME: remove?

	JavaPairRDD<Long, Long> rdd2 = rdd1x.flatMapToPair(new PairFlatMapFunction<String, Long, Long>() {

	    @Override
	    public Iterator<Tuple2<Long, Long>> call(String t) {
		String[] edgePair = t.split("\t");
		assert edgePair.length == 2 : "found an input TSV edge list without exactly 2 entries after split";
		long srcVID = Long.parseLong(edgePair[0]);
		long sinkVID = Long.parseLong(edgePair[1]);

		List<Tuple2<Long, Long>> biDirectedEdges = new ArrayList<Tuple2<Long, Long>>(2);
		biDirectedEdges.add(new Tuple2<Long, Long>(srcVID, sinkVID));
		biDirectedEdges.add(new Tuple2<Long, Long>(sinkVID, srcVID));
		return biDirectedEdges.iterator();
	    }
	});


	///////////////////////////////
	// 2) Convert bi-directed edge list to adjacency list
	// retain src VID+adj list in binary form. Used by native GraphUtils adj list.
	JavaPairRDD<Long, Iterable<Long>> rdd3 = rdd2.groupByKey();
	JavaRDD<LongArraySerializable> rdd4 = rdd3
		.map(new Function<Tuple2<Long, Iterable<Long>>, LongArraySerializable>() {
		    @Override
		    public LongArraySerializable call(Tuple2<Long, Iterable<Long>> kv) throws Exception {
			long srcVID = kv._1;
			TLongList adjArrList = new TLongArrayList();
			adjArrList.add(srcVID); // include src and sink VIDs in arraylist
			for (Long v : kv._2)
			    adjArrList.add(v);

			// wrap in an array primitive writable and return
			return new LongArraySerializable(adjArrList.toArray());
		    }
		});

	// cache this since we're going to use this again
	rdd4.cache();


	///////////////////////////////
	// 3) Split into odd and even adj lists
	// adj.length has 1 source+degree sinks
	JavaRDD<LongArraySerializable> evenRDD = rdd4.filter(adj -> (((long[]) adj.get()).length - 1) % 2 == 0);
	JavaRDD<LongArraySerializable> oddRDD = rdd4.filter(adj -> (((long[]) adj.get()).length - 1) % 2 == 1);

	// tempdel
	// List<Tuple2<String, String>> tmp;
	// evenRDD.cache();
	// oddRDD.cache();
	// tmp = evenRDD.mapPartitions(new WriteToBinArrParHip(hdfsConf, outputPathBC)).collect();
	// for (Tuple2<String, String> part : tmp)
	// System.out.printf("evenRDD,%s,%s%n", part._1, part._2);
	//
	// tmp = oddRDD.mapPartitions(new WriteToBinArrParHip(hdfsConf, outputPathBC)).collect();
	// for (Tuple2<String, String> part : tmp)
	// System.out.printf("oddRDD,%s,%s%n", part._1, part._2);

	///////////////////////////////
	// 4) Convert odd RDD into eulerian graph
	JavaRDD<LongArraySerializable> odd1RDD = oddRDD.coalesce(1);
	// tempdel
	// odd1RDD.cache();
	// tmp = odd1RDD.mapPartitions(new WriteToBinArrParHip(hdfsConf, outputPathBC)).collect();
	// for (Tuple2<String, String> part : tmp)
	// System.out.printf("odd1RDD,%s,%s%n", part._1, part._2);


	JavaRDD<LongArraySerializable> euOddRDD = odd1RDD.mapPartitions(new ConvertToEulerGraph());
	// tempdel
	// euOddRDD.cache();
	// tmp = euOddRDD.mapPartitions(new WriteToBinArrParHip(hdfsConf, outputPathBC)).collect();
	// for (Tuple2<String, String> part : tmp)
	// System.out.printf("euOddRDD,%s,%s%n", part._1, part._2);

	///////////////////////////////
	// 5) Union of eulerian odd and earlier even RDD
	JavaRDD<LongArraySerializable> eulerianRDD = evenRDD.union(euOddRDD);
	eulerianRDD.cache();


	///////////////////////////////
	// 6) i) Write to native binary array. This can be done in a distributed setting.

	JavaRDD<Tuple2<String, String>> outRDD = eulerianRDD.mapPartitions(new WriteToBinArr(hdfsConf, outputPathBC));
	outRDD.cache();

	///////////////////////////////
	// 6) ii) Write to parhip binary file and a mapping file.
	// Too complex for distributed mem. Use EulerGraphParHip shared memory version.


	///////////////////////////////
	// Trigger transformations
	outRDD.saveAsTextFile(outputPath);
	List<Tuple2<String, String>> outParts = outRDD.collect();
	for (Tuple2<String, String> part : outParts) {
	    System.out.printf("%s,%s%n", part._1, part._2);
	}

	// rdd5.saveAsTextFile(outputPath);
	sc.stop();
    }

    /**
     * Class to write the eulerian graph to HDFS file system.
     * This writes the adjacency list to native binary format.
     * It returns the partition file name for the file and the number of vertices written.
     * 
     * TODO FIXME (we need the mapping file first to allow parhip to's adj list to contain the parhip VIDs)
     * It returns a mapping from the native vertex ID to the byte offset for that vertex in this partition output file
     * for parhip. This is used to construct the header file.
     * This can be used to generate a mapping file from VID 0-(n-1) used in parhip to other VIDs used natively.
     * This also generates the ordering in which the partitions should be concated.
     * FIXME
     * 
     * @author simmhan
     *
     */
    @SuppressWarnings("serial")
    public static class WriteToBinArr
	    implements FlatMapFunction<Iterator<LongArraySerializable>, Tuple2<String, String>> {

	Broadcast<SerializableWritable<Configuration>> hdfsConf;
	Broadcast<String> outputPathBC;


	public WriteToBinArr(Broadcast<SerializableWritable<Configuration>> hdfsConf, Broadcast<String> outputPathBC) {
	    this.hdfsConf = hdfsConf;
	    this.outputPathBC = outputPathBC;
	}


	@Override
	public Iterator<Tuple2<String, String>> call(Iterator<LongArraySerializable> aw) throws Exception {
	    // access to HDFS
	    FileSystem fileSystem = FileSystem.get(hdfsConf.getValue().value());


	    // write each adj list entry to native binary file format
	    // [degreeCount+1 (4b)][source (8b)][sink1 (8b)][sink2]...[sink_d (8b)]
	    UUID partName = UUID.randomUUID();
	    String fname = outputPathBC.getValue() + "/euler-out-" + partName + ".bin";
	    Path hf = new Path(fname);
	    // Limit replication factor to 1
	    FSDataOutputStream fos = fileSystem.create(hf, (short) 1);
	    long vc = 0, bc = 0, ec = 0;
	    while (aw.hasNext()) {
		long[] adjArr = (long[]) aw.next().get();
		fos.writeInt(adjArr.length); // we have degree+1 entries in a row
		// write source and all sinks
		for (int j = 0; j < adjArr.length; j++) {
		    fos.writeLong(adjArr[j]);
		}
		ec += (adjArr.length - 1); // edge count
		bc += (4 + 8 * adjArr.length); // byte count
		vc++; // vertex count
	    }
	    fos.flush();
	    fos.close();

	    // return iterator over singleton
	    ArrayList<Tuple2<String, String>> al = new ArrayList<Tuple2<String, String>>(1);
	    al.add(new Tuple2<String, String>(fname, new StringBuffer().append(vc).append(',').append(ec).append(',')
		    .append(bc).append(',').toString()));
	    return al.iterator();
	}
    }

    /**
     * Converts a graph (with odd degree vertices) passed as an adjacency array list [source,sink*] into an output graph
     * that is eulerian.
     * 
     * See in.dream_lab.graph.euler.EulerGraphGenBin
     * 
     * @author simmhan
     *
     */
    @SuppressWarnings("serial")
    public static class ConvertToEulerGraph
	    implements FlatMapFunction<Iterator<LongArraySerializable>, LongArraySerializable> {

	@Override
	public Iterator<LongArraySerializable> call(Iterator<LongArraySerializable> adjIter) throws Exception {
	    /////////////////////////////////////////////
	    // Convert adj list to map grouped by degree
	    // See GraphUtils.loadBinGraphAsArrayByDegree()
	    long startTime = System.currentTimeMillis(), lastTime = startTime;
	    Map<Integer, ElasticList<long[]>> adjArrOddDegreeMap = new HashMap<>(1000); // degree to adj list
	    int vcount = 0;
	    {
		ElasticList<long[]> adjList;
		int dcount = 0, ecount = 0;
		// iterate thru array writable
		while (adjIter.hasNext()) {
		    long[] adj = (long[]) adjIter.next().get();
		    int degree = adj.length - 1; // ignore source vertex in count
		    assert degree % 2 == 1 : "found even degree vertex: " + adj[0];
		    // System.out.printf("loadBinGraphAsArrayByDegree,vid,%d,degree,%d%n", adj[0], adj.length - 1);
		    if (degree % 2 == 0) continue; // skip even vertex
		    adjList = adjArrOddDegreeMap.get(degree);
		    if (adjList == null) {
			adjList = new ElasticList<long[]>(n -> new long[n][], 1000, 100);
			adjArrOddDegreeMap.put(degree, adjList);
			dcount++;
		    }
		    adjList.add(adj);
		    vcount++;
		    ecount += degree;
		    if (vcount % 1000000 == 0) {
			System.out.printf(
				"loadBinGraphAsArrayByDegree,VerticesLoaded,%d,DegreeBuckets,%d,TimeTakenMS=%d%n",
				vcount, dcount, (System.currentTimeMillis() - lastTime));
			lastTime = System.currentTimeMillis();
		    }
		}
		System.out.printf(
			"loadBinGraphAsArrayByDegree,TotalVertexLoaded,%d,TotalEdgesLoaded,%d,TotalDegreeBuckets,%d,TimeTakenMS=%d%n",
			vcount, ecount, dcount, (System.currentTimeMillis() - startTime));
		// Done with input iterator
		adjIter = null;
	    }

	    ////////////////////////////////////////////
	    // Convert map to inline adj list
	    Map<Integer, BucketEntry> bucketMap = new HashMap<>(adjArrOddDegreeMap.size());
	    {
		startTime = System.currentTimeMillis();

		//
		// OPTION 2: using entry iterator and implicit remove. Fails at 2.5GB/OK at 3GB heap for 50M vertices
		// (29M
		// odd
		// vertices). BEST OF THE LOT!
		//
		Entry<Integer, ElasticList<long[]>> entry;
		for (Iterator<Entry<Integer, ElasticList<long[]>>> entries = adjArrOddDegreeMap.entrySet()
			.iterator(); entries.hasNext();) {
		    entry = entries.next();
		    // start common
		    int degreeId = entry.getKey();
		    ElasticList<long[]> adjArrList = entry.getValue();
		    int adjArrListSize = (int) adjArrList.size();
		    long[] adjListInline = GraphUtils.concatAdjacencyArrays(adjArrList, degreeId);
		    bucketMap.put(degreeId, new BucketEntry(adjArrListSize, adjListInline));
		    // end common
		    entries.remove();
		}

		System.out.printf("ConvertToInline,TimeTakenMS=%d%n", (System.currentTimeMillis() - startTime));
		// done with adjArrOddDegreeMap
		adjArrOddDegreeMap = null;
	    }


	    ////////////////////////////////////////////
	    // process matches
	    {
		Integer[] sortedBuckets = bucketMap.keySet().toArray(new Integer[0]);
		// go from highest to lowest degree
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
		    bucketEntry.addedEdges = EulerGraphGenBin.matchBucket(bucketId, bucketMap, stragglerEntryList);

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
		    newPendingMatchCount = EulerGraphGenBin.countStragglers(stragglerEntryList);
		    int newStragglersMatchCount = oldPendingMatchCount + newNoMatchCount - newPendingMatchCount;
		    newMatchSum += newStragglersMatchCount;
		    oldPendingMatchCount = newPendingMatchCount;

		    System.out.printf("matchBucket,%d,%d,%d,%d,%d,%d,%d%n", bucketId, bucketEntry.vertexCount,
			    (bucketEntry.vertexCount - newNoMatchCount), newNoMatchCount, newStragglersMatchCount,
			    newPendingMatchCount, (intervalEndTime - intervalStartTime));
		}
		long endTime = System.currentTimeMillis();
		System.out.printf("TotalStragglerNoMatch=%d,TotalStragglerMatch=%d,TotalMatchComputeTime=%d%n",
			noMatchSum, newMatchSum, (endTime - startTime));
	    }

	    ////////////////////////////////////////////////////////////////////
	    // Write to iterator of primitive array writable of long[]
	    // see GraphUtils.saveGraphInlineAdj
	    // see GraphUtils.writeConcatInterleaveAdjacencyArray
	    startTime = System.currentTimeMillis();
	    List<LongArraySerializable> adjListIter = new ArrayList<>(vcount);
	    {
		int totvc = 0, totec = 0;
		// iterate thru degree buckets
		for (Entry<Integer, BucketEntry> bmEntry : bucketMap.entrySet()) {
		    int degree = bmEntry.getKey();
		    BucketEntry bucket = bmEntry.getValue();
		    long[] vn = bucket.inlineAdjList;
		    long[] addedN = bucket.addedEdges;
		    int i = 0, vc = 0;
		    while (i < vn.length) {
			// write source and one more than the degree number of sink
			long[] updatedAdjArr = new long[degree + 2];
			// write source and degree number of sinks
			System.arraycopy(vn, i, updatedAdjArr, 0, degree + 1);
			// write the newly added sink vertex
			updatedAdjArr[degree + 1] = addedN[vc];
			adjListIter.add(new LongArraySerializable(updatedAdjArr));
			// move source list to next vertex
			i += (degree + 1);
			totec += (degree + 1);
			// increment to next vertex count
			vc++;
			totvc++;
		    }

		    // Done with values for this degree. GC the value of bmEntry.
		    bmEntry.setValue(null);
		}
		System.out.printf("saveToLongArray,TotalVertices,%d,TotalEdges,%d,TimeTakenMS=%d%n", totvc, totec,
			(System.currentTimeMillis() - startTime));
	    }
	    return adjListIter.iterator();
	}

    }

    public static class LongArraySerializable implements Serializable {

	private static final long serialVersionUID = -7926337462988926088L;
	private long[] item;


	public LongArraySerializable() {
	    item = new long[0];
	}


	public LongArraySerializable(long[] arr) {
	    item = arr;
	}


	public long[] get() {
	    return item;
	}


	private void writeObject(ObjectOutputStream out) throws IOException {
	    out.writeInt(item.length);
	    for (int i = 0; i < item.length; i++)
		out.writeLong(item[i]);
	    // System.out.println("LongArraySerializable.writeObject"); // tempdel: not printed!
	}


	private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
	    int len = in.readInt();
	    item = new long[len];
	    for (int i = 0; i < item.length; i++)
		item[i] = in.readLong();
	    // System.out.println("LongArraySerializable.readObject"); // tempdel: not printed!
	}


	private void readObjectNoData() throws ObjectStreamException {
	    item = new long[0];
	    // System.out.println("LongArraySerializable.readObjectNoData"); // tempdel: not printed!
	}
    }
}
