package in.dreamlab.Euler;

import java.io.BufferedInputStream;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;

import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.Map.Entry;
import java.util.function.Predicate;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.HashPartitioner;
import org.apache.spark.SerializableWritable;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.input.PortableDataStream;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gnu.trove.iterator.TLongObjectIterator;
import gnu.trove.list.TLongList;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.list.linked.TLongLinkedList;
import gnu.trove.map.TLongObjectMap;
import gnu.trove.map.hash.TByteObjectHashMap;
import gnu.trove.map.hash.TLongObjectHashMap;
import in.dream_lab.graph.euler.GraphUtils;
import in.dream_lab.graph.euler.GraphUtils.PartitionedAdjacencyMapEntry;
import in.dream_lab.graph.euler.RMatToEulerGraph.LongArraySerializable;
import scala.Tuple2;


/**
 * 
 * @author siddharthj
 * @author simmhan
 *
 */
public class SubgraphEulerTourPhase1 {
    static Logger LOGGER = LoggerFactory.getLogger(SubgraphEulerTourPhase1.class);

    static final boolean DUMP_PATHS = false;

    static final boolean DEEP_ASSERT = false;

    private static final boolean DUMP_AdjListObject = false;

    private static final boolean DUMP_SourcePartitionPhase1 = false;

    private static final boolean DUMP_Phase1FinalObject = false;

    private static final boolean FINALIZE_ROOT_AT_DRIVER = false;

    // FIXME: later figure out how to set this dynamically. For now, setting this to 1 disables some of the assert
    // failures from the last level
    // NOTE: We're updating a static variable. Race condition possible if multiple levels active at the same time in
    // this JVM.
    public static boolean IS_LAST_LEVEL_ASSERT = true;


    static String assertPrintf(String format, Object... args) {
        StringBuilder sb = new StringBuilder();
        try (Formatter formatter = new Formatter(sb)) {
            formatter.format(format, args);
            return sb.toString();
        }
    }


    /////////////////////////////////////////////////////////////////////////////////////////
    // Transform from Binary File to Adj Object
    /////////////////////////////////////////////////////////////////////////////////////////
    @SuppressWarnings("serial")
    public static final class ParseBinaryGraph
    implements PairFlatMapFunction<Tuple2<String, PortableDataStream>, Byte, AdjListObject> {
        // FIXME: if we are passing 1 binary file per partition, we can change this to mapToPair rather than flat map to
        // pair, and use the partition ID in the file name
        @Override
        public final Iterator<Tuple2<Byte, AdjListObject>> call(Tuple2<String, PortableDataStream> binfile) {
            long startTime = System.currentTimeMillis();
            System.out.printf("ParseBinaryGraph.call Start,TimestampMS,%d%n", startTime);

            // ignore the file name in _1
            PortableDataStream stm = binfile._2; // read the input stream in _2

            return new BinaryGraphIterator(stm);
            // return adjObjList.iterator();
        }


        /**
         * This gives an iterator view over the inpute data stream that is converted to adjobj stream.
         * Two advantages:
         * 1) it avoids keeping all the data in a list in memory and returning an iter over it. Prevents OOMEx.
         * 2) Allows streaming execution of the next stage before this stage completes.
         * 
         * @author simmhan
         *
         */
        static final class BinaryGraphIterator implements Iterator<Tuple2<Byte, AdjListObject>> {
            long startTime = System.currentTimeMillis();
            BufferedInputStream is; // input stream with the binary adj list data
            DataInputStream in;
            Tuple2<Byte, AdjListObject> cachedItem; // pre-read the next item to support hasNext op
            PartitionedAdjacencyMapEntry nextEntry; // avoid creating new obj for each call to next


            public BinaryGraphIterator(PortableDataStream stm) {
                System.out.printf("ParseBinaryGraph.call creating Iterator,TimestampMS,%d,StmPath,%s%n",
                        System.currentTimeMillis(), stm.getPath());
                is = new BufferedInputStream(stm.open());
                in = new DataInputStream(is);
                nextEntry = new PartitionedAdjacencyMapEntry();
                cachedItem = getNext();
                System.out.printf("ParseBinaryGraph.call creating next,TimestampMS,%d,CachedItem,%s%n",
                        System.currentTimeMillis(), cachedItem);
            }


            @Override
            public boolean hasNext() {
                if (cachedItem == null) {
                    long endTime = System.currentTimeMillis();
                    System.out.printf("ParseBinaryGraph.call Done Iterator,TotalTimeMS,%d,TimestampMS,%d%n",
                            (endTime - startTime), endTime);
                    if (is != null) try {
                        in.close();
                        is.close();
                        is = null;
                        in = null;
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                    return false;
                } else return true;
            }


            @Override
            public Tuple2<Byte, AdjListObject> next() {

                Tuple2<Byte, AdjListObject> prevItem = cachedItem;
                cachedItem = getNext();
                // // tempdel
                // System.out.printf("ParseBinaryGraph:next got new item,TimestampMS,%d%n", System.currentTimeMillis());
                return prevItem;
            }


            Tuple2<Byte, AdjListObject> getNext() {
                try {
                    if (GraphUtils.readPartitionedAdjacencyMap(in, nextEntry)) {
                        AdjListObject adjList = new AdjListObject(nextEntry);
                        // return an Pair RDD where key is part ID and value if AdjList object for a vertex
                        Tuple2<Byte, AdjListObject> tmp = new Tuple2<Byte, AdjListObject>(nextEntry.partitionID,
                                adjList);
                        nextEntry.reset();
                        return tmp;
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
                return null;
            }
        }
    }


    @SuppressWarnings("serial")
    public static final class ParseTextGraph implements PairFunction<String, Byte, AdjListObject> {
        @Override
        public Tuple2<Byte, AdjListObject> call(String s) {
            String[] records = s.split("@");
            byte partId = Integer.valueOf(records[0]).byteValue();
            String adjListStr = records[1];
            AdjListObject adjList = new AdjListObject(adjListStr);
            // return an Pair RDD where key is part ID and value if AdjList object for a vertex
            return new Tuple2<Byte, AdjListObject>(partId, adjList);
        }
    }

    /////////////////////////////////////////////////////////////////////////////////////////
    // Transform from AdjObj to SourcePartition
    // FIXME: Merge with transform below?
    /////////////////////////////////////////////////////////////////////////////////////////

    /**
     * Converts and AdjListObject for a single partition to a SourcePartition object for phase 1 input for that
     * partiton
     * 
     * @author siddharthj
     * @author simmhan
     *
     */
    public static final class AdjObjToSourcePartition
    implements PairFlatMapFunction<Iterator<Tuple2<Byte, AdjListObject>>, Byte, SourcePartitionPhase1> {

        @Override
        public final Iterator<Tuple2<Byte, SourcePartitionPhase1>> call(
                Iterator<Tuple2<Byte, AdjListObject>> partAdjListIter) throws Exception {
            long startTime = System.currentTimeMillis();
            System.out.printf("AdjObjToSourcePartition.call Start,TimestampMS,%d%n", startTime); // FIXME: Get PartID
            // here

            ////////////////////////////
            // Build source partition from Adj Object
            SourcePartitionPhase1 partPhase1Obj = new SourcePartitionPhase1();

            // since all vertices (values) in this spark partition belong to the same graph partition,
            // populate the details for the graph partition object just once from the first vertex
            byte initPartId = -1;
            if (partAdjListIter.hasNext()) {
                Tuple2<Byte, AdjListObject> partAdjList = partAdjListIter.next(); // FIXME: Get PartID here
                initPartId = partAdjList._1();
                System.out.printf("AdjObjToSourcePartition.call Iterator Start,PartID,%d,TimestampMS,%d%n", initPartId,
                        System.currentTimeMillis());
                partPhase1Obj.setPartitionId(initPartId);
                AdjListObject adjList = partAdjList._2();
                partPhase1Obj.addAdjacencyList(adjList.getVid(), adjList.getLocalAdjList(), adjList.getRemoteAdjMap());
                // pX.setMetaEdgeMap();
            }
            // add the rest of the adj list items in the spark partition to the graph partition
            while (partAdjListIter.hasNext()) {
                Tuple2<Byte, AdjListObject> partAdjList = partAdjListIter.next();
                // mapParition must guarantee that all items are from same partition
                assert initPartId == partAdjList._1() : "Found a different graph partition in this spark partition!";
                AdjListObject adjList = partAdjList._2();
                partPhase1Obj.addAdjacencyList(adjList.getVid(), adjList.getLocalAdjList(), adjList.getRemoteAdjMap());
            }

            // indicate to part object that all vertex adj lists have been added
            partPhase1Obj.finalizeAdjacencyList();

            // build adjacency list with edge weights for this partition which is a metavertex in the
            // metagraph
            // partPhase1Obj.resetMetaAdjList();
            LOGGER.info("SOURCE_PARTITION_CREATED,PART_ID: " + partPhase1Obj.getPartitionId());

            long endTime = System.currentTimeMillis();
            System.out.printf("AdjObjToSourcePartition.call Done,PartID,%d,TotalTimeMS,%d,TimestampMS,%d%n",
                    partPhase1Obj.getPartitionId(), (endTime - startTime), endTime);
            // return single item for this partition key
            return Collections
                    .singletonList(
                            new Tuple2<Byte, SourcePartitionPhase1>(partPhase1Obj.getPartitionId(), partPhase1Obj))
                    .iterator();
        }
    }


    /////////////////////////////////////////////////////////////////////////////////////////
    // Transform from source partition to phase 1
    /////////////////////////////////////////////////////////////////////////////////////////

    /**
     * 
     * @author siddharthj
     * @author simmhan
     *
     */
    @SuppressWarnings("serial")
    public static final class Phase1Tour
    implements PairFunction<Tuple2<Byte, SourcePartitionPhase1>, Byte, Phase1FinalObject> {
        @Override
        public final Tuple2<Byte, Phase1FinalObject> call(Tuple2<Byte, SourcePartitionPhase1> t) throws IOException {

            long startTime = System.currentTimeMillis();
            System.out.printf("Phase1Tour.call Start,PartID,%d,TimestampMS,%d%n", t._1(), startTime);
            ////////////////////////////
            // PROCESS SOURCE PARITION
            byte partitionId = t._1();
            SourcePartitionPhase1 partition = t._2(); // Object of the partition
            // YS: partition is accurate here. matches with input part binary file.
            // YS: bugs are present going fwd! FIXME
            TuplePair<Byte, Phase1FinalObject> output = doPhase1Stuff(partitionId, partition);

            Tuple2<Byte, Phase1FinalObject> tuple2 = new Tuple2<Byte, Phase1FinalObject>(output._1(), output._2());

            //SJ Removed Logger statement
            /*LOGGER.info("Phase1Tour.tuple2,partitionId," + tuple2._1() + ",partPathMapCount,"
		    + tuple2._2().getPathMap().size() + ",multiPathVMapCount,"
		    + tuple2._2().getPivotVertexMap().size());*/

            long endTime = System.currentTimeMillis();
            System.out.printf("Phase1Tour.call Done,PartID,%d,TotalTimeMS,%d,TimestampMS,%d%n", output._1(),
                    (endTime - startTime), endTime);

            return tuple2;
        }


        /**
         * Does Phase 1 path detection on a given source partition for a given spark partition.
         * Returns a Phase 1 output object for this partition.
         * 
         * @param partitionId
         * @param partition
         * @return
         * @throws IOException
         */
        public static final TuplePair<Byte, Phase1FinalObject> doPhase1Stuff(byte partitionId,
                SourcePartitionPhase1 partition) throws IOException {


            long startTime = System.currentTimeMillis();
            System.out.printf("doPhase1Stuff.call Start,PartID,%d,TimeStampMS,%d%n", partitionId, startTime);

            ////////////////////////////
            // Get input variables
            LOGGER.info("PHASE1STUFF,PARTID," + partitionId + ",LIST_SIZE_ODBV," + partition.getOBList().size()
                    + ",LIST_SIZE_EDBV," + partition.getEBList().size() + ",LIST_SIZE_INTERNAL,"
                    + partition.getIntList().size());

            // Map<Byte, Long> metaEdgeMap = new HashMap<>(partition.getMetaEdgeMap());
            // // Log meta edge info if required
            // for (Byte remPartID : metaEdgeMap.keySet()) {
            // LOGGER.info("META GRAPH INFO: " + "PARTID: " + partitionId + " REMOTE_PARTID: " + remPartID
            // + " METAEDGEWT: " + metaEdgeMap.get(remPartID));
            // }

            // local copy of the partition's vertex adj lists
            TLongObjectHashMap<VertexValuesPhase1> vertexMap = partition.getVertexMap();


            ////////////////////////////
            // Init output variables
            // populate per partition
            TLongObjectHashMap<PathSinkSource> partPathMap = new TLongObjectHashMap<PathSinkSource>();
            TLongObjectHashMap<TLongLinkedList> pivotVertexMap = new TLongObjectHashMap<TLongLinkedList>();

            int pathCount = -1;
            TLongObjectMap<List<TLongList>> ALL_PATHS = null;
            if (DUMP_PATHS) {
                ALL_PATHS = new TLongObjectHashMap<>();
            }


            ////////////////////////////
            // Processing OB Paths--
            // tempdel
            LOGGER.info("PartID," + partitionId + "," + System.currentTimeMillis() + ",StartingOB");
            {
                TLongArrayList obList = partition.getOBList();
                // assert obList.contains(4616437L) : "OB list does not contain 4616437L"; // tempdel
                pathCount = findPathsFromSourceList(obList, vertexMap, partPathMap, partitionId,
                        VertexValuesPhase1.TYPE_OB, ALL_PATHS);

                // assert: all OB's processed
                assert obList.isEmpty() : "There are OB vertices left to process!";
                obList.clear(0); // do OB cleanup

                // assert: all OB vertices have some path ID assigned to them at this point. This guarantees that they
                // have all been visited at least on one path.
                assert new Predicate<TLongObjectHashMap<VertexValuesPhase1>>() {
                    @Override
                    public boolean test(TLongObjectHashMap<VertexValuesPhase1> vm) {
                        TLongObjectIterator<VertexValuesPhase1> vmIter = vm.iterator();
                        while (vmIter.hasNext()) {
                            vmIter.advance();
                            VertexValuesPhase1 entryVal = vmIter.value();
                            if (entryVal.getType() == VertexValuesPhase1.TYPE_OB && entryVal.getPathId().size() == 0) {
                                // System.out.printf("Found OB vertices in vertex map with path IDs not set for
                                // %d=[%s]", entry.getKey(), entry.getValue()); // tempdel
                                return false;
                            }
                        }
                        return true;
                    }
                }.test(vertexMap) : "Found OB vertices in vertex map with path IDs not set";

                // Do all local neighbors of OB vertices have size 0? No, not necessary.
                // END OB ASSERTIONS
            }
            LOGGER.info("PartID," + partitionId + "," + System.currentTimeMillis() + ",DoneOB,PATH_COUNT," + pathCount);
            /////////////////////////////


            /////////////////////////////
            LOGGER.info("PartID," + partitionId + "," + System.currentTimeMillis() + ",StartingEB");
            {
                // Processing EB Paths--
                TLongArrayList ebList = partition.getEBList();
                pathCount = findPathsFromSourceList(ebList, vertexMap, partPathMap, partitionId,
                        VertexValuesPhase1.TYPE_EB, ALL_PATHS);


                // assert: all EB's processed
                assert ebList.isEmpty() : "There are EB vertices left to process!";
                ebList.clear(0); // do EB cleanup

                // assert: all EB vertices have some path ID assigned to them at this point. This guarantees that they
                // have all been visited at least on one path.
                assert new Predicate<TLongObjectHashMap<VertexValuesPhase1>>() {
                    @Override
                    public boolean test(TLongObjectHashMap<VertexValuesPhase1> vm) {
                        TLongObjectIterator<VertexValuesPhase1> vmIter = vm.iterator();
                        while (vmIter.hasNext()) {
                            vmIter.advance();
                            VertexValuesPhase1 entryVal = vmIter.value();
                            if (entryVal.getType() == VertexValuesPhase1.TYPE_EB && entryVal.getPathId().size() == 0)
                                return false;
                        }
                        return true;
                    }
                }.test(vertexMap) : "Found EB vertices in vertex map with path IDs not set";

                // assert: all local neighbors of EB vertices have size 0
                assert new Predicate<TLongObjectHashMap<VertexValuesPhase1>>() {
                    @Override
                    public boolean test(TLongObjectHashMap<VertexValuesPhase1> vm) {
                        TLongObjectIterator<VertexValuesPhase1> vmIter = vm.iterator();
                        while (vmIter.hasNext()) {
                            vmIter.advance();
                            VertexValuesPhase1 entryVal = vmIter.value();
                            if (entryVal.getType() == VertexValuesPhase1.TYPE_EB
                                    && entryVal.getUnvisitedLocalNeighbourList().size() != 0)
                                return false;
                        }
                        return true;
                    }
                }.test(vertexMap) : "Found EB vertices with UnvisitedLocalNeighbourList that is not empty";
                // END EB ASSERTIONS
            }
            LOGGER.info("PartID," + partitionId + "," + System.currentTimeMillis() + ",DoneEB,PATH_COUNT," + pathCount);
            /////////////////////////////


            /////////////////////////////
            LOGGER.info("PartID," + partitionId + "," + System.currentTimeMillis() + ",StartingIV");
            {
                // Processing Internal Paths--
                TLongArrayList intlList = partition.getIntList();
                pathCount = findPathsFromSourceList(intlList, vertexMap, partPathMap, partitionId,
                        VertexValuesPhase1.TYPE_INTERNAL, ALL_PATHS);

                // assert: all internal vertices are processed
                assert intlList.isEmpty() : "There are internal vertices left to process!";
                intlList.clear(0); // do cleanup

                // assert: all internal vertices have some path ID assigned to them at this point. This guarantees that
                // they have all been assigned at a pivot path.
                assert new Predicate<TLongObjectHashMap<VertexValuesPhase1>>() {
                    @Override
                    public boolean test(TLongObjectHashMap<VertexValuesPhase1> vm) {
                        TLongObjectIterator<VertexValuesPhase1> vmIter = vm.iterator();
                        while (vmIter.hasNext()) {
                            vmIter.advance();
                            VertexValuesPhase1 entryVal = vmIter.value();

                            if (entryVal.getType() == VertexValuesPhase1.TYPE_INTERNAL
                                    && entryVal.getPathId().size() == 0 && !IS_LAST_LEVEL_ASSERT)
                                return false;
                        }
                        return true;
                    }
                }.test(vertexMap) : "Found Internal vertices in vertex map with path IDs not set";

                // assert: all local neighbors of internal vertices have size 0
                assert new Predicate<TLongObjectHashMap<VertexValuesPhase1>>() {
                    @Override
                    public boolean test(TLongObjectHashMap<VertexValuesPhase1> vm) {
                        TLongObjectIterator<VertexValuesPhase1> vmIter = vm.iterator();
                        while (vmIter.hasNext()) {
                            vmIter.advance();
                            VertexValuesPhase1 entryVal = vmIter.value();
                            if (entryVal.getType() == VertexValuesPhase1.TYPE_INTERNAL
                                    && entryVal.getUnvisitedLocalNeighbourList().size() != 0)
                                return false;
                        }
                        return true;
                    }
                }.test(vertexMap) : "Found Internal vertices with UnvisitedLocalNeighbourList that is not empty";
                // END Internal ASSERTIONS
            }
            LOGGER.info("PartID," + partitionId + "," + System.currentTimeMillis() + ",DoneIV,PATH_COUNT," + pathCount);
            /////////////////////////////

            // dump path to disk/RDD
            // listOfPaths.add(new Tuple2<Long, List<Long>>(t._1(), path));
            if (DUMP_PATHS) {
                System.out.println(System.currentTimeMillis() + ",Starting Dump");
                String dumpFile = "PATH_DUMP-" + System.currentTimeMillis() + ".csv";
                try (BufferedWriter bw = Files.newBufferedWriter(Paths.get(dumpFile), Charset.forName("UTF-8"),
                        StandardOpenOption.CREATE)) {
                    long[] pathIds = ALL_PATHS.keys();
                    for (int i = 0; i < pathIds.length; i++) {
                        List<TLongList> paths = ALL_PATHS.get(pathIds[i]);
                        StringBuffer sb = new StringBuffer().append(pathIds[i]);
                        for (TLongList path : paths) {
                            assert pathIds[i] == path
                                    .get(0) : "The first entry in the path list was NOT equal to the path ID";
                                    for (int j = 1; j < path.size(); j++) {
                                        sb.append(",").append(path.get(j));
                                    }
                        }
                        sb.append('\n');
                        bw.write(sb.toString());
                    }
                }
                System.out.println(System.currentTimeMillis() + ",Done Dump," + Paths.get(dumpFile).toAbsolutePath());
            }


            // Add all OB/EB vertices, and internal vertices with more than one path, to multipath map.
            TLongObjectIterator<VertexValuesPhase1> vmIter = vertexMap.iterator();
            while (vmIter.hasNext()) {
                vmIter.advance();
                VertexValuesPhase1 entryVal = vmIter.value();
                long entryKey = vmIter.key();

                if (entryVal.getType() == VertexValuesPhase1.TYPE_OB || entryVal.getType() == VertexValuesPhase1.TYPE_EB
                        || (entryVal.getType() == VertexValuesPhase1.TYPE_INTERNAL
                        && entryVal.getPathId().size() > 1)) {
                    pivotVertexMap.put(entryKey, entryVal.getPathId());
                }
            }


            // generate output object
            Phase1FinalObject phase1Output = new Phase1FinalObject(partitionId, partPathMap, pivotVertexMap);
            LOGGER.info("Phase1FinalObject,partitionId," + partitionId + ",partPathMapCount," + partPathMap.size()
            + ",multiPathVMapCount," + pivotVertexMap.size());

            if (DUMP_PATHS) {
                for (Long key : partPathMap.keys()) {
                    LOGGER.info("PATHMAP_ID," + key + ",SRC_VID," + partPathMap.get(key).getSource() + ",SINK_VID,"
                            + partPathMap.get(key).getSink());
                }

                for (Long key : pivotVertexMap.keys()) {
                    LOGGER.info("PIVOT_VID," + key + ",PATH_COUNT," + pivotVertexMap.get(key).size());
                }
            }

            long endTime = System.currentTimeMillis();
            System.out.printf("Phase1TStuff.call Done,PartID,%d,TotalTimeMS,%d,TimestampMS,%d%n", partitionId,
                    (endTime - startTime), endTime);

            // FIXME: We are resetting the pivot map since we dont use it
            // TODO: It needs to be written to HDFS for later use in phase 3
            phase1Output.resetPivotVertexMap();

            return new TuplePair<Byte, Phase1FinalObject>(partitionId, phase1Output);
        }


        /**
         * Given a set of source vertex IDs, this finds all paths of length >= 1 edge that start at each source vertex.
         * Paths which start at an OB or EB source are added to the partPathMap; those that start at an internal vertex
         * are not.
         * On completion, the source vertex list is empty.
         * This returns the number of paths found.
         * 
         * @param srcList
         * @param vertexMap
         * @param partPathMap
         * @param partitionId
         */
        public static final int findPathsFromSourceList(TLongArrayList srcList,
                TLongObjectHashMap<VertexValuesPhase1> vertexMap, TLongObjectMap<PathSinkSource> partPathMap,
                byte partitionId, byte sourceVertexType, TLongObjectMap<List<TLongList>> ALL_PATHS) {

            Long availableSrcVid;
            int pathCount = 0, pathVCount = 0;
            // NOTE: We create a singleton path with 1 vertex for an EB even if it does not have local
            // neighbors. So we should iterate thru all EBs irrespective of having unvisited locals.
            while ((availableSrcVid = tryRemoveNextSrcVertex(srcList, vertexMap, sourceVertexType)) != null) {

                // get the object for the source vertex
                long srcVertexID = availableSrcVid;
                VertexValuesPhase1 srcVertexObj = vertexMap.get(srcVertexID);
                // LOGGER.info("SOURCE_VID," + srcVertexID + ",TYPE," + sourceVertexType);


                // initialize the path object and add path ID to source vertex
                // First item in the path is the path ID. Subsequent items are the vertex IDs, starting from source and
                // ending in sink. Min of 3 entries MUST be present.
                TLongArrayList path = new TLongArrayList();

                // Internal vertices will have their paths merged with an existing pivot path. Assign a temp path ID for
                // now.
                long pathID = sourceVertexType == VertexValuesPhase1.TYPE_INTERNAL ? Long.MIN_VALUE
                        : createPathId(partitionId, srcVertexID);

                path.add(pathID); // Adding pathID to path list as well! Keep note of this!
                path.add(srcVertexID);

                // initialize map of source and sink remote vertices.
                // this will be carried over to the next level after merging.
                // Add path ID to source vertex's path list.
                // These are relevant only for OB and EB
                PathSinkSource srcSinkRemoteVList = null;

                if (sourceVertexType == VertexValuesPhase1.TYPE_OB || sourceVertexType == VertexValuesPhase1.TYPE_EB) {
                    srcSinkRemoteVList = new PathSinkSource();
                    PathSinkSource tmpPath = partPathMap.put(pathID, srcSinkRemoteVList);
                    assert tmpPath == null : "found previous entry in partPathMap with same PathID as one being added. PathID="
                            + pathID + "; SrcVID=" + srcVertexID;
                    srcSinkRemoteVList.setSource(srcVertexID, srcVertexObj.getRemoteNeighbourMap());
                    srcVertexObj.appendPathId(pathID);
                }

                // Start the path traversal and return the sink ID.
                // This will have added intermediate vertices to path, and added the pathID to their set of
                // intersecting paths
                long sinkVertexID = addToLocalPath(vertexMap, srcVertexObj, srcVertexID, path, pathID,
                        sourceVertexType);

                if (ALL_PATHS != null) {
                    // trim path list to conserve space. Anyway we are printing it and discarding it?
                    // Only trim if we're retaining the path. Else it will be GC at the end of this method
                    path.trimToSize();
                }

                // add pivot path ID to internal vertices
                if (sourceVertexType == VertexValuesPhase1.TYPE_INTERNAL) {
                    Long pivotVertex = addPivotPathToInternalPathVertices(path, vertexMap);
                    // This assert is not valid when we have only 1 partition. Is there a way to check this?
                    assert IS_LAST_LEVEL_ASSERT
                    || pivotVertex != null : "Could not merge the internal path with another OB/EB pivot path!";
                    pathID = path.get(0);

                    // TODO: merge local path with pivot path
                    // TODO: Embed the local path list into the EB or OB path (after rotating it such that the pivot
                    // vertex is the start of this local cycle)
                    if (ALL_PATHS != null) { // if DUMP_PATHS, add internal path to path list
                        // NOTE: Internal path will have a existing entry for its path ID that matches an OB/EB path
                        List<TLongList> pivotPathList = ALL_PATHS.get(pathID);
                        assert pivotPathList != null : "No entry exists in allPaths for Pivot Path for internal path";
                        pivotPathList.add(path);
                    }
                } else { // OB or EB path
                    if (ALL_PATHS != null) { // if DUMP_PATHS, add OB/EB path to path list
                        // NOTE: OB/EB path will not have an existing entry in paths. So create one.
                        List<TLongList> pivotPathList = ALL_PATHS.get(pathID);
                        assert pivotPathList == null : "Entry exists in allPaths for Pivot Path for OB/EB path";
                        pivotPathList = new ArrayList<TLongList>();
                        pivotPathList.add(path);
                        ALL_PATHS.put(pathID, pivotPathList);
                    }
                }

                // add sink's remote vertices
                // Relevant only for OB
                if (sourceVertexType == VertexValuesPhase1.TYPE_OB) {
                    VertexValuesPhase1 sinkVertexObj = vertexMap.get(sinkVertexID);
                    srcSinkRemoteVList.setSink(sinkVertexID, sinkVertexObj.getRemoteNeighbourMap());
                    assert sinkVertexObj.getType() == VertexValuesPhase1.TYPE_OB : "source vertex " + srcVertexID
                            + " was OB but sink vertex " + sinkVertexID + " has type " + sinkVertexObj.getType()
                            + assertPrintf(";sourceVertexType=%d,srcVertexID=%d,sinkVertexID=%d,pathID=%d,pathSize=%d",
                                    sourceVertexType, srcVertexID, sinkVertexID, pathID, path.size());
                } else {
                    assert srcSinkRemoteVList == null || // this can be null in the last level with only 1 partition
                            srcSinkRemoteVList
                            .hasSink() == false : "SourceSink Pair's hasSink was true even when it was not an OB. "
                                    + assertPrintf(
                                            "sourceVertexType=%d,srcVertexID=%d,sinkVertexID=%d,pathID=%d,pathSize=%d",
                                            sourceVertexType, srcVertexID, sinkVertexID, pathID, path.size());
                }

                // increment path count
                pathCount++;
                pathVCount += (path.size() - 1);

                // assert
                assert sourceVertexType == VertexValuesPhase1.TYPE_OB
                        || sinkVertexID == srcVertexID : "Source and sink vertex IDs were different for EB/Internal vertex path. They MUST form a cycle!"
                                + assertPrintf(
                                        "sourceVertexType=%d,srcVertexID=%d,sinkVertexID=%d,pathID=%d,pathSize=%d",
                                        sourceVertexType, srcVertexID, sinkVertexID, pathID, path.size());

                // for OB, is it REQUIRED that source != sink? i.e., no singletons allowed
                assert sourceVertexType != VertexValuesPhase1.TYPE_OB
                        || sinkVertexID != srcVertexID : "Source and sink VIDs were the same for an OB path. VID="
                                + assertPrintf(
                                        "sourceVertexType=%d,srcVertexID=%d,sinkVertexID=%d,pathID=%d,pathSize=%d",
                                        sourceVertexType, srcVertexID, sinkVertexID, pathID, path.size());

                assert sourceVertexType == VertexValuesPhase1.TYPE_EB || path
                        .size() >= 3 : "Paths should have a min of 3 path entries for OB and Internal: path id, source vid, sink vid. Singletons allowed for EB."
                                + assertPrintf(
                                        "sourceVertexType=%d,srcVertexID=%d,sinkVertexID=%d,pathID=%d,pathSize=%d",
                                        sourceVertexType, srcVertexID, sinkVertexID, pathID, path.size());

                        assert sourceVertexType != VertexValuesPhase1.TYPE_EB || path
                                .size() >= 2 : "Paths should have a min of 2 path entries for EB: path id, source vid, and optionally sink vid if it is not a singleton."
                                        + assertPrintf(
                                                "sourceVertexType=%d,srcVertexID=%d,sinkVertexID=%d,pathID=%d,pathSize=%d",
                                                sourceVertexType, srcVertexID, sinkVertexID, pathID, path.size());

                                assert sourceVertexType != VertexValuesPhase1.TYPE_EB
                                        || sinkVertexID == srcVertexID : "EV path has different source and sink VIDs: path id, source vid, and optionally sink vid if it is not a singleton."
                                                + assertPrintf(
                                                        "sourceVertexType=%d,srcVertexID=%d,sinkVertexID=%d,pathID=%d,pathSize=%d",
                                                        sourceVertexType, srcVertexID, sinkVertexID, pathID, path.size());

                                assert vertexMap.get(sinkVertexID).getUnvisitedLocalNeighbourList()
                                .isEmpty() : "Sink vertex of path has some local unvisited neighbour" + assertPrintf(
                                        "sourceVertexType=%d,srcVertexID=%d,sinkVertexID=%d,pathID=%d,pathSize=%d",
                                        sourceVertexType, srcVertexID, sinkVertexID, pathID, path.size());


                                // TODO: add path list to output file for verification
                                // key is part id, value is path array
                                // listOfPaths.add(new Tuple2<Long, List<Long>>(t._1(), path));

                                // LOGGER.info("PART_ID: " + t._1() + " ,PATH_ID: " + pathID + ",PATH_ODBV: " + path);
                                // LOGGER.info("PART_ID," + partitionId + ",PATH_ID," + pathID + ",PATH_VLENGTH," + (path.size() - 1) +
                                // ",PATH_TYPE," + sourceVertexType);
                                // multiPathVMap.put(vertex, path);
            }

            LOGGER.info("PART_ID," + partitionId + ",PATH_COUNT," + pathCount + ",PATH_VCOUNT," + pathVCount
                    + ",SRC_VTYPE," + sourceVertexType);
            return pathCount;
        }


        /**
         * This removes and returns (1) the next source vertex from the candidate list that has unvisited out edges, for
         * a non-EB source vertex list, and (2) return the last item in the list, if it is an EB source vertex vertex
         * list.
         * 
         * @param srcVertexList
         *            list of candidate source vertices to start a path from
         * @param vertexMap
         * @return NULL if no candidate source vertices have unvisited out edges, or the EB source vertex list is empty.
         *         Else source vertex with at least one unvisited neighbor, or an EB source vertex.
         */
        public static final Long tryRemoveNextSrcVertex(TLongArrayList srcVertexList,
                TLongObjectHashMap<VertexValuesPhase1> vertexMap, byte sourceVertexType) {

            // if source vertex List is empty, return NULL
            int srcListSize = srcVertexList.size();
            while (srcListSize > 0) {
                // Remove next candidate source vertex from list.
                // We start traversing and removing from the end since it is more efficient to remove (trim) the
                // trailing elements in an arraylist.
                Long srcVertexID = srcVertexList.removeAt(srcListSize - 1);
                srcListSize--;

                // return this source vertex if EB
                if (sourceVertexType == VertexValuesPhase1.TYPE_EB) return srcVertexID;

                // Else, for OB or Internal, Check if it has unvisited neighbors.
                // NOTE: Unvisited neighbors are present if getUnvisitedLocalNeighbourList is not empty AND there is at
                // least one value that is not MAX_VALUE
                VertexValuesPhase1 srcVertexObj = vertexMap.get(srcVertexID);
                TLongArrayList srcLocalUnvisited = srcVertexObj.getUnvisitedLocalNeighbourList();
                int srcLocalUnvisitedSize = srcLocalUnvisited.size();
                for (int i = 0; i < srcLocalUnvisitedSize; i++) {
                    long nextVidInPath = srcLocalUnvisited.get(i);
                    if (nextVidInPath != Long.MAX_VALUE) // this source vertex has unvisited out edges
                        return srcVertexID;
                }

                // This is a valid case. In fact, half of all OB's would have been consumed as the path sink for another
                // OB
                // TODO:ASSERT: This OB is already a sink in another OB path. Can we verify?
                // assert sourceVertexType != VertexValuesPhase1.TYPE_OB : "OB src vertex was in srcVertexList but did
                // not have any unvisited neighbors. OB VID="
                // + srcVertexID + "; Initial srcLocalUnvisitedSize=" + srcLocalUnvisitedSize;


                // if none of the neighbors of the source vertex are not visited, delete the unvisited local list for
                // this candidate source vertex and test next source
                srcLocalUnvisited.clear(0);

                // TODO: This assert will fail in last level with only internal vertices. Comment out for now. Restore
                // later...
                assert IS_LAST_LEVEL_ASSERT || srcVertexObj.getPathId().size() > 0 : assertPrintf(
                        "Dropping source vertex %d with type %d since it has no unvisited neighbors. But its path ID list is empty! Is this a disconnected component?",
                        srcVertexID, sourceVertexType, srcVertexObj.getPathId());
            }

            // no more source vertices are remaining in list
            return null;
        }


        /**
         * Find a local path starting from currentVertexID and add traversed vertices to path object. returns the sink
         * vertex id in path.
         * This will append to the path list, unless the source vertex is an INTERNAL vertex.
         * This will update and remove contents of localNeighbourList of visited vertices in vertexTable.
         * For singleton EB (valid case), the path length will be 1 vertex.
         * 
         * 
         * @param vertexTable
         * @param currentVertexObject
         * @param currentVertexID
         * @param path
         * @param pathID
         * @return
         */
        public static final long addToLocalPath(TLongObjectHashMap<VertexValuesPhase1> vertexTable,
                VertexValuesPhase1 currentVertexObject, long currentVertexID, TLongArrayList path, long pathID,
                byte sourceVertexType) {

            TLongArrayList currLocalUnvisited = currentVertexObject.getUnvisitedLocalNeighbourList();
            int currLocalSize = currLocalUnvisited.size();

            while (currLocalSize > 0) {
                // Remove last. more efficient in array list
                long nextVertexID = currLocalUnvisited.removeAt(currLocalSize - 1);
                currLocalSize--;
                // garbage collect all capacity if list is empty
                if (currLocalSize == 0) currLocalUnvisited.clear(0);

                // check if visited before by testing if it is set to MAX_LONG
                if (nextVertexID == Long.MAX_VALUE) continue;

                // add next VID to path
                path.add(nextVertexID);
                VertexValuesPhase1 nextVertexObj = vertexTable.get(nextVertexID);

                assert nextVertexObj != null : "found nextVertexObj as NULL for nextVertexID=" + nextVertexID
                        + ". currLocalSize=" + currLocalSize + ". pathID=" + pathID + ". currentVertexID="
                        + currentVertexID + ". vertexTable.size=" + vertexTable.size();

                // Add path to nextVID, if NOT an internal vertex. Internal vertices will have their path ID set to a
                // pivot path ID later.
                if (sourceVertexType != VertexValuesPhase1.TYPE_INTERNAL) nextVertexObj.appendPathId(pathID);

                // replace the value of current VID present in the neighbors list of next VId with MAX_LONG
                TLongArrayList nextLocalUnvisited = nextVertexObj.getUnvisitedLocalNeighbourList();
                // YS: TODO: O(n) scan. Optionally, sort the neighbors and later do binary search.
                int currVidOffsetInNextLocalList = nextLocalUnvisited.indexOf(currentVertexID);
                assert currVidOffsetInNextLocalList != -1;
                nextLocalUnvisited.set(currVidOffsetInNextLocalList, Long.MAX_VALUE);

                // move current to next vertex
                currentVertexID = nextVertexID;
                currLocalUnvisited = nextLocalUnvisited;
                currLocalSize = currLocalUnvisited.size();
            }


            return currentVertexID;
        }


        /**
         * Paths from internal source vertex MUST intersect with a path from an OB or EB, i.e., there must be at least 1
         * PIVOT vertex on this internal path such that the vertex lies on another OB or EB path.
         * We logically attach this internal path to the other OB or EB path that the PIVOT lies on, by adding the OB/EB
         * path ID to the path list of each vertex on this internal path.
         * 
         * FIXME: we are NOT checking if the OB/EB path we add to the path list of each vertex is not already present
         * for the vertex, i.e., we should check if this vertex on the local path is already on the EB/OB path.
         * 
         * This returns the pivot vertex whose OB/EB path ID that this path is attached to.
         * We also set the path ID for the given path (which is the first item on the path list) to be equal to the
         * pivot path ID.
         * 
         * @param path
         * @param vertexTable
         */
        public static final Long addPivotPathToInternalPathVertices(TLongArrayList path,
                TLongObjectHashMap<VertexValuesPhase1> vertexTable) {


            // ASSERT: the local path with start and end at the same vertex
            // we should not add the local path ID to the vertex obj's path ID list
            // instead find the local path
            // then traverse the local path to find the first vertex that has a path ID in its list
            // this will be an OB or EB path
            // Append this OB/EB path ID to all vertices in this local path
            Long pivotPathId = null;
            Long pivotVertex = null;

            // first entry on path is the path ID. Second entry is the source vertex. Last entry is the sink ID, which
            // is same as source ID. Ensure there is second entry.
            int pathSize = path.size();
            assert pathSize >= 3 : "path size is less than 3!";
            assert path.get(1) == path.get(pathSize - 1) : "source and sink VIDs were different for Internal Path";

            // Skip first entry which is path ID.
            // Skip last sink ID which is same as source ID.
            for (int i = 1; i < pathSize - 1; i++) {
                pivotVertex = path.get(i);
                if (vertexTable.get(pivotVertex).getPathId().size() > 1) {
                    pivotPathId = vertexTable.get(pivotVertex).getPathId().get(0);
                    // LOGGER.info("PATH_ID: " + pathId);
                    break;
                }
            }

            // did not find a path ID. ERROR!
            assert IS_LAST_LEVEL_ASSERT || pivotPathId != null : "could not find a pivot vertex for internal path!";
            // TODO: This assert is not valid when we have only 1 partition. Is there a way to check this?
            if (pivotPathId == null) return null;

            // set the Path ID for this path to be same as the pivot path ID
            path.set(0, pivotPathId);

            // Skip first entry which is path ID.
            // Skip last sink ID which is same as source ID.
            for (int i = 1; i < pathSize - 1; i++) {
                long vertex = path.get(i);
                vertexTable.get(vertex).appendPathId(pivotPathId);
            }

            return pivotVertex;
        }


        /**
         * First 8 bits must indicate partition ID (2^8), 9th MSB indicates if this is a path ID, and remaining 55
         * bits are for source vertex ID
         * 
         * @param partitionId
         * @param srcVertex
         * @return
         */
        public static long createPathId(byte partitionId, long srcVertex) { // FIXME: Consider adding current level as
            // part of bitmask at which this pathID is
            // created
            // only 55 LSB (3 + 4*13) should have vertex ID. else overflow.
            assert srcVertex >>> 55 == 0 : "Vertex ID out of bounds, overflow";

            return (((long) partitionId) << 56) | // partID is first 8 MSB
                    (1L << 55) | // set 9th MSB to 1 to indicate its a path ID
                    (srcVertex & 0x7FFFFFFFFFFFFFL); // set 55 LSB (3 + 4*13) to vertex ID
        }
    }


    /////////////////////////////////////////////////////////////////////////////////////////
    // Support methods for driver
    /////////////////////////////////////////////////////////////////////////////////////////

    /**
     * 
     * @param edgeList
     * @param edgeIndex
     * @param vSet
     * @param remainingVertices
     * @param numEdges
     * @param matching
     * @return
     */
    public static final boolean recursiveEdgeFind(List<MetaGraphEdge> edgeList, Set<Byte> metaVertices, int edgeIndex,
            HashSet<Byte> vSet, int remainingVertices, int numEdges, Stack<Tuple2<Byte, Byte>> matching) {

        // no more edges present, but unmatched vertices still remain
        if (edgeIndex >= numEdges && remainingVertices != 0) { return false; }

        // not more vertices remain. All done.
        if (remainingVertices == 0) { return true; }

        byte src = edgeList.get(edgeIndex).getSrcPartition();
        byte sink = edgeList.get(edgeIndex).getSinkPartition();
        boolean result;
        if (vSet.contains(src) || vSet.contains(sink)) {
            // source/sink vertex of this edge has already been used in another matching. Move to next edge.
            result = recursiveEdgeFind(edgeList, metaVertices, edgeIndex + 1, vSet, remainingVertices, numEdges,
                    matching);
            /* if(result == true) {
             * return true;
             * } */
            // matching.pop();
        } else {
            // Combining source/sink vertices of this edge is possible. Add them to vertex set as a candidate match and
            // move to next edge.
            vSet.add(src);
            vSet.add(sink);
            matching.add(new Tuple2<Byte, Byte>(src, sink));
            metaVertices.remove(src);
            metaVertices.remove(sink);
            result = recursiveEdgeFind(edgeList, metaVertices, edgeIndex + 1, vSet, remainingVertices - 2, numEdges,
                    matching);

            if (result == false) {
                // vSet.remove(src);
                // vSet.remove(sink);
                // matching.pop();
                // Return all left over vertices with a null neighbor
                for (Iterator<Byte> i = metaVertices.iterator(); i.hasNext();) {
                    byte srcMetaVertex = i.next();
                    matching.add(new Tuple2<Byte, Byte>(srcMetaVertex, null));
                }
                return true;

            }
        }
        return result;
    }


    /**
     * Create a new MetaMap based on matching at previous level
     * 
     * @param metaMap
     * @param matching
     * @param parentMap
     */
    // TODO: SJ documenting all changes with a comment and initials
    public static final Map<Byte, Map<Byte, Long>> createNewMetaMap(Map<Byte, Map<Byte, Long>> metaMap,
            Stack<Tuple2<Byte, Byte>> matching, Map<Byte, Byte> parentMap) {
        Map<Byte, Map<Byte, Long>> newMetaMap = new HashMap<>(); // new meta map to return
        Map<Byte, Map<Byte, Long>> oldMetaMap = new HashMap<>(metaMap); // create a copy as spark maps don't allow put
        // operation

        byte[] newVertexList = new byte[(matching.size())]; // list of vertices in newly created merged metagraph being
        // added; size is size of matching

        // remove all entries from i to j in the meta graph map if (i, j) is a matching
        for (int i = 0; i < matching.size(); i++) {
            if (oldMetaMap.containsKey(matching.get(i)._1()) && matching.get(i)._2() != null) {
                // System.out.println("Removing " + matching.get(i)._2() + " for " + matching.get(i)._1());
                LOGGER.info("Removing src-sink, " + matching.get(i)._1() + "," + matching.get(i)._2());
                oldMetaMap.get(matching.get(i)._1()).remove(matching.get(i)._2());
            }

            if (matching.get(i)._2() != null) {
                if (oldMetaMap.containsKey(matching.get(i)._2())) {
                    // System.out.println("Removing " + matching.get(i)._1() + " for " + matching.get(i)._2());
                    LOGGER.info("Removing sink-src, " + matching.get(i)._2() + "," + matching.get(i)._1());
                    oldMetaMap.get(matching.get(i)._2()).remove(matching.get(i)._1());
                }
            }

        }

        // sanity check- print metaMap
        LOGGER.info("AFTER REMOVAL OF EDGES IN MATCHING, UPDATED_METAGRAPH: " + metaMap);
        // printMetaGraph(oldMetaMap);

        // update the mapping of partitionIDs from self to its matching partner; initialize the new hashmap with the
        // keys; array is for convenience, update later; TODO: maybe able to optimise later
        for (int a = 0; a < newVertexList.length; a++) {
            if (matching.get(a)._2() != null) {
                if (matching.get(a)._1() > matching.get(a)._2()) {
                    newVertexList[a] = matching.get(a)._1();
                    parentMap.put(matching.get(a)._2(), matching.get(a)._1()); // update the mapping of one vertex id to
                    // another in the map
                } else {
                    newVertexList[a] = matching.get(a)._2();
                    parentMap.put(matching.get(a)._1(), matching.get(a)._2()); // update the mapping of one vertex id to
                    // another in the map
                }

            } else {
                newVertexList[a] = matching.get(a)._1();
            }
            newMetaMap.put(newVertexList[a], new HashMap<>()); // create empty entries for each new merged vertex id
        }

        LOGGER.info("Parent for MetaVertex after update based on matching, is " + parentMap);

        // update remote mapping information to include only remote neighbours that are part of merging

        for (Iterator<Byte> it = oldMetaMap.keySet().iterator(); it.hasNext();) { // it iterates over src meta vertices
            Byte srcMetaVertex = it.next(); // meta src vertex
            List<Byte> innerList = new ArrayList<Byte>(oldMetaMap.get(srcMetaVertex).keySet()); // gets sink meta vertex
            // list for
            // every src meta vertex
            Map<Byte, Long> metaNeighbour = new HashMap<>(oldMetaMap.get(srcMetaVertex)); // returns the map of sinks
            // and edge
            // weights
            for (Byte metaNeighbourId : innerList) {
                if (metaNeighbour.get(parentMap.get(metaNeighbourId)) == null) { // if sink doesn't exist
                    LOGGER.info("Adding entry for " + parentMap.get(metaNeighbourId) + " at " + metaNeighbourId);
                    metaNeighbour.put(parentMap.get(metaNeighbourId), 0L);
                }
                byte mapping = parentMap.get(metaNeighbourId);
                if (mapping != metaNeighbourId) {
                    metaNeighbour.put(mapping, (metaNeighbour.get(mapping) + metaNeighbour.remove(metaNeighbourId)));
                }
            }
            oldMetaMap.put(srcMetaVertex, metaNeighbour);
        }

        // SJ: Can enable these print statements if needed
        // sanity check- print metaMap
        // System.out.println("After updating old Meta Map ");
        // printMetaGraph(oldMetaMap);

        // populate the new hashmap now
        for (Map.Entry<Byte, Map<Byte, Long>> meta : oldMetaMap.entrySet()) {
            if (newMetaMap.get(parentMap.get(meta.getKey())).isEmpty()) {
                newMetaMap.put(parentMap.get(meta.getKey()), meta.getValue());
                continue;
            }
            Map<Byte, Long> remoteMap = new HashMap<>(meta.getValue());
            HashMap<Byte, Long> updatedRemote = new HashMap<>();
            for (Map.Entry<Byte, Long> remoteVertex : remoteMap.entrySet()) {
                updatedRemote.put(remoteVertex.getKey(), remoteVertex.getValue()
                        + newMetaMap.get(parentMap.get(meta.getKey())).get(remoteVertex.getKey()));
            }
            newMetaMap.put(parentMap.get(meta.getKey()), updatedRemote);
        }
        // SJ: Can enable these print statements if needed
        // System.out.println("Final updated meta graph map is: ");
        // printMetaGraph(newMetaMap);
        return newMetaMap;
    }


    // TODO: SJ documenting all changes with a comment and initials
    public static final Stack<Tuple2<Byte, Byte>> findMatching(Map<Byte, Map<Byte, Long>> initMetaMap) {
        // Set<Tuple2<Long, Long>> vertexPair = new HashSet<>(); //set of all meta vertex pairs that form an edge
        // List<MetaGraphEdge> edges = new ArrayList<>(); //each edge stores information for src, sink and edge weight

        Set<Integer> vertexPair = new HashSet<>(); // SJ added YS's declaration here
        List<MetaGraphEdge> edges = new ArrayList<>(); // SJ added YS's declaration here
        Stack<Tuple2<Byte, Byte>> matching = new Stack<>(); // return this after finding matching at any level i
        Set<Byte> metaVertices = new HashSet<>();

        for (Map.Entry<Byte, Map<Byte, Long>> metaAdjList : initMetaMap.entrySet()) {
            // Map<Long, Long> map2 = new HashMap<>(key.getValue());
            Byte src = metaAdjList.getKey(); // meta graphs's source Partition ID
            // adjacency list for source part, with weights
            // sink part meta VID, weight
            Map<Byte, Long> adjList = metaAdjList.getValue();
            for (Map.Entry<Byte, Long> neighbours : adjList.entrySet()) {
                Byte sink = neighbours.getKey();
                int pair = src < sink ? (((int) src) << 8 | sink) : (((int) sink) << 8 | src); // added YS's logic for
                // finding pair
                // MetaGraphEdge edge = new MetaGraphEdge(key.getKey(), key2.getKey(), key2.getValue()); //the source
                // and sink order is fixed here
                // Tuple2<Long, Long> vPairFront = new Tuple2<>(key.getKey(), key2.getKey()); //src-sink in front order
                // Tuple2<Long, Long> vPairReverse = new Tuple2<>(key2.getKey(), key.getKey()); //src-sink in reverse
                // order
                if (vertexPair.contains(pair)) {
                    LOGGER.info("Skipping vertex pair," + src + "," + sink); // %d,%d\n", src, sink);
                    continue;
                }
                LOGGER.info("Adding vertex pair to edge list," + src + "," + sink); // %d,%d\n", src, sink);
                // add each edge in meta graph exactly once to vertex pair set for uniqueness test,
                // and to the meta edge list for further processing, with their weight as the remote edge cut
                vertexPair.add(pair); // Pair is encapsulated in bit masking
                MetaGraphEdge edge = new MetaGraphEdge(src, sink, neighbours.getValue());
                edges.add(edge);
            }
        }// done adding each undirected edge in meta graph to the meta edge list

        // find pairings greedily
        // sort edges in ascending order of their meta edge weight
        Collections.sort(edges, (edge1, edge2) -> edge2.getMetaEdgeWeight().compareTo(edge1.getMetaEdgeWeight()));
        int numVertices = initMetaMap.size();
        // metaVertices = initMetaMap.keySet();
        metaVertices = new HashSet<>(initMetaMap.keySet());
        int numEdges = edges.size();

        // Print the undirected edges of the meta graph in sorted order
        for (MetaGraphEdge item : edges) {
            LOGGER.info("EDGES: " + item.getSrcPartition() + "," + item.getSinkPartition() + ","
                    + item.getMetaEdgeWeight());
        }

        int i = 0;
        // FIXME: No while loop

        recursiveEdgeFind(edges, metaVertices, i, new HashSet<>(), numVertices, numEdges, matching);

        /* while (!recursiveEdgeFind(edges, i++, new HashSet<>(), numVertices, numEdges, matching)) {
         * matching = new Stack<>();
         * continue;
         * } */
        for (Tuple2<Byte, Byte> k : matching) {
            LOGGER.info("Matching is " + k);
        }
        Stack<Tuple2<Byte, Byte>> finalMatching = matching;
        return finalMatching;
    }


    /**
     * Builds the full weighted metagraph from meta adjacency lists and finds partition pairings with max weights
     * 
     * TODO: Later, recursively build a tree of the merged pairs of vertices at the first step itself. This will help
     * understand the redirection of remote vertices to new partitions in future merges.
     * 
     * @param metaGraph
     * @return
     */
    public static final Map<Byte, List<Tuple2<Byte, Byte>>> partitionMatching(Map<Byte, Map<Byte, Long>> metaGraph) {
        long startTime = System.currentTimeMillis();
        System.out.printf("partitionMatching.call Start,TimestampMS,%d%n", startTime);


        // Create a meta edge list from meta adjacency list
        // Set<TuplePair<Byte, Byte>> vertexPair = new HashSet<>();
        // TODO: SJ documenting all changes with a comment and initials
        Map<Byte, List<Tuple2<Byte, Byte>>> mergePairsLineage = new HashMap<>(); // SJ added; store lineage to return
        Map<Byte, Byte> parentMap = new HashMap<>(); // SJ added; store parent for each partition at current level
        // Map<Byte, Map<Byte, Long>> initialMetaGraph = new HashMap<>(metaGraph); //Scala maps don't allow
        // modification; use local copy for next statement
        Map<Byte, Map<Byte, Long>> localCopyMetaGraph = new HashMap<>(metaGraph); // SJ added; keep the final map to
        // return in this; TODO: can optimise?
        Stack<Tuple2<Byte, Byte>> matching = new Stack<>();
        printMetaGraph(localCopyMetaGraph); // print the initial meta graph
        LOGGER.info("CALL_TO_PARTITION_MATCHING,SIZE_OF_INITIAL_METAGRAPH: " + metaGraph.size() + " ,TimestampMS,"
                + System.currentTimeMillis());


        // SJ added; Set the initial parent of a partition to be itself
        for (Map.Entry<Byte, Map<Byte, Long>> initialPartId : metaGraph.entrySet()) {
            parentMap.put(initialPartId.getKey(), initialPartId.getKey());
        }

        byte k = 0; // SJ added this and following loop; Keep track of level of merging in the following loop
        while (localCopyMetaGraph.size() > 1) {
            // System.out.println("Updating the meta map now!");
            matching = findMatching(localCopyMetaGraph); // FIXME: Modify this to find matchings with null sinks; Done,
            // Testing is left
            mergePairsLineage.put(k, matching);
            localCopyMetaGraph = createNewMetaMap(localCopyMetaGraph, matching, parentMap); // FIXME: Modify this to
            // store null pairs
            k++;
        }

        long endTime = System.currentTimeMillis();
        System.out.printf("partitionMatching.call Done,TotalTimeMS,%d,TimestampMS,%d%n", (endTime - startTime),
                endTime);

        return mergePairsLineage;
    }


    /**
     * 
     * @param metaMap
     */
    public static void printMetaGraph(Map<Byte, Map<Byte, Long>> metaMap) {
        for (Byte key : metaMap.keySet()) {
            Map<Byte, Long> map2 = new HashMap<>(metaMap.get(key));
            for (Byte key2 : map2.keySet()) {
                System.out.println("PARTID," + key + ",REMOTE_PARTID," + key2 + ",META_EDGEWT," + map2.get(key2));
            }
        }
    }


    /////////////////////////////////////////////////////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////////////////

    @SuppressWarnings("serial")
    public static class WritePartitionedAdjacencyListFromAdjListObject
    implements PairFlatMapFunction<Iterator<Tuple2<Byte, AdjListObject>>, Byte, String> {
        // TEMPDEL
        Broadcast<SerializableWritable<Configuration>> hdfsConf;
        Broadcast<String> outputPathBC;


        public WritePartitionedAdjacencyListFromAdjListObject(Broadcast<SerializableWritable<Configuration>> hdfsConf,
                Broadcast<String> outputPathBC) {
            this.hdfsConf = hdfsConf;
            this.outputPathBC = outputPathBC;
        }


        @Override
        public Iterator<Tuple2<Byte, String>> call(Iterator<Tuple2<Byte, AdjListObject>> partAdjListIter)
                throws Exception {
            // access to HDFS
            FileSystem fileSystem = FileSystem.get(hdfsConf.getValue().value());
            String fname = outputPathBC.getValue() + "/AdjListObjectToPartBin.bin." + UUID.randomUUID();
            Path hf = new Path(fname);
            // Limit replication factor to 1
            FSDataOutputStream fos = fileSystem.create(hf, (short) 1);

            // add the rest of the adj list items in the spark partition to the graph partition
            long vc = 0;
            Byte initPartId = -1;
            while (partAdjListIter.hasNext()) {
                Tuple2<Byte, AdjListObject> partAdjList = partAdjListIter.next();
                initPartId = partAdjList._1();
                AdjListObject adjList = partAdjList._2();
                vc += adjList.getLocalAdjList().size();

                adjList.writePartitionedAdjacencyList(initPartId, fos);
            }
            fos.flush();
            fos.close();

            // return iterator over singleton
            ArrayList<Tuple2<Byte, String>> al = new ArrayList<Tuple2<Byte, String>>(1);
            al.add(new Tuple2<Byte, String>(initPartId,
                    new StringBuffer().append(fname).append(",").append(vc).toString()));

            return al.iterator();
        }


    }


    @SuppressWarnings("serial")
    public static class WritePartitionedAdjacencyListFromSourcePartitionPhase1
    implements PairFunction<Tuple2<Byte, SourcePartitionPhase1>, Byte, String> {

        // TEMPDEL
        Broadcast<SerializableWritable<Configuration>> hdfsConf;
        Broadcast<String> outputPathBC;


        public WritePartitionedAdjacencyListFromSourcePartitionPhase1(
                Broadcast<SerializableWritable<Configuration>> hdfsConf, Broadcast<String> outputPathBC) {
            this.hdfsConf = hdfsConf;
            this.outputPathBC = outputPathBC;
        }


        @Override
        public final Tuple2<Byte, String> call(Tuple2<Byte, SourcePartitionPhase1> t) throws IOException {

            ////////////////////////////
            // PROCESS SOURCE PARITION
            byte partitionId = t._1();
            SourcePartitionPhase1 partition = t._2(); // Object of the partition

            // access to HDFS
            FileSystem fileSystem = FileSystem.get(hdfsConf.getValue().value());
            String fname = outputPathBC.getValue() + "/SourcePartitionPhase1ToPartBin.bin." + UUID.randomUUID();
            Path hf = new Path(fname);
            // Limit replication factor to 1
            FSDataOutputStream fos = fileSystem.create(hf, (short) 1);

            partition.writePartitionedAdjacencyList(fos);

            fos.flush();
            fos.close();

            // return iterator over singleton
            return new Tuple2<Byte, String>(partitionId, fname);
        }


    }


    @SuppressWarnings("serial")
    public static class WritePartitionedAdjacencyListFromPhase1FinalObject
    implements PairFlatMapFunction<Iterator<Tuple2<Byte, Phase1FinalObject>>, Byte, String> {
        // TEMPDEL
        Broadcast<SerializableWritable<Configuration>> hdfsConf;
        Broadcast<String> outputPathBC;


        public WritePartitionedAdjacencyListFromPhase1FinalObject(
                Broadcast<SerializableWritable<Configuration>> hdfsConf, Broadcast<String> outputPathBC) {
            this.hdfsConf = hdfsConf;
            this.outputPathBC = outputPathBC;
        }


        @Override
        public Iterator<Tuple2<Byte, String>> call(Iterator<Tuple2<Byte, Phase1FinalObject>> ph1ObjIter)
                throws Exception {
            // access to HDFS
            FileSystem fileSystem = FileSystem.get(hdfsConf.getValue().value());
            String fname = outputPathBC.getValue() + "/Phase1FinalObjectToPartBin.bin." + UUID.randomUUID();
            Path hf = new Path(fname);
            // Limit replication factor to 1
            FSDataOutputStream fos = fileSystem.create(hf, (short) 1);

            // add the rest of the adj list items in the spark partition to the graph partition
            long vc = 0;
            Byte initPartId = -1;
            while (ph1ObjIter.hasNext()) {
                Tuple2<Byte, Phase1FinalObject> t = ph1ObjIter.next();
                initPartId = t._1();
                Phase1FinalObject ph1Obj = t._2();
                ph1Obj.writePartitionedAdjacencyListText(fos);
            }
            fos.flush();
            fos.close();

            // return iterator over singleton
            ArrayList<Tuple2<Byte, String>> al = new ArrayList<Tuple2<Byte, String>>(1);
            al.add(new Tuple2<Byte, String>(initPartId,
                    new StringBuffer().append(fname).append(",").append(vc).toString()));

            return al.iterator();
        }


    }

/* 
	//50m8p has following random merging
    public static final Map<Byte, List<Tuple2<Byte, Byte>>> newPartitionMatching() {
	Tuple2<Byte,Byte> l0p1 = new Tuple2<>((byte)2,(byte)7);
	Tuple2<Byte,Byte> l0p2 = new Tuple2<>((byte)0,(byte)4);
	Tuple2<Byte,Byte> l0p3 = new Tuple2<>((byte)1,(byte)3);
	Tuple2<Byte,Byte> l0p4 = new Tuple2<>((byte)5,(byte)6);
	Tuple2<Byte,Byte> l1p5 = new Tuple2<>((byte)6,(byte)7);
	Tuple2<Byte,Byte> l1p6 = new Tuple2<>((byte)3,(byte)4);
	Tuple2<Byte,Byte> l2p7 = new Tuple2<>((byte)4,(byte)7);

	ArrayList<Tuple2<Byte,Byte>> l0 = new ArrayList<>();
	l0.add(l0p1);
	l0.add(l0p2);
	l0.add(l0p3);
	l0.add(l0p4);
	ArrayList<Tuple2<Byte,Byte>> l1 = new ArrayList<>();
	l1.add(l1p5);
	l1.add(l1p6);
	ArrayList<Tuple2<Byte,Byte>> l2 = new ArrayList<>();
	l2.add(l2p7);
	
	Map<Byte, List<Tuple2<Byte, Byte>>> matchingTree = new HashMap<>();
	matchingTree.put((byte)0,l0);
	matchingTree.put((byte)1,l1);
	matchingTree.put((byte)2,l2);
	return matchingTree;
    }
*/

/*
	//40m8p has following random merging 
    public static final Map<Byte, List<Tuple2<Byte, Byte>>> newPartitionMatching() {
        Tuple2<Byte,Byte> l0p1 = new Tuple2<>((byte)1,(byte)4);
        Tuple2<Byte,Byte> l0p2 = new Tuple2<>((byte)6,(byte)7);
        Tuple2<Byte,Byte> l0p3 = new Tuple2<>((byte)3,(byte)5);
        Tuple2<Byte,Byte> l0p4 = new Tuple2<>((byte)0,(byte)2);
        Tuple2<Byte,Byte> l1p5 = new Tuple2<>((byte)5,(byte)7);
        Tuple2<Byte,Byte> l1p6 = new Tuple2<>((byte)2,(byte)4);
        Tuple2<Byte,Byte> l2p7 = new Tuple2<>((byte)4,(byte)7);

        ArrayList<Tuple2<Byte,Byte>> l0 = new ArrayList<>();
        l0.add(l0p1);
        l0.add(l0p2);
        l0.add(l0p3);
        l0.add(l0p4);
        ArrayList<Tuple2<Byte,Byte>> l1 = new ArrayList<>();
        l1.add(l1p5);
        l1.add(l1p6);
        ArrayList<Tuple2<Byte,Byte>> l2 = new ArrayList<>();
        l2.add(l2p7);

        Map<Byte, List<Tuple2<Byte, Byte>>> matchingTree = new HashMap<>();
        matchingTree.put((byte)0,l0);
        matchingTree.put((byte)1,l1);
        matchingTree.put((byte)2,l2);
        return matchingTree;
    }

*/
/*
	//40m4p has following random merging
    public static final Map<Byte, List<Tuple2<Byte, Byte>>> newPartitionMatching() {
        Tuple2<Byte,Byte> l0p1 = new Tuple2<>((byte)1,(byte)2);
        Tuple2<Byte,Byte> l0p2 = new Tuple2<>((byte)0,(byte)3);
        Tuple2<Byte,Byte> l1p3 = new Tuple2<>((byte)2,(byte)3);
        
        ArrayList<Tuple2<Byte,Byte>> l0 = new ArrayList<>();
        l0.add(l0p1);
        l0.add(l0p2);
        ArrayList<Tuple2<Byte,Byte>> l1 = new ArrayList<>();
        l1.add(l1p3);
        
        Map<Byte, List<Tuple2<Byte, Byte>>> matchingTree = new HashMap<>();
        matchingTree.put((byte)0,l0);
        matchingTree.put((byte)1,l1);
        return matchingTree;
    }
*/
    /**
     * Spark Driver Code for Phase 1
     * 
     * @param args
     * @throws IOException
     */
    @SuppressWarnings("serial")
    public static void main(String[] args) throws IOException {
        SparkConf conf = new SparkConf().setAppName("Phase1Test");

        // conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        // List<Class<?>> classes = Arrays.<Class<?>>asList(Phase1FinalObject.class, PathSinkSource.class,
        // TLongObjectHashMap.class, TLongLinkedList.class, TLongArrayList.class, VertexValuesPhase1.class,
        // AdjListObject.class, TByteObjectHashMap.class, SourcePartitionPhase1.class);
        //
        // conf.registerKryoClasses((Class<?>[]) classes.toArray());
        // conf.set("spark.kryoserializer.buffer.max", "2047m");
        // conf.set("spark.kryoserializer.buffer", "32m");

        JavaSparkContext sc = new JavaSparkContext(conf);
        final boolean IS_FILE_BINARY = true;

        // Input and Output filenames are given as userargs
        String inputPath = args[0]; // to read input adjacency list
        String outputPath = args[1]; // to save Paths calculated
        Byte numPartitions = Byte.valueOf(args[2]);

        long startTime = System.currentTimeMillis();
        System.out.printf("Driver.call Start,TimestampMS,%d%n", startTime);

        // Key will be source partition ID. Value will be adjacency list with key as sink partID and value as meta
        // weight
        Map<Byte, Map<Byte, Long>> metaEdgeGraph = new HashMap<>();

        ///////////////////////////////
        // Read each line of string input and convert to (K,V) pair with partitionID as key(K) and the rest of the
        // string as the vertex adjacency list (V, LV*, P:RV*)
        JavaPairRDD<Byte, AdjListObject> rdd2;
        if (IS_FILE_BINARY) {
            ///////////////////////////////
            // read the graph adjacency list as binary input
            JavaPairRDD<String, PortableDataStream> rdd1 = sc.binaryFiles(inputPath); // numPartitions not supported

            rdd2 = rdd1.flatMapToPair(new ParseBinaryGraph());
        } else {
            ///////////////////////////////
            // read the graph adjacency list as text input
            JavaRDD<String> rdd1 = sc.textFile(inputPath);


            rdd2 = rdd1.mapToPair(new ParseTextGraph());
        }

        // FIXME: If the binary files are partitioned, we can hash on the datastream and load the stream locally into
        // the sourcepartiion RDD

        ///////////////////////////////
        // Repartition the input such that each vertex in a graph partition goes to its intended spark partition
        JavaPairRDD<Byte, AdjListObject> rdd3 = rdd2.partitionBy(new HashPartitioner(numPartitions));


        // YS: Verified that the adj list object is identical to the original parted binary file
        // Passed thru metagraph verfier
        //////////////////////////////
        // TEMPDEL
        // send the HDFS filesystem info to partitions to directly write to remote FS
        if (DUMP_AdjListObject) {

            Configuration configuration = JavaSparkContext.toSparkContext(sc).hadoopConfiguration();
            Broadcast<SerializableWritable<Configuration>> hdfsConf = sc
                    .broadcast(new SerializableWritable<Configuration>(configuration));
            Broadcast<String> outputPathBC = sc.broadcast(outputPath);

            JavaPairRDD<Byte, String> rddFoo = rdd3
                    .mapPartitionsToPair(new WritePartitionedAdjacencyListFromAdjListObject(hdfsConf, outputPathBC));

            List<Tuple2<Byte, String>> tmpOut = rddFoo.collect();
            for (Tuple2<Byte, String> t2 : tmpOut) {
                System.out.printf("%d,%s%n", t2._1, t2._2);
            }
        }
        // TEMPDEL
        //////////////////////////////

        ///////////////////////////////
        // reads all adjacency lists for a graph partition and creates a graph partition object for phase 1
        JavaPairRDD<Byte, SourcePartitionPhase1> rdd4 = rdd3.mapPartitionsToPair(new AdjObjToSourcePartition(), //
                true // preserve partitioning
                );

        // cache this since we're going to use this again
        //rdd4.cache();
        rdd4.persist(StorageLevel.MEMORY_ONLY_SER());

        // YS: Verified that the SourcePartitionPhase1 object is identical to the original parted binary file
        // Passed thru metagraph verfier
        //////////////////////////////
        // TEMPDEL
        // send the HDFS filesystem info to partitions to directly write to remote FS
        if (DUMP_SourcePartitionPhase1) {
            Configuration configuration = JavaSparkContext.toSparkContext(sc).hadoopConfiguration();
            Broadcast<SerializableWritable<Configuration>> hdfsConf = sc
                    .broadcast(new SerializableWritable<Configuration>(configuration));
            Broadcast<String> outputPathBC = sc.broadcast(outputPath);

            JavaPairRDD<Byte, String> rddFoo = rdd4
                    .mapToPair(new WritePartitionedAdjacencyListFromSourcePartitionPhase1(hdfsConf, outputPathBC));

            List<Tuple2<Byte, String>> tmpOut = rddFoo.collect();
            for (Tuple2<Byte, String> t2 : tmpOut) {
                System.out.printf("%d,%s%n", t2._1, t2._2);
            }
        }
        // TEMPDEL
        //////////////////////////////


        ///////////////////////////////
        // extract the meta adjacency list from each partition
	//Commenting out for now to test random partition matching
        JavaPairRDD<Byte, Map<Byte, Long>> rddMetaMap = rdd4
                .mapToPair(new PairFunction<Tuple2<Byte, SourcePartitionPhase1>, Byte, Map<Byte, Long>>() {

                    @Override
                    public Tuple2<Byte, Map<Byte, Long>> call(Tuple2<Byte, SourcePartitionPhase1> t) throws Exception {
                        long startTime = System.currentTimeMillis();
                        System.out.printf("BuildMetaEdgeMap.call Start,PartID,%d,TimestampMS,%d%n", t._1(), startTime);

                        Byte partID = t._1();
                        LOGGER.info("META_MAP_ENCOUNTERED,PART_ID: " + partID);
                        SourcePartitionPhase1 partitionObject = t._2();
                        Map<Byte, Long> metaEdgeMap = SourcePartitionPhase1
                                .buildMetaEdgeMap(partitionObject.getVertexMap());

                        long endTime = System.currentTimeMillis();
                        System.out.printf("BuildMetaEdgeMap.call Done,PartID,%d,TotalTimeMS,%d,TimestampMS,%d%n",
                                partID, (endTime - startTime), endTime);

                        return new Tuple2<Byte, Map<Byte, Long>>(partID, metaEdgeMap);
                    }
                });


        ///////////////////////////////
        // Action to collect metagraph
        // Find partition pairings
        //Commenting everything for now to test random partition matching
	metaEdgeGraph = rddMetaMap.collectAsMap();
        System.out.println("META_GRAPH_COLLECTED_AT_DRIVER: " + metaEdgeGraph);
        LOGGER.info("Size of metaEdgeGraph " + metaEdgeGraph.size());
        assert metaEdgeGraph.size() == numPartitions : "Number of partitions passed in args " + numPartitions
                + " did not match number of parts (meta vertices) seen in meta graph " + metaEdgeGraph.size();
        
	Map<Byte, List<Tuple2<Byte, Byte>>> mergePairsLineage = new HashMap<>();
        mergePairsLineage = partitionMatching(metaEdgeGraph);
        //Hardcoding the values to complete work presently.
	//mergePairsLineage = newPartitionMatching();	
	System.out.println("Phase1,Merging Lineage: " + mergePairsLineage);


        ///////////////////////////////
        // Perform Phase 1 Tour
        // YS: FIXME: Why dont we merge this with rdd3.mapPartitionsToPair?
        JavaPairRDD<Byte, Phase1FinalObject> rdd5 = rdd4.mapToPair(new Phase1Tour());


        ///////////////////////////////
        // Trigger the above transforms
        //rdd5.cache();
        rdd5.persist(StorageLevel.MEMORY_ONLY_SER());
        long count = rdd5.count();
        LOGGER.info("SubgraphEulerTourPhase1Count," + count);


        ///////////////////////////////
        // Use pairings to decide on which partitions from Phase 1 output to coalesce
        List<Byte> levels = new ArrayList<>(mergePairsLineage.keySet());
        // sort the levels from 0 to n-1
        Collections.sort(levels);
        // sanity check for total levels
        LOGGER.info("TOTAL_MERGING_LEVELS: " + levels); // print the list of levels


        // Iterate over all levels to find paths in Phase2
        int lcount = levels.size();
        for (int i = 0; i < lcount; i++) {
            Byte currentLevel = levels.get(i);
            boolean isLastLevel = (i == lcount - 1);

            // get the part pairs to merge at this level
            List<Tuple2<Byte, Byte>> currentMergingPairs = mergePairsLineage.get(currentLevel);

            LOGGER.info("PHASE_2,LEVEL," + currentLevel + ",IsLast," + isLastLevel + "; pairs="
                    + Arrays.toString(currentMergingPairs.toArray()));


            if (isLastLevel && FINALIZE_ROOT_AT_DRIVER) {
                SubgraphEulerTourPhase2 p2 = new SubgraphEulerTourPhase2(currentMergingPairs);
                List<Tuple2<Byte, Phase1FinalObject>> rddRoot = rdd5.collect();
                assert rddRoot.size() == 2 : "Did not find exactly 2 Phase1FinalObjects in rddRoot. size="
                        + rddRoot.size();
                Iterator<Tuple2<Byte, Phase1FinalObject>> output = p2.call(rddRoot.iterator());
                Tuple2<Byte, Phase1FinalObject> outT2 = output.next();
                System.out.printf("Final out,PartID,%d,pathMapSize,%d%n", outT2._1, outT2._2.getPathMap().size());
                break;
            } else {

                Broadcast<List<Tuple2<Byte, Byte>>> mergePairsBcast = sc.broadcast(currentMergingPairs);

                // Call the partitioner to merge the partitions of previous level
                JavaPairRDD<Byte, Phase1FinalObject> rdd6 = rdd5
                        .partitionBy(new PartitionCoalescer(currentMergingPairs));

                // print as sanity check to see how many partitions after merging
                int numParts = rdd6.getNumPartitions();
                LOGGER.info("MERGED_PARTITION_COUNT," + numParts);
                // Broadcast<String> outputPathBC = sc.broadcast(outputPath);


                //////////////////////////////
                // TEMPDEL
                // send the HDFS filesystem info to partitions to directly write to remote FS
                if (DUMP_Phase1FinalObject) {
                    Configuration configuration = JavaSparkContext.toSparkContext(sc).hadoopConfiguration();
                    Broadcast<SerializableWritable<Configuration>> hdfsConf = sc
                            .broadcast(new SerializableWritable<Configuration>(configuration));
                    Broadcast<String> outputPathBC = sc.broadcast(outputPath);

                    JavaPairRDD<Byte, String> rddFoo = rdd6.mapPartitionsToPair(
                            new WritePartitionedAdjacencyListFromPhase1FinalObject(hdfsConf, outputPathBC));

                    List<Tuple2<Byte, String>> tmpOut = rddFoo.collect();
                    for (Tuple2<Byte, String> t2 : tmpOut) {
                        System.out.printf("%d,%s%n", t2._1, t2._2);
                    }
                }
                // TEMPDEL
                //////////////////////////////

                // call phase2 tour on the merged partition to get phase 1 output
                rdd5 = rdd6.mapPartitionsToPair(new SubgraphEulerTourPhase2(mergePairsBcast));

                // trigger transform
                //rdd5.cache();
                rdd5.persist(StorageLevel.MEMORY_ONLY_SER());
                count = rdd5.count();
                LOGGER.info("SubgraphEulerTourPhase2Count," + count);
            }
            // continue to next level
        }

        long endTime = System.currentTimeMillis();
        System.out.printf("Driver.call Done,TotalTimeMS,%d,TimestampMS,%d%n", (endTime - startTime), endTime);

        ///////////////////////////////
        // Trigger another transformation path.
        // int numParts = rdd6.getNumPartitions();
        // Do sanity Check for partitions being merged
        // System.out.println("New Partitions are: " + numParts); */
        // rdd5.saveAsTextFile(outputPath);
        sc.stop();
    }

}


