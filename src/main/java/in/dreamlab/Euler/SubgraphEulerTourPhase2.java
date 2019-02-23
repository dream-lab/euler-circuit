package in.dreamlab.Euler;

import java.io.*;
import java.util.*;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import in.dreamlab.Euler.SubgraphEulerTourPhase1.Phase1Tour;
import scala.Tuple2;


/**
 * @author siddharth
 * @author simmhan
 */

@SuppressWarnings("serial")
public class SubgraphEulerTourPhase2
	implements PairFlatMapFunction<Iterator<Tuple2<Byte, Phase1FinalObject>>, Byte, Phase1FinalObject> {
    static Logger LOGGER = LoggerFactory.getLogger(SubgraphEulerTourPhase2.class);
    static final boolean DUMP_PATHS = false; // FIXME


    List<Tuple2<Byte, Byte>> currentMergingPairs;


    public SubgraphEulerTourPhase2(List<Tuple2<Byte, Byte>> mergePairs) {
	this.currentMergingPairs = mergePairs;
	LOGGER.info("Received constructor currentMergingPairs," + Arrays.toString(currentMergingPairs.toArray()));
	// FIXME: does not work. Asserts are active even in last level.
	SubgraphEulerTourPhase1.IS_LAST_LEVEL_ASSERT = (currentMergingPairs.size() == 1) ? true : false;
    }


    public SubgraphEulerTourPhase2(Broadcast<List<Tuple2<Byte, Byte>>> mergePairsBcasts) {
	this.currentMergingPairs = mergePairsBcasts.getValue();
	LOGGER.info("Received broadcast currentMergingPairs," + Arrays.toString(currentMergingPairs.toArray()));
	// FIXME: does not work. Asserts are active even in last level.
	SubgraphEulerTourPhase1.IS_LAST_LEVEL_ASSERT = (currentMergingPairs.size() == 1) ? true : false;
    }


    @Override
    public Iterator<Tuple2<Byte, Phase1FinalObject>> call(Iterator<Tuple2<Byte, Phase1FinalObject>> partFinalObjectIter)
	    throws IOException {

	////////////////////////////
	// Get access to the 1 or 2 final objs from the iterator. This is done lazily by spark,
	// TODO: OPTIMIZE! This appears to take 40secs to get the iter objs
	// Get the two (or 1) final object in this iterator...20+20secs
	long startTime = System.currentTimeMillis();
	System.out.printf("SubgraphEulerTourPhase2.call Start,TimeStampMS,%d%n", startTime);
	LOGGER.info("Received broadcast currentMergingPairs," + Arrays.toString(currentMergingPairs.toArray()));

	long startTimeIter = System.currentTimeMillis();
	System.out.printf("phase1OutToPhase2In.partFinalObjectIter.call Start,TimeStampMS,%d%n", startTimeIter);

	Phase1FinalObject part1Obj, part2Obj;

	assert partFinalObjectIter.hasNext() : "Did not find even 1 final obj in this part!";
	part1Obj = partFinalObjectIter.next()._2();

	long endTimeIter = System.currentTimeMillis();
	System.out.printf("phase1OutToPhase2In.partFinalObjectIter.call Done,PartID,%d,TotalTimeMS,%d,TimestampMS,%d%n",
		part1Obj.getPartitionId(), (endTimeIter - startTimeIter), endTimeIter);

	assert part1Obj != null : "part1Obj was NULL";

	////////////////////////////

	startTimeIter = endTimeIter;

	part2Obj = partFinalObjectIter.hasNext() ? partFinalObjectIter.next()._2() : null;

	endTimeIter = System.currentTimeMillis();
	System.out.printf("phase1OutToPhase2In.partFinalObjectIter.call Done,PartID,%s,TotalTimeMS,%d,TimestampMS,%d%n",
		(part2Obj != null ? Byte.toString(part2Obj.getPartitionId()) : "NULL"), (endTimeIter - startTimeIter),
		endTimeIter);

	LOGGER.info("Found partObjs,1=" + (part1Obj != null ? part1Obj.getPartitionId() : "NULL") + ",2="
		+ (part2Obj != null ? part2Obj.getPartitionId() : "NULL"));


	///////////////////////////
	// Swap things such that Part1Obj is the larger part ID and Part2OBj (if present) is the smaller
	// TODO: Ensure that the lineage tree uses the same logic as well
	if (part2Obj != null) {
	    if (part2Obj.getPartitionId() > part1Obj.getPartitionId()) {
		Phase1FinalObject tmpPartObj = part1Obj;
		part1Obj = part2Obj;
		part2Obj = tmpPartObj;
		LOGGER.info(
			"Swapping partObjs to make Part1Obj as the one with larger part ID, and to be merged into from part2Obj");
	    }
	}

	byte partitionId = part1Obj.getPartitionId();


	////////////////////////////
	// Update the mapping pairs to be High, Low (To Part, From Part)
	// add the pairs of part IDs being merged at this level in sorted order (higher is 1, lower is 2)
	// Only add those parts that have pairs. Nothing to do for rest
	List<TuplePair<Byte, Byte>> partPairs = new ArrayList<>(currentMergingPairs.size());
	for (Tuple2<Byte, Byte> mp : currentMergingPairs) {
	    assert mp._1() != null || mp._2() != null : "Both partition pairs were null";
	    Byte maxPartId, minPartId;
	    if (mp._1() == null || mp._2() == null) continue; // nothing to merge in remote part neighbors
	    assert mp._1() != mp._2() : "Both partition pairs were equal";
	    maxPartId = mp._1() > mp._2() ? mp._1() : mp._2();
	    minPartId = mp._1() > mp._2() ? mp._2() : mp._1();
	    // }
	    partPairs.add(new TuplePair<Byte, Byte>(maxPartId, minPartId)); // to part, from part
	}
	LOGGER.info("Found partPairs," + Arrays.toString(partPairs.toArray()));


	// SourcePartitionPhase1 currentSourceObj = phase1OutToPhase2In(partFinalObjectIter, currentMergingPairs);

	// create source obj...2-3secs
	SourcePartitionPhase1 nextSrcObj = SourcePartitionPhase1.createFromPhase1FinalObjects(part1Obj, part2Obj,
		partPairs);
	TuplePair<Byte, Phase1FinalObject> output;
	if (nextSrcObj != null) { // multiple parts were merged, and we need to do phase 1 tour
	    // Call phase 1 tour
	    output = Phase1Tour.doPhase1Stuff(partitionId, nextSrcObj);
	    // FIXME: what do we use the Phase1FinalObject.pivotVertexMap for? Nothing. Write to disk.
	} else {
	    // return back the Part Obj 1. NOTE that we have updated the remote parts of remote VIDs with their merged
	    // part ID. Not other change has been made to input part obj.
	    output = new TuplePair<>(partitionId, part1Obj);
	}

	List<Tuple2<Byte, Phase1FinalObject>> list = new ArrayList<>(1);
	list.add(new Tuple2<Byte, Phase1FinalObject>(output._1(), output._2()));
	long endTime = System.currentTimeMillis();
	System.out.printf("SubgraphEulerTourPhase2.call Done %s,MergedPartID,%d,TotalTimeMS,%d,TimestampMS,%d%n",
		(nextSrcObj == null ? "singleton" : "merged tour"), partitionId, (endTime - startTime), endTime);

	return list.iterator();
    }

}

