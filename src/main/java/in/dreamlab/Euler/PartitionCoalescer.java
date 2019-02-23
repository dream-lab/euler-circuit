package in.dreamlab.Euler;

import java.util.*;

import org.apache.spark.Partitioner;
import scala.Tuple2;


@SuppressWarnings("serial")
public class PartitionCoalescer extends Partitioner {

    private List<Tuple2<Byte, Byte>> matchedPairs;


    public PartitionCoalescer(List<Tuple2<Byte, Byte>> mergePairs) {
	this.matchedPairs = mergePairs;
    }


    @Override
    public int getPartition(Object part) {
	int index = 0;
	for (Tuple2<Byte, Byte> partMatching : matchedPairs) {
	    if (partMatching._1() == null || partMatching._1().equals(part) || partMatching._2() == null
		    || partMatching._2().equals(part)) { return index; }
	    index += 1;
	}
	throw new RuntimeException("Could not find a valid partition matching for " + part
		+ ". Contents of matched pair list is: " + Arrays.deepToString(matchedPairs.toArray()));
    }


    @Override
    public int numPartitions() {
	return matchedPairs.size();
    }

}

