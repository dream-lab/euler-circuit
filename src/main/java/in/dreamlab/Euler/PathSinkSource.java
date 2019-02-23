package in.dreamlab.Euler;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.Serializable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gnu.trove.iterator.TByteObjectIterator;
import gnu.trove.list.TLongList;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.map.TByteObjectMap;
import gnu.trove.map.hash.TByteObjectHashMap;


/**
 * stores the source and sink vertex IDs for a path, along with their remote neighbors map
 * 
 * @author simmhan
 *
 */
@SuppressWarnings("serial")
public class PathSinkSource implements Serializable {

    static Logger LOGGER = LoggerFactory.getLogger(PathSinkSource.class);

    private boolean hasSink = false;
    private long source, sink;
    // FIXME: Serializing this appears to be costly. Write our own class that extends from trove and overrides the
    // Externalizable?
    private TByteObjectHashMap<TLongArrayList> sourceRemoteNeighbours, sinkRemoteNeighbours;


    PathSinkSource() {

    }


    void setSink(long sink, TByteObjectHashMap<TLongArrayList> sinkRemoteNeighbours) {
	this.sink = sink;
	this.sinkRemoteNeighbours = sinkRemoteNeighbours;
	hasSink = true;
    }


    void setSource(long source, TByteObjectHashMap<TLongArrayList> sourceRemoteNeighbours) {
	this.source = source;
	this.sourceRemoteNeighbours = sourceRemoteNeighbours;
    }


    /**
     * this will remove the remote neighbors entry associated with this other partition and return it.
     * It is possible for this vertex to not have any local edges to vertices in the merged partition.
     * So the returned value can be null.
     * 
     * @param part
     * @return
     */
    TLongArrayList getAndRemoveSourceNeighbor(byte part) {
	return sourceRemoteNeighbours.remove(part);
    }


    TByteObjectHashMap<TLongArrayList> getRemainingSourceNeighbors() {
	return sourceRemoteNeighbours;
    }


    TLongArrayList getAndRemoveSinkNeighbor(byte part) {
	return sinkRemoteNeighbours.remove(part);
    }


    TByteObjectHashMap<TLongArrayList> getRemainingSinkNeighbors() {
	return sinkRemoteNeighbours;
    }


    void mergeSourceParts(byte fromPart, byte toPart) {
	mergeParts(fromPart, toPart, sourceRemoteNeighbours);
    }


    void mergeSinkParts(byte fromPart, byte toPart) {
	mergeParts(fromPart, toPart, sinkRemoteNeighbours);
    }


    /**
     * this verifes that the partition this vertex is part of is not set as a remote partition key for the source or
     * sink
     * 
     * return 1: source contained it, 2: sink contained it, 3: both contained it
     * 
     * @param myPartId
     * @return
     */
    byte verifySelfPartNotRemote(byte myPartId) {
	byte result = 0;
	if (sourceRemoteNeighbours.contains(myPartId)) result |= 1;
	if (sinkRemoteNeighbours != null && sinkRemoteNeighbours.contains(myPartId)) result |= 2;
	return result;

    }


    static void mergeParts(byte fromPart, byte toPart, TByteObjectMap<TLongArrayList> remoteNeighbours) {
	// this will merge the remote neighbors present in the from partition with the remote neighbors of the to
	// partition. It will remove the entry for the fromPart.
	TLongArrayList remoteFrom = remoteNeighbours.get(fromPart);

	if (remoteFrom == null) { return; }

	TLongArrayList remoteTo = remoteNeighbours.get(toPart);

	if (remoteTo == null) {
	    // NOTE: The to partition need not exist, and if so, we can set the reference of the from to the to
	    remoteNeighbours.put(toPart, remoteFrom);
	} else {
	    // else, copy over contents
	    remoteTo.addAll(remoteFrom);
	}
	// remove the entry for the from part
	remoteNeighbours.remove(fromPart);
    }


    long getSource() {
	return source;
    }


    long getSink() {
	return sink;
    }


    boolean hasSink() {
	return hasSink;
    }


    public void writePartitionedAdjacencyListText(byte pid, BufferedWriter os) throws IOException {

	{
	    // write prefix
	    os.write(Byte.toString(pid)); // 1b partition ID
	    os.write(':');
	    os.write(Long.toString(source)); // source VID
	    os.write(' ');

	    // write local for OB
	    if (hasSink()) {
		os.write(Long.toString(sink)); // local sink ID
		os.write(' ');
	    }

	    // write remote
	    TByteObjectIterator<TLongArrayList> remIter = sourceRemoteNeighbours.iterator();
	    while (remIter.hasNext()) {
		remIter.advance();

		byte rp = remIter.key(); // get remote PID
		TLongArrayList rvals = remIter.value();
		assert rp != pid : "source and remote sink part IDs were the same for source VID " + source
			+ " and remote VID of size " + rvals.size() + " with part ID " + pid;
		os.write(Byte.toString(rp)); // 1b partition ID
		os.write(':');
		int rlen = rvals.size();
		for (int i = 0; i < rlen; i++) {
		    long rv = rvals.get(i);
		    os.write(Long.toString(rv)); // source VID
		    os.write(i == (rlen - 1) ? ' ' : ',');
		}
		// os.write(';');
	    }
	    os.write('\n');
	}

	if (hasSink()) {
	    // write prefix
	    os.write(Byte.toString(pid)); // 1b partition ID
	    os.write(':');
	    os.write(Long.toString(sink)); // source VID
	    os.write(' ');

	    // write local for OB
	    os.write(Long.toString(source)); // local sink ID
	    os.write(' ');

	    // write remote
	    TByteObjectIterator<TLongArrayList> remIter = sinkRemoteNeighbours.iterator();
	    while (remIter.hasNext()) {
		remIter.advance();

		byte rp = remIter.key(); // get remote PID
		TLongArrayList rvals = remIter.value();
		assert rp != pid : "source and remote sink part IDs were the same for source VID " + sink
			+ " and remote VID of size " + rvals.size() + " with part ID " + pid;
		os.write(Byte.toString(rp)); // 1b partition ID
		os.write(':');
		int rlen = rvals.size();
		for (int i = 0; i < rlen; i++) {
		    long rv = rvals.get(i);
		    os.write(Long.toString(rv)); // source VID
		    os.write(i == (rlen - 1) ? ' ' : ',');
		}
		// os.write(';');
	    }
	    os.write('\n');
	}
    }
}
