package in.dreamlab.Euler;

import java.io.*;

import gnu.trove.iterator.TByteObjectIterator;
import gnu.trove.iterator.TLongObjectIterator;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.list.linked.TLongLinkedList;
import gnu.trove.map.hash.TLongObjectHashMap;


@SuppressWarnings("serial")
public class Phase1FinalObject implements Serializable {

    private byte partitionId;

    // Key: pathID; values: source and sink
    // IDs and their remote neighbours
    // FIXME: when we dump pathMap to disk, we also need to include the level at which it was generated to help with
    // phase 3 recreation
    // FIXME: Serialization and transfer of this seems costly! ~ 20secs
    private TLongObjectHashMap<PathSinkSource> pathMap;

    // Key: vID; values: list of pathIds
    // FIXME: This can be written to disk. we don't need it in phase 2.
    private TLongObjectHashMap<TLongLinkedList> pivotVertexMap;

    private static final TLongObjectHashMap<TLongLinkedList> EMPTY_MAP = new TLongObjectHashMap<TLongLinkedList>(0);


    private Phase1FinalObject() {

    }


    public Phase1FinalObject(byte partitionId, TLongObjectHashMap<PathSinkSource> pathMap2,
	    TLongObjectHashMap<TLongLinkedList> multiPathVertexMap) {
	this.pathMap = pathMap2;
	this.pivotVertexMap = multiPathVertexMap;
	this.partitionId = partitionId;
    }


    public TLongObjectHashMap<PathSinkSource> getPathMap() {
	return pathMap;
    }


    public TLongObjectHashMap<TLongLinkedList> getPivotVertexMap() {
	return pivotVertexMap;
    }


    public TLongObjectHashMap<TLongLinkedList> resetPivotVertexMap() {
	TLongObjectHashMap<TLongLinkedList> tmp = pivotVertexMap;
	pivotVertexMap = EMPTY_MAP;
	return tmp;
    }


    public byte getPartitionId() {
	return partitionId;
    }


    public int getOBEBCount() {
	TLongObjectIterator<PathSinkSource> pathMapIter = pathMap.iterator();
	int count = 0;
	while (pathMapIter.hasNext()) {
	    pathMapIter.advance();
	    PathSinkSource srcSink = pathMapIter.value();
	    count++;
	    if (srcSink.hasSink()) count++;
	}
	return count;
    }


    public boolean containsAsSourceOrSink(long l) {
	TLongObjectIterator<PathSinkSource> adjListIter = pathMap.iterator();
	while (adjListIter.hasNext()) {
	    adjListIter.advance();
	    PathSinkSource srcSink = adjListIter.value();
	    if (l == srcSink.getSource() || (srcSink.hasSink() && l == srcSink.getSink())) return true;
	}
	return false;
    }


    public void writePartitionedAdjacencyListText(OutputStream os) throws IOException {

	BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(os));
	TLongObjectIterator<PathSinkSource> pathIter = pathMap.iterator();
	byte partId = getPartitionId();
	while (pathIter.hasNext()) {
	    pathIter.advance();
	    PathSinkSource sinkSrc = pathIter.value();
	    sinkSrc.writePartitionedAdjacencyListText(partId, bw);
	}

    }
}

