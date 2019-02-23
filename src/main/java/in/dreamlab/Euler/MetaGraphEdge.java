package in.dreamlab.Euler;

import java.io.*;

public class MetaGraphEdge implements Serializable{
	private Byte srcPartId;
	private Byte sinkPartId;
	private Long edgeWeight;
	
	public MetaGraphEdge(byte src, byte sink, long weight) {
		// TODO Auto-generated constructor stub
		this.srcPartId = src;
		this.sinkPartId = sink;
		this.edgeWeight = weight;
	}
	
	public Byte getSrcPartition() {
		return this.srcPartId;
	}
	
	public Byte getSinkPartition() {
		return this.sinkPartId;
	}
	
	public Long getMetaEdgeWeight() {
		return this.edgeWeight;
	}
}


