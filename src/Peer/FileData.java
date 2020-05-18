package Peer;

import java.io.Serializable;

public class FileData implements Serializable {
	
	private static final long serialVersionUID = 1L;

	
	private String filepath;
	private int desiredReplicationDegree;
	
	private int numberOfChunks;
	
	FileData(String fp, int nChunks,int rep){
		filepath = fp;
		desiredReplicationDegree = rep;
		numberOfChunks = nChunks;
	}
	
	public int getNumChunks() {
		return this.numberOfChunks;
	}
	public int getReplicationDegree() {
		return this.desiredReplicationDegree;
	}
	
}
