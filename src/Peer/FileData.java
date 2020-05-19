package Peer;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class FileData implements Serializable {
	
	private static final long serialVersionUID = 1L;

	
	private String filepath;
	private int desiredReplicationDegree;
	
	private int numberOfChunks;
	
	private List<Integer> chunksIds = new ArrayList<Integer>();
	
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
	
	public void addId(int key) {
		this.chunksIds.add(key);
	}
	
	public boolean isChunkFromHere(int key) {
		for(Integer k : this.chunksIds)
			if(k == key)
				return true;
		return false;			
	}
	
	
}
