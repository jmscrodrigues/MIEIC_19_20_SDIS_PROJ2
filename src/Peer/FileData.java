package Peer;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class FileData implements Serializable {
	
	private static final long serialVersionUID = 1L;

	
	private String filepath;
	private int desiredReplicationDegree;
	
	private int numberOfChunks;
	
	private ConcurrentHashMap<Integer,Integer> chunksIds = new ConcurrentHashMap<Integer,Integer>();
	
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
		this.chunksIds.put(key,1);
	}
	
	public boolean isChunkFromHere(int key) {
		return this.chunksIds.get(key) != null;	
	}
	
	
}
