package Chord;

import Peer.FileData;

import java.io.*;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class Memory implements Serializable{
    
	
	/**
	 *
	 */
	private static final long serialVersionUID = 1L;

	/*
	 * Files backedup by this peer
	 */
	private final ConcurrentHashMap<String, FileData> backupFiles = new ConcurrentHashMap<>();
	
	/*
	 * chunks stored by this peer
	 */
	private final ConcurrentHashMap<Integer,Integer> chunksStored = new ConcurrentHashMap<>();
	
	/*
	 * Chunks not able to store here and sent to successor 
	 */
	private final ConcurrentHashMap<Integer,Integer> chunksRedirected = new ConcurrentHashMap<>();

	
	private int maxMemory = 10000000;
	private int memoryInUse;
	
	String path;

    public Memory(String p) {
    	this.path = p + "/";
    }

    public byte[] get(int fileId) {
    	byte[] buf = null;
		File file = new File(this.path + String.valueOf(fileId));
		if(file.exists() == false)
			return null;
		try(FileInputStream fileInputStream = new FileInputStream(file)){
			buf = new byte[(int) file.length()];
			fileInputStream.read(buf);
		} catch (IOException e) {
            e.printStackTrace();
            return null;
        } 
		return buf;
    }

    public boolean put(int chunkId, byte[] data) {
    	try (FileOutputStream fileOuputStream = new FileOutputStream(this.path + String.valueOf(chunkId))) {
    		fileOuputStream.write(data);
    	}catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    	memoryInUse += data.length;
    	this.chunksStored.put(chunkId,data.length);
		return true;
    }

    /*
        Returns data on success, null on error
    */
    public byte[] remove(int chunkId) {
    	this.removeRedirectedChunk(chunkId);
    	File file = new File(this.path + String.valueOf(chunkId));
    	byte[] d = this.get(chunkId);
    	if(file.exists() == false) {
    		System.out.println("File to remove does not exists : " + this.path + String.valueOf(chunkId));
    		return null;
    	}
    	System.out.println("Deleting file : " + this.path + String.valueOf(chunkId));
    	file.delete();
    	this.chunksStored.remove(chunkId);

    	memoryInUse-=d.length;
    	return d;
    }
    
    public boolean canStoreChunk(int d) {
    	return this.memoryInUse + d <= this.maxMemory;
    }
    
    public void addRedirectedChunk(int key, int replication) {
    	this.chunksRedirected.put(key, replication);
    }
    public boolean chunkRedirected(int key) {
    	return this.chunksRedirected.get(key) != null;
    }
    public void removeRedirectedChunk(int key) {
    	this.chunksRedirected.remove(key);
    }
    
    
    public void addBackupFile(String file_id, FileData f) {
    	this.backupFiles.put(file_id, f);
    }
    public void removeBackupFile(String file_id) {
    	this.backupFiles.remove(file_id);
    }
    
    public FileData getFileChunks(String file_id) {
    	return this.backupFiles.get(file_id);
    }

    public Set<Entry<Integer,Integer>> getStoredChunks(){
    	return this.chunksStored.entrySet();
    }
	
	public int getMemoryInUse() {
		return this.memoryInUse;
	}
	
	public void setMaxMemory(int space) {
    	this.maxMemory = space;
    }
	
	public boolean isStoredHere(int key) {
		return this.chunksStored.get(key) != null;
	}
	
	public boolean wasInitiatedHere(int key) {
		for (Entry<String, FileData> entry : this.backupFiles.entrySet()) {
		      FileData value = entry.getValue();
		      if(value.isChunkFromHere(key))
		    	  return true;
		    }
		return false;
	}
    
    public String status() {
    	String str = "Chunks Stored:\nKey \t Length\n";
    	for (ConcurrentHashMap.Entry<Integer, Integer> entry : this.chunksStored.entrySet()) {
    		str += entry.getKey() + " -> " + entry.getValue()  + "\n";
    	}
    	str+="\n\nChunksRedirected:\nKey\n";
    	for (ConcurrentHashMap.Entry<Integer, Integer> entry : this.chunksRedirected.entrySet()) {
    		str+=entry.getKey()+"\n";
    	}
    	str += "\n\nMax memory: " + this.maxMemory + "\t Memory in use: " + this.memoryInUse;
    	return str;
    }
	
	public String getPath() {
		return this.path;
	}
}