package Chord;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import Peer.FileData;

public class Memory {
    
	//private final ConcurrentHashMap<Integer, byte[]> data = new ConcurrentHashMap<>();
	
	/*
	 * Files backedup by this peer
	 */
	private final ConcurrentHashMap<String, FileData> backupFiles = new ConcurrentHashMap<>();
	
	/*
	 * chunks stored by this peer
	 */
	private final List<Pair<Integer,Integer>> chunksStored = new ArrayList<Pair<Integer,Integer>>();
	
	/*
	 * Chunks not able to store here and sent to sucessor 
	 */
	private final List<Integer> chunksRedirected = new ArrayList<Integer>();

	//private final List<Pair<Integer,Integer>> chunkSize = new ArrayList<Pair<Integer,Integer>>();
	
	private final int maxMemory = 100000;
	private int memoryInUse;
	
	String path;

    public Memory(String p) {
    	this.path = p + "/";
    }

    public byte[] get(int fileId) {
        /*return data.get(fileId);*/
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
    	/*data.put(fileId, chunk);*/
    	try (FileOutputStream fileOuputStream = new FileOutputStream(this.path + String.valueOf(chunkId))) {
    		fileOuputStream.write(data);
    	}catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    	memoryInUse += data.length;
		this.chunksStored.add(new Pair<>(chunkId,data.length));
		//Pair<Integer,Integer> cSize = new Pair<Integer, Integer>(chunkId, data.length);
		//this.chunkSize.add(cSize);
		return true;
    }

    /*
        Returns data on success, null on error
    */
    public byte[] remove(int chunkId) {
        /*if (data.get(fileId) == null)
            return null;
        return data.remove(fileId);*/
    	File file = new File(this.path + String.valueOf(chunkId));
    	byte[] d = this.get(chunkId);
    	if(file.exists() == false) {
    		System.out.println("File to remove does not exists : " + this.path + String.valueOf(chunkId));
    		return null;
    	}
    	System.out.println("Deleting file : " + this.path + String.valueOf(chunkId));
    	file.delete();
    	for(int i = 0; i < this.chunksStored.size();i++) {
    		if(this.chunksStored.get(i).getKey() == chunkId) {
    			this.chunksStored.remove(i);
    			break;
    		}
		}
		/*for(Pair<Integer,Integer> pair : this.chunkSize) {
			if(pair.getKey() == chunkId) {
				this.chunkSize.remove(pair);
				break;
			}
		}*/
    	memoryInUse-=d.length;
    	return d;
    }
    
    public boolean canStoreChunk(int d) {
    	return this.memoryInUse + d <= this.maxMemory;
    }
    
    public void addRedirectedChunk(int key) {
    	this.chunksRedirected.add(key);
    }
    public boolean chunkRedirected(int key) {
    	for(int i = 0; i < this.chunksRedirected.size();i++)
    		if(this.chunksRedirected.get(i) == key)
    			return true;
    	return false;
    }
    public void removeRedirectedChunk(int key) {
    	for(int i = 0; i < this.chunksRedirected.size();i++)
    		if(this.chunksRedirected.get(i) == key) {
    			this.chunksRedirected.remove(i);
    			return;
    		}
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
    
    public List<Pair<Integer,Integer>> getStoredChunks(){
    	return this.chunksStored;
	}
	
	public int getMemoryInUse() {
		return this.memoryInUse;
	}

	/*public List<Pair<Integer,Integer>> getChunkSizeList() {
		return this.chunkSize;
	} */
	
	public boolean isStoredHere(int key) {
		for(int i = 0; i < this.chunksStored.size();i++) {
    		Pair<Integer,Integer> p = this.chunksStored.get(i);
    		if(p.getKey() == key)
    			return true;
    	}
		return false;
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
    	String str = "Key \t Length\n";
    	for(int i = 0; i < this.chunksStored.size();i++) {
    		Pair<Integer,Integer> p = this.chunksStored.get(i);
    		str += p.getKey() + " -> " + p.getValue()  + "\n";
    	}
    	str += "\n\nMax memory: " + this.maxMemory + "\t Memory in use: " + this.memoryInUse;
    	return str;
    }
    
}