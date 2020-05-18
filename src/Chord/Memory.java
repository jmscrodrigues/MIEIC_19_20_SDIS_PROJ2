package Chord;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class Memory {
    
	//private final ConcurrentHashMap<Integer, byte[]> data = new ConcurrentHashMap<>();
	
	/*
	 * Files backedup by this peer
	 */
	private final ConcurrentHashMap<String, Integer> backupFiles = new ConcurrentHashMap<>();
	
	/*
	 * chunks stored by this peer
	 */
	private final List<Integer> chunksStored = new ArrayList<Integer>();
	
	/*
	 * Chunks not able to store here and sent to sucessor 
	 */
	private final List<Integer> chunksRedirected = new ArrayList<Integer>();
	
	private final int maxMemory = 40000;
	private int memoryInUse;
	
	String path;

    public Memory(String p) {
    	this.path = p + "/";
    }

    public byte[] get(int fileId) {
        /*return data.get(fileId);*/
    	byte[] buf = null;
		File file = new File(this.path + String.valueOf(fileId));
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
    	this.chunksStored.add(chunkId);
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
    	file.delete();
    	for(int i = 0; i < this.chunksStored.size();i++) {
    		if(this.chunksStored.get(i) == chunkId) {
    			this.chunksStored.remove(i);
    			break;
    		}
    	}
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
    
    
    public void addBackupFile(String file, Integer num_chunks) {
    	this.backupFiles.put(file, num_chunks);
    }
    public void removeBackupFile(String file) {
    	this.backupFiles.remove(file);
    }
    
    public Integer getFileChunks(String file) {
    	return this.backupFiles.get(file);
    }
    
    public List<Integer> getStoredChunks(){
    	return this.chunksStored;
    }
    
}