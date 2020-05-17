package Chord;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

public class Memory {
    
	private final ConcurrentHashMap<Integer, byte[]> data = new ConcurrentHashMap<>();
	
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

    public boolean put(int fileId, byte[] data) {
    	/*data.put(fileId, chunk);*/
    	try (FileOutputStream fileOuputStream = new FileOutputStream(this.path + String.valueOf(fileId))) {
    		fileOuputStream.write(data);
    	}catch (IOException e) {
            e.printStackTrace();
            return false;
        }
		return true;
    }

    /*
        Returns data on success, null on error
    */
    public byte[] remove(int fileId) {
        /*if (data.get(fileId) == null)
            return null;
        return data.remove(fileId);*/
    	File file = new File(this.path + String.valueOf(fileId));
    	byte[] d = this.get(fileId);
    	file.delete();
    	return d;
    }
    
    public ConcurrentHashMap<Integer, byte[]> getData(){
    	return this.data;
    }
}