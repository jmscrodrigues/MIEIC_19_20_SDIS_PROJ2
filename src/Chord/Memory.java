package Chord;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class Memory {
    
    private ConcurrentHashMap<Integer,byte[]> data = new ConcurrentHashMap<Integer,byte[]>();

    public Memory() {
    }


    public byte[] get(int fileId) {
        return data.get(fileId);
    }

    public void put(int fileId, byte[] chunk) {
    	data.put(fileId, chunk);
        return;
    }

    /*
        Returns data on success, null on error
    */
    public byte[] remove(int fileId) {
        if (data.get(fileId) == null) {
            return null;
        }

        return data.remove(fileId);
    }
    
    public ConcurrentHashMap<Integer,byte[]> getData(){
    	return this.data;
    }
}