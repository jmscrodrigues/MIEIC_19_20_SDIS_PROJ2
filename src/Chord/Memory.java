package Chord;
import java.util.concurrent.ConcurrentHashMap;

public class Memory {
    
    private final ConcurrentHashMap<Integer, byte[]> data = new ConcurrentHashMap<>();

    public byte[] get(int fileId) {
        return data.get(fileId);
    }

    public void put(int fileId, byte[] chunk) {
    	data.put(fileId, chunk);
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