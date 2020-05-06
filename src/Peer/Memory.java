package Peer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class Memory {
    
    private ConcurrentHashMap<Integer,byte[]> chunksStored = new ConcurrentHashMap<Integer,byte[]>();

    Memory() {
    }


    public byte[] getChunksStored(int fileId) {
        return chunksStored.get(fileId);
    }

    public void addChunk(int fileId, byte[] chunk) {
        chunksStored.put(fileId, chunk);
        return;
    }

    /*
        Returns 0 on success, -1 on error
    */
    public int removeChunk(int fileId) {
        if (chunksStored.get(fileId) == null) {
            return -1;
        }

        chunksStored.remove(fileId);
        return 0;
    }
}