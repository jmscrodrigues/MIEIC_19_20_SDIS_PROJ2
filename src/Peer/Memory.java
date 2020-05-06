package Peer;
import java.util.concurrent.CopyOnWriteArrayList;
import java.io.Serializable;
import java.util.ArrayList;


public class Memory {
    
    private CopyOnWriteArrayList<Byte> chunksStored = new CopyOnWriteArrayList<Byte>();
    private int fileId;

    Memory(int fileId) {
        this.fileId = fileId;
    }

    public int getFileId() {
        return this.fileId;
    }

    public CopyOnWriteArrayList<Byte> getChunksStored() {
        return chunksStored;
    }

    public void addChunk(byte chunk) {
        chunksStored.add(chunk);
    }

}