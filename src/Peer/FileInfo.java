package Peer;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

public class FileInfo {
	
	private ArrayList<byte[]> fileParts;
	private File file;
	
	private FileData fileData;
	
	private AtomicInteger restoredChunks = new AtomicInteger(0);
	private AtomicInteger fullChunks = new AtomicInteger(0);
	
	public FileInfo(String path, int replicationDegree) {
        this.file = new File(path);
        this.fileParts = new ArrayList<byte[]>();
        if(this.file.exists())
        	fileDivision();
        else
        	return;
        this.fileData = new FileData(path,this.fileParts.size(),replicationDegree);
    }
	
    public FileInfo(int numChunks) {
        this.file = null;
        this.fileParts = new ArrayList<byte[]>(numChunks);        
        System.out.println("No of chunks:" + numChunks);
        for (int i = 0; i < numChunks; i++) {
            this.fileParts.add(null);
        }
    }
    
    public void addId(int key) {
    	this.fileData.addId(key);
    }
	
	private void fileDivision() {
        int divSize = 16000;
        byte[] buf = new byte[divSize];

        try (FileInputStream inputStream = new FileInputStream(this.file);
                BufferedInputStream bufInputStream = new BufferedInputStream(inputStream)) {
            int bytesAmount = 0;
            while ((bytesAmount = bufInputStream.read(buf, 0, divSize)) > 0) {
                byte[] newBuf = Arrays.copyOf(buf, bytesAmount);
                this.fileParts.add(newBuf);
                buf = new byte[divSize];
            }

            if ((this.file.length() % divSize) == 0)
                this.fileParts.add(new byte[0]);

        }catch (IOException e) {
            e.printStackTrace();
        }
    }
	
    public File exportFile(String path, String dir) {
        File file = new File(dir + path);
        System.out.println("Writing into " + dir + path);
        try(OutputStream os = new FileOutputStream(file);) {
            for(int t = 0; t < this.fileParts.size(); t++) {
            	byte [] data = this.getFilePart(t);
            	if(data == null) System.out.println("DATA is NULL");
            	else os.write(data);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return file;
    }
	
	public boolean doesFileExists() {
    	return this.file.exists();
    }
	
    public Integer getNumberOfParts() {
        return new Integer(this.fileParts.size());
    }
	
    public byte[] getFilePart(int i) {
        return this.fileParts.get(i);
    }
    
    public void putFilePart(int i, byte[] d) {
        this.fileParts.set(i,d);
        this.restoredChunks.incrementAndGet();
        if(d != null)
        	this.fullChunks.incrementAndGet();
    }
    
    public boolean isFileRestored() {
    	return this.restoredChunks.get() == this.getNumberOfParts();
    }
    
    public boolean isCorrupted() {
    	return this.restoredChunks.get() != this.fullChunks.get();
    }
    
    public FileData getFileData() {
    	return this.fileData;
    }
	
}
