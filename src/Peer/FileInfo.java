package Peer;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

public class FileInfo {
	
	private ArrayList<byte[]> fileParts;
	private File file;
	
	public FileInfo(String path, int replicationDegree) {
        this.file = new File(path);
        this.fileParts = new ArrayList<byte[]>();
        if(this.file.exists())
        	fileDivision();
        else
        	return;

        //this.fileData = new FileData(path, createFileId(), replicationDegree, this.fileParts.size(),this.file.getName());
    }
	
	private void fileDivision() {

        int divSize = 1100;
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
	
	public boolean doesFileExists() {
    	return this.file.exists();
    }
	
    public int getNumberOfParts() {
        return this.fileParts.size();
    }
	
    public byte[] getFilePart(int i) {
        return this.fileParts.get(i);
    }
	
}
