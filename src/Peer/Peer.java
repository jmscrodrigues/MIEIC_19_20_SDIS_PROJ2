package Peer;

import Chord.Chord;
import Chord.SSLMessage;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledThreadPoolExecutor;

public class Peer {

	private final Chord chord;
	private ServerSocket serverSocket;
    private final ScheduledThreadPoolExecutor scheduler_executor;

	public static void main(String[] args) {
		if (args.length != 2 && args.length != 3) {
			System.out.println("Usage: Peer <peer_port> <chord_port> [<peer_ip>:<peer_port>]");
			System.exit(0);
		}

		int serve_port = Integer.parseInt(args[0]);
		int chord_port = Integer.parseInt(args[1]);
		InetSocketAddress access_peer = null;

		if (args.length == 3) {
			String[] parts = args[2].split(":");
			access_peer = new InetSocketAddress(parts[0], Integer.parseInt(parts[1]));
		}

		Peer peer = new Peer(serve_port, chord_port, access_peer);

		System.out.println("Hello world");
	}

	Peer(int server_port, int chord_port, InetSocketAddress access_peer) {
		
		this.scheduler_executor = (ScheduledThreadPoolExecutor) Executors.newScheduledThreadPool(64);
		this.chord = new Chord(this, chord_port);
		
		File directory = new File("./peer" + this.chord.getKey());
		if (!directory.exists()){
	        directory.mkdir();
	    }

		try {
			serverSocket = new ServerSocket(server_port);
		} catch (IOException e) {
			System.err.println("Could not create the server socket");
			e.printStackTrace();
			System.exit(-1);
		}

		this.scheduler_executor.execute(new PeerServer(this));

		if (access_peer != null) {
			this.chord.joinRing(access_peer);
		}

		Runtime.getRuntime().addShutdownHook(new Thread(chord::leaveRing));
	}
	
	public String put(String key, String value) {
		this.chord.put(key, value.getBytes(),1);
		return "Inserted with success";
	}
	
	public String get(String key) {
		byte[] ret = this.chord.get(key,1);
		return new String(ret, StandardCharsets.UTF_8);
	}

	public String remove(String key) {
		byte[] ret = this.chord.remove(key,1);
		return new String(ret, StandardCharsets.UTF_8);
	}
	
	public String backup(String file_name, int replication) {
		
		FileInfo file = new FileInfo(file_name, replication);
        if (!file.doesFileExists()) {
        	return "File " + file_name +" not found";
        }        
        //TODO generate unique id through hash
        this.chord.getMemory().addBackupFile(file_name, file.getFileData());
        for (int i = 0; i < file.getNumberOfParts(); i++) {
			this.getExecutor().execute(new PeerBackupThread(this.chord, file, file_name, i, replication));
        }
		
        return "Backup with success";
	}
	
	public String restore(String file_name) {
		
		FileData fileData = this.chord.getMemory().getFileChunks(file_name);
		if(fileData == null)
			return "File not known";
		int numChunks = fileData.getNumChunks();
		FileInfo file = new FileInfo(file_name, numChunks);
		for(int i = 0; i < numChunks ; i++) {
			this.getExecutor().execute(new PeerRestoreThread(this.chord, file, fileData, file_name, i));
		}
		
		while(!file.isFileRestored()) {
		}
		if(file.isCorrupted())
			return "Restor failed";
		
        file.exportFile(file_name, "./peer" + this.chord.getKey() + "/");
        return "Restored with success";
	}
	public String delete(String file_name) {
		
		FileData fileData = this.chord.getMemory().getFileChunks(file_name);
		if(fileData == null)
			return "File not known";
		int numChunks = fileData.getNumChunks();
		for(int i = 0; i < numChunks ; i++) {
			this.getExecutor().execute(new PeerDeleteThread(this.chord, fileData, file_name, i));
		}
        this.chord.getMemory().removeBackupFile(file_name);
        return "Deleted with success";
	}


	public String reclaim(int space) {
		if (space < 0) {
			return "New memory space must be greater than 0";
		}

		if(space > this.chord.getMemory().getMemoryInUse()) {

		}
		else {
			this.chord.reclaim(space);
		}

		return "New memory now at " + space;
	}
	
	public String status() {
		return this.chord.status();
	}
	
	public ServerSocket getServerSocket() {
		return this.serverSocket;
	}
	
	public ScheduledThreadPoolExecutor getExecutor() {
		return this.scheduler_executor;
	}

	public class PeerBackupThread implements Runnable {
		private Chord chord;
		private FileInfo file;
		private String file_name;
		private int index;
		private int replication;

		public PeerBackupThread (Chord chord, FileInfo file, String file_name, int index, int replication) {
			this.chord = chord;
			this.file = file;
			this.file_name = file_name;
			this.index = index;
			this.replication = replication;
		}

		@Override
		public void run() {
			int key = this.chord.hash(this.file_name + "_" + this.index);
			this.file.addId(key);
			this.chord.put(this.file_name + "_" + this.index, file.getFilePart(this.index), this.replication);
			System.out.println(this.file_name + ":" + (this.index + 1) + "/" + file.getNumberOfParts());
		}
		
	}

	public class PeerRestoreThread implements Runnable {
		private Chord chord;
		private FileInfo file;
		private String file_name;
		private int index;
		private FileData fileData; 

		public PeerRestoreThread (Chord chord, FileInfo file, FileData fileData, String file_name, int index) {
			this.chord = chord;
			this.file = file;
			this.file_name = file_name;
			this.index = index;
			this.fileData = fileData;
		}

		@Override
		public void run() {
			byte[] ret = this.chord.get(this.file_name + "_" + this.index , this.fileData.getReplicationDegree());
			this.file.putFilePart(this.index, ret);
			System.out.println(this.file_name + ":" + (this.index + 1) + "/" + this.file.getNumberOfParts());
		}
		
	}

	public class PeerDeleteThread implements Runnable {
		private Chord chord;
		private String file_name;
		private int index;
		private FileData fileData; 

		public PeerDeleteThread (Chord chord, FileData fileData, String file_name, int index) {
			this.chord = chord;
			this.file_name = file_name;
			this.index = index;
			this.fileData = fileData;
		}

		@Override
		public void run() {
			System.out.println("sending delete of chunk: " + this.index);
			this.chord.remove(this.file_name + "_" + this.index, this.fileData.getReplicationDegree());
		}
		
	}
	

}



