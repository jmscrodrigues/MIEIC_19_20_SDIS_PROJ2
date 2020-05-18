package Peer;

import Chord.Chord;

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

		if (access_peer != null)
			this.chord.joinRing(access_peer);

		Runtime.getRuntime().addShutdownHook(new Thread(chord::leaveRing));
	}
	
	public String put(String key, String value) {
		this.chord.put(key, value.getBytes());
		return "Inserted with success";
	}
	
	public String get(String key) {
		byte[] ret = this.chord.get(key);
		return new String(ret, StandardCharsets.UTF_8);
	}

	public String remove(String key) {
		byte[] ret = this.chord.remove(key);
		return new String(ret, StandardCharsets.UTF_8);
	}
	
	public String backup(String file_name) {
		
		FileInfo file = new FileInfo(file_name, 1);
        if(file.doesFileExists() == false) {
        	return "File " + file_name +" not found";
        }        
        for(int i = 0; i < file.getNumberOfParts(); i++) {
        	this.chord.put(file_name + "_" + i, file.getFilePart(i));
        	System.out.println(i);
        	try{
				Thread.sleep(1000);
			}catch(Exception e) {
	        	System.out.println(e);
	        }  
        }
        
        //TODO generate unique id thorugh hash
        this.chord.getMemory().addBackupFile(file_name, file.getNumberOfParts());
        
        return "Backup with sucess";
	}
	
	public String restore(String file_name) {
		
		Integer numChunks = this.chord.getMemory().getFileChunks(file_name);
		if(numChunks == null)
			return "File not known";
		FileInfo file = new FileInfo(file_name, numChunks);
		for(int i = 0; i < numChunks ; i++) {
			byte[] ret = this.chord.get(file_name + "_" + i);
			file.putFilePart(i, ret);
		}
        file.exportFile(file_name, "./peer" + this.chord.getKey() + "/");
        return "Backup with sucess";
	}
	
	public ServerSocket getServerSocket() {
		return this.serverSocket;
	}
	
	public ScheduledThreadPoolExecutor getExecutor() {
		return this.scheduler_executor;
	}

}
