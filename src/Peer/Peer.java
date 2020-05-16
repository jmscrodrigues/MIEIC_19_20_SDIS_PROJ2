package Peer;

import Chord.Chord;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledThreadPoolExecutor;

public class Peer {

	private Chord chord;
	private ScheduledThreadPoolExecutor scheduler_executor;

	private ServerSocket serverSocket;

	Peer(int server_port, int chord_port, InetSocketAddress access_peer) {

		this.scheduler_executor = (ScheduledThreadPoolExecutor) Executors.newScheduledThreadPool(64);

		try {
			serverSocket = new ServerSocket(server_port);
		} catch (IOException e) {
			System.err.println("Could not create the server socket");
			e.printStackTrace();
		}

		this.scheduler_executor.execute(new PeerServer(this));

		this.chord = new Chord(this, chord_port);

		if (access_peer != null) {
			try {
				this.chord.joinRing(access_peer);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			try {
				chord.leaveRing();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		  }));
		
		this.chord.printFingerTable();
		
	}
	
	public String put(String key, String value) throws Exception {
		this.chord.put(key, value.getBytes());
		return "Inserted with sucess";
	}
	
	public String get(String key) throws Exception {
		byte[] ret = this.chord.get(key);
		return new String(ret, StandardCharsets.UTF_8);
	}
	public String remove(String key) throws Exception {
		byte[] ret = this.chord.remove(key);
		return new String(ret, StandardCharsets.UTF_8);
	}
	
	public ServerSocket getServerSocket() {
		return this.serverSocket;
	}
	
	public ScheduledThreadPoolExecutor getExecutor() {
		return this.scheduler_executor;
	}
	
	public static void main(String[] args) {
		
		if(args.length != 2 && args.length != 3) {
			System.out.println("Usage: Peer <peer_port> <chord_port> [<peer_ip>:<peer_port>]");
			System.exit(0);
		}
		
		int serve_port = Integer.parseInt(args[0]);
		int chord_port = Integer.parseInt(args[1]);
		InetSocketAddress access_peer = null;
		
		if(args.length == 3) {
			String[] parts = args[2].split(":");
			access_peer = new InetSocketAddress(parts[0],Integer.parseInt(parts[1]));
		}
		
		Peer peer = new Peer(serve_port, chord_port, access_peer);
		
		System.out.println("Hello world");

	}

}
