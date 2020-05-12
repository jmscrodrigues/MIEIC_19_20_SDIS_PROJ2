package Peer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import Chord.Chord;

public class Peer {

	private Chord chord;
	private ScheduledThreadPoolExecutor scheduler_executer;

	private ServerSocket serverSocket;

	Peer(int server_port, int chord_port, InetSocketAddress access_peer) {

		this.scheduler_executer = (ScheduledThreadPoolExecutor) Executors.newScheduledThreadPool(64);

		try {
			serverSocket = new ServerSocket(server_port);
		} catch (IOException e) {
			System.err.println("Could not create the server socket");
			e.printStackTrace();
		}

		this.scheduler_executer.execute(new PeerServer(this));

		this.chord = new Chord(this, chord_port);

		if (access_peer != null) {
			try {
				this.chord.joinRing(access_peer);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		Runtime.getRuntime().addShutdownHook(new Thread() {
			public void run() {
				try {
					chord.leaveRing();
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		      } 
		 });
		
		
	}
	
	public String put(String key, String value) throws Exception {
		this.chord.put(key, value.getBytes());
		return "Inserted with sucess";
	}
	
	public String get(String key) throws Exception {
		byte[] ret = this.chord.get(key);
		String value = new String(ret, StandardCharsets.UTF_8);
		return value;
	}
	public String remove(String key) throws Exception {
		byte[] ret = this.chord.remove(key);
		String value = new String(ret, StandardCharsets.UTF_8);
		return value;
	}
	
	public ServerSocket getServerSocket() {
		return this.serverSocket;
	}
	
	public ScheduledThreadPoolExecutor getExecuter() {
		return this.scheduler_executer;
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
