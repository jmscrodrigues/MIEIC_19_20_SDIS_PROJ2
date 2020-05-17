package Peer;

import Chord.Chord;

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

	Peer(int server_port, int chord_port, InetSocketAddress access_peer) {
		
		this.scheduler_executor = (ScheduledThreadPoolExecutor) Executors.newScheduledThreadPool(64);
		this.chord = new Chord(this, chord_port);

		try {
			serverSocket = new ServerSocket(server_port);
		} catch (IOException e) {
			System.err.println("Could not create the server socket");
			e.printStackTrace();
			System.exit(-1);

		}

		this.scheduler_executor.execute(new PeerServer(this));

		if (access_peer != null)
			try {
				this.chord.joinRing(access_peer);
			} catch (Exception e1) {
				e1.printStackTrace();
			}

		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			try {
				chord.leaveRing();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}));

	}
	
	public String put(String key, String value) throws Exception {
		this.chord.put(key, value.getBytes());
		return "Inserted with success";
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

		if (args.length != 2 && args.length != 3) {
			System.out.println("Usage: Peer <peer_port> <chord_port> [<peer_ip>:<peer_port>]");
			System.exit(0);
		}
		
		int serve_port = Integer.parseInt(args[0]);
		int chord_port = Integer.parseInt(args[1]);
		InetSocketAddress access_peer = null;
		
		if(args.length == 3) {
			String[] parts = args[2].split(":");
			access_peer = new InetSocketAddress(parts[0], Integer.parseInt(parts[1]));
		}
		
		Peer peer = new Peer(serve_port, chord_port, access_peer);

		System.out.println("Hello world");
	}

}
