package Peer;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import Chord.Chord;

public class Peer {
	
	private Chord chord;
    private ScheduledThreadPoolExecutor scheduler_executer;
	
	Peer(int port, InetSocketAddress access_peer) {
		
		this.scheduler_executer = (ScheduledThreadPoolExecutor) Executors.newScheduledThreadPool(64);
		
		this.chord = new Chord(this,port);
		
		
		if(access_peer != null)
			this.chord.joinRing(access_peer);
		
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		System.out.println(this.chord.lookup(1000));
		
		Runtime.getRuntime().addShutdownHook(new Thread() 
	    { 
		      public void run() { 
		    	  chord.leaveRing();
		      } 
		 });
		
	}
	
	
	
	public ScheduledThreadPoolExecutor getExecuter() {
		return this.scheduler_executer;
	}
	
	public static void main(String[] args) {
		
		if(args.length != 1 && args.length != 2) {
			System.out.println("Usage: Peer <port> [<peer_ip>:<peer_port>]");
			System.exit(0);
		}
		
		
		
		int port = Integer.parseInt(args[0]);
		InetSocketAddress access_peer = null;
		
		if(args.length == 2) {
			String[] parts = args[1].split(":");
			access_peer = new InetSocketAddress(parts[0],Integer.parseInt(parts[1]));
		}
		
		Peer peer = new Peer(port, access_peer);
		
	
		System.out.println("Hello world");

		
	}

}
