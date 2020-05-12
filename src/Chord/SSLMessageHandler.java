package Chord;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;

import javax.net.ssl.SSLEngine;

public class SSLMessageHandler implements Runnable{
	
	SSLServer server;
	Chord chord;
	SocketChannel channel;
	SSLEngine engine;
	byte[] data;
	
	SSLMessageHandler(SSLServer s, Chord c, SocketChannel sc, SSLEngine eng, byte[] d){
		this.server = s;
		this.chord = c;
		this.channel = sc;
		this.engine = eng;
		this.data = d;
	}

	@Override
	public void run() {
		
		byte [] toSend = "SUCCESS".getBytes();
		
		String message = new String(data, StandardCharsets.UTF_8);
	    System.out.println("TO HANDLE " + message);
	    String[] parts = message.split(" ");
	    String op = parts[0];
		
	    if(op.equals("LOOKUP")) {
			int key = Integer.parseInt(parts[1].replaceAll("\\D", ""));
			InetSocketAddress ret = null;
			try {
				ret = this.chord.lookup(key);
			} catch (Exception e) {
				e.printStackTrace();
			}
			if(ret != null)
				toSend = new String("LOOKUPRET " +  this.chord.getName() + " " + this.chord.getPort() + " " + ret.getHostName() + " " + ret.getPort()).getBytes();
			else 
				toSend = new String("ERROR").getBytes();
	    }else if(op.equals("SETSUCCESSOR")) {
	    	String ip = parts[2];
	    	int port = Integer.parseInt(parts[3].replaceAll("\\D", ""));
	    	InetSocketAddress sucessor = new InetSocketAddress(ip,port);
	    	this.chord.setSuccessor(sucessor);
	    }
	    else if(op.equals("SETPREDECCESSOR")) {
	    	String ip = parts[2];
	    	int port = Integer.parseInt(parts[3].replaceAll("\\D", ""));
	    	InetSocketAddress sucessor = new InetSocketAddress(ip,port);
	    	this.chord.setPredeccessor(sucessor);
	    }if(op.equals("GETSUCCESSOR")) {
	    	int key = Integer.parseInt(parts[1]);
	    	String ip = parts[2];
	    	int port = Integer.parseInt(parts[3].replaceAll("\\D", ""));
	    	try {
				this.chord.find_successor(key,ip,port,-1);
			} catch (Exception e) {
				e.printStackTrace();
			}
	    }



		try {
			this.server.write(channel, engine, toSend);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	
	
}
