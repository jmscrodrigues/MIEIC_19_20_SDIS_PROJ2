package Chord;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;

public class ChordSSLMessageHandler implements Runnable{
	
	ChordSSLServer server;
	SocketChannel socketChannel;
	SSLEngine engine;
	
	Chord chord;
	
	byte[] data = null;
	
	ChordSSLMessageHandler(Chord ch, byte[] d, ChordSSLServer s, SocketChannel c, SSLEngine eng){
		this.chord = ch;
		this.server = s;
		this.socketChannel = c;
		this.engine = eng;
		this.data = d;
		
	}

	@Override
	public void run() {
		// TODO Auto-generated method stub
		
		System.out.println("TO HANDLE: " + new String(data));
		
		byte [] toSend = null;
		
		String message = new String(data, StandardCharsets.UTF_8);
	    System.out.println(message);
	    String[] parts = message.split(" ");
	    
	    String op = parts[0];
	    
	    if(op.equals("LOOKUP")) {
			int key = Integer.parseInt(parts[1].replaceAll("\\D", ""));
			InetSocketAddress ret = this.chord.lookup(key);
			toSend = new String("LOOKUPRET " +  this.chord.getName() + " " + this.chord.getPort() + " " + ret.getHostName() + " " + ret.getPort()).getBytes();
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
	    	this.chord.find_successor(key,ip,port,-1);
	    }
		
	    
		try {
			server.write(socketChannel, engine, toSend);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}

	
	
}
