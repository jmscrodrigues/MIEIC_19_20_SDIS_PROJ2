package Chord;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import javax.net.ssl.SSLEngine;

import SSL.NioSSLServer;
import SSLEngine.SSLServer;

public class ChordSSLServer extends SSLServer implements Runnable{

	private Chord chord;
	
	public ChordSSLServer(Chord c, String protocol, String hostAddress, int port) throws Exception {
		super(protocol, hostAddress, port);
		this.chord = c;
	}
	
	
	@Override
	public void start() throws Exception {

    	System.out.println("Initialized and waiting for new connections...");
    	
    	ScheduledThreadPoolExecutor scheduler_executer = this.chord.getPeer().getExecuter();
    	
        while (isActive()) {
            selector.select();
            Iterator<SelectionKey> selectedKeys = selector.selectedKeys().iterator();
            while (selectedKeys.hasNext()) {
                SelectionKey key = selectedKeys.next();
                selectedKeys.remove();
                if (!key.isValid()) {
                    continue;
                }
                if (key.isAcceptable()) {
                    accept(key);
                } else if (key.isReadable()) {
                    byte[] d = read((SocketChannel) key.channel(), (SSLEngine) key.attachment());
                	//scheduler_executer.execute(new ChordSSLMessageHandler(this.chord,d,this,
                			//(SocketChannel) key.channel(), (SSLEngine) key.attachment()));
                    if(d != null) {
                    	 String toSend = this.handle(d);
                         this.write((SocketChannel) key.channel(), (SSLEngine) key.attachment(), toSend);
                    }
                }
            }
        }
        System.out.println("Goodbye!");
    }


	
	public String handle(byte [] data) {
		// TODO Auto-generated method stub
		
		System.out.println("TO HANDLE: " + new String(data));
		
		String toSend = "";
		
		String message = new String(data, StandardCharsets.UTF_8);
	    System.out.println(message);
	    String[] parts = message.split(" ");
	    
	    String op = parts[0];
	    
	    if(op.equals("LOOKUP")) {
			int key = Integer.parseInt(parts[1].replaceAll("\\D", ""));
			InetSocketAddress ret = this.chord.lookup(key);
			toSend = new String("LOOKUPRET " +  this.chord.getName() + " " + this.chord.getPort() + " " + ret.getHostName() + " " + ret.getPort());
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
	    }else if(op.equals("GETSUCCESSOR")) {
	    	int key = Integer.parseInt(parts[1]);
	    	String ip = parts[2];
	    	int port = Integer.parseInt(parts[3].replaceAll("\\D", ""));
	    	this.chord.find_successor(key,ip,port,-1);
	    }
		
	    
		return toSend;
		
	}
	
	
	
	
	
	

	@Override
	public void run() {
		try {
			this.start();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Should be called in order to gracefully stop the server.
	 */
	public void stop() {
		super.stop();
	}

}
