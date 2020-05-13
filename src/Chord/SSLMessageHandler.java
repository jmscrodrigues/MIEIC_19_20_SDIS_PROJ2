package Chord;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import javax.net.ssl.SSLEngine;

public class SSLMessageHandler implements Runnable{
	
	SSLServer server;
	Chord chord;
	SocketChannel channel;
	SSLEngine engine;
	byte[] data;
	
	byte[] header = null;
	byte[] body = null;
	
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
	    }else if(op.equals("PUT")) {
			int key = Integer.parseInt(parts[1]);
			this.getHeaderAndBody();
			this.chord.putInMemory(key, body);
		}else if(op.equals("GET")) {
			int key = Integer.parseInt(parts[1]);
			toSend = this.chord.getInMemory(key);
			if(toSend == null) {
				toSend = "FAIL".getBytes();
			}
		}else if(op.equals("REMOVE")) {
			int key = Integer.parseInt(parts[1]);
			toSend = this.chord.removeInMemory(key);
		}else if(op.equals("GETDATA")) {
			int key = Integer.parseInt(parts[1]);
	    	String ip = parts[2];
	    	int port = Integer.parseInt(parts[3]);
			try {
				this.chord.sendData(key,ip,port);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}else if (op.equals("DELETEFINGER")) {
			int exitKey = Integer.parseInt(parts[1]);
			int oldKey = Integer.parseInt(parts[2]);
			String ip = parts[3];
			int port = Integer.parseInt(parts[4]);
			this.chord.deleteFinger(oldKey, new InetSocketAddress(ip,port));
			try {
				this.chord.sendNotifyDeleteFinger(exitKey, oldKey, ip, port);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}else if(op.equals("NEWFINGER")) {
	    	int originKey = Integer.parseInt(parts[1]);
	    	String ip = parts[2];
	    	int port = Integer.parseInt(parts[3]);
	    	this.chord.foundNewFinger(new InetSocketAddress(ip,port));
	    	try {
				this.chord.sendNotifyNewFinger(originKey, ip, port);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}else if(op.equals("GETPREDECCESSOR")) {
	    	InetSocketAddress pre = this.chord.getPredeccessor();
	    	toSend = new String("PREDECCESSOR " + pre.getHostName() + " " + pre.getPort()).getBytes();
	    	System.out.println("Sending Predecessor");
	    	if(toSend == null) {
				toSend = new String("").getBytes();
			}
	    }
	    else if(op.equals("NOTIFY")) {
	    	int key = Integer.parseInt(parts[1]);
	    	String ip = parts[2];
	    	int port = Integer.parseInt(parts[3]);
	    	this.chord.notify(new InetSocketAddress(ip, port));
	    }
	    


		try {
			this.server.write(channel, engine, toSend);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	
	public void getHeaderAndBody() {
		byte[] buf = data;
		for(int i = 0; i <= buf.length - 4 ; ++i) {
			if(buf[i] == 0xD && buf[i+1] == 0xA && buf[i+2] == 0xD && buf[i+3] == 0xA) {
				header = Arrays.copyOf(buf, i);
				body = Arrays.copyOfRange(buf,i+4,buf.length);
				break;
			}
		}
	}
	
	
	
}
