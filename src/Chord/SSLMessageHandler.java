package Chord;

import java.io.IOException;
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
	    //System.out.println("TO HANDLE " + message);
	    String[] parts = message.split(" ");
	    String op = parts[0];
	    System.out.println("To handle: " + op);
		switch (op) {
			case ChordOps.LOOKUP: {
				int key = Integer.parseInt(parts[1].replaceAll("\\D", ""));
				InetSocketAddress ret = null;
				ret = this.chord.lookup(key);
				if (ret != null)
					toSend = ("LOOKUPRET " + this.chord.getName() + " " + this.chord.getPort() + " " + ret.getHostName() + " " + ret.getPort()).getBytes();
				else
					toSend = "ERROR".getBytes();
				break;
			}
			case ChordOps.SET_SUCCESSOR: {
				String ip = parts[2];
				int port = Integer.parseInt(parts[3].replaceAll("\\D", ""));
				InetSocketAddress successor = new InetSocketAddress(ip, port);
				this.chord.setSuccessor(successor);
				break;
			}
			case ChordOps.SET_PREDECESSOR: {
				String ip = parts[2];
				int port = Integer.parseInt(parts[3].replaceAll("\\D", ""));
				InetSocketAddress successor = new InetSocketAddress(ip, port);
				this.chord.setPredecessor(successor);
				break;
			}
			case ChordOps.GET_SUCCESSOR: {
				int key = Integer.parseInt(parts[1]);
				String ip = parts[2];
				int port = Integer.parseInt(parts[3].replaceAll("\\D", ""));
				this.chord.find_successor(key, ip, port);
				break;
			}
			case ChordOps.PUT: {
				int key = Integer.parseInt(parts[1]);
				int replication = Integer.parseInt(parts[2]);
				this.getHeaderAndBody();
				if(this.chord.getMemory().canStoreChunk(body.length) == true) {
					this.chord.putInMemory(key, body);
					if(replication > 1)
						this.chord.putInSuccessor(key,body,replication-1);
				}else {
					System.out.println("Could not store file, redirecting");
					this.chord.putInSuccessor(key,body,replication);
				}
				break;
			}
			case ChordOps.GET: {
				int key = Integer.parseInt(parts[1]);
				int replication = Integer.parseInt(parts[2]);
				if(this.chord.getMemory().chunkRedirected(key) == false)
					toSend = this.chord.getInMemory(key);
				else
					toSend = this.chord.getFromSuccessor(key,replication);
				if (toSend == null) {
					if(replication > 1)
						this.chord.getFromSuccessor(key,replication-1);
					toSend = "ERROR".getBytes();
				}
				break;
			}
			case ChordOps.REMOVE: {
				int key = Integer.parseInt(parts[1]);
				int replication = Integer.parseInt(parts[2]);
				if(this.chord.getMemory().chunkRedirected(key) == false)
					toSend = this.chord.removeInMemory(key);
					if(replication > 1)
						this.chord.removeFromSuccessor(key,replication-1);
				else
					toSend = this.chord.removeFromSuccessor(key,replication-1);
				if (toSend == null) {
					toSend = "ERROR".getBytes();
				}
				break;
			}
			case ChordOps.GET_DATA: {
				int key = Integer.parseInt(parts[1]);
				String ip = parts[2];
				int port = Integer.parseInt(parts[3]);
				this.chord.sendData(key, ip, port);
				break;
			}
			case ChordOps.DELETE_FINGER: {
				int exitKey = Integer.parseInt(parts[1]);
				int oldKey = Integer.parseInt(parts[2]);
				String ip = parts[3];
				int port = Integer.parseInt(parts[4]);
				this.chord.deleteFinger(oldKey, new InetSocketAddress(ip, port));
				this.chord.sendNotifyDeleteFinger(exitKey, oldKey, ip, port);
				break;
			}
			case ChordOps.NEW_FINGER: {
				int originKey = Integer.parseInt(parts[1]);
				String ip = parts[2];
				int port = Integer.parseInt(parts[3]);
				this.chord.foundNewFinger(new InetSocketAddress(ip, port));
				this.chord.sendNotifyNewFinger(originKey, ip, port);
				break;
			}
			case ChordOps.GET_PREDECESSOR:
				InetSocketAddress pre = this.chord.getPredecessor();
				toSend = ("PREDECESSOR " + pre.getHostName() + " " + pre.getPort()).getBytes();
				System.out.println("Sending Predecessor");
				break;
			case ChordOps.NOTIFY: {
				String ip = parts[2];
				int port = Integer.parseInt(parts[3]);
				this.chord.notify(new InetSocketAddress(ip, port));
				break;
			}
		}

		try {
			this.server.write(channel, engine, toSend);
		} catch (IOException e) {
			System.out.println(e);
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
