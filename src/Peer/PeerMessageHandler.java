package Peer;

import Chord.ChordOps;

import java.io.*;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

public class PeerMessageHandler implements Runnable{
	Peer peer;
	Socket socket;
	
	PeerMessageHandler(Peer p, Socket s){
		this.peer = p;
		this.socket = s;
	}

	@Override
	public void run() {
		byte[] buf = null;
		try {
			InputStream in = socket.getInputStream();
		    DataInputStream dis = new DataInputStream(in);
		    int len = dis.readInt();
		    buf = new byte[len];
		    if (len > 0) {
		        dis.readFully(buf);
		    }
		      
		} catch(IOException e) {
			e.printStackTrace();
			System.exit(-1);
		}
		
		String message = new String(buf, StandardCharsets.UTF_8);
	    System.out.println(message);
	    String[] parts = message.split(" ");
	    
	    String op = parts[0];

	    byte[] toSend = null;

		switch (op) {
			case ChordOps.PUT: {
				String key = parts[1];
				String value = parts[2];
				try {
					this.peer.put(key, value);
				} catch (Exception e) {
					e.printStackTrace();
				}
				toSend = ("Put with success").getBytes();
				break;
			}
			case ChordOps.GET: {
				String key = parts[1];
				try {
					toSend = this.peer.get(key).getBytes();
				} catch (Exception e) {
					e.printStackTrace();
				}
				break;
			}
			case ChordOps.REMOVE: {
				String key = parts[1];
				try {
					toSend = this.peer.remove(key).getBytes();
				} catch (Exception e) {
					e.printStackTrace();
				}
				break;
			}
			default:
				return;
		}

	    
	    try {
			OutputStream out = socket.getOutputStream(); 
		    DataOutputStream dos = new DataOutputStream(out);
		    dos.writeInt(toSend.length);
		    dos.write(toSend,0,toSend.length);
		} catch(IOException e) {
			System.err.println("Error sending message.");
            e.printStackTrace();
            System.exit(-1);
		}
	    
	    try {
			socket.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
}
