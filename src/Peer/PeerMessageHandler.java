package Peer;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

import Chord.Chord;

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
		      
		}catch(IOException e) {
			e.printStackTrace();
		}
		
		String message = new String(buf, StandardCharsets.UTF_8);
	    System.out.println(message);
	    String[] parts = message.split(" ");
	    
	    String op = parts[0];
	    byte[] toSend = null;
	    
	    if(op.equals("PUT")) {
	    	String key = parts[1];
	    	String value = parts[2];
	    	this.peer.put(key, value);
	    	toSend = new String("Put with sucess").getBytes();
	    }else if(op.equals("GET")) {
	    	String key = parts[1];
	    	toSend = this.peer.get(key).getBytes();
	    }else if(op.equals("REMOVE")) {
	    	String key = parts[1];
	    	toSend = this.peer.remove(key).getBytes();
	    }
	    
	    try {
			OutputStream out = socket.getOutputStream(); 
		    DataOutputStream dos = new DataOutputStream(out);
		    dos.writeInt(toSend.length);
		    dos.write(toSend,0,toSend.length);
		}catch(IOException e) {
			System.err.println("Error sending message.");
            System.err.println(e);
            System.exit(-1);
		}
	    
	    try {
			socket.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
}
