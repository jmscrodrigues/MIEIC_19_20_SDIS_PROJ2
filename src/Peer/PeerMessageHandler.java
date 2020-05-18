package Peer;

import java.io.*;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

public class PeerMessageHandler implements Runnable {

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

		    if (len > 0)
		        dis.readFully(buf);
		      
		} catch(IOException e) {
			e.printStackTrace();
			System.exit(-1);
		}
		
		String message = new String(buf, StandardCharsets.UTF_8);
	    System.out.println(message);
	    String[] parts = message.split(" ");
	    
	    String op = parts[0];

	    byte[] toSend;

		switch (op) {
			case PeerOps.PUT: {
				String key = parts[1];
				String value = parts[2];
				this.peer.put(key, value);
				toSend = ("Put with success").getBytes();
				break;
			}
			case PeerOps.GET: {
				String key = parts[1];
				toSend = this.peer.get(key).getBytes();
				break;
			}
			case PeerOps.REMOVE: {
				String key = parts[1];
				toSend = this.peer.remove(key).getBytes();
				break;
			}
			case PeerOps.BACKUP: {
				String file = parts[1];
				toSend = this.peer.backup(file).getBytes();
				break;
			}
			case PeerOps.RESTORE: {
				String file = parts[1];
				toSend = this.peer.restore(file).getBytes();
				break;
			}
			case PeerOps.DELETE: {
				String file = parts[1];
				toSend = this.peer.delete(file).getBytes();
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
