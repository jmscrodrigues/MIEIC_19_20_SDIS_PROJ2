import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

public class ChordMessageHandler implements Runnable {
	
	Chord chord;
	Socket socket;
	
	ChordMessageHandler(Chord c, Socket s){
		this.chord = c;
		this.socket = s;
	}

	@Override
	public void run() {
		try {
			
			InputStream in = socket.getInputStream();
		    DataInputStream dis = new DataInputStream(in);
		    int len = dis.readInt();
		    byte[] buf = new byte[len];
		    if (len > 0) {
		        dis.readFully(buf);
		    }
		    socket.close();
		    
		    String message = new String(buf, StandardCharsets.UTF_8);
		    System.out.println(message);
		    String[] parts = message.split(" ");
		    
		    String op = parts[0];
		    
		    if(op.equals("GETSUCCESSOR")) {
		    	int key = Integer.parseInt(parts[1]);
		    	String ip = parts[2];
		    	int port = Integer.parseInt(parts[3]);
		    	this.chord.find_successor(key,ip,port,-1);
		    }
		    else if(op.equals("FINDFINGER")) {
		    	int key = Integer.parseInt(parts[1]);
		    	String ip = parts[2];
		    	int port = Integer.parseInt(parts[3]);
		    	int index = Integer.parseInt(parts[4]);
		    	this.chord.find_successor(key,ip,port,index);
		    }
		    else if(op.equals("SETFINGER")) {
		    	int key = Integer.parseInt(parts[1]);
		    	String ip = parts[2];
		    	int port = Integer.parseInt(parts[3]);
		    	int index = Integer.parseInt(parts[4]);
		    	this.chord.setFinger(index, new InetSocketAddress(ip,port));
		    	//System.out.println("SETFINGER_RECEIVED");
		    }
		    else if(op.equals("SETSUCCESSOR")) {
		    	int key = Integer.parseInt(parts[1]);
		    	String ip = parts[2];
		    	int port = Integer.parseInt(parts[3]);
		    	InetSocketAddress sucessor = new InetSocketAddress(ip,port);
		    	this.chord.setSuccessor(sucessor);
		    }
		    else if(op.equals("SETPREDECCESSOR")) {
		    	int key = Integer.parseInt(parts[1]);
		    	String ip = parts[2];
		    	int port = Integer.parseInt(parts[3]);
		    	InetSocketAddress sucessor = new InetSocketAddress(ip,port);
		    	this.chord.setPredeccessor(sucessor);
		    }else if(op.equals("UPDATEFINGERTABLE")) {
		    	int destiny = Integer.parseInt(parts[1]);
		    	int max = Integer.parseInt(parts[2]);
		    	int ttl = Integer.parseInt(parts[3]);
		    	this.chord.checkUpdateFingerTable(destiny, max, ttl);
		    }
		    else if(op.equals("NEWFINGER")) {
		    	int originKey = Integer.parseInt(parts[1]);
		    	String ip = parts[2];
		    	int port = Integer.parseInt(parts[3]);
		    	this.chord.foundNewFinger(new InetSocketAddress(ip,port));
		    	this.chord.sendNotifyNewFinger(originKey, ip, port);
			}
			else if (op.equals("DELETEFINGER")) {
				int oldKey = Integer.parseInt(parts[1]);
				int exitKey = Integer.parseInt(parts[2]);
				String delIp = parts[3];
				String ip = parts[4];
				int port = Integer.parseInt(parts[5]);
				this.chord.deleteFinger(oldKey, new InetSocketAddress(ip,port));
				this.chord.sendNotifyDeleteFinger(exitKey, delIp);
			}
		    
		    
		    
		}catch(IOException e) {
			e.printStackTrace();
		}

	}

}
