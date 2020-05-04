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
		    	this.chord.find_successor(key,ip,port);
		    }else if(op.equals("SETSUCCESSOR")) {
		    	int key = Integer.parseInt(parts[1]);
		    	String ip = parts[2];
		    	int port = Integer.parseInt(parts[3]);
		    	InetSocketAddress sucessor = new InetSocketAddress(ip,port);
		    	this.chord.setSuccessor(sucessor);
		    }else if(op.equals("SETPREDECCESSOR")) {
		    	int key = Integer.parseInt(parts[1]);
		    	String ip = parts[2];
		    	int port = Integer.parseInt(parts[3]);
		    	InetSocketAddress sucessor = new InetSocketAddress(ip,port);
		    	this.chord.setPredeccessor(sucessor);
		    }
		    
		    
		    
		}catch(IOException e) {
			e.printStackTrace();
		}

	}

}
