package Chord;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
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
		    }
		    else if(op.equals("NEWFINGER")) {
		    	int originKey = Integer.parseInt(parts[1]);
		    	String ip = parts[2];
		    	int port = Integer.parseInt(parts[3]);
		    	this.chord.foundNewFinger(new InetSocketAddress(ip,port));
		    	this.chord.sendNotifyNewFinger(originKey, ip, port);
			}
			else if (op.equals("DELETEFINGER")) {
				int exitKey = Integer.parseInt(parts[1]);
				int oldKey = Integer.parseInt(parts[2]);
				String ip = parts[3];
				int port = Integer.parseInt(parts[4]);
				this.chord.deleteFinger(oldKey, new InetSocketAddress(ip,port));
				this.chord.sendNotifyDeleteFinger(exitKey, oldKey, ip, port);
			}
			else if(op.equals("LOOKUP")) {
				int key = Integer.parseInt(parts[1]);
				InetSocketAddress ret = this.chord.lookup(key);
				byte[] toSend = new String("LOOKUPRET " + ret.getHostName() + " " + ret.getPort()).getBytes();
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
			}
			else if(op.equals("PUT")) {
				int key = Integer.parseInt(parts[1]);
				Message m = new Message(buf);
				m.getHeaderAndBody();
				this.chord.putInMemory(key, m.body);
			}
			else if(op.equals("GET")) {
				int key = Integer.parseInt(parts[1]);
				Message m = new Message(buf);
				m.getHeaderAndBody();
				byte[] toSend = this.chord.getInMemory(key);
				if(toSend == null) {
					toSend = new String("").getBytes();
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
			}
			else if(op.equals("REMOVE")) {
				int key = Integer.parseInt(parts[1]);
				Message m = new Message(buf);
				m.getHeaderAndBody();
				byte[] toSend = this.chord.removeInMemory(key);
				if(toSend == null) {
					toSend = new String("").getBytes();
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
			}
			else if(op.equals("GETDATA")) {
				int key = Integer.parseInt(parts[1]);
		    	String ip = parts[2];
		    	int port = Integer.parseInt(parts[3]);
				this.chord.sendData(key,ip,port);
			}
		    else if(op.equals("GETPREDECCESSOR")) {
		    	InetSocketAddress pre = this.chord.getPredeccessor();
		    	//Message out = new Message("PREDECESSOR " + pre.getHostName() + " " + pre.getPort());
		    	//out.sendMessage(socket);
		    	byte[] toSend = new String("PREDECCESSOR " + pre.getHostName() + " " + pre.getPort()).getBytes();
		    	System.out.println("Sending Predecessor");
		    	if(toSend == null) {
					toSend = new String("").getBytes();
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
		    }
		    else if(op.equals("NOTIFY")) {
		    	int key = Integer.parseInt(parts[1]);
		    	String ip = parts[2];
		    	int port = Integer.parseInt(parts[3]);
		    	this.chord.notify(new InetSocketAddress(ip, port));
		    }
		    
		    socket.close();
		    
		    
		}catch(Exception e) {
			e.printStackTrace();
		}

	}

}
