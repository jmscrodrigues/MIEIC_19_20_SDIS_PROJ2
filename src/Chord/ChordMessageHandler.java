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

			switch (op) {
				case "GETSUCCESSOR": {
					int key = Integer.parseInt(parts[1]);
					String ip = parts[2];
					int port = Integer.parseInt(parts[3]);
					this.chord.find_successor(key, ip, port, -1);
					break;
				}
				case "FINDFINGER": {
					int key = Integer.parseInt(parts[1]);
					String ip = parts[2];
					int port = Integer.parseInt(parts[3]);
					int index = Integer.parseInt(parts[4]);
					this.chord.find_successor(key, ip, port, index);
					break;
				}
				case "SETFINGER": {
					String ip = parts[2];
					int port = Integer.parseInt(parts[3]);
					int index = Integer.parseInt(parts[4]);
					this.chord.setFinger(index, new InetSocketAddress(ip, port));
					//System.out.println("SETFINGER_RECEIVED");
					break;
				}
				case "SETSUCCESSOR": {
					String ip = parts[2];
					int port = Integer.parseInt(parts[3]);
					InetSocketAddress successor = new InetSocketAddress(ip, port);
					this.chord.setSuccessor(successor);
					break;
				}
				case "SETPREDECESSOR": {
					String ip = parts[2];
					int port = Integer.parseInt(parts[3]);
					InetSocketAddress successor = new InetSocketAddress(ip, port);
					this.chord.setPredecessor(successor);
					break;
				}
				case "NEWFINGER": {
					int originKey = Integer.parseInt(parts[1]);
					String ip = parts[2];
					int port = Integer.parseInt(parts[3]);
					this.chord.foundNewFinger(new InetSocketAddress(ip, port));
					this.chord.sendNotifyNewFinger(originKey, ip, port);
					break;
				}
				case "DELETEFINGER": {
					int exitKey = Integer.parseInt(parts[1]);
					int oldKey = Integer.parseInt(parts[2]);
					String ip = parts[3];
					int port = Integer.parseInt(parts[4]);
					this.chord.deleteFinger(oldKey, new InetSocketAddress(ip, port));
					this.chord.sendNotifyDeleteFinger(exitKey, oldKey, ip, port);
					break;
				}
				case "LOOKUP": {
					int key = Integer.parseInt(parts[1]);
					InetSocketAddress ret = this.chord.lookup(key);
					byte[] toSend = ("LOOKUPRET " + ret.getHostName() + " " + ret.getPort()).getBytes();
					SendMessage(toSend);
					break;
				}
				case "PUT": {
					int key = Integer.parseInt(parts[1]);
					Message m = new Message(buf);
					m.getHeaderAndBody();
					this.chord.putInMemory(key, m.body);
					break;
				}
				case "GET": {
					int key = Integer.parseInt(parts[1]);
					Message m = new Message(buf);
					m.getHeaderAndBody();
					byte[] toSend = this.chord.getInMemory(key);
					if (toSend == null) {
						toSend = "".getBytes();
					}
					SendMessage(toSend);
					break;
				}
				case "REMOVE": {
					int key = Integer.parseInt(parts[1]);
					Message m = new Message(buf);
					m.getHeaderAndBody();
					byte[] toSend = this.chord.removeInMemory(key);
					if (toSend == null) {
						toSend = "".getBytes();
					}
					SendMessage(toSend);
					break;
				}
				case "GETDATA": {
					int key = Integer.parseInt(parts[1]);
					String ip = parts[2];
					int port = Integer.parseInt(parts[3]);
					this.chord.sendData(key, ip, port);
					break;
				}
				case "GETPREDECESSOR": {
					InetSocketAddress pre = this.chord.getPredecessor();
					//Message out = new Message("PREDECESSOR " + pre.getHostName() + " " + pre.getPort());
					//out.sendMessage(socket);
					byte[] toSend = ("PREDECESSOR " + pre.getHostName() + " " + pre.getPort()).getBytes();
					System.out.println("Sending Predecessor");
					SendMessage(toSend);
					break;
				}
				case "NOTIFY": {
					String ip = parts[2];
					int port = Integer.parseInt(parts[3]);
					this.chord.notify(new InetSocketAddress(ip, port));
					break;
				}
			}
		    
		    socket.close();
		    
		    
		}catch(Exception e) {
			e.printStackTrace();
		}

	}

	private void SendMessage(byte[] toSend) {
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

}
