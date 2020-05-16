package Chord;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class Message {
	
	static final String CRLF = String.valueOf((char)0xD) + String.valueOf((char)0xA) ;
	
	byte[] data;
	
	byte[] header;
	byte[] body;
	
	Message(String d){
		this.data = d.getBytes();
	}
	Message(byte[] d){
		this.data = d;
	}
	Message(String d, byte [] body){
		String header = d + " " + CRLF + CRLF;
		byte[] headerB = header.getBytes();
		data = new byte[headerB.length + body.length];
		System.arraycopy(headerB, 0, data, 0, headerB.length);
		System.arraycopy(body, 0, data, headerB.length, body.length);
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
	public void sendMessage(InetSocketAddress address) {
		this.sendMessage(address.getHostName(),address.getPort());
	}
	
	public void sendMessage(Socket socket) {	
		byte[] message = data;
		SendMessageToSocket(socket, message);
	}
	
	public void sendMessage(String ip, int port) {
		Socket socket = null;
		try {
			socket = new Socket(ip, port);
        } catch (IOException e) {
            System.err.println("Could not connect to peer");
            System.err.println(e);
            System.exit(-1);
        }
		
		byte[] message = data;
		SendMessageToSocket(socket, message);
	}

	private void SendMessageToSocket(Socket socket, byte[] message) {
		try {
			OutputStream out = socket.getOutputStream();
		    DataOutputStream dos = new DataOutputStream(out);
		    dos.writeInt(message.length);
		    dos.write(message,0,message.length);
		    socket.close();
		}catch(IOException e) {
			System.err.println("Error sending message.");
            System.err.println(e);
            e.printStackTrace();
            System.exit(-1);
		}
	}

	public byte[] sendAndReceive(InetSocketAddress address) {
		return this.sendAndReceive(address.getHostName(),address.getPort());
	}
	
	public byte[] sendAndReceive(String ip, int port) {
		Socket socket = null;
		try {
			socket = new Socket(ip, port);
        } catch (IOException e) {
            System.err.println("Could not connect to peer");
            System.err.println(e);
            System.exit(-1);
        }
		
		byte[] message = data;
		try {
			OutputStream out = socket.getOutputStream(); 
		    DataOutputStream dos = new DataOutputStream(out);
		    dos.writeInt(message.length);
		    dos.write(message,0,message.length);
		}catch(IOException e) {
			System.err.println("Error sending message.");
            System.err.println(e);
            e.printStackTrace();
            System.exit(-1);
		}
		byte[] buf = null;
		try {
			InputStream in = socket.getInputStream();
		    DataInputStream dis = new DataInputStream(in);
		    int len = dis.readInt();
		    buf = new byte[len];
		    if (len > 0) {
		        dis.readFully(buf);
		    }
		    socket.close();
		}catch(IOException e) {
            e.printStackTrace();
			System.err.println("Error reading message!");
            System.err.println(e);
            e.printStackTrace();
            System.exit(-1);
		}	
		return buf;
	}
	
	public InetSocketAddress lookup(String ip, int port) {
		byte[] buf = this.sendAndReceive(ip, port);
		String received = new String(buf, StandardCharsets.UTF_8);
	    String[] parts = received.split(" ");
	    return new InetSocketAddress(parts[1],Integer.parseInt(parts[2]));
	}
	
	
	
	
}
