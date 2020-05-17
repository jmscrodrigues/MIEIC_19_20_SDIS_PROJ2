package Peer;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

public class TestApp {

	public static void main(String[] args) {
		
		if (args.length < 3) {
			System.out.println("Usage: TestApp <peer_ip>:<peer_port> op [args]");
			System.exit(0);
		}
		
		String[] ip_ports = args[0].split(":");
		String ip = ip_ports[0];
		int port = Integer.parseInt(ip_ports[1]);
		
		Socket socket = null;
		try {
			socket = new Socket(ip, port);
        } catch (IOException e) {
            System.err.println("Could not connect to peer");
            e.printStackTrace();
            System.exit(-1);
        }
		
		String op = args[1];
		
		String toSend = null;


		switch (op) {
			case "PUT": {
				String key = args[2];
				String value = args[3];
				toSend = "PUT " + key + " " + value;
				break;
			}
			case "GET": {
				String key = args[2];
				toSend = "GET " + key;
				break;
			}
			case "REMOVE": {
				String key = args[2];
				toSend = "REMOVE " + key;
				break;
			}
			default: {
				System.out.print("Not a valid operation\n");
				return;
			}

		}
		
		byte[] message = toSend.getBytes();
		try {
			OutputStream out = socket.getOutputStream(); 
		    DataOutputStream dos = new DataOutputStream(out);
		    dos.writeInt(message.length);
		    dos.write(message,0,message.length);
		} catch(IOException e) {
			System.err.println("Error sending message.");
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
		} catch(IOException e) {
            e.printStackTrace();
			System.err.println("Error reading message!");
            System.exit(-1);
		}
		
		String received = new String(buf, StandardCharsets.UTF_8);
		
		System.out.println(received);
	}
	
	
}
