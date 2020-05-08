package SSLEngine;

import java.io.IOException;
import java.net.InetSocketAddress;

public class SSLMessage {

	byte[] message;
	
	String remoteAddress;
	int port;
	
	SSLClient client;
	
	private SSLMessage(String remoteAddress, int port) {
		this.remoteAddress = remoteAddress;
		this.port = port;
	}
	
	public SSLMessage(InetSocketAddress addrss, byte[] m){
		this(addrss.getHostName(),addrss.getPort());
		this.message = m;
	}
	
	public SSLMessage(InetSocketAddress addrss, String m){
		this(addrss.getHostName(),addrss.getPort());
		this.message = m.getBytes();
	}
	
	public SSLMessage(String remoteAddress, int port, byte[] m){
		this(remoteAddress,port);
		this.message = m;
	}
	public SSLMessage(String remoteAddress, int port, String m){
		this(remoteAddress,port);
		this.message = m.getBytes();
	}
	
	private void buildClient() {
		try {
			client = new SSLClient("TLSv1.2", this.remoteAddress, this.port);
			client.connect();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public byte[] sendMessage() {
		System.out.println("Message to send " + new String(message));
		byte[] data = null;
		this.buildClient();
		if(client == null) {
			System.out.println("Could not connect to client");
			return null;
		}
		try {
			client.write(message);
			client.shutdown();
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return data;
	}
	
	public byte[] sendMessageAndRead() {
		byte[] data = null;
		this.buildClient();
		if(client == null) {
			System.out.println("Could not connect to client");
			return null;
		}
		try {
			client.write(message);
			data = client.read();
			client.shutdown();
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return data;
	}
	
}
