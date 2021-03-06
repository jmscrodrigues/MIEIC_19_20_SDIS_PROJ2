package Peer;

import Chord.ChordOps;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

public class TestApp {

	private final String peer_ip;
	private final int peer_port;
	private final String op;
	private final String[] args;

	private Socket peer_socket;

	public TestApp(String peer_ip, int peer_port, String op, String[] args) {
		this.peer_ip = peer_ip;
		this.peer_port = peer_port;
		this.op = op;
		this.args = args;
	}

	public static void main(String[] args) {
		if (args.length < 2) {
			System.out.println("Usage: TestApp <peer_ip>:<peer_port> op [args]");
			System.exit(0);
		}
		
		String[] ip_ports = args[0].split(":");
		String ip = ip_ports[0];
		int port = Integer.parseInt(ip_ports[1]);

		TestApp testApp = new TestApp(ip, port, args[1], args);

		String operation = testApp.buildOperation();
		testApp.openPeerSocket();
		String response = testApp.executeOperation(operation);

		System.out.println(response);
	}
	
	private void openPeerSocket() {
		try {
			this.peer_socket = new Socket(this.peer_ip, this.peer_port);
		} catch (IOException e) {
			System.err.println("Could not connect to peer");
			e.printStackTrace();
			System.exit(-1);
		}
	}
	
	private String buildOperation() {
		String toSend = null;
		
		switch (op.toUpperCase()) {
			case PeerOps.PUT: {
				if (args.length != 4) {
					System.out.println("Usage: TestApp <peer_ip>:<peer_port> PUT <key> <value>");
					System.exit(0);
				}

				String key = args[2];
				String value = args[3];

				toSend = ChordOps.PUT + " " + key + " " + value;
				break;
			}
			case PeerOps.GET:
			case PeerOps.REMOVE: {
				if (args.length != 3) {
					System.err.println("Usage: TestApp <peer_ip>:<peer_port> " + op + " <key>");
					System.exit(-1);
				}
				String key = args[2];
				toSend = op + " " + key;
				break;
			}
			case PeerOps.BACKUP: {
				if (args.length != 4) {
					System.err.println("Usage: TestApp <peer_ip>:<peer_port> " + op + " <filename> <replication_degree>");
					System.exit(-1);
				}
				String file = args[2];
				int replication = Integer.parseInt(args[3]);
				toSend = op + " " + file + " " + replication;
				break;
			}
			case PeerOps.RESTORE:
			case PeerOps.DELETE: {
				if (args.length != 3) {
					System.err.println("Usage: TestApp <peer_ip>:<peer_port> " + op + " <filename>");
					System.exit(-1);
				}
				String file = args[2];
				toSend = op + " " + file;
				break;
			}
			case PeerOps.STATUS: {
				if (args.length != 2) {
					System.err.println("Usage: TestApp <peer_ip>:<peer_port> op");
					System.exit(-1);
				}
				toSend = op;
				break;
			}
			case PeerOps.RECLAIM: {
				if (args.length != 3) {
					System.err.println("Usage: TestApp <peer_ip>:<peer_port> " + op + "<reclaim_space>");
					break;
				}
				int space = Integer.parseInt(args[2]);
				toSend = op + " " + space;
				break;
			}
			default: {
				System.err.print("Not a valid operation\n");
				System.exit(-1);
			}
		}
		return toSend;
	}

	private String executeOperation(String toSend) {

		byte[] message = toSend.getBytes();
		try {
			DataOutputStream dos = new DataOutputStream(peer_socket.getOutputStream());
			dos.writeInt(message.length);
			dos.write(message,0,message.length);
		} catch(IOException e) {
			System.err.println("Error sending message.");
			e.printStackTrace();
			System.exit(-1);
		}

		byte[] buf = null;
		try {
			DataInputStream dis = new DataInputStream(peer_socket.getInputStream());
			int len = dis.readInt();
			buf = new byte[len];
			if (len > 0)
				dis.readFully(buf);

			peer_socket.close();

		} catch(IOException e) {
			e.printStackTrace();
			System.err.println("Error reading message!");
			System.exit(-1);
		}

		return new String(buf, StandardCharsets.UTF_8);
	}


}
