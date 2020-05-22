package Chord;
import javax.net.ssl.SSLSocket;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.Arrays;

public class ChordMessageHandler implements Runnable {
	
	static final String CRLF = "\r\n" ;
	
	Chord chord;
	SSLSocket socket;

	ChordMessage message;
	
	private boolean debug = false;
	
	ChordMessageHandler(Chord c, SSLSocket s){
		this.chord = c;
		this.socket = s;

		if(debug) System.out.print("New connection established\n");

		this.message = this.readSocket();
	}

	@Override
	public void run() {
		if (message == null)
			return;

		if(debug) System.out.print("Chord handling message: " + new String(message.data) + "\n" + "Operation: " + message.op + "\n");

		byte [] toSend = "SUCCESS".getBytes();
		switch (message.op) {
			case ChordOps.LOOKUP: {
				InetSocketAddress ret = this.chord.lookup(message.key);
				if (ret != null)
					toSend = ("LOOKUPRET " + this.chord.getName() + " " + this.chord.getPort() + " " + ret.getHostName() + " " + ret.getPort()).getBytes();
				else
					toSend = "ERROR".getBytes();
				break;
			}
			case ChordOps.GET_SUCCESSOR: {
				this.chord.find_successor(message.key, message.ip, message.port);
				break;
			}
			case ChordOps.SET_SUCCESSOR: {
				this.chord.setSuccessor(new InetSocketAddress(message.ip, message.port));
				break;
			}
			case ChordOps.GET_PREDECESSOR: {
				InetSocketAddress pre = this.chord.getPredecessor();
				toSend = ("PREDECESSOR " + pre.getHostName() + " " + pre.getPort()).getBytes();
				System.out.println("Sending Predecessor");
				break;
			}
			case ChordOps.SET_PREDECESSOR: {
				this.chord.setPredecessor(new InetSocketAddress(message.ip, message.port));
				break;
			}
			case ChordOps.NEW_FINGER: {
				System.out.println("here1");
				this.chord.foundNewFinger(new InetSocketAddress(message.ip, message.port));
				System.out.println("here2");
				this.chord.sendNotifyNewFinger(message.key, message.ip, message.port);
				System.out.println("here3");
				break;
			}
			case ChordOps.DELETE_FINGER: {
				this.chord.deleteFinger(message.oldKey, new InetSocketAddress(message.ip, message.port));
				this.chord.sendNotifyDeleteFinger(message.key, message.oldKey, message.ip, message.port);
				break;
			}
			case ChordOps.PUT: {
				if (this.chord.getMemory().canStoreChunk(message.body.length)
						&& !this.chord.getMemory().isStoredHere(message.key)
						&& !this.chord.getMemory().wasInitiatedHere(message.key)) {
					this.chord.putInMemory(message.key, message.body);
					if (message.replication > 1)
						this.chord.putInSuccessor(message.key, message.oldKey, message.body,message.replication - 1);
				} else {
					if(this.chord.getMemory().canStoreChunk(message.body.length) == false)
						System.out.println("No space for chunk, redirecting");
					if(this.chord.getMemory().isStoredHere(message.key))
						System.out.println("Chunk already stored, redirecting");
					if(this.chord.getMemory().wasInitiatedHere(message.key))
						System.out.println("Chunk backedup from here, redirecting");
					this.chord.putInSuccessor(message.key, message.oldKey, message.body, message.replication);
				}
				toSend = null;
				break;
			}
			case ChordOps.GET: {
				if(this.chord.getMemory().isStoredHere(message.key)) {
					toSend = this.chord.getInMemory(message.key);
					if(toSend == null) {
						if (this.chord.getMemory().chunkRedirected(message.key) && message.replication > 1)
							toSend = this.chord.getFromSuccessor(message.key,message.replication - 1);
					}else {
						String header = "VALID" + " " + CRLF + CRLF;
						byte[] headerB = header.getBytes();
				        byte [] temp = new byte[headerB.length + toSend.length];

				        System.arraycopy(headerB, 0, temp, 0, headerB.length);
				        System.arraycopy(toSend, 0, temp, headerB.length, toSend.length);
				        toSend = temp;
					}
					
					if(toSend == null) {
						toSend = new String("FAIL" + " " + CRLF + CRLF).getBytes();
					}
						
				}else {
					if (this.chord.getMemory().chunkRedirected(message.key))
						toSend = this.chord.getFromSuccessor(message.key, message.replication);
				}
				/*if (!this.chord.getMemory().chunkRedirected(message.key))
					toSend = this.chord.getInMemory(message.key);
				else
					toSend = this.chord.getFromSuccessor(message.key, message.replication);*/

				if (toSend == null) {
					toSend = "ERROR".getBytes();
				}
				break;
			}
			case ChordOps.REMOVE: {
				if (!this.chord.getMemory().chunkRedirected(message.key)) {
					toSend = this.chord.removeInMemory(message.key);
					if (message.replication > 1)
						this.chord.removeFromSuccessor(message.key,message.replication - 1);
				} else {
					System.out.println("Chunk redirected, removing here if exists and from sucessor");
					toSend = this.chord.removeInMemory(message.key);
					if(toSend == null)
						this.chord.removeFromSuccessor(message.key,message.replication);
					else if (message.replication > 1)
						this.chord.removeFromSuccessor(message.key,message.replication - 1);
				}

				/*if (toSend == null)
					toSend = "ERROR".getBytes();*/
				toSend = null;
				break;
			}
			case ChordOps.GET_DATA: {
				this.chord.sendData(message.key, message.ip, message.port);
				break;
			}
			case ChordOps.NOTIFY: {
				this.chord.notify(new InetSocketAddress(message.ip, message.port));
				break;
			}
		}
		
		if(toSend != null)
			this.writeToSocket(toSend);

		try {
			socket.close();
		} catch (IOException e) {
			System.err.print("Failed to close socket\n");
		}
	}

	private ChordMessage readSocket() {
		int len;
		byte[] buf = null;;
		if(debug) System.out.println("reading from socket");
		try {
			DataInputStream dis = new DataInputStream(socket.getInputStream());
			len = dis.readInt();
			buf = new byte[len];
			if (len > 0) dis.readFully(buf);


		} catch (IOException e) {
			System.err.println("Error reading message.");
			e.printStackTrace();
			//return null;
		}
		if(debug) System.out.println("done reading from socket :" + new String(buf));
		return new ChordMessage(buf);
	}

	private void writeToSocket(byte[] message) {
		if(debug) System.out.println("to respond: " + new String(message));
		try {
			DataOutputStream dos = new DataOutputStream(socket.getOutputStream());

			dos.writeInt(message.length);
			dos.write(message, 0, message.length);

		} catch (IOException e) {
			System.err.println("Error writing message.");
			e.printStackTrace();
			System.exit(-1);
		}
		if(debug) System.out.println("done responding");
	}

}
