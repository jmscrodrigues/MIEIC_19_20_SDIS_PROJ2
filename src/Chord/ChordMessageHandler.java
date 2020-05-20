package Chord;
import javax.net.ssl.SSLSocket;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.Arrays;

public class ChordMessageHandler implements Runnable {
	Chord chord;
	SSLSocket socket;

	ChordMessage message;
	
	ChordMessageHandler(Chord c, SSLSocket s){
		this.chord = c;
		this.socket = s;

		System.out.print("New connection established\n");

		this.message = this.readSocket();
	}

	@Override
	public void run() {
		if (message == null)
			return;

		System.out.print("Chord handling message: " + new String(message.data) + "\n" +
			"Operation: " + message.op + "\n");

		byte [] toSend = "SUCCESS".getBytes();
		switch (message.op) {
			case ChordOps.LOOKUP: {
				System.out.println("a fazer lookup");
				InetSocketAddress ret = this.chord.lookup(message.key);
				if (ret != null)
					toSend = ("LOOKUPRET " + this.chord.getName() + " " + this.chord.getPort() + " " + ret.getHostName() + " " + ret.getPort()).getBytes();
				else
					toSend = "ERROR".getBytes();
				System.out.println("a sair lookup");
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
				this.chord.foundNewFinger(new InetSocketAddress(message.ip, message.port));
				this.chord.sendNotifyNewFinger(message.key, message.ip, message.port);
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
						this.chord.putInSuccessor(message.key, message.body,message.replication - 1);
				} else {
					System.out.println("Could not store file, redirecting");
					this.chord.putInSuccessor(message.key, message.body, message.replication);
				}
				break;
			}
			case ChordOps.GET: {
				if (!this.chord.getMemory().chunkRedirected(message.key))
					toSend = this.chord.getInMemory(message.key);
				else
					toSend = this.chord.getFromSuccessor(message.key, message.replication);

				if (toSend == null) {
					if (message.replication > 1)
						this.chord.getFromSuccessor(message.key,message.replication - 1);

					toSend = "ERROR".getBytes();
				}
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
			case ChordOps.REMOVE: {
				if (!this.chord.getMemory().chunkRedirected(message.key)) {
					toSend = this.chord.removeInMemory(message.key);
					if (message.replication > 1)
						this.chord.removeFromSuccessor(message.key,message.replication - 1);
				} else {
					toSend = this.chord.removeInMemory(message.key);
					if (message.replication > 1)
						toSend = this.chord.removeFromSuccessor(message.key,message.replication - 1);
				}

				if (toSend == null)
					toSend = "ERROR".getBytes();

				break;
			}
		}

		this.writeToSocket(toSend);

		try {
			socket.close();
		} catch (IOException e) {
			System.err.print("Failed to close socket\n");
		}
	}

	private ChordMessage readSocket() {
		int len;
		byte[] buf;
		System.out.println("reading from socket");
		try {
			DataInputStream dis = new DataInputStream(socket.getInputStream());
			len = dis.readInt();
			buf = new byte[len];
			if (len > 0) dis.readFully(buf);

		} catch (IOException e) {
			System.err.println("Error reading message.");
			e.printStackTrace();
			return null;
		}
		System.out.println("done reading from socket :" + new String(buf));
		return new ChordMessage(buf);
	}

	private void writeToSocket(byte[] temp) {
		byte[] message = new byte[temp.length+1];
		System.arraycopy(temp, 0, message, 0, temp.length);
		message[message.length-1] = 0;
		System.out.println("to respond: " + new String(message));
		try {
			OutputStream out = this.socket.getOutputStream(); 
    	    DataOutputStream dos = new DataOutputStream(out);
    	    dos.writeInt(message.length);
    	    dos.write(message,0,message.length);
    	    dos.flush();

		} catch (IOException e) {
			System.err.println("Error writing message.");
			e.printStackTrace();
			System.exit(-1);
		}
		System.out.println("done responding");
	}

}
