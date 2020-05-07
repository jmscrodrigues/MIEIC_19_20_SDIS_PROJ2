package Chord;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;

public class ChordMessageHandler implements Runnable {
	Chord chord;
	Socket socket;
	
	ChordMessageHandler(Chord c, Socket s){
		this.chord = c;
		this.socket = s;
	}

	@Override
	public void run() {
		ChordMessage message = this.readSocket();

		if (message == null)
			return;

		switch (message.op) {
			case ChordOps.GET_SUCCESSOR: {
				this.chord.find_successor(message.key, message.ip, message.port, -1);
				break;
			}
			case ChordOps.FIND_FINGER: {
				this.chord.find_successor(message.key, message.ip, message.port, message.index);
				break;
			}
			case ChordOps.SET_FINGER: {
				this.chord.setFinger(message.index, new InetSocketAddress(message.ip, message.port));
				//System.out.println("SETFINGER_RECEIVED");
				break;
			}
			case ChordOps.SET_SUCCESSOR: {
				this.chord.setSuccessor(new InetSocketAddress(message.ip, message.port));
				break;
			}
			case ChordOps.SET_PREDECCESSOR: {
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
			case ChordOps.LOOKUP: {
				InetSocketAddress ret = this.chord.lookup(message.key);
				this.writeToSocket("LOOKUPRET " + ret.getHostName() + " " + ret.getPort());
				break;
			}
			case ChordOps.PUT: {
				Message m = new Message(message.buf);
				m.getHeaderAndBody();
				this.chord.putInMemory(message.key, m.body);
				break;
			}
			case ChordOps.GET: {
				Message m = new Message(message.buf);
				m.getHeaderAndBody();
				byte[] toSend = this.chord.getInMemory(message.key);
				if (toSend == null)
					toSend = "".getBytes();

				this.writeToSocket(toSend);
				break;
			}
			case ChordOps.GET_DATA: {
				this.chord.sendData(message.key, message.ip, message.port);
				break;
			}
			case ChordOps.GET_PREDECCESSOR: {
				InetSocketAddress pre = this.chord.getPredecessor();
				//Message out = new Message("PREDECESSOR " + pre.getHostName() + " " + pre.getPort());
				//out.sendMessage(socket);
				System.out.println("Sending Predecessor");
				writeToSocket("PREDECCESSOR " + pre.getHostName() + " " + pre.getPort());
				break;
			}
			case ChordOps.NOTIFY: {
				this.chord.notify(new InetSocketAddress(message.ip, message.port));
				break;
			}
			case ChordOps.REMOVE: {
				Message m = new Message(message.buf);
				m.getHeaderAndBody();

				byte[] toSend = this.chord.removeInMemory(message.key);
				if(toSend == null) 
					toSend = "".getBytes();
				
				this.writeToSocket(toSend);
				break;
			}
		}

		try {
			socket.close();
		} catch (IOException e) {
			System.err.print("Failed to close socket\n");
		}

	}

	private ChordMessage readSocket() {
		int len = 0;
		byte[] buf = null;

		try {
			DataInputStream dis = new DataInputStream(socket.getInputStream());
			len = dis.readInt();
			buf = new byte[len];
			if (len > 0) dis.readFully(buf);

		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}

		return new ChordMessage(buf);
	}

	private void writeToSocket(String message) {
		this.writeToSocket(message.getBytes());
	}

	private void writeToSocket(byte[] message) {
		try {
			DataOutputStream dos = new DataOutputStream(socket.getOutputStream());

			dos.writeInt(message.length);
			dos.write(message, 0, message.length);

		} catch (IOException e) {
			System.err.println("Error sending message.");
			e.printStackTrace();
			System.exit(-1);
		}
	}



}
