package Chord;

import Peer.Peer;

import java.io.*;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;


public class Chord {
	
	static final int M = 15;
	static final int R = 30;
	static final int UPDATE_TIME = 10;
	
	private final Peer peer;
	
	private final InetSocketAddress selfAddress;
	private InetSocketAddress predecessor;
	private InetSocketAddress access_peer;

	private ChordServer chordServer;
	
	private final ConcurrentHashMap<Integer,InetSocketAddress> fingerTable = new ConcurrentHashMap<>();
	private final ConcurrentHashMap<Integer,InetSocketAddress> successorList = new ConcurrentHashMap<>();

	int port;
	private int key;
	
	private AtomicBoolean connected = new AtomicBoolean(false);
	
    private Memory memory;
    
    int nextFinger = 0;
    int nextSuccessor = 0;
	
	public Chord(Peer p , int port) {
		this.selfAddress = new InetSocketAddress("localhost" , port);
		this.setKey(this.hash(selfAddress));
		try {
			ObjectInputStream ois = new ObjectInputStream(new FileInputStream("./peer" + key + "/" + key + ".ser"));

			memory = (Memory) ois.readObject();

			ois.close();
			System.out.println("Read memory from file");

		} catch (FileNotFoundException e) {
			memory = new Memory("./peer" + key);
			System.out.println("File not read from memory");
		} catch(IOException ioe) {
			ioe.printStackTrace();
		} catch (ClassNotFoundException cnfe) {
			cnfe.printStackTrace();
		}

		this.peer = p;
        this.port = port;

        this.chordServer = new ChordServer(this, port);
        try {
        	this.peer.getExecutor().execute(this.chordServer);
        } catch (Exception e) {
            System.err.println("Could not create the server");
            e.printStackTrace();
        }
		this.getPeer().getExecutor().schedule(new ChordStabilizer(this), UPDATE_TIME, TimeUnit.SECONDS);
        System.out.println("Chord initiated with key " + this.getKey());
    }
	
	public void joinRing(InetSocketAddress peer) {

		this.access_peer = peer;
		
		System.out.println("Started join process");
		
		String data = ChordOps.GET_SUCCESSOR + " " + this.getKey() + " " + this.selfAddress.getHostName() + " " +  this.selfAddress.getPort();
		SSLMessage m;
		try {
			m = new SSLMessage(this.access_peer);
			m.write(data);
			m.read();
			m.close();
		} catch (ConnectException e) {
			System.out.println("Could not connect to access peer");
			e.printStackTrace();
			System.exit(-1);
		}

		System.out.print("Awaiting connection...");
		while(!this.connected.get()) {
		}
		System.out.print("Connected!");
		
		this.updateFingerTable();
		this.updateSuccessors();
		
		System.out.println("Asking data to successor initiated");
		
		try {
			m = new SSLMessage(this.getSuccessor());
			m.write(ChordOps.GET_DATA + " " + this.getKey() + " " + this.selfAddress.getHostName() + " " +  this.selfAddress.getPort());
			m.read();
			m.close();
		} catch (ConnectException e) {
			System.out.println("Could not connect to successor peer");
			e.printStackTrace();
			System.exit(-1);
		}

		System.out.println("Node joined ring");
		
	}
	
	public void leaveRing() {
		System.out.println("Started leaving process");
		
		if(this.getSuccessor() != null && this.predecessor != null) {
			this.sendAndReadFromSuccessor(ChordOps.SET_PREDECESSOR + " " + this.hash(predecessor) + " " + this.predecessor.getHostName() + " " +  this.predecessor.getPort());
			
			SSLMessage m;
			try {
				m = new SSLMessage(this.predecessor);
				m.write(ChordOps.SET_SUCCESSOR + " " + this.hash(this.getSuccessor()) + " " + this.getSuccessor().getHostName() + " " +  this.getSuccessor().getPort());
				m.read();
				m.close();
				
				m = new SSLMessage(this.predecessor);
				m.write(ChordOps.DELETE_FINGER + " " + this.hash(predecessor) + " " + this.getKey() + " " + this.getSuccessor().getHostName() + " " +  this.getSuccessor().getPort() );
				m.read();
				m.close();
				System.out.println("Sent new delete to predecessor");
			} catch (ConnectException e) {
				System.out.println("Could not connect to predecessor");
				e.printStackTrace();
			}
			
			System.out.println("Transferring data to successor initiated");
			
			for (ConcurrentHashMap.Entry<Integer, Integer> entry : this.getMemory().getStoredChunks()) {
				int chunkId = entry.getKey();
				byte[] data = this.memory.get(chunkId);
				if(data != null)
					this.putInSuccessor(chunkId, this.key, this.memory.get(chunkId), 1);
				this.removeInMemory(chunkId);
			}
			
			System.out.println("Transferring data to successor done");	
			
			
		}
		
		FileOutputStream fileOut;
		ObjectOutputStream out;
		try {
			fileOut = new FileOutputStream(memory.getPath() + key +  ".ser");
			out = new ObjectOutputStream(fileOut);
			out.writeObject(memory);
			out.close();
			fileOut.close();
			System.out.println("Serialized data has been saved in " + memory.getPath() + key + ".ser");

		} catch (IOException e) {
			e.printStackTrace();
		}


		this.chordServer.stop();
		System.out.println("Node left ring");
	}
	
	
	/**
	 * 
	 * @param key Key of node joining
	 * @param ip Its ip
	 * @param port Its port
	 * @param 'type': -1-> Node joining  >=0: Updating fingerTable of index type
	 * @throws Exception
	 */
	public void find_successor(int key, String ip, int port) { 
		//se nao existir successor, significa que e o unico na rede
		if(this.getSuccessor() == null) {
			InetSocketAddress new_peer = new InetSocketAddress(ip,port);
			this.setPredecessor(new_peer);
			SSLMessage m;
			try {
				m = new SSLMessage(this.predecessor);
				this.setSuccessor(new_peer);
	    		m.write(ChordOps.SET_SUCCESSOR + " " + this.getKey() + " " + this.selfAddress.getHostName() + " " +  this.selfAddress.getPort());
	    		m.read();
	    		m.close();
	    		m = new SSLMessage(this.predecessor);
	    		m.write(ChordOps.SET_PREDECESSOR + " " + this.getKey() + " " + this.selfAddress.getHostName() + " " +  this.selfAddress.getPort());
	    		m.read();
	    		m.close();
			} catch (ConnectException e) {
				System.out.println("Could not connect to predecessor");
				e.printStackTrace();
			}
    		this.populateFingerTable(new_peer);
    		this.updateSuccessors();
			return;
		}
		//se a chave que se procura estiver entre mim e o meu successor, logo deu hit!
		if(this.betweenOpenClose(this.getKey(), this.hash(this.getSuccessor()), key)) {
			this.sendAndReadFromSuccessor(ChordOps.SET_PREDECESSOR + " " + key + " " + ip + " " +  port);
			
			SSLMessage m;
			try {
				m = new SSLMessage(ip, port);
				m.write(ChordOps.SET_SUCCESSOR + " " + this.hash(getSuccessor()) + " " + this.getSuccessor().getHostName() + " " +  this.getSuccessor().getPort());
	    		m.read();
	    		m.close();
	    		m = new SSLMessage(ip, port);
	    		m.write(ChordOps.SET_PREDECESSOR + " " + this.getKey() + " " + this.selfAddress.getHostName() + " " +  this.selfAddress.getPort());
	    		m.read();
	    		m.close();
			} catch (ConnectException e) {
				System.out.println("Could not connect to requesting peer");
				e.printStackTrace();
			}

			InetSocketAddress newFinger = new InetSocketAddress(ip, port);
			this.setSuccessor(newFinger);

    		//novo successor encontrado, por isso mudar os fingers
    		this.foundNewFinger(newFinger);
    		
		} else { //se a chave estiver para la do successor
			InetSocketAddress closest = closest_preceding_node(key);
			String data = ChordOps.GET_SUCCESSOR + " " + key + " " + ip + " " +  port;
			SSLMessage m;
			try {
				m = new SSLMessage(closest);
				m.write(data);
				m.read();
				m.close();
			} catch (ConnectException e) {
				System.out.println("Could not connect to closest");
				e.printStackTrace();
			}
		
		}
	}	
	
	public InetSocketAddress lookup(int key) {
		if(this.getSuccessor() == null) {
			return this.selfAddress;
		} else if (this.betweenOpenClose(this.getKey(), this.hash(getSuccessor()), key)) {
			return this.getSuccessor();
		} else {
			InetSocketAddress closest = closest_preceding_node(key);
			SSLMessage m;
			byte [] data = null;
			try {
				m = new SSLMessage(closest.getHostName(), closest.getPort());
				m.write(ChordOps.LOOKUP + " " + key);
				data = m.read();
				m.close();
			} catch (ConnectException e) {
				System.out.println("Could not connect to closest");
				return null;
			}
    		String [] parts = new String(data).split(" ");
			return new InetSocketAddress(parts[3],Integer.parseInt(parts[4].replaceAll("\\D", "")));
		}
	}
	
	public int put(String identifier, byte[] data ,  int replication) {
		int key = this.hash(identifier);
		InetSocketAddress dest = this.lookup(key);
		SSLMessage m;
		try {
			m = new SSLMessage(dest);
			m.write(ChordOps.PUT + " " + key + " " + key + " "  + replication, data);
			m.close();
		} catch (ConnectException e) {
			System.out.println("Could not connect to destiny");
		}
		return key;
	}
	
	public byte[] get(String identifier, int replication) {
		int key = this.hash(identifier);
		InetSocketAddress dest = this.lookup(key);
		SSLMessage m;
		byte[] data = null;
		try {
			m = new SSLMessage(dest);
			m.write(ChordOps.GET + " " + key + " " + replication);
			data =  m.read();
			m.close();
		} catch (ConnectException e) {
			System.out.println("Could not connect to destiny");
			return null;
		}
		ChordMessage message = new ChordMessage(data);
		if(message.op.equals("FAIL"))
			return null;
		return message.body;
	}
	
	public void remove(String identifier , int replication) {
		int key = this.hash(identifier);
		InetSocketAddress dest = this.lookup(key);
		SSLMessage m;
		byte[] data = null;
		try {
			m = new SSLMessage(dest);
			m.write(ChordOps.REMOVE + " " + key + " " + replication);
			m.close();
		} catch (ConnectException e) {
			System.out.println("Could not connect to destiny");
		}
	}
	
	public void putInMemory(int key, byte[] data) {
		if(this.getMemory().chunkRedirected(key))
			this.getMemory().removeRedirectedChunk(key);
		this.getMemory().put(key, data);
	}
	
	public byte[] getInMemory(int key) {
		return this.getMemory().get(key);
	}
	
	public byte[] removeInMemory(int key) {
		return this.getMemory().remove(key);
	}
	
	public void putInSuccessor(int key, int originKey, byte[] body, int replication) {
		if(this.getSuccessor() == null)
			return;
		System.out.println("Sending to successor");
		if(this.betweenOpenClose(this.key, this.hash(getSuccessor()), originKey)) { // If it has made a complete turn around the ring
			System.out.println("Could not sent, full turn -> " + this.key + " " + this.hash(getSuccessor()) + "  + originkey");
			return;
		}
		this.memory.addRedirectedChunk(key,replication);
		this.sendToSuccessor(ChordOps.PUT + " " + key + " " + originKey + " "  + replication, body);
	}
	public byte[] getFromSuccessor(int key, int replication) {
		if(this.getSuccessor() == null)
			return null;
		byte[] d = this.sendAndReadFromSuccessor(ChordOps.GET + " " + key + " " + replication);
		return d;
	}
	public void removeFromSuccessor(int key, int replication) {
		if(this.getSuccessor() == null)
			return;
		this.sendToSuccessor(ChordOps.REMOVE + " " + key + " " + replication);
		this.memory.removeRedirectedChunk(key);
		return;
	}

	/*
	 * Send data to node that has just joined
	 */
	public void sendData(int key, String ip, int port) {
		InetSocketAddress pre = new InetSocketAddress(ip,port);
		for (ConcurrentHashMap.Entry<Integer, Integer> entry : this.getMemory().getStoredChunks()) {		
			int chunkId = entry.getKey();
			if (betweenOpenClose(this.getKey(), key, chunkId)) {
				byte[] data = this.memory.get(chunkId);
				if(data != null) {
					SSLMessage m;
					try {
						m = new SSLMessage(pre);
						m.write(ChordOps.PUT + " " + chunkId + " " + this.key + " "  + "1", this.memory.get(chunkId));
						m.read();
						m.close();
					} catch (ConnectException e) {
						System.out.println("Could not send data to predecessor");
					}
				}
				this.removeInMemory(chunkId);
			}
		}
	}
	
	/*
	 * Get predecessor of node passed as argument
	 */
	public InetSocketAddress getPredecessor(InetSocketAddress node) {
		byte [] buf = this.sendAndReadFromSuccessor(ChordOps.GET_PREDECESSOR + " " + this.getKey() + " " + this.selfAddress.getHostName() + " " +  this.selfAddress.getPort());

		String message = new String(buf, StandardCharsets.UTF_8);
	    System.out.println(message);
	    String[] parts = message.split(" ");

		InetSocketAddress ret = new InetSocketAddress(parts[1], Integer.parseInt(parts[2]));
	    System.out.println("Get predecessor done!");
		return ret;
	}
	
	public void notify(InetSocketAddress pre) {
		if(this.predecessor == null || betweenOpenOpen(hash(this.predecessor),this.getKey(),hash(pre))) {
			this.setPredecessor(pre);
		}
	}
	
	
	public void stabilize() {
		if(this.getSuccessor() == null)
			return;
		InetSocketAddress x = this.getPredecessor(this.getSuccessor());
		if(betweenOpenOpen(this.getKey(),hash(this.getSuccessor()),hash(x))) {
			this.setSuccessor(x);
		}
		this.sendAndReadFromSuccessor(ChordOps.NOTIFY + " " + this.getKey() + " " + this.selfAddress.getHostName() + " " +  this.selfAddress.getPort());
	}
	
	public void foundNewFinger(InetSocketAddress finger) {
		int key = this.hash(finger);
		System.out.println(key);
		for (int i = 0; i < M; i++) {
			System.out.println(i);
			int prev_key = this.hash(this.fingerTable.get(i));
			if(betweenOpenOpen(this.getKey(), prev_key, key)) {
				this.fingerTable.replace(i, finger);
			}
		}
	}
	
	public void deleteFinger(int oldKey, InetSocketAddress finger) {
		for(int i = 0; i < M; i++) {
			if (hash(this.fingerTable.get(i)) == oldKey)
				this.fingerTable.replace(i, finger);
		}
		this.printFingerTable();
	}
	
	public void sendNotifyNewFinger(int originKey , String ip, int port) {
		int pred_key = this.hash(this.predecessor);
		
		System.out.println(this.positiveModule((int) (originKey - Math.pow(2, M-1)), (int)  Math.pow(2, M)) + "  " + originKey + " " + pred_key);

		if (!betweenOpenOpen( this.positiveModule((int) (originKey - Math.pow(2, M-1)), (int)  Math.pow(2, M)) , originKey , pred_key ))
			return;
		SSLMessage m;
		try {
			m = new SSLMessage(this.predecessor);
			m.write(ChordOps.NEW_FINGER + " " + originKey + " " + ip + " " +  port + " ");
			m.read();
			m.close();
		} catch (ConnectException e) {
			System.out.println("Could not connect to predecessor");
		}
		System.out.println("Sent new finger to predecessor");
	}
	
	public void sendNotifyDeleteFinger(int originKey , int oldKey, String ip, int port) {
		if(this.predecessor == null)
			return;
		int pred_key = this.hash(this.predecessor);
		
		int max_origin = this.positiveModule((int) (originKey - Math.pow(2, M-1)), (int)  Math.pow(2, M));
		System.out.println(max_origin + "  " + originKey + " " + pred_key);
		if (!betweenOpenOpen( max_origin, originKey ,pred_key ))
			return;

		SSLMessage m;
		try {
			m = new SSLMessage(this.predecessor);
			m.write(ChordOps.DELETE_FINGER + " " + originKey + " " + oldKey + " " + ip + " " +  port + " ");
			m.read();
			m.close();
		} catch (ConnectException e) {
			System.out.println("Could not connect to predecessor");
		}
		System.out.println("Sent new delete to predecessor");
	}
	
	public void updateFingerTable() {
		System.out.println("Going to update fingers");
		InetSocketAddress finger = null;
		for(int i = 0; i < M; i++) {
			int index = this.positiveModule((int) (this.getKey() + Math.pow(2, i)), (int)  Math.pow(2, M));
			if(finger == null || !betweenOpenOpen(this.getKey(),hash(finger),index))
				finger = this.lookup(index);
			this.fingerTable.put(i, finger);
		}
	}
	
	public void updateSuccessors() {
		System.out.println("Going to update successors");
		for(int i = 0; i < R; i++) {
			int index;
			if(i == 0)
				index = this.key + 1;
			else
				index = this.positiveModule(this.hash(this.successorList.get(i-1)) + 1,(int)  Math.pow(2, M));
			this.successorList.put(i, this.lookup(index));
		}
	}
	
	public void removeSuccessor() {
		System.out.println("Going to remove first successor");
		for(int i = 0; i < R; i++) {
			int index;
			if(i == R-1) {
				index = this.positiveModule(this.hash(this.successorList.get(i-1)) + 1,(int)  Math.pow(2, M));
				InetSocketAddress suc = this.lookup(index);
				this.successorList.put(i, suc);
			}else {
				this.successorList.put(i, this.successorList.get(i+1));
			}
		}
	}
	public void setSuccessor(InetSocketAddress suc) {
		System.out.println("Going to set first successor");
		
		for(int i = 1; i < R; i++) {
			InetSocketAddress prev = this.successorList.get(i-1);
			if(prev != null)
				this.successorList.put(i, this.successorList.get(i-1));
		}
		this.successorList.put(0,suc);
		this.connected.set(true);
	}
	
	public InetSocketAddress getSuccessor() {
		InetSocketAddress suc = this.successorList.get(0);
		if(suc != null && suc.toString().equals(this.selfAddress.toString()))
			suc = null;
		return suc;
	}
	
	public void fix_fingers() {
		if(this.nextFinger == M)
			this.nextFinger = 0;
		int index = this.positiveModule((int) (this.getKey() + Math.pow(2, this.nextFinger)), (int)  Math.pow(2, M));
		this.fingerTable.put(this.nextFinger, this.lookup(index));
		this.nextFinger++;
	}
	
	public void fix_successor() {
		if(this.nextSuccessor == R)
			this.nextSuccessor = 0;
		int index;
		if(this.nextSuccessor == 0)
			index = this.positiveModule((int) (this.getKey() + 1), (int)  Math.pow(2, M));
		else
			index = this.positiveModule((int) (this.hash(this.successorList.get(this.nextSuccessor - 1)) + 1), (int)  Math.pow(2, M));
		this.successorList.put(this.nextSuccessor, this.lookup(index));
		this.nextSuccessor++;
	}
	
	public void setPredecessor(InetSocketAddress pre) {
		if(this.predecessor == null)
			this.predecessor = pre;
		else {
			if(this.selfAddress.getHostName().equals(pre.getHostName())
					&& this.selfAddress.getPort() == pre.getPort())
				this.predecessor = null;
			else
				this.predecessor = pre;
		}
		
		System.out.println("New predecessor found");
		printKnowns();
	}
	
	public void setFinger(int index, InetSocketAddress addrs ) {
		this.fingerTable.put(index, addrs);
		this.printFingerTable();
	}
	
	public Peer getPeer() {
		return this.peer;
	}
	
	public void populateFingerTable(InetSocketAddress address) {
		for(int i = 0; i < M; i++)
			this.fingerTable.put(i, address);
	}
	
	private InetSocketAddress closest_preceding_node(int key) {
		InetSocketAddress ret;

		for(int i = M-1; i >= 0; i--) {
			ret = this.fingerTable.get(M);
			if(ret == null) continue;
			if(betweenOpenOpen(this.getKey(),key,hash(ret)))
				return ret;
		}
		return this.getSuccessor();
	}

	public void printKnowns() {
		System.out.println("My key: " + this.getKey());

		if(this.predecessor != null)
			System.out.println("Predecessor: " + this.predecessor.getHostName() + "   " + this.predecessor.getPort() + " " + hash(this.predecessor) );
		if(this.getSuccessor() != null)
			System.out.println("Successor: " + this.getSuccessor().getHostName() + "   " + this.getSuccessor().getPort()  + " " + hash(this.getSuccessor()) );
	}
	
	public void printFingerTable() {
		for(int i = 0; i < M; i++) {
			System.out.println(i+": " + this.fingerTable.get(i) + "  maps to " + (int) ((this.getKey() + Math.pow(2, i)) %  Math.pow(2, M)));
		}
	}

	private boolean betweenOpenClose(int a, int b, int c) {
		if(a < c && c <= b)
			return true;
		return a > b && (c > a || c <= b);
	}

	
	private boolean betweenOpenOpen(int a, int b, int c) {
		if(a < c && c < b)
			return true;
		return a > b && (c > a || c < b);
	}
	
	private int positiveModule(int a, int m) {
		while(a < 0)
			a+=m;
		return a % m;
	}
	
	public InetSocketAddress getPredecessor() {
		return this.predecessor;
	}
	
	public String getName() {
		return this.selfAddress.getHostName();
	}
	public int getPort() {
		return this.selfAddress.getPort();

	}
	
	public Memory getMemory() {
		return this.memory;
	}
	
	private int hash(InetSocketAddress addrss) {
		return hash(addrss.getHostName()+":"+addrss.getPort());
	}
	public int hash(String addrss) {
		return hash(addrss,M);
	}
	private int hash(String fileString, int bits) {
		int r = 0;
		int m = (int) Math.ceil(bits/4.0);
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] encodedhash = digest.digest(fileString.getBytes(StandardCharsets.UTF_8));

            if(m > encodedhash.length) m = encodedhash.length;
            for (int i = m-1; i >=0 ; i--) {
            	r*=16;
            	r+= 0xff & encodedhash[i];
            }

        } catch (NoSuchAlgorithmException e) {
        }

        return (int) (r % Math.pow(2, bits));
    }

	public int getKey() {
		return key;
	}

	public void setKey(int key) {
		this.key = key;
	}

	public void reclaim(int space) {
		this.memory.setMaxMemory(space);
		if(this.getMemory().getMemoryInUse() <= space)
			return;
		int toRemoveKey = 0;
		int toRemoveSize = 0;
		for (ConcurrentHashMap.Entry<Integer, Integer> entry : this.getMemory().getStoredChunks())
			if (entry.getValue() > toRemoveSize) {
				toRemoveSize = entry.getValue();
				toRemoveKey = entry.getKey();
			}
		byte[] data;
		data = this.removeInMemory(toRemoveKey);

		this.putInSuccessor(toRemoveKey, this.key, data, 1);
		System.out.println("Got here!!!");

		
		while(this.getMemory().getMemoryInUse() > space) {
			System.out.println("On the while!!!");
			toRemoveKey = 0;
			toRemoveSize = 0;
			for (ConcurrentHashMap.Entry<Integer, Integer> entry : this.getMemory().getStoredChunks())
				if (entry.getValue() > toRemoveSize) {
					toRemoveSize = entry.getValue();
					toRemoveKey = entry.getKey();
				}
			data = this.removeInMemory(toRemoveKey);
			this.putInSuccessor(toRemoveKey, this.key, data, 1);
		}
	} 
	
	public String status() {
		String str = "Peer with key " + this.key+ "\n";
		str += this.getMemory().status();
		return str;
	}
	
	private void sendToSuccessor(byte[] data) {
		InetSocketAddress suc = this.getSuccessor();
		boolean sent = false;
		while(suc != null && sent == false) {
			SSLMessage m;
			try {
				m = new SSLMessage(this.getSuccessor());
				m.write(data);
				m.close();
				sent = true;
			} catch (ConnectException e) {
				System.out.println("Successor not available, trying anotherone");
				this.removeSuccessor();
				suc = this.getSuccessor();
			}
		}
		if(suc == null) {
			System.out.println("No known successor available");
			return;
		}
	}
	
	private byte[] sendAndReadFromSuccessor(String str) {
		return this.sendAndReadFromSuccessor(str.getBytes());
	}
	
	private byte[] sendAndReadFromSuccessor(byte[] data) {
		byte [] d = null;
		InetSocketAddress suc = this.getSuccessor();
		boolean sent = false;
		while(suc != null && sent == false) {
			SSLMessage m;
			try {
				m = new SSLMessage(this.getSuccessor());
				m.write(data);
				d = m.read();
				m.close();
				sent = true;
			} catch (ConnectException e) {
				System.out.println("Successor not available, trying anotherone");
				this.removeSuccessor();
				suc = this.getSuccessor();
			}
		}
		if(suc == null) {
			System.out.println("No known successor available");
			return null;
		}
		return d;
	}
	private void sendToSuccessor(String str, byte[] b) {
		InetSocketAddress suc = this.getSuccessor();
		boolean sent = false;
		while(suc != null && sent == false) {
			SSLMessage m;
			try {
				m = new SSLMessage(this.getSuccessor());
				m.write(str,b);
				m.close();
				sent = true;
			} catch (ConnectException e) {
				System.out.println("Successor not available, trying anotherone");
				this.removeSuccessor();
				suc = this.getSuccessor();
			}
		}
		if(suc == null) {
			System.out.println("No known successor available");
			return;
		}
	}
	private void sendToSuccessor(String str) {
		InetSocketAddress suc = this.getSuccessor();
		boolean sent = false;
		while(suc != null && sent == false) {
			SSLMessage m;
			try {
				m = new SSLMessage(this.getSuccessor());
				m.write(str);
				m.close();
				sent = true;
			} catch (ConnectException e) {
				System.out.println("Successor not available, trying anotherone");
				this.removeSuccessor();
				suc = this.getSuccessor();
			}
		}
		if(suc == null) {
			System.out.println("No known successor available");
			return;
		}
	}
	
}
