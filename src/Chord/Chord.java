package Chord;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import Peer.Peer;

public class Chord {
	
	static final int M = 15;
	static final int UPDATE_TIME = 8;
	
	private final Peer peer;
	
	private final InetSocketAddress selfAddress;
	private InetSocketAddress successor;
	private InetSocketAddress predecessor;
	private InetSocketAddress access_peer;
	
	private ServerSocket serverSocket;
	
	private final ConcurrentHashMap<Integer,InetSocketAddress> fingerTable = new ConcurrentHashMap<>();
	
	int port;
	int key;
	
	private AtomicBoolean connected = new AtomicBoolean(false);
	private AtomicBoolean updating_fingers = new AtomicBoolean(false);
	
    private final Memory memory;
    
    int nextFinger = 0;
	
	public Chord(Peer p , int port) {
		this.memory = new Memory();
		this.peer = p;
        this.port = port;

        try {
            serverSocket = new ServerSocket(port);
        } catch (IOException e) {
            System.err.println("Could not create the server socket");
            e.printStackTrace();
        }

        this.selfAddress = new InetSocketAddress("localhost" , port);
        this.successor = null;
        this.predecessor = null;
        this.key = this.hash(selfAddress);
        
        this.peer.getExecutor().execute(new ChordThread(this));
        
        this.getPeer().getExecutor().schedule(new ChordStabilizer(this), UPDATE_TIME, TimeUnit.SECONDS);
        
        System.out.println("Chord initiated with key " + this.key);
        
    }

	public void joinRing(InetSocketAddress peer) {
		this.access_peer = peer;
		
		System.out.println("Started join process");
		
		String data = ChordOps.GET_SUCCESSOR + " " + this.key + " " + this.selfAddress.getHostName() + " " +  this.selfAddress.getPort();
		Message m = new Message(data);
		m.sendMessage(peer);
		
		while(!this.connected.get()) {
		}

		this.updateFingerTable();
		
		System.out.println("Asking data to successor initiated");
		
		m = new Message(ChordOps.GET_DATA + " " + this.key + " " + this.selfAddress.getHostName() + " " +  this.selfAddress.getPort());
		m.sendMessage(successor);

		System.out.println("Node joined ring");
		
	}
	
	public void leaveRing() {
		System.out.println("Started leaving process");
		
		if(this.successor != null && this.predecessor != null) {
			
			System.out.println("Transferring data to successor initiated");
			
			ConcurrentHashMap<Integer,byte[]> data = this.memory.getData();

			for (Entry<Integer, byte[]> entry : data.entrySet()) {
				Message m = new Message(ChordOps.PUT + " " + entry.getKey(), entry.getValue());
				m.sendMessage(this.successor);
			}
			
			System.out.println("Transferring data to sucessor done");
			
			Message m = new Message(ChordOps.SET_PREDECCESSOR + " " + this.hash(predecessor) + " " + this.predecessor.getHostName() + " " +  this.predecessor.getPort());
			m.sendMessage(this.successor.getHostName(), this.successor.getPort());
			
			m = new Message(ChordOps.SET_SUCCESSOR + " " + this.hash(successor) + " " + this.successor.getHostName() + " " +  this.successor.getPort());
			m.sendMessage(this.predecessor.getHostName(), this.predecessor.getPort());
			
			m = new Message(ChordOps.DELETE_FINGER + " " + this.hash(predecessor) + " " + this.key + " " + this.successor.getHostName() + " " +  this.successor.getPort() );
			m.sendMessage(this.predecessor.getHostName(), this.predecessor.getPort());

			System.out.println("Sent new delete to predecessor");
		}
		
		System.out.println("Node left ring");
	}
	
	
	/**
	 * 
	 * @param key Key of node joining
	 * @param ip Its ip
	 * @param port Its port
	 * @param type: -1: Node joining  >=0: Updating fingerTable of index type
	 */
	public void find_successor(int key, String ip, int port, int type) { 
		
		//se nao existir sucessor, significa que � o unico na rede
		if (this.successor == null) {
			if(type == -1) { // se for um lookup do successor
				InetSocketAddress new_peer = new InetSocketAddress(ip, port);

				this.setPredecessor(new_peer);
				Message m = new Message(ChordOps.SET_SUCCESSOR + " " + this.key + " " + this.selfAddress.getHostName() + " " +  this.selfAddress.getPort());
	    		m.sendMessage(ip, port);
	    		
				this.setSuccessor(new_peer);
	    		m = new Message(ChordOps.SET_PREDECCESSOR + " " + this.key + " " + this.selfAddress.getHostName() + " " +  this.selfAddress.getPort());
	    		m.sendMessage(ip, port);

	    		this.populateFingerTable(new_peer);
			}
			return;
		}

		//se a chave que se procura estiver entre mim e o meu sucessor, logo deu hit!
		if (this.betweenOpenClose(this.key, this.hash(successor), key)) {
			if (type == -1) { // se for um lookup do successor
				Message m = new Message(ChordOps.SET_PREDECCESSOR + " " + key + " " + ip + " " +  port);
	    		m.sendMessage(this.successor.getHostName(), this.successor.getPort());
				
	    		m = new Message(ChordOps.SET_SUCCESSOR + " " + this.hash(successor) + " " + this.successor.getHostName() + " " +  this.successor.getPort());
	    		m.sendMessage(ip, port);
				
	    		InetSocketAddress newFinger = new InetSocketAddress(ip, port);
				this.setSuccessor(newFinger);
				m = new Message(ChordOps.SET_PREDECCESSOR + " " + this.key + " " + this.selfAddress.getHostName() + " " +  this.selfAddress.getPort());
	    		m.sendMessage(ip, port);
	    		
	    		//novo sucessor encontrado, por isso mudar os fingers
	    		this.foundNewFinger(newFinger);
	    		this.sendNotifyNewFinger(this.key, ip, port);
	    		
			}  else { // se for uma procura de um finger
				Message m = new Message(ChordOps.SET_FINGER + " " + this.hash(this.successor) + " " + this.successor.getHostName() + " " +  this.successor.getPort() + " " + type);
	    		m.sendMessage(ip, port);
			}
		} else { //se a chave estiver para la do sucessor
			InetSocketAddress closest = closest_preceding_node(key);
			System.out.println("Closest to " + key + " is:  " + closest);

			if (type == -1) { // se for um lookup do successor
				String data = ChordOps.GET_SUCCESSOR + " " + key + " " + ip + " " +  port;
				Message m = new Message(data);
				m.sendMessage(closest.getHostName(), closest.getPort());
			} else { // se for uma prucura de finger
				Message m = new Message(ChordOps.FIND_FINGER + " " + key + " " + ip + " " +  port + " " + type);
	    		m.sendMessage(closest.getHostName(), closest.getPort());
			}

		}
	}

	public InetSocketAddress lookup(int key) {
		InetSocketAddress ret;
		
		if (this.successor == null) {
			return this.selfAddress;
		} else if (this.betweenOpenClose(this.key, this.hash(successor), key)) {
			return this.successor;
		} else {
			InetSocketAddress closest = closest_preceding_node(key);
			Message m = new Message(ChordOps.LOOKUP + " " + key);
    		ret = m.lookup(closest.getHostName(), closest.getPort());
		}
		
		return ret;
	}

	public void put	(String identifier, byte[] data) {
		int key = this.hash(identifier);
		InetSocketAddress dest = this.lookup(key);
		Message m = new Message(ChordOps.PUT + " " + key , data);
		m.sendMessage(dest);
	}
	
	public byte[] get(String identifier) {
		int key = this.hash(identifier);
		InetSocketAddress dest = this.lookup(key);
		Message m = new Message(ChordOps.GET + " " + key);
		return m.sendAndReceive(dest);
	}
	
	public byte[] remove(String identifier) {
		int key = this.hash(identifier);
		InetSocketAddress dest = this.lookup(key);
		Message m = new Message(ChordOps.REMOVE + " " + key);
		return m.sendAndReceive(dest);
	}
	
	public void putInMemory(int key, byte[] data) {
		this.getMemory().put(key, data);
	}
	
	public byte[] getInMemory(int key) {
		return this.getMemory().get(key);
	}
	
	public byte[] removeInMemory(int key) {
		return this.getMemory().remove(key);
	}

	/*
	 * Send data to node that has just joined
	 */
	public void sendData(int key, String ip, int port) {
		InetSocketAddress pre = new InetSocketAddress(ip,port);
		ConcurrentHashMap<Integer,byte[]> data = this.memory.getData();

		for (Entry<Integer, byte[]> entry : data.entrySet()) {
			int id = entry.getKey();
			if (betweenOpenClose(this.key, key, id)) {
				Message m = new Message(ChordOps.PUT + " " + entry.getKey(), entry.getValue());
				data.remove(id);
				m.sendMessage(pre);
			}
		}
	}
	
	/*
	 * Get predecessor of node passed as argument
	 */
	public InetSocketAddress getPredecessor(InetSocketAddress node) {  //TODO node nao esta a ser usado
		InetSocketAddress ret;
		Message m = new Message(ChordOps.GET_PREDECCESSOR + " " + this.key + " " + this.selfAddress.getHostName() + " " +  this.selfAddress.getPort());
		byte [] buf = m.sendAndReceive(this.successor);
		
		String message = new String(buf, StandardCharsets.UTF_8);
	    System.out.println(message);
	    String[] parts = message.split(" ");
	    
	    ret = new InetSocketAddress(parts[1], Integer.parseInt(parts[2]));

	    System.out.println("Get predeccessor done!");
		return ret;
	}
	
	public void notify(InetSocketAddress pre) {
		if (this.predecessor == null || betweenOpenOpen(hash(this.predecessor), this.key, hash(pre)))
			this.setPredecessor(pre);
	}

	public void stabilize() {
		if (this.successor == null)
			return;

		InetSocketAddress x = this.getPredecessor(this.successor);
		if(betweenOpenOpen(this.key, hash(this.successor), hash(x)))
			this.setSuccessor(x);

		Message m = new Message(ChordOps.NOTIFY + " " + this.key + " " + this.selfAddress.getHostName() + " " +  this.selfAddress.getPort());
		m.sendMessage(successor);
	}
	
	public void foundNewFinger(InetSocketAddress finger) {
		int key = this.hash(finger);
		for (int i = 0; i < M; i++) {
			int prev_key = this.hash(this.fingerTable.get(i));
			if(betweenOpenOpen(this.key, prev_key, key)) {
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

		Message m = new Message(ChordOps.NEW_FINGER + " " + originKey + " " + ip + " " +  port + " ");
		m.sendMessage(this.predecessor);

		System.out.println("Sent new finger to predecessor");
	}
	
	public void sendNotifyDeleteFinger(int originKey , int oldKey, String ip, int port) {
		int pred_key = this.hash(this.predecessor);
		
		int max_origin = this.positiveModule((int) (originKey - Math.pow(2, M-1)), (int)  Math.pow(2, M));
		System.out.println(max_origin + "  " + originKey + " " + pred_key);
		if (!betweenOpenOpen( max_origin, originKey ,pred_key ))
			return;

		Message m = new Message(ChordOps.DELETE_FINGER + " " + originKey + " " + oldKey + " " + ip + " " +  port + " ");
		m.sendMessage(this.predecessor);

		System.out.println("Sent new delete to predecessor");
	}
	
	//TODO:: UPDATE THIS TO USER LOOKUP!
	/*public void updateFingerTable() {
		for(int i = 0; i < M; i++) {
			int index = this.positiveModule((int) (this.key + Math.pow(2, i)), (int)  Math.pow(2, M));
			this.find_successor(index, this.selfAddress.getHostName(), this.selfAddress.getPort(), i);
		}
	}*/
	//Maybe done here ??
	public void updateFingerTable() {
		InetSocketAddress finger = null;
		for(int i = 0; i < M; i++) {
			int index = this.positiveModule((int) (this.key + Math.pow(2, i)), (int)  Math.pow(2, M));
			if(finger == null || !betweenOpenOpen(this.key,hash(finger),index))
				finger = this.lookup(index);
			this.fingerTable.put(i, finger);
		}
	}
	
	public void fix_fingers() {
		if(this.nextFinger == M)
			this.nextFinger = 0;
		int index = this.positiveModule((int) (this.key + Math.pow(2, this.nextFinger)), (int)  Math.pow(2, M));
		this.fingerTable.put(this.nextFinger, this.lookup(index));
		this.nextFinger++;
	}
	
	public void setSuccessor(InetSocketAddress suc) {
		if (this.successor == null)
			this.successor = suc;
		else {
			if (this.selfAddress.getHostName().equals(suc.getHostName())
					&& this.selfAddress.getPort() == suc.getPort())
				this.successor = null;
			else
				this.successor = suc;
		}
		this.connected.set(true);

		System.out.println("New successor found");
		printKnowns();
	}
	
	public void setPredecessor(InetSocketAddress pre) {
		if (this.predecessor == null)
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
		//System.out.println("Setted finger");
		this.fingerTable.put(index, addrs);
		this.printFingerTable();
	}
	
	public Peer getPeer() {
		return this.peer;
	}
	
	public ServerSocket getServerSocket() {
		return this.serverSocket;
	}
	
	public void populateFingerTable(InetSocketAddress addrs) {
		for(int i = 0; i < M; i++)
			this.fingerTable.put(i, addrs);
	}
	
	private InetSocketAddress closest_preceding_node(int key) {
		InetSocketAddress ret;

		for(int i = M-1; i >= 0; i--) {
			ret = this.fingerTable.get(M);
			if(ret == null) continue;
			if(betweenOpenOpen(this.key,key,hash(ret)))
				return ret;
		}
		return this.successor;
	}

	public void printKnowns() {
		System.out.println("My key: " + this.key);
		if (this.predecessor != null)
			System.out.println("Predeccessor: " + this.predecessor.getHostName() + "   " + this.predecessor.getPort() + " " + hash(this.predecessor) );

		if (this.successor != null)
			System.out.println("Successor: " + this.successor.getHostName() + "   " + this.successor.getPort()  + " " + hash(this.successor) );
	}
	
	public void printFingerTable() {
		for(int i = 0; i < M; i++) {
			System.out.println(i+": " + this.fingerTable.get(i) + "  maps to " + (int) ((this.key + Math.pow(2, i)) %  Math.pow(2, M)));
		}
	}

	/*private boolean between(int a, int b, int c) {
		if(a < b && a < c && c < b)
			return true;
		if(a > b && (c > a || c < b))
			return true;
		return false;
	}*/
	
	private boolean betweenOpenClose(int a, int b, int c) {
		if(a < b && a < c && c <= b)
			return true;
		if (a > b && (c > a || c <= b))
			return true;
		return false;
	}
	
	private boolean betweenOpenOpen(int a, int b, int c) {
		if(a < b && a < c && c < b)
			return true;
		if(a > b && (c > a || c < b))
			return true;
		return false;
	}
	
	private int positiveModule(int a, int m) {
		while(a < 0)
			a+=m;
		return a % m;
	}
	
	public InetSocketAddress getPredecessor() {
		return this.predecessor;
	}
	
	private Memory getMemory() {
		return this.memory;
	}
	
	private int hash(InetSocketAddress addrss) {
		return hash(addrss.getHostName()+":"+addrss.getPort());
	}
	private int hash(String addrss) {
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
            e.printStackTrace();
        }

        return (int) (r % Math.pow(2, bits));
    }
}
