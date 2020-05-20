package Chord;

import Peer.Peer;

import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;


public class Chord {
	
	static final int M = 15;
	static final int UPDATE_TIME = 10;
	
	private final Peer peer;
	
	private final InetSocketAddress selfAddress;
	private InetSocketAddress successor;
	private InetSocketAddress predecessor;
	private InetSocketAddress access_peer;

	private ChordServer chordServer;
	
	private final ConcurrentHashMap<Integer,InetSocketAddress> fingerTable = new ConcurrentHashMap<>();

	int port;
	private int key;
	
	private AtomicBoolean connected = new AtomicBoolean(false);
	private AtomicBoolean updating_fingers = new AtomicBoolean(false);
	
    private final Memory memory;
    
    int nextFinger = 0;
	
	public Chord(Peer p , int port) {
		this.selfAddress = new InetSocketAddress("localhost" , port);
		this.setKey(this.hash(selfAddress));
		this.memory = new Memory("./peer" + key);
		this.peer = p;
        this.port = port;

        this.chordServer = new ChordServer(this, port);
        try {
        	this.peer.getExecutor().execute(this.chordServer);
        } catch (Exception e) {
            System.err.println("Could not create the server");
            e.printStackTrace();
        }

        this.successor = null;
        this.predecessor = null;
        
        //this.getPeer().getExecutor().schedule(new ChordStabilizer(this), UPDATE_TIME, TimeUnit.SECONDS);
        
        System.out.println("Chord initiated with key " + this.getKey());
    }
	
	public void joinRing(InetSocketAddress peer) {

		this.access_peer = peer;
		
		System.out.println("Started join process");
		
		String data = ChordOps.GET_SUCCESSOR + " " + this.getKey() + " " + this.selfAddress.getHostName() + " " +  this.selfAddress.getPort();
		SSLMessage m = new SSLMessage(this.access_peer);
		System.out.print("Created ssl message \n");
		m.write(data);
		m.read();
		m.close();

		/*System.out.print("Awaiting connection...");
		while(!this.connected.get()) {
		}
		System.out.print("Connected!");
		
		this.updateFingerTable();
		
		System.out.println("Asking data to successor initiated");
		
		m = new SSLMessage(this.successor);
		m.write(ChordOps.GET_DATA + " " + this.getKey() + " " + this.selfAddress.getHostName() + " " +  this.selfAddress.getPort());
		m.read();
		m.close();*/

		System.out.println("Node joined ring");
		
	}
	
	public void leaveRing() {
		System.out.println("Started leaving process");
		
		if(this.successor != null && this.predecessor != null) {
			
			SSLMessage m = new SSLMessage(this.successor);
			m.write(ChordOps.SET_PREDECESSOR + " " + this.hash(predecessor) + " " + this.predecessor.getHostName() + " " +  this.predecessor.getPort());
			m.read();
			m.close();
			
			m = new SSLMessage(this.predecessor);
			m.write(ChordOps.SET_SUCCESSOR + " " + this.hash(successor) + " " + this.successor.getHostName() + " " +  this.successor.getPort());
			m.read();
			m.close();
			
			m = new SSLMessage(this.predecessor);
			m.write(ChordOps.DELETE_FINGER + " " + this.hash(predecessor) + " " + this.getKey() + " " + this.successor.getHostName() + " " +  this.successor.getPort() );
			m.read();
			m.close();

			System.out.println("Sent new delete to predecessor");
			
			System.out.println("Transferring data to successor initiated");
			
			m = new SSLMessage(this.successor);
			List<Pair<Integer,Integer>> list = this.memory.getStoredChunks();
			for(int i = 0; i < list.size();i++) {
				int chunkId = list.get(i).getKey();
				m.write(ChordOps.PUT + " " + chunkId, this.memory.get(chunkId));
				m.read();
				this.removeInMemory(chunkId);
				i--;
			}
			m.close();
			
			System.out.println("Transferring data to successor done");	
			
			
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
		System.out.println("a");
		//se nao existir successor, significa que � o unico na rede
		if(this.successor == null) {
			InetSocketAddress new_peer = new InetSocketAddress(ip,port);
			this.setPredecessor(new_peer);
			SSLMessage m = new SSLMessage(this.predecessor);
			this.setSuccessor(new_peer);
    		m.write(ChordOps.SET_SUCCESSOR + " " + this.getKey() + " " + this.selfAddress.getHostName() + " " +  this.selfAddress.getPort());
    		m.read();
    		m.write(ChordOps.SET_PREDECESSOR + " " + this.getKey() + " " + this.selfAddress.getHostName() + " " +  this.selfAddress.getPort());
    		m.read();
    		m.close();
    		//this.populateFingerTable(new_peer);
			return;
		}
		//se a chave que se procura estiver entre mim e o meu successor, logo deu hit!
		if(this.betweenOpenClose(this.getKey(), this.hash(successor), key)) {
			SSLMessage m = new SSLMessage(this.successor);
    		m.write(ChordOps.SET_PREDECESSOR + " " + key + " " + ip + " " +  port);
    		m.read();
    		m.close();
			
    		m = new SSLMessage(ip, port);
    		m.write(ChordOps.SET_SUCCESSOR + " " + this.hash(successor) + " " + this.successor.getHostName() + " " +  this.successor.getPort());
    		m.read();

			
    		InetSocketAddress newFinger = new InetSocketAddress(ip, port);
			this.setSuccessor(newFinger);

			m = new SSLMessage(ip, port);
    		m.write(ChordOps.SET_PREDECESSOR + " " + this.getKey() + " " + this.selfAddress.getHostName() + " " +  this.selfAddress.getPort());
    		m.read();
    		m.close();

    		//novo successor encontrado, por isso mudar os fingers
    		this.foundNewFinger(newFinger);
    		this.sendNotifyNewFinger(this.getKey(), ip, port);
		} else { //se a chave estiver para la do successor
			InetSocketAddress closest = closest_preceding_node(key);
			System.out.println("Closest to " + key + " is:  " + closest);
			String data = ChordOps.GET_SUCCESSOR + " " + key + " " + ip + " " +  port;
			SSLMessage m = new SSLMessage(closest);
			m.write(data);
			m.read();
			m.close();
		
		}
	}	
	
	public InetSocketAddress lookup(int key) {
		if(this.successor == null) {
			return this.selfAddress;
		} else if (this.betweenOpenClose(this.getKey(), this.hash(successor), key)) {
			return this.successor;
		} else {
			InetSocketAddress closest = closest_preceding_node(key);
			SSLMessage m = new SSLMessage(closest.getHostName(), closest.getPort());
			m.write(ChordOps.LOOKUP + " " + key);
			byte [] data = m.read();
			m.close();
    		String [] parts = new String(data).split(" ");
			return new InetSocketAddress(parts[3],Integer.parseInt(parts[4].replaceAll("\\D", "")));
		}
	}
	
	public int put(String identifier, byte[] data ,  int replication) {
		//TODO:: it is possible to check here if chunk targets for this peer
		int key = this.hash(identifier);
		InetSocketAddress dest = this.lookup(key);
		SSLMessage m = new SSLMessage(dest);
		m.write(ChordOps.PUT + " " + key + " "  + replication, data);
		m.read();
		m.close();
		return key;
	}
	
	public byte[] get(String identifier, int replication) {
		int key = this.hash(identifier);
		InetSocketAddress dest = this.lookup(key);
		SSLMessage m = new SSLMessage(dest);
		m.write(ChordOps.GET + " " + key + " " + replication);
		byte[] data =  m.read();
		m.close();
		return data;
	}
	
	public byte[] remove(String identifier , int replication) {
		int key = this.hash(identifier);
		InetSocketAddress dest = this.lookup(key);
		SSLMessage m = new SSLMessage(dest);
		m.write(ChordOps.REMOVE + " " + key + " " + replication);
		byte[] data =  m.read();
		m.close();
		return data;
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
	
	public void putInSuccessor(int key, byte[] body, int replication) {
		if(this.betweenOpenClose(this.key, this.hash(successor), key)) // If it has made a complete turn arrounf the ring
			return;
		this.memory.addRedirectedChunk(key);
		SSLMessage m = new SSLMessage(this.successor);
		m.write(ChordOps.PUT + " " + key + " "  + replication, body);
		m.read();
		m.close();
	}
	public byte[] getFromSuccessor(int key, int replication) {
		SSLMessage m = new SSLMessage(this.successor);
		m.write(ChordOps.GET + " " + key + " " + replication);
		byte[] d = m.read();
		m.close();
		return d;
	}
	public byte[] removeFromSuccessor(int key, int replication) {
		SSLMessage m = new SSLMessage(this.successor);
		m.write(ChordOps.REMOVE + " " + key + " " + replication);
		byte[] d = m.read();
		m.close();
		this.memory.removeRedirectedChunk(key);
		return d;
	}

	/*
	 * Send data to node that has just joined
	 */
	public void sendData(int key, String ip, int port) {
		InetSocketAddress pre = new InetSocketAddress(ip,port);
		List<Pair<Integer,Integer>> storedChunks = this.memory.getStoredChunks();
		SSLMessage m = new SSLMessage(pre);
		for(int i=0;i<storedChunks.size();i++) {		
			int chunkId = storedChunks.get(i).getKey();
			if (betweenOpenClose(this.getKey(), key, chunkId)) {
				m.write(ChordOps.PUT + " " + chunkId, this.memory.get(chunkId));
				m.read();
				this.removeInMemory(chunkId);
				i--;
			}
		}
		m.close();
	}
	
	/*
	 * Get predecessor of node passed as argument
	 */
	public InetSocketAddress getPredecessor(InetSocketAddress node) {
		SSLMessage m = new SSLMessage(this.successor);
		m.write(ChordOps.GET_PREDECESSOR + " " + this.getKey() + " " + this.selfAddress.getHostName() + " " +  this.selfAddress.getPort());
		byte [] buf = m.read();
		m.close();

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
		if(this.successor == null)
			return;
		InetSocketAddress x = this.getPredecessor(this.successor);
		if(betweenOpenOpen(this.getKey(),hash(this.successor),hash(x))) {
			this.setSuccessor(x);
		}
		SSLMessage m = new SSLMessage(this.successor);
		m.write(ChordOps.NOTIFY + " " + this.getKey() + " " + this.selfAddress.getHostName() + " " +  this.selfAddress.getPort());
		m.read();
		m.close();
	}
	
	public void foundNewFinger(InetSocketAddress finger) {
		int key = this.hash(finger);
		for (int i = 0; i < M; i++) {
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
		SSLMessage m = new SSLMessage(this.predecessor);
		m.write(ChordOps.NEW_FINGER + " " + originKey + " " + ip + " " +  port + " ");
		m.read();
		m.close();
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

		SSLMessage m = new SSLMessage(this.predecessor);
		m.write(ChordOps.DELETE_FINGER + " " + originKey + " " + oldKey + " " + ip + " " +  port + " ");
		m.read();
		m.close();
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
	
	public void fix_fingers() {
		if(this.nextFinger == M)
			this.nextFinger = 0;
		int index = this.positiveModule((int) (this.getKey() + Math.pow(2, this.nextFinger)), (int)  Math.pow(2, M));
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
		//System.out.println("Setted finger");
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
		return this.successor;
	}

	public void printKnowns() {
		System.out.println("My key: " + this.getKey());

		if(this.predecessor != null)
			System.out.println("Predecessor: " + this.predecessor.getHostName() + "   " + this.predecessor.getPort() + " " + hash(this.predecessor) );
		if(this.successor != null)
			System.out.println("Successor: " + this.successor.getHostName() + "   " + this.successor.getPort()  + " " + hash(this.successor) );
	}
	
	public void printFingerTable() {
		for(int i = 0; i < M; i++) {
			System.out.println(i+": " + this.fingerTable.get(i) + "  maps to " + (int) ((this.getKey() + Math.pow(2, i)) %  Math.pow(2, M)));
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

	public int getKey() {
		return key;
	}

	public void setKey(int key) {
		this.key = key;
	}

	public void reclaim(int space) {
		List<Pair<Integer,Integer>> chunkStored = this.getMemory().getStoredChunks();
		int toRemoveKey = 0;
		int toRemoveSize = 0;
		for (Pair<Integer,Integer> pair : chunkStored)
			if (pair.getValue() > toRemoveSize) {
				toRemoveSize = pair.getValue();
				toRemoveKey = pair.getKey();
			}
		byte[] data;
		data = this.removeInMemory(toRemoveKey);

		this.putInSuccessor(toRemoveKey, data, 1);
		System.out.println("Got here!!!");

		
		while(this.getMemory().getMemoryInUse() > space) {
			System.out.println("On the while!!!");
			toRemoveKey = 0;
			toRemoveSize = 0;
			chunkStored = this.getMemory().getStoredChunks();
			for (Pair<Integer,Integer> pair : chunkStored)
				if (pair.getValue() > toRemoveSize) {
					toRemoveSize = pair.getValue();
					toRemoveKey = pair.getKey();
				}
			data = this.removeInMemory(toRemoveKey);
			this.putInSuccessor(toRemoveKey, data, 1);
		}
	} 
	
	public String status() {
		String str = "Peer with key " + this.key+ "\n";
		str += this.getMemory().status();
		return str;
	}
	
}
