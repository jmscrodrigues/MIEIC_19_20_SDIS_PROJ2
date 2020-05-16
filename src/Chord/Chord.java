package Chord;

import Peer.Peer;

import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class Chord {
	
	static final int M = 15;
	static final int UPDATE_TIME = 10;
	
	private Peer peer;
	
	private InetSocketAddress selfAddress;
	private InetSocketAddress successor;
	private InetSocketAddress predecessor;
	private InetSocketAddress access_peer;
	
	private ServerSocket serverSocket;
	
	public ServerThread sllServer;
	
	private ConcurrentHashMap<Integer,InetSocketAddress> fingerTable = new ConcurrentHashMap<>();
	
	int port;
	int key;
	
	private AtomicBoolean connected = new AtomicBoolean(false);
	private AtomicBoolean updating_fingers = new AtomicBoolean(false);
	
    private Memory memory;
    
    int nextFinger = 0;
	
	public Chord(Peer p ,int port) {
		this.memory = new Memory();
		this.peer = p;
        this.port = port;
        try {
        	sllServer = new ServerThread(this,"localhost",port);
        	this.peer.getExecutor().execute(sllServer);
        } catch (Exception e) {
            System.err.println("Could not create the server");
            e.printStackTrace();
        }
        this.selfAddress = new InetSocketAddress("localhost" , port);
        this.successor = null;
        this.predecessor = null;
        this.key = this.hash(selfAddress);
        
        
        this.getPeer().getExecutor().schedule(new ChordStabilizer(this), UPDATE_TIME, TimeUnit.SECONDS);
        
        System.out.println("Chord initiated with key " + this.key);
        
    }
	
	
	public void joinRing(InetSocketAddress peer) throws Exception {
		this.access_peer = peer;
		
		System.out.println("Started join process");
		
		String data = "GETSUCCESSOR " + this.key + " " + this.selfAddress.getHostName() + " " +  this.selfAddress.getPort();
		SSLMessage m = new SSLMessage(this.access_peer);
		m.write(data);
		m.read();
		m.close();
		
		while(!this.connected.get()) {
		}
		
		this.updateFingerTable();
		
		System.out.println("Asking data to successor initiated");
		
		m = new SSLMessage(this.successor);
		m.write("GETDATA " + this.key + " " + this.selfAddress.getHostName() + " " +  this.selfAddress.getPort());
		m.read();
		m.close();

		System.out.println("Node joined ring");
		
	}
	
	public void leaveRing() throws Exception {
		System.out.println("Started leaving process");
		
		if(this.successor != null && this.predecessor != null) {
			
			System.out.println("Transferring data to successor initiated");
			
			SSLMessage m = new SSLMessage(this.successor);
			ConcurrentHashMap<Integer,byte[]> data = this.memory.getData();
			for (Entry<Integer, byte[]> entry : data.entrySet()) {
				m.write("PUT " + entry.getKey(), entry.getValue());
				m.read();
			}
			m.close();
			
			System.out.println("Transferring data to successor done");
			
			m = new SSLMessage(this.successor);
			m.write("SETPREDECESSOR " + this.hash(predecessor) + " " + this.predecessor.getHostName() + " " +  this.predecessor.getPort());
			//m.read();
			m.close();
			
			m = new SSLMessage(this.predecessor);
			m.write("SETSUCCESSOR " + this.hash(successor) + " " + this.successor.getHostName() + " " +  this.successor.getPort());
			//m.read();
			m.close();
			
			m = new SSLMessage(this.predecessor);
			m.write("DELETEFINGER " + this.hash(predecessor) + " " + this.key + " " + this.successor.getHostName() + " " +  this.successor.getPort() );
			m.read();
			m.close();
			System.out.println("Sent new delete to predecessor");
		}
		sllServer.stop();
		System.out.println("Node left ring");
	}
	
	
	/**
	 * 
	 * @param key Key of node joining
	 * @param ip Its ip
	 * @param port Its port
	 * @param type: -1-> Node joining  >=0: Updating fingerTable of index type
	 * @throws Exception
	 */
	public void find_successor(int key, String ip, int port, int type) throws Exception { 
		System.out.println("a");
		//se nao existir sucessor, significa que � o unico na rede
		if(this.successor == null) {
			if(type == -1) { // se for um lookup do successor
				InetSocketAddress new_peer = new InetSocketAddress(ip,port);
				this.setPredecessor(new_peer);
				SSLMessage m = new SSLMessage(this.predecessor);
				this.setSuccessor(new_peer);
	    		m.write("SETSUCCESSOR " + this.key + " " + this.selfAddress.getHostName() + " " +  this.selfAddress.getPort());
	    		m.read();
	    		m.write("SETPREDECESSOR " + this.key + " " + this.selfAddress.getHostName() + " " +  this.selfAddress.getPort());
	    		m.read();
	    		m.close();
	    		this.populateFingerTable(new_peer);
			}
			return;
		}
		//se a chave que se procura estiver entre mim e o meu sucessor, logo deu hit!
		if(this.betweenOpenClose(this.key, this.hash(successor), key)) {
			if(type == -1) { // se for um lookup do successor
				SSLMessage m = new SSLMessage(this.successor);
	    		m.write("SETPREDECESSOR " + key + " " + ip + " " +  port);
	    		m.read();
	    		m.close();
				
	    		m = new SSLMessage(ip, port);
	    		m.write("SETSUCCESSOR " + this.hash(successor) + " " + this.successor.getHostName() + " " +  this.successor.getPort());
	    		m.read();
				
	    		InetSocketAddress newFinger = new InetSocketAddress(ip,port);
				this.setSuccessor(newFinger);
				m = new SSLMessage(ip, port);
	    		m.write("SETPREDECESSOR " + this.key + " " + this.selfAddress.getHostName() + " " +  this.selfAddress.getPort());
	    		m.read();
	    		m.close();
	    		
	    		//novo sucessor encontrado, por isso mudar os fingers
	    		this.foundNewFinger(newFinger);
	    		this.sendNotifyNewFinger(this.key,ip,port);
	    		
			}
    		return;
		}else { //se a chave estiver para la do sucessor
			InetSocketAddress closest = closest_preceding_node(key);
			System.out.println("Closest to " + key + " is:  " + closest);
			if(type == -1) { // se for um lookup do successor
				String data = "GETSUCCESSOR " + key + " " + ip + " " +  port;
				SSLMessage m = new SSLMessage(closest);
				m.write(data);
				m.read();
				m.close();
			}

		}
	}
	
	
	public InetSocketAddress lookup(int key) throws Exception {

		if(this.successor == null) {
			return this.selfAddress;
		}else if(this.betweenOpenClose(this.key, this.hash(successor), key)) {
			return this.successor;
		}else {
			InetSocketAddress closest = closest_preceding_node(key);
			SSLMessage m = new SSLMessage(closest.getHostName(), closest.getPort());
			m.write("LOOKUP " + key);
			byte [] data = m.read();
			m.close();
    		String [] parts = new String(data).split(" ");
			return new InetSocketAddress(parts[3],Integer.parseInt(parts[4].replaceAll("\\D", "")));
		}
	}
	
	
	public void put	(String identifier, byte[] data) throws Exception {
		int key = this.hash(identifier);
		InetSocketAddress dest = this.lookup(key);
		SSLMessage m = new SSLMessage(dest);
		m.write("PUT " + key , data);
		m.read();
		m.close();
	}
	
	public byte[] get(String identifier) throws Exception {
		int key = this.hash(identifier);
		InetSocketAddress dest = this.lookup(key);
		SSLMessage m = new SSLMessage(dest);
		m.write("GET " + key);
		byte[] data =  m.read();
		m.close();
		return data;
	}
	
	public byte[] remove(String identifier) throws Exception {
		int key = this.hash(identifier);
		InetSocketAddress dest = this.lookup(key);
		SSLMessage m = new SSLMessage(dest);
		m.write("REMOVE " + key);
		byte[] data =  m.read();
		m.close();
		return data;
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
	public void sendData(int key, String ip, int port) throws Exception {
		InetSocketAddress pre = new InetSocketAddress(ip,port);
		ConcurrentHashMap<Integer,byte[]> data = this.memory.getData();
		SSLMessage m = new SSLMessage(pre);
		for (Entry<Integer, byte[]> entry : data.entrySet()) {
			int id = entry.getKey();
			if (betweenOpenClose(this.key, key, id)) {
				m.write("PUT " + entry.getKey(), entry.getValue());
				m.read();
				data.remove(id);
			}
		}
		m.close();
	}
	
	/*
	 * Get predecessor of node passed as argument
	 */
	public InetSocketAddress getPredecessor(InetSocketAddress node) throws Exception {
		SSLMessage m = new SSLMessage(this.successor);
		m.write("GETPREDECESSOR " + this.key + " " + this.selfAddress.getHostName() + " " +  this.selfAddress.getPort());
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
		if(this.predecessor == null || betweenOpenOpen(hash(this.predecessor),this.key,hash(pre))) {
			this.setPredecessor(pre);
		}
	}
	
	
	public void stabilize() throws Exception {
		if(this.successor == null)
			return;
		InetSocketAddress x = this.getPredecessor(this.successor);
		if(betweenOpenOpen(this.key,hash(this.successor),hash(x))) {
			this.setSuccessor(x);
		}
		SSLMessage m = new SSLMessage(this.successor);
		m.write("NOTIFY " + this.key + " " + this.selfAddress.getHostName() + " " +  this.selfAddress.getPort());
		m.read();
		m.close();
	}
	
	public void foundNewFinger(InetSocketAddress finger) {
		int key = this.hash(finger);
		for(int i = 0; i < M; i++) {
			int prev_key = this.hash(this.fingerTable.get(i));
			if(betweenOpenOpen(this.key,prev_key,key)) {
				this.fingerTable.replace(i, finger);
			}
		}
	}
	
	public void deleteFinger(int oldKey, InetSocketAddress finger) {
		for(int i = 0; i < M; i++) {
			if(hash(this.fingerTable.get(i)) == oldKey)
				this.fingerTable.replace(i, finger);
		}
		this.printFingerTable();
	}
	
	public void sendNotifyNewFinger(int originKey , String ip, int port) throws Exception {
		int pred_key = this.hash(this.predecessor);
		
		System.out.println(this.positiveModule((int) (originKey - Math.pow(2, M-1)), (int)  Math.pow(2, M)) + "  " + originKey + " " + pred_key);
		if(! betweenOpenOpen( this.positiveModule((int) (originKey - Math.pow(2, M-1)), (int)  Math.pow(2, M)) , originKey , pred_key ))
			return;
		SSLMessage m = new SSLMessage(this.predecessor);
		m.write("NEWFINGER " + originKey + " " + ip + " " +  port + " ");
		m.read();
		m.close();
		System.out.println("Sent new finger to predecessor");
	}
	
	public void sendNotifyDeleteFinger(int originKey , int oldKey, String ip, int port) throws Exception {
		int pred_key = this.hash(this.predecessor);
		
		int max_origin = this.positiveModule((int) (originKey - Math.pow(2, M-1)), (int)  Math.pow(2, M));
		System.out.println(max_origin + "  " + originKey + " " + pred_key);
		if(! betweenOpenOpen( max_origin , originKey , pred_key ))
			return;
		SSLMessage m = new SSLMessage(this.predecessor);
		m.write("DELETEFINGER " + originKey + " " + oldKey + " " + ip + " " +  port + " ");
		m.read();
		m.close();
		System.out.println("Sent new delete to predecessor");
	}
	
	public void updateFingerTable() throws Exception {
		System.out.println("Going to update fingers");
		InetSocketAddress finger = null;
		for(int i = 0; i < M; i++) {
			int index = this.positiveModule((int) (this.key + Math.pow(2, i)), (int)  Math.pow(2, M));
			if(finger == null || !betweenOpenOpen(this.key,hash(finger),index))
				finger = this.lookup(index);
			this.fingerTable.put(i, finger);
		}
	}
	
	public void fix_fingers() throws Exception {
		if(this.nextFinger == M)
			this.nextFinger = 0;
		int index = this.positiveModule((int) (this.key + Math.pow(2, this.nextFinger)), (int)  Math.pow(2, M));
		this.fingerTable.put(this.nextFinger, this.lookup(index));
		this.nextFinger++;
	}
	
	public void setSuccessor(InetSocketAddress suc) {
		if(this.successor == null)
			this.successor = suc;
		else {
			if(this.selfAddress.getHostName().equals(suc.getHostName())
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
	
	public ServerSocket getServerSocket() {
		return this.serverSocket;
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
			if(betweenOpenOpen(this.key,key,hash(ret)))
				return ret;
		}
		return this.successor;
	}
	
	
	
	
	
	
	public void printKnowns() {
		System.out.println("My key: " + this.key);
		if(this.predecessor != null)
			System.out.println("Predecessor: " + this.predecessor.getHostName() + "   " + this.predecessor.getPort() + " " + hash(this.predecessor) );
		if(this.successor != null)
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
