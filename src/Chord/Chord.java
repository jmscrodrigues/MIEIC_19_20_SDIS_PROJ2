package Chord;

import java.io.IOException;
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

import Peer.Peer;


public class Chord {
	
	static final int M = 15;
	static final int UPDATE_TIME = 10;
	
	private Peer peer;
	
	private InetSocketAddress selfAddress;
	private InetSocketAddress successor;
	private InetSocketAddress predeccessor;
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
        	this.peer.getExecuter().execute(sllServer);
        } catch (Exception e) {
            System.err.println("Could not create the server");
            e.printStackTrace();
        }
        this.selfAddress = new InetSocketAddress("localhost" , port);
        this.successor = null;
        this.predeccessor = null;
        this.key = this.hash(selfAddress);
        
        
        this.getPeer().getExecuter().schedule(new ChordStabilizer(this), UPDATE_TIME, TimeUnit.SECONDS);
        
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
		
		while(this.connected.get() != true) {
		}
		
		this.updateFingerTable();
		
		System.out.println("Asking data to sucessor initiated");
		
		m = new SSLMessage(this.successor);
		m.write("GETDATA " + this.key + " " + this.selfAddress.getHostName() + " " +  this.selfAddress.getPort());
		m.read();
		m.close();

		System.out.println("Node joined ring");
		
	}
	
	public void leaveRing() throws Exception {
		System.out.println("Started leaving process");
		
		if(this.successor != null && this.predeccessor != null) {
			
			System.out.println("Transfering data to sucessor initiated");
			
			SSLMessage m = new SSLMessage(this.successor);
			ConcurrentHashMap<Integer,byte[]> data = this.memory.getData();
			Set<Entry<Integer, byte[]>> entrySet = data.entrySet();
			for(Iterator<ConcurrentHashMap.Entry<Integer, byte[]>> itr = entrySet.iterator(); itr.hasNext();) {
				ConcurrentHashMap.Entry<Integer, byte[]> entry = itr.next();
				m.write("PUT " + entry.getKey() , entry.getValue());
				m.read();
			}
			m.close();
			
			System.out.println("Transfering data to sucessor done");
			
			m = new SSLMessage(this.successor);
			m.write("SETPREDECCESSOR " + this.hash(predeccessor) + " " + this.predeccessor.getHostName() + " " +  this.predeccessor.getPort());
			//m.read();
			m.close();
			
			m = new SSLMessage(this.predeccessor);
			m.write("SETSUCCESSOR " + this.hash(successor) + " " + this.successor.getHostName() + " " +  this.successor.getPort());
			//m.read();
			m.close();
			
			m = new SSLMessage(this.predeccessor);
			m.write("DELETEFINGER " + this.hash(predeccessor) + " " + this.key + " " + this.successor.getHostName() + " " +  this.successor.getPort() );
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
		//se nao existir sucessor, significa que é o unico na rede
		if(this.successor == null) {
			if(type == -1) { // se for um lookup do successor
				InetSocketAddress new_peer = new InetSocketAddress(ip,port);
				this.setPredeccessor(new_peer);
				SSLMessage m = new SSLMessage(this.predeccessor);
				this.setSuccessor(new_peer);
	    		m.write("SETSUCCESSOR " + this.key + " " + this.selfAddress.getHostName() + " " +  this.selfAddress.getPort());
	    		m.read();
	    		m.write("SETPREDECCESSOR " + this.key + " " + this.selfAddress.getHostName() + " " +  this.selfAddress.getPort());
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
	    		m.write("SETPREDECCESSOR " + key + " " + ip + " " +  port);
	    		m.read();
	    		m.close();
				
	    		m = new SSLMessage(ip, port);
	    		m.write("SETSUCCESSOR " + this.hash(successor) + " " + this.successor.getHostName() + " " +  this.successor.getPort());
	    		m.read();
				
	    		InetSocketAddress newFinger = new InetSocketAddress(ip,port);
				this.setSuccessor(newFinger);
				m = new SSLMessage(ip, port);
	    		m.write("SETPREDECCESSOR " + this.key + " " + this.selfAddress.getHostName() + " " +  this.selfAddress.getPort());
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
		InetSocketAddress ret = null;
		
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
    		ret = new InetSocketAddress(parts[3],Integer.parseInt(parts[4].replaceAll("\\D", "")));
		}
		return ret;
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
		Set<Entry<Integer, byte[]>> entrySet = data.entrySet();
		SSLMessage m = new SSLMessage(pre);
		for(Iterator<ConcurrentHashMap.Entry<Integer, byte[]>> itr = entrySet.iterator(); itr.hasNext();) {
			ConcurrentHashMap.Entry<Integer, byte[]> entry = itr.next();
			int id = entry.getKey();
			if(betweenOpenClose(this.key,key,id)) {
				m.write("PUT " + entry.getKey() , entry.getValue());
				m.read();
				data.remove(id);
			}
		}
		m.close();
	}
	
	/*
	 * Get predecessor of node passed as argument
	 */
	public InetSocketAddress getPredeccessor(InetSocketAddress node) throws Exception {
		InetSocketAddress ret = null;
		SSLMessage m = new SSLMessage(this.successor);
		m.write("GETPREDECCESSOR " + this.key + " " + this.selfAddress.getHostName() + " " +  this.selfAddress.getPort());
		byte [] buf = m.read();
		m.close();
		
		String message = new String(buf, StandardCharsets.UTF_8);
	    System.out.println(message);
	    String[] parts = message.split(" ");
	    
	    ret = new InetSocketAddress(parts[1],Integer.parseInt(parts[2]));
	    System.out.println("Get predeccessor done!");
		return ret;
	}
	
	public void notify(InetSocketAddress pre) {
		if(this.predeccessor == null || betweenOpenOpen(hash(this.predeccessor),this.key,hash(pre))) {
			this.setPredeccessor(pre);
		}
	}
	
	
	public void stabilize() throws Exception {
		if(this.successor == null)
			return;
		InetSocketAddress x = this.getPredeccessor(this.successor);
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
		int pred_key = this.hash(this.predeccessor);
		
		System.out.println(this.positiveModule((int) (originKey - Math.pow(2, M-1)), (int)  Math.pow(2, M)) + "  " + originKey + " " + pred_key);
		if(! betweenOpenOpen( this.positiveModule((int) (originKey - Math.pow(2, M-1)), (int)  Math.pow(2, M)) , originKey , pred_key ))
			return;
		SSLMessage m = new SSLMessage(this.predeccessor);
		m.write("NEWFINGER " + originKey + " " + ip + " " +  port + " ");
		m.read();
		m.close();
		System.out.println("Sent new finger to predecessor");
	}
	
	public void sendNotifyDeleteFinger(int originKey , int oldKey, String ip, int port) throws Exception {
		int pred_key = this.hash(this.predeccessor);
		
		int max_origin = this.positiveModule((int) (originKey - Math.pow(2, M-1)), (int)  Math.pow(2, M));
		System.out.println(max_origin + "  " + originKey + " " + pred_key);
		if(! betweenOpenOpen( max_origin , originKey , pred_key ))
			return;
		SSLMessage m = new SSLMessage(this.predeccessor);
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
	
	public void setPredeccessor(InetSocketAddress pre) {
		if(this.predeccessor == null)
			this.predeccessor = pre;
		else {
			if(this.selfAddress.getHostName().equals(pre.getHostName())
					&& this.selfAddress.getPort() == pre.getPort())
				this.predeccessor = null;
			else
				this.predeccessor = pre;
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
		InetSocketAddress ret = null;
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
		if(this.predeccessor != null)
			System.out.println("Predeccessor: " + this.predeccessor.getHostName() + "   " + this.predeccessor.getPort() + " " + hash(this.predeccessor) );
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
		if(a < b && a < c && c <= b)
			return true;
		if(a > b && (c > a || c <= b))
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
	
	public InetSocketAddress getPredeccessor() {
		return this.predeccessor;
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
