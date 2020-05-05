import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;


public class Chord {
	
	static final int M = 6;
	
	private Peer peer;
	
	private InetSocketAddress selfAddress;
	private InetSocketAddress successor;
	private InetSocketAddress predeccessor;
	private InetSocketAddress access_peer;
	
	private ServerSocket serverSocket;
	
	private ConcurrentHashMap<Integer,InetSocketAddress> fingerTable = new ConcurrentHashMap<>();
	
	int port;
	int key;
	
	private AtomicBoolean connected = new AtomicBoolean(false);
	private AtomicBoolean updating_fingers = new AtomicBoolean(false);
	
	public Chord(Peer p ,int port) {
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
        this.predeccessor = null;
        this.key = this.hash(selfAddress);
        
        this.peer.getExecuter().execute(new ChrodThread(this));
        
    }
	
	private int hash(InetSocketAddress addrss) {
		return Math.abs((int) (addrss.hashCode() % Math.pow(2, M)));
	}
	
	
	public void joinRing(InetSocketAddress peer) {
		this.access_peer = peer;
		
		System.out.println("Started join process");
		
		String data = "GETSUCCESSOR " + this.key + " " + this.selfAddress.getHostName() + " " +  this.selfAddress.getPort();
		Message m = new Message(data);
		m.sendMessage(peer.getHostName(), peer.getPort());
		
		while(this.connected.get() == false) {
		}
		//updating_fingers.set(true);
		//System.out.println("Will find fingers");
		this.updateFingerTable();
		/*while(updating_fingers.get() == true) {
		}*/
		//System.out.println("Will update others fingers");
		//this.updateOthersFingers();
		
		
		System.out.println("Node joined ring");
	}
	
	public void leaveRing() {
		System.out.println("Started leaving process");
		
		if(this.successor != null && this.predeccessor != null) {
			Message m = new Message("SETPREDECCESSOR " + this.hash(predeccessor) + " " + this.predeccessor.getHostName() + " " +  this.predeccessor.getPort());
			m.sendMessage(this.successor.getHostName(), this.successor.getPort());
			
			m = new Message("SETSUCCESSOR " + this.hash(successor) + " " + this.successor.getHostName() + " " +  this.successor.getPort());
			m.sendMessage(this.predeccessor.getHostName(), this.predeccessor.getPort());
			
			//this.sendNotifyDeleteFinger(this.hash(predeccessor), this.key , this.successor.getHostName(), this.successor.getPort());
			
			m = new Message("DELETEFINGER " + this.hash(predeccessor) + " " + this.key + " " + this.successor.getHostName() + " " +  this.successor.getPort() );
			m.sendMessage(this.predeccessor.getHostName(), this.predeccessor.getPort());
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
		
		//se nao existir sucessor, significa que é o unico na rede
		if(this.successor == null) {
			if(type == -1) { // se for um lookup do successor
				InetSocketAddress new_peer = new InetSocketAddress(ip,port);
				this.setPredeccessor(new_peer);
				Message m = new Message("SETSUCCESSOR " + this.key + " " + this.selfAddress.getHostName() + " " +  this.selfAddress.getPort());
	    		m.sendMessage(ip, port);
	    		
				this.setSuccessor(new_peer);
	    		m = new Message("SETPREDECCESSOR " + this.key + " " + this.selfAddress.getHostName() + " " +  this.selfAddress.getPort());
	    		m.sendMessage(ip, port);
	    		this.populateFingerTable(new_peer);
			}
			return;
		}
		//se a chave que se procura estiver entre mim e o meu sucessor, logo deu hit!
		if(this.between(this.key, this.hash(successor), key)) {
			if(type == -1) { // se for um lookup do successor
				Message m = new Message("SETPREDECCESSOR " + key + " " + ip + " " +  port);
	    		m.sendMessage(this.successor.getHostName(), this.successor.getPort());
				
	    		m = new Message("SETSUCCESSOR " + this.hash(successor) + " " + this.successor.getHostName() + " " +  this.successor.getPort());
	    		m.sendMessage(ip, port);
				
	    		InetSocketAddress newFinger = new InetSocketAddress(ip,port);
				this.setSuccessor(newFinger);
				m = new Message("SETPREDECCESSOR " + this.key + " " + this.selfAddress.getHostName() + " " +  this.selfAddress.getPort());
	    		m.sendMessage(ip, port);
	    		
	    		//novo sucessor encontrado, por isso mudar os fingers
	    		this.foundNewFinger(newFinger);
	    		this.sendNotifyNewFinger(this.key,ip,port);
	    		
			}else { // se for uma procura de um finger
				Message m = new Message("SETFINGER " + this.hash(this.successor) + " " + this.successor.getHostName() + " " +  this.successor.getPort() + " " + type);
	    		m.sendMessage(ip, port);
			}
    		return;
		}else { //se a chave estiver para la do sucessor
			InetSocketAddress closest = closest_preceding_node(key);
			System.out.println("Closest to " + key + " is:  " + closest);
			if(type == -1) { // se for um lookup do successor
				String data = "GETSUCCESSOR " + key + " " + ip + " " +  port;
				Message m = new Message(data);
				m.sendMessage(closest.getHostName(), closest.getPort());
			}else { // se for uma prucura de finger
				Message m = new Message("FINDFINGER " + key + " " + ip + " " +  port + " " + type);
	    		m.sendMessage(closest.getHostName(), closest.getPort());
			}

		}
	}
	
	public void foundNewFinger(InetSocketAddress finger) {
		int key = this.hash(finger);
		for(int i = 0; i < M; i++) {
			int prev_key = this.hash(this.fingerTable.get(i));
			if(between(this.key,prev_key,key)) {
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
	
	public void sendNotifyNewFinger(int originKey , String ip, int port) {
		int pred_key = this.hash(this.predeccessor);
		
		System.out.println(this.positiveModule((int) (originKey - Math.pow(2, M-1)), (int)  Math.pow(2, M)) + "  " + originKey + " " + pred_key);
		if(! between( this.positiveModule((int) (originKey - Math.pow(2, M-1)), (int)  Math.pow(2, M)) , originKey , pred_key ))
			return;
		Message m = new Message("NEWFINGER " + originKey + " " + ip + " " +  port + " ");
		m.sendMessage(this.predeccessor.getHostName(), this.predeccessor.getPort());
		System.out.println("Sent new finger to predecessor");
	}
	
	public void sendNotifyDeleteFinger(int originKey , int oldKey, String ip, int port) {
		int pred_key = this.hash(this.predeccessor);
		
		System.out.println(this.positiveModule((int) (originKey - Math.pow(2, M-1)), (int)  Math.pow(2, M)) + "  " + originKey + " " + pred_key);
		if(! between( this.positiveModule((int) (originKey - Math.pow(2, M-1)), (int)  Math.pow(2, M)) , originKey , pred_key ))
			return;
		Message m = new Message("DELETEFINGER " + originKey + " " + oldKey + " " + ip + " " +  port + " ");
		m.sendMessage(this.predeccessor.getHostName(), this.predeccessor.getPort());
		System.out.println("Sent new delete to predecessor");
	}
	
	
	public void updateFingerTable() {
		for(int i = 0; i < M; i++) {
			int index = this.positiveModule((int) (this.key + Math.pow(2, i)), (int)  Math.pow(2, M));
			this.find_successor(index, this.selfAddress.getHostName(), this.selfAddress.getPort(), i);
		}
	}
	
	public void updateOthersFingers() {
		/*for(int i = 0; i < M; i++) {
			//Message m = new Message("FINDFINGER " + key + " " + ip + " " +  port + " " + type);
    		//m.sendMessage(closest.getHostName(), closest.getPort());
		}*/
		
		int destiny = this.positiveModule((int) (this.hash(predeccessor) - Math.pow(2, M)), (int)  Math.pow(2, M));
		int max = this.positiveModule((int) (this.hash(selfAddress) - Math.pow(2, 0)), (int)  Math.pow(2, M));
		InetSocketAddress closest = closest_preceding_node(destiny);
		
		Message m = new Message("UPDATEFINGERTABLE " + destiny + " " + max + " " + 1);
		m.sendMessage(closest.getHostName(), closest.getPort());
	}
	
	public void checkUpdateFingerTable(int destiny, int max , int ttl) {
		if(between(destiny,max,this.key)) {
			this.updateFingerTable();
			if(hash(this.successor) < max) {
				Message m = new Message("UPDATEFINGERTABLE " + destiny + " " + max + " " + 1);
				m.sendMessage(successor.getHostName(), successor.getPort());
			}
		}else if(ttl < 0){
			ttl--;
			InetSocketAddress closest = closest_preceding_node(destiny);
			Message m = new Message("UPDATEFINGERTABLE " + destiny + " " + max + " " + ttl);
			m.sendMessage(closest.getHostName(), closest.getPort());
		}
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
			if(between(this.key,key,hash(ret)))
				return ret;
		}
		return this.successor;
	}
	
	public void printKnowns() {
		if(this.predeccessor != null)
			System.out.println("Predeccessor: " + this.predeccessor.getHostName() + "   " + this.predeccessor.getPort() );
		if(this.successor != null)
			System.out.println("Successor: " + this.successor.getHostName() + "   " + this.successor.getPort() );
	}
	
	public void printFingerTable() {
		for(int i = 0; i < M; i++) {
			System.out.println(i+": " + this.fingerTable.get(i) + "  maps to " + (int) ((this.key + Math.pow(2, i)) %  Math.pow(2, M)));
		}
	}
	
	private boolean between(int a, int b, int c) {
		if(a < b && a <= c && c < b)
			return true;
		if(a > b && (c >= a || c < b))
			return true;
		return false;
	}
	
	private int positiveModule(int a, int m) {
		while(a < 0)
			a+=m;
		return a % m;
	}
	
}
