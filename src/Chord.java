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
	
	static final int M = 15;
	
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
		return (int) (addrss.hashCode() % Math.pow(2, M));
	}
	
	
	public void joinRing(InetSocketAddress peer) {
		this.access_peer = peer;
		
		System.out.println("Started join process");
		
		String data = "GETSUCCESSOR " + this.key + " " + this.selfAddress.getHostName() + " " +  this.selfAddress.getPort();
		Message m = new Message(data);
		m.sendMessage(peer.getHostName(), peer.getPort());
		
		while(this.connected.get() == false) {
		}
		System.out.println("Will find fingers");
		this.updateFingerTable();
		
	}
	
	
	/**
	 * 
	 * @param key Key of node joining
	 * @param ip Its ip
	 * @param port Its port
	 * @param type: -1: Node joining  >=0: Updating fingerTable of index type
	 */
	public void find_successor(int key, String ip, int port, int type) { 
		if(this.successor == null) {
			if(type == -1) {
				InetSocketAddress new_peer = new InetSocketAddress(ip,port);
				this.setPredeccessor(new_peer);
				Message m = new Message("SETSUCCESSOR " + this.key + " " + this.selfAddress.getHostName() + " " +  this.selfAddress.getPort());
	    		m.sendMessage(ip, port);
	    		
				this.setSuccessor(new_peer);
	    		m = new Message("SETPREDECCESSOR " + this.key + " " + this.selfAddress.getHostName() + " " +  this.selfAddress.getPort());
	    		m.sendMessage(ip, port);
	    		this.populateFingerTable(new_peer);
			}
		}else {
			if((this.key <= key && key < this.hash(successor))
					|| (this.key > this.hash(successor) && (this.key <= key || key < this.hash(successor)))) {
				if(type == -1) {
					Message m = new Message("SETPREDECCESSOR " + key + " " + ip + " " +  port);
		    		m.sendMessage(this.successor.getHostName(), this.successor.getPort());
					
		    		m = new Message("SETSUCCESSOR " + this.hash(successor) + " " + this.successor.getHostName() + " " +  this.successor.getPort());
		    		m.sendMessage(ip, port);
					
					this.setSuccessor(new InetSocketAddress(ip,port));
					m = new Message("SETPREDECCESSOR " + this.key + " " + this.selfAddress.getHostName() + " " +  this.selfAddress.getPort());
		    		m.sendMessage(ip, port);
				}else {
					Message m = new Message("SETFINGER " + this.hash(successor) + " " + this.successor.getHostName() + " " +  this.successor.getPort() + " " + type);
		    		m.sendMessage(ip, port);
				}
	    		
			}else {
				//InetSocketAddress closest = this.successor;
				InetSocketAddress closest = closest_preceding_node(key);
				System.out.println("Closest:  " + closest);
				if(type == -1) {
					String data = "GETSUCCESSOR " + key + " " + ip + " " +  port;
					Message m = new Message(data);
					m.sendMessage(closest.getHostName(), closest.getPort());
				}else {
					Message m = new Message("FINDFINGER " + key + " " + ip + " " +  port + " " + type);
		    		m.sendMessage(closest.getHostName(), closest.getPort());
				}
			}
		}
	}
	
	
	public void updateFingerTable() {
		for(int i = 0; i < M; i++) {
			int index = (int) ((this.key + Math.pow(2, i)) %  Math.pow(2, M));
			this.find_successor(index, this.selfAddress.getHostName(), this.selfAddress.getPort(), i);
		}
	}
	
	public void setSuccessor(InetSocketAddress suc) {
		System.out.println("New successor found");
		this.successor = suc;
		this.connected.set(true);
		printKnowns();
	}
	public void setPredeccessor(InetSocketAddress pre) {
		System.out.println("New predecessor found");
		this.predeccessor = pre;
		printKnowns();
	}
	
	public void setFinger(int index, InetSocketAddress addrs ) {
		//System.out.println("Setted finger");
		this.fingerTable.put(index, addrs);
		//this.printFingerTable();
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
		for(int i = M - 1; i >= 0; i--) {
			ret = this.fingerTable.get(i);
			if(ret == null)
				continue;
			if(hash(ret) < key)
				break;
			else
				ret = this.fingerTable.get(i);
		}
		return ret;
	}
	
	public void printKnowns() {
		if(this.predeccessor != null)
			System.out.println("Predeccessor: " + this.predeccessor.getHostName() + "   " + this.predeccessor.getPort() );
		if(this.successor != null)
			System.out.println("Successor: " + this.successor.getHostName() + "   " + this.successor.getPort() );
	}
	
	public void printFingerTable() {
		for(int i = 0; i < M; i++) {
			System.out.println(i+": " + this.fingerTable.get(i));
		}
	}
	
	
}
