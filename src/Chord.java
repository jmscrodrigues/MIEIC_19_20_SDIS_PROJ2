import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;


public class Chord {
	
	static final int M = 15;
	
	private Peer peer;
	
	private InetSocketAddress selfAddress;
	private InetSocketAddress successor;
	private InetSocketAddress predeccessor;
	private InetSocketAddress access_peer;
	
	private ServerSocket serverSocket;
	
	int port;
	int key;
	
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
		
	}
	
	
	
	public void find_successor(int key, String ip, int port) {
		if(this.successor == null) {
			this.setPredeccessor(new InetSocketAddress(ip,port));
			Message m = new Message("SETSUCCESSOR " + this.key + " " + this.selfAddress.getHostName() + " " +  this.selfAddress.getPort());
    		m.sendMessage(ip, port);
    		
			this.setSuccessor(new InetSocketAddress(ip,port));
    		m = new Message("SETPREDECCESSOR " + this.key + " " + this.selfAddress.getHostName() + " " +  this.selfAddress.getPort());
    		m.sendMessage(ip, port);
		}else {
			if((this.key < key && key < this.hash(successor))
					|| (this.key > this.hash(successor) && this.key < key)) {
				Message m = new Message("SETPREDECCESSOR " + key + " " + ip + " " +  port);
	    		m.sendMessage(this.successor.getHostName(), this.successor.getPort());
				
	    		m = new Message("SETSUCCESSOR " + this.hash(successor) + " " + this.successor.getHostName() + " " +  this.successor.getPort());
	    		m.sendMessage(ip, port);
				
				this.setSuccessor(new InetSocketAddress(ip,port));
				m = new Message("SETPREDECCESSOR " + this.key + " " + this.selfAddress.getHostName() + " " +  this.selfAddress.getPort());
	    		m.sendMessage(ip, port);
	    		
			}else {
				String data = "GETSUCCESSOR " + key + " " + ip + " " +  port;
				Message m = new Message(data);
				m.sendMessage(this.successor.getHostName(), this.successor.getPort());
			}
		}
	}
	
	public void setSuccessor(InetSocketAddress suc) {
		System.out.println("New successor found");
		this.successor = suc;
		printKnowns();
	}
	public void setPredeccessor(InetSocketAddress pre) {
		System.out.println("New predecessor found");
		this.predeccessor = pre;
		printKnowns();
	}
	
	public Peer getPeer() {
		return this.peer;
	}
	
	public ServerSocket getServerSocket() {
		return this.serverSocket;
	}
	
	public void printKnowns() {
		if(this.predeccessor != null)
			System.out.println("Predeccessor: " + this.predeccessor.getHostName() + "   " + this.predeccessor.getPort() );
		if(this.successor != null)
			System.out.println("Successor: " + this.successor.getHostName() + "   " + this.successor.getPort() );
	}
	
	
}
