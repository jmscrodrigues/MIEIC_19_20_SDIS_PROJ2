package Peer;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.concurrent.ScheduledThreadPoolExecutor;

public class PeerServer implements Runnable{

	private final Peer peer;
	
	PeerServer(Peer p){
		this.peer = p;
	}
	
	@Override
	public void run() {
		ScheduledThreadPoolExecutor scheduler_executor = this.peer.getExecutor();
		ServerSocket server = this.peer.getServerSocket();

		while (true) {

            try {
            	
            	scheduler_executor.execute(new PeerMessageHandler(this.peer,server.accept()));
                
            } catch (IOException e) {
            	if(server.isClosed())
            		System.out.println("Server closed");
            	else
            		System.out.println(e.toString());
                break;
            }
        }
	}
}
