package Chord;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.concurrent.ScheduledThreadPoolExecutor;

public class ChordThread implements Runnable{

	private final Chord chord;

	
	ChordThread(Chord c){
		this.chord = c;
	}
	
	@Override
	public void run() {
		ScheduledThreadPoolExecutor scheduler_executor = this.chord.getPeer().getExecutor();
		ServerSocket server = this.chord.getServerSocket();

		while (true) {
            try {
            	scheduler_executor.execute(new ChordMessageHandler(this.chord,server.accept()));
                
            } catch (IOException e) {
            	if (server.isClosed())
            		System.out.println("Server closed");
            	else
            		System.out.println(e.toString());
                break;
            }
        }
	}

}
