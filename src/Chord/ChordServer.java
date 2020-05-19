package Chord;
import java.io.IOException;
import java.util.concurrent.ScheduledThreadPoolExecutor;

public class ChordServer extends SSLServer implements Runnable{

	private final Chord chord;
	private final ScheduledThreadPoolExecutor scheduler_executor;

	ChordServer(Chord c, int port){
		super(port);
		this.chord = c;
		this.scheduler_executor = this.chord.getPeer().getExecutor();
	}
	
	@Override
	public void run() {
		while (this.isServerActive()) {
            try {
            	this.scheduler_executor.execute(new ChordMessageHandler(this.chord, this.acceptConnection()));
                
            } catch (IOException e) {
            	if (this.serverIsClosed())
            		System.out.println("Server closed");
            	else
            		System.out.println(e.toString());
                break;
            }
        }
	}
}
