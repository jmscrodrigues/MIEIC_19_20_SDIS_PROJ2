import java.io.IOException;
import java.net.ServerSocket;
import java.util.concurrent.ScheduledThreadPoolExecutor;


public class ChrodThread implements Runnable{

	private Chord chord;
	
	ChrodThread(Chord c){
		this.chord = c;
	}
	
	@Override
	public void run() {
		ScheduledThreadPoolExecutor scheduler_executer = this.chord.getPeer().getExecuter();
		ServerSocket server = this.chord.getServerSocket();
		while (true) {
            try {
            	
            	scheduler_executer.execute(new ChordMessageHandler(this.chord,server.accept()));
                
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
