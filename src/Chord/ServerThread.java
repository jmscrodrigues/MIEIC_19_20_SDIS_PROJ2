package Chord;


public class ServerThread implements Runnable {

	Chord chord;
	SSLServer server;
	
	ServerThread(Chord c){
		this.chord = c;
		server = this.chord.sllServer;
	}
	
	@Override
	public void run() {
		try {
			server.start();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public void stop() {
		server.stop();
	}
	
}