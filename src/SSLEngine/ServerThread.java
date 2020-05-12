package SSLEngine;



public class ServerThread implements Runnable {

	SSLServer server;
	
	@Override
	public void run() {
		try {
			server = new  SSLServer("TLSv1.2", "localhost", 9222);
			server.start();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Should be called in order to gracefully stop the server.
	 */
	public void stop() {
		server.stop();
	}
	
}
