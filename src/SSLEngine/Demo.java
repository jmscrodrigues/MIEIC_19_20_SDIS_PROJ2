package SSLEngine;



public class Demo {
	
	ServerThread serverRunnable;
	
	public Demo() {
		serverRunnable = new ServerThread();
		Thread server = new Thread(serverRunnable);
		server.start();
	}
	
	public void runDemo() throws Exception {
		
		// System.setProperty("javax.net.debug", "all");
		
		for(int i = 0; i < 10 ; i++) {
			SSLMessage m = new SSLMessage("localhost",9222,"ola ola");
			System.out.println(new String(m.sendMessageAndRead()));
		}
		

		serverRunnable.stop();
	}
	
	public static void main(String[] args) throws Exception {
		Demo demo = new Demo();
		Thread.sleep(1000);	// Give the server some time to start.
		demo.runDemo();
	}
	
}
