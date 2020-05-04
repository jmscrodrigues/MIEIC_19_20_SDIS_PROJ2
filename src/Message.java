import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;

public class Message {
	
	String data;
	
	Message(String d){
		this.data = d;
	}
	
	public void sendMessage(String ip, int port) {
		Socket socket = null;
		try {
			socket = new Socket(ip, port);
        } catch (IOException e) {
            System.err.println("Could not connect to peer");
            System.err.println(e);
            System.exit(-1);
        }
		

		byte[] message = data.getBytes();
		try {
			OutputStream out = socket.getOutputStream(); 
		    DataOutputStream dos = new DataOutputStream(out);
		    dos.writeInt(message.length);
		    dos.write(message,0,message.length);
		    socket.close();
		}catch(IOException e) {
			System.err.println("Error sending message.");
            System.err.println(e);
            System.exit(-1);
		}
	}
	
	
}
