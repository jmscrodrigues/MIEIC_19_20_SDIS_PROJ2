package Chord;


import javax.net.ssl.*;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;


public class SSLMessage extends SSLBase {
	
	static final String CRLF = String.valueOf((char)0xD) + String.valueOf((char)0xA) ;
	
	private String ip;

	private int port;

    private SSLEngine engine;

    private SocketChannel socketChannel;
    
    public SSLMessage(InetSocketAddress address) {
    	this(address.getHostName(),address.getPort());
    }
    
    public SSLMessage(String ip, int port)  {
    	this.debug = false;
    	this.ip = ip;
    	this.port = port;

        SSLContext context = null;
		try {
			context = SSLContext.getInstance("SSL");
			KeyManager[] keys = createKeyManagers("./client.jks", "storepass", "keypass");
			TrustManager[] trusts = createTrustManagers("./trustedCerts.jks", "storepass");
			context.init(keys, trusts, null);
		} catch (Exception e) {
			System.out.println(e);
			e.printStackTrace();
		}
        
        engine = context.createSSLEngine(ip, port);
        engine.setUseClientMode(true);

        SSLSession session = engine.getSession();
        send_plainData = ByteBuffer.allocate(64000 + 50);
        send_encryptedData = ByteBuffer.allocate(session.getPacketBufferSize());
        rcv_plainData = ByteBuffer.allocate(64000 + 50);
        rcv_encryptedData = ByteBuffer.allocate(session.getPacketBufferSize());
        
        try {
			this.connect();
		} catch (Exception e) {
			System.out.println(e);
			e.printStackTrace();
		}
        
    }

    private boolean connect() throws Exception {
    	socketChannel = SocketChannel.open();
    	socketChannel.configureBlocking(false);
    	socketChannel.connect(new InetSocketAddress(this.ip, this.port));
    	
    	while (!socketChannel.finishConnect()) {
    	}

    	engine.beginHandshake();
    	return doHandshake(socketChannel, engine);
    }

    public void write(String message) {
        try {
			write(socketChannel, engine, message.getBytes());
		} catch (IOException e) {
			System.out.println(e);
			e.printStackTrace();
		}
    }
    
    public void write(byte[] message) {
        try {
			write(socketChannel, engine, message);
		} catch (IOException e) {
			System.out.println(e);
			e.printStackTrace();
		}
    }
    
    public void write(String d, byte [] body){
		String header = d + " " + CRLF + CRLF;
		byte[] headerB = header.getBytes();
		byte [] data = new byte[headerB.length + body.length];
		System.arraycopy(headerB, 0, data, 0, headerB.length);
		System.arraycopy(body, 0, data, headerB.length, body.length);
		this.write(data);
	}


    @Override
    protected synchronized void write(SocketChannel socketChannel, SSLEngine engine, byte[] message) throws IOException {

    	if(debug) System.out.println("Writing to server");
        send_plainData.clear();
        //System.out.println("message length: " +  message.length);
        //System.out.println("buffer length: " + send_plainData.capacity());
        send_plainData.put(message);
        send_plainData.flip();
        while (send_plainData.hasRemaining()) {
            // The loop has a meaning for (outgoing) messages larger than 16KB.
            // Every wrap call will remove 16KB from the original message and send it to the remote peer.
        	send_encryptedData.clear();
            SSLEngineResult result = engine.wrap(send_plainData, send_encryptedData);
            switch (result.getStatus()) {
            case OK:
            	send_encryptedData.flip();
                while (send_encryptedData.hasRemaining()) {
                    socketChannel.write(send_encryptedData);
                }
                //System.out.println("Message sent to server: " + new String(message));
                break;
            case BUFFER_OVERFLOW:
            	send_encryptedData = enlargePacketBuffer(engine, send_encryptedData);
                break;
            case BUFFER_UNDERFLOW:
                throw new SSLException("Buffer underflow occured after a wrap. I don't think we should ever get here.");
            case CLOSED:
                closeConnection(socketChannel, engine);
                return;
            default:
                throw new IllegalStateException("Invalid SSL status: " + result.getStatus());
            }
        }
    }

    public byte[] read() {
        try {
			return read(socketChannel, engine);
		} catch (Exception e) {
			System.out.println(e);
			e.printStackTrace();
		}
        return null;
    }

    @Override
    protected byte[] read(SocketChannel socketChannel, SSLEngine engine) throws Exception  {

    	if(debug) System.out.println("Reading from server");
    	
    	byte[] data = null;

        rcv_encryptedData.clear();
        int timeout = 50;
        boolean exit = false;
        while (!exit) {
            int bytesRead = socketChannel.read(rcv_encryptedData);
            if (bytesRead > 0) {
            	rcv_encryptedData.flip();
                while (rcv_encryptedData.hasRemaining()) {
                	rcv_plainData.clear();
                    SSLEngineResult res = engine.unwrap(rcv_encryptedData, rcv_plainData);
                    switch (res.getStatus()) {
                    case OK:
                    	rcv_plainData.flip();
                    	data = Arrays.copyOfRange(rcv_plainData.array(), 0, res.bytesProduced());
                        //System.out.println("Message: " + new String(data));
                        exit = true;
                        break;
                    case BUFFER_OVERFLOW:
                    	rcv_plainData = enlargeApplicationBuffer(engine, rcv_plainData);
                        break;
                    case BUFFER_UNDERFLOW:
                    	rcv_encryptedData = handleBufferUnderflow(engine, rcv_encryptedData);
                        break;
                    case CLOSED:
                        closeConnection(socketChannel, engine);
                        return data;
                    default:
                        throw new IllegalStateException("Invalid SSL status: " + res.getStatus());
                    }
                }
            } else if (bytesRead < 0) {
                handleEndOfStream(socketChannel, engine);
                return null;
            }
            Thread.sleep(timeout);
        }
        return data;
    }

    public void close() {
    	if(debug) System.out.println("About to close connection with the server...");
        try {
			closeConnection(socketChannel, engine);
		} catch (IOException e) {
			System.out.println(e);
			e.printStackTrace();
		}
        executor.shutdown();
        if(debug) System.out.println("Goodbye!");
    }

}