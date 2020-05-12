package Chord;


import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.security.SecureRandom;

import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLSession;
import javax.net.ssl.TrustManager;


public class SSLMessage extends SLLBase {
	
	
	private String ip;

	private int port;

    private SSLEngine engine;

    private SocketChannel socketChannel;


    public SSLMessage(String ip, int port) throws Exception  {
    	this.ip = ip;
    	this.port = port;

        SSLContext context = SSLContext.getInstance("TLS");
        KeyManager[] keys = createKeyManagers("./client.jks", "storepass", "keypass");
        TrustManager[] trusts = createTrustManagers("./trustedCerts.jks", "storepass");
        context.init(keys, trusts, null);
        engine = context.createSSLEngine(ip, port);
        engine.setUseClientMode(true);

        SSLSession session = engine.getSession();
        send_plainData = ByteBuffer.allocate(1024);
        send_encryptedData = ByteBuffer.allocate(session.getPacketBufferSize());
        rcv_plainData = ByteBuffer.allocate(1024);
        rcv_encryptedData = ByteBuffer.allocate(session.getPacketBufferSize());
        
        this.connect();
        
    }

    public boolean connect() throws Exception {
    	socketChannel = SocketChannel.open();
    	socketChannel.configureBlocking(false);
    	socketChannel.connect(new InetSocketAddress(this.ip, this.port));

    	engine.beginHandshake();
    	return doHandshake(socketChannel, engine);
    }

    public void write(String message) throws IOException {
        write(socketChannel, engine, message.getBytes());
    }
    
    public void write(byte[] message) throws IOException {
        write(socketChannel, engine, message);
    }

    @Override
    protected void write(SocketChannel socketChannel, SSLEngine engine, byte[] message) throws IOException {

        System.out.println("Writting to server");

        send_plainData.clear();
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
                System.out.println("Message sent to server: " + new String(message));
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

    public void read() throws Exception {
        read(socketChannel, engine);
    }

    @Override
    protected void read(SocketChannel socketChannel, SSLEngine engine) throws Exception  {

        System.out.println("Readding from server");

        rcv_encryptedData.clear();
        int timeout = 50;
        boolean exit = false;
        while (!exit) {
            int bytesRead = socketChannel.read(rcv_encryptedData);
            if (bytesRead > 0) {
            	rcv_encryptedData.flip();
                while (rcv_encryptedData.hasRemaining()) {
                	rcv_plainData.clear();
                    SSLEngineResult r = engine.unwrap(rcv_encryptedData, rcv_plainData);
                    switch (r.getStatus()) {
                    case OK:
                    	rcv_plainData.flip();
                        System.out.println("Server response: " + new String(rcv_plainData.array()));
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
                        return;
                    default:
                        throw new IllegalStateException("Invalid SSL status: " + r.getStatus());
                    }
                }
            } else if (bytesRead < 0) {
                handleEndOfStream(socketChannel, engine);
                return;
            }
            Thread.sleep(timeout);
        }
    }

    public void close() throws IOException {
    	System.out.println("About to close connection with the server...");
        closeConnection(socketChannel, engine);
        executor.shutdown();
        System.out.println("Goodbye!");
    }

}