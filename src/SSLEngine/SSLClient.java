package SSLEngine;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.security.SecureRandom;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLSession;


public class SSLClient extends SSLPeer {
	

	private String remoteAddress;

	private int port;

    private SSLEngine engine;

    private SocketChannel socketChannel;


    public SSLClient(String protocol, String remoteAddress, int port) throws Exception  {
    	this.remoteAddress = remoteAddress;
    	this.port = port;

        SSLContext context = SSLContext.getInstance(protocol);
        context.init(createKeyManagers("./client.jks", "storepass", "keypass"), createTrustManagers("./trustedCerts.jks", "storepass"), new SecureRandom());
        engine = context.createSSLEngine(remoteAddress, port);
        engine.setUseClientMode(true);

        SSLSession session = engine.getSession();
        myAppData = ByteBuffer.allocate(65000);
        myNetData = ByteBuffer.allocate(session.getPacketBufferSize());
        setPeerAppData(ByteBuffer.allocate(65000));
        setPeerNetData(ByteBuffer.allocate(session.getPacketBufferSize()));
    }

    /**
     * Opens a socket channel to communicate with the configured server and tries to complete the handshake protocol.
     *
     * @return True if client established a connection with the server, false otherwise.
     * @throws Exception
     */
    public boolean connect() throws Exception {
    	socketChannel = SocketChannel.open();
    	socketChannel.configureBlocking(false);
    	socketChannel.connect(new InetSocketAddress(remoteAddress, port));
    	while (!socketChannel.finishConnect()) {
    		// can do something here...
    	}

    	engine.beginHandshake();
    	return doHandshake(socketChannel, engine);
    }

    /**
     * Public method to send a message to the server.
     *
     * @param message - message to be sent to the server.
     * @throws IOException if an I/O error occurs to the socket channel.
     */
    public void write(String message) throws IOException {
        write(socketChannel, engine, message);
    }
    public void write(byte[] message) throws IOException {
        write(socketChannel, engine, message);
    }


    /**
     * Public method to try to read from the server.
     *
     * @throws Exception
     */
    public byte[] read() throws Exception {
        return read(socketChannel, engine);
    }


    /**
     * Should be called when the client wants to explicitly close the connection to the server.
     *
     * @throws IOException if an I/O error occurs to the socket channel.
     */
    public void shutdown() throws IOException {
    	System.out.println("About to close connection with the server...");
        closeConnection(socketChannel, engine);
        executor.shutdown();
        System.out.println("Goodbye!");
    }

	@Override
	protected byte[] read(SocketChannel socketChannel, SSLEngine engine) throws Exception {

        System.out.println("About to read...");
        
        byte[] data = null;

        getPeerNetData().clear();
        int waitToReadMillis = 50;
        boolean exitReadLoop = false;
        while (!exitReadLoop) {
            int bytesRead = socketChannel.read(getPeerNetData());
            if (bytesRead > 0) {
                getPeerNetData().flip();
                while (getPeerNetData().hasRemaining()) {
                    getPeerAppData().clear();
                    SSLEngineResult result = engine.unwrap(getPeerNetData(), getPeerAppData());
                    switch (result.getStatus()) {
                    case OK:
                        getPeerAppData().flip();
                        data = getPeerAppData().array();
                        System.out.println("Server response: " + new String(data));
                        exitReadLoop = true;
                        break;
                    case BUFFER_OVERFLOW:
                        setPeerAppData(enlargeApplicationBuffer(engine, getPeerAppData()));
                        break;
                    case BUFFER_UNDERFLOW:
                        setPeerNetData(handleBufferUnderflow(engine, getPeerNetData()));
                        break;
                    case CLOSED:
                        closeConnection(socketChannel, engine);
                        return null;
                    default:
                        throw new IllegalStateException("Invalid SSL status: " + result.getStatus());
                    }
                }
            } else if (bytesRead < 0) {
                handleEndOfStream(socketChannel, engine);
                return null;
            }
            Thread.sleep(waitToReadMillis);
        }
        return data;
    }

	@Override
	protected void write(SocketChannel socketChannel, SSLEngine engine, byte[] message) throws IOException {

        System.out.println("Going to write to a client...");

        myAppData.clear();
        myAppData.put(message);
        myAppData.flip();
        while (myAppData.hasRemaining()) {
            // The loop has a meaning for (outgoing) messages larger than 16KB.
            // Every wrap call will remove 16KB from the original message and send it to the remote peer.
            myNetData.clear();
            SSLEngineResult result = engine.wrap(myAppData, myNetData);
            switch (result.getStatus()) {
            case OK:
                myNetData.flip();
                while (myNetData.hasRemaining()) {
                    socketChannel.write(myNetData);
                }
                System.out.println("Message sent to the client: " + message);
                break;
            case BUFFER_OVERFLOW:
                myNetData = enlargePacketBuffer(engine, myNetData);
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

}