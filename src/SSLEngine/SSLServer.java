package SSLEngine;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.spi.SelectorProvider;
import java.security.SecureRandom;
import java.util.Iterator;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLSession;


public class SSLServer extends SSLPeer {
	

	private boolean active;
	

    private SSLContext context;

    /**
     * A part of Java NIO that will be used to serve all connections to the server in one thread.
     */
    protected Selector selector;


    /**
     * Server is designed to apply an SSL/TLS protocol and listen to an IP address and port.
     *
     * @param protocol - the SSL/TLS protocol that this server will be configured to apply.
     * @param hostAddress - the IP address this server will listen to.
     * @param port - the port this server will listen to.
     * @throws Exception
     */
    public SSLServer(String protocol, String hostAddress, int port) throws Exception {

        context = SSLContext.getInstance(protocol);
        context.init(createKeyManagers("./server.jks", "storepass", "keypass"), createTrustManagers("./trustedCerts.jks", "storepass"), new SecureRandom());

        SSLSession dummySession = context.createSSLEngine().getSession();
        myAppData = ByteBuffer.allocate(dummySession.getApplicationBufferSize());
        myNetData = ByteBuffer.allocate(dummySession.getPacketBufferSize());
        setPeerAppData(ByteBuffer.allocate(dummySession.getApplicationBufferSize()));
        setPeerNetData(ByteBuffer.allocate(dummySession.getPacketBufferSize()));
        dummySession.invalidate();

        selector = SelectorProvider.provider().openSelector();
        ServerSocketChannel serverSocketChannel = ServerSocketChannel.open();
        serverSocketChannel.configureBlocking(false);
        serverSocketChannel.socket().bind(new InetSocketAddress(hostAddress, port));
        serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);
        
        active = true;
        
    }

    /**
     * Should be called in order the server to start listening to new connections.
     * This method will run in a loop as long as the server is active. In order to stop the server
     * you should use {@link NioSslServer#stop()} which will set it to inactive state
     * and also wake up the listener, which may be in blocking select() state.
     *
     * @throws Exception
     */
    public void start() throws Exception {

    	System.out.println("Initialized and waiting for new connections...");

        while (isActive()) {
            selector.select();
            Iterator<SelectionKey> selectedKeys = selector.selectedKeys().iterator();
            while (selectedKeys.hasNext()) {
                SelectionKey key = selectedKeys.next();
                selectedKeys.remove();
                if (!key.isValid()) {
                    continue;
                }
                if (key.isAcceptable()) {
                    accept(key);
                } else if (key.isReadable()) {
                    read((SocketChannel) key.channel(), (SSLEngine) key.attachment());
                }
            }
        }
        
        System.out.println("Goodbye!");
        
    }
    
    /**
     * Sets the server to an inactive state, in order to exit the reading loop in {@link NioSslServer#start()}
     * and also wakes up the selector, which may be in select() blocking state.
     */
    public void stop() {
    	System.out.println("Will now close server...");
    	active = false;
    	executor.shutdown();
    	selector.wakeup();
    }

    /**
     * Will be called after a new connection request arrives to the server. Creates the {@link SocketChannel} that will
     * be used as the network layer link, and the {@link SSLEngine} that will encrypt and decrypt all the data
     * that will be exchanged during the session with this specific client.
     *
     * @param key - the key dedicated to the {@link ServerSocketChannel} used by the server to listen to new connection requests.
     * @throws Exception
     */
    protected void accept(SelectionKey key) throws Exception {

    	System.out.println("New connection request!");

        SocketChannel socketChannel = ((ServerSocketChannel) key.channel()).accept();
        socketChannel.configureBlocking(false);

        SSLEngine engine = context.createSSLEngine();
        engine.setUseClientMode(false);
        engine.beginHandshake();

        if (doHandshake(socketChannel, engine)) {
            socketChannel.register(selector, SelectionKey.OP_READ, engine);
        } else {
            socketChannel.close();
            System.out.println("Connection closed due to handshake failure.");
        }
    }

    protected boolean isActive() {
        return active;
    }
    
	public byte[] read(SocketChannel socketChannel, SSLEngine engine) throws IOException {

        System.out.println("About to read from a client...");
        
        byte[] data = null;
        
        getPeerNetData().clear();
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
                    System.out.println("Incoming message: " + new String(data));
                    break;
                case BUFFER_OVERFLOW:
                    setPeerAppData(enlargeApplicationBuffer(engine, getPeerAppData()));
                    break;
                case BUFFER_UNDERFLOW:
                    setPeerNetData(handleBufferUnderflow(engine, getPeerNetData()));
                    break;
                case CLOSED:
                    System.out.println("Client wants to close connection...");
                    closeConnection(socketChannel, engine);
                    System.out.println("Goodbye client!");
                    return null;
                default:
                    throw new IllegalStateException("Invalid SSL status: " + result.getStatus());
                }
            }

            //write(socketChannel, engine, "Hello! I am your server!");

        } else if (bytesRead < 0) {
            //log.error("Received end of stream. Will try to close connection with client...");
            handleEndOfStream(socketChannel, engine);
            System.out.println("Goodbye client!");
        }
        return data;
    }


	public void write(SocketChannel socketChannel, SSLEngine engine, byte[] message) throws IOException {

        System.out.println("Going to write to a client...");
        
        if(message == null)
        	message = new String("").getBytes();
        
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
                System.out.println("Message sent to the client: " + new String(message));
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