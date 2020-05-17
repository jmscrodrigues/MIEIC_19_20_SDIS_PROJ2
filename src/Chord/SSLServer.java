package Chord;

import javax.net.ssl.*;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.spi.SelectorProvider;
import java.util.Arrays;
import java.util.Iterator;



public class SSLServer extends SSLBase{
    private boolean serverActive;
    private SSLContext context =  SSLContext.getInstance("TLS");
    protected Selector selector;

    public SSLServer(String hostAddress, int port) throws Exception { 

    KeyManager[] keys = createKeyManagers("./client.jks", "storepass", "keypass");
    TrustManager[] trusts = createTrustManagers("./trustedCerts.jks", "storepass");
    context.init(keys, trusts, null); 


    SSLSession engineSession = context.createSSLEngine().getSession();
    send_plainData = ByteBuffer.allocate(engineSession.getApplicationBufferSize());
    rcv_plainData = ByteBuffer.allocate(engineSession.getApplicationBufferSize());

    send_encryptedData = ByteBuffer.allocate(engineSession.getPacketBufferSize());
    rcv_encryptedData = ByteBuffer.allocate(engineSession.getPacketBufferSize());
    engineSession.invalidate();

    selector = SelectorProvider.provider().openSelector();
    ServerSocketChannel channel = ServerSocketChannel.open();
    channel.configureBlocking(false);
    channel.socket().bind(new InetSocketAddress(hostAddress, port));
    channel.register(selector, SelectionKey.OP_ACCEPT);

    serverActive = true;
    }


    public void start() throws Exception {

    	if(debug) System.out.println("SSLServer initialized");

        while (isServerActive()) {
            selector.select();
            Iterator<SelectionKey> selected = selector.selectedKeys().iterator();
            while (selected.hasNext()) {
                SelectionKey k = selected.next();
                selected.remove();
                if (!k.isValid()) {
                    continue;
                }
                if (k.isAcceptable()) {
                    accept(k);
                } else if (k.isReadable()) {
                    read((SocketChannel) k.channel(), (SSLEngine) k.attachment());
                }
            }
        }
        
        if(debug) System.out.println("Goodbye!");
    }

    @Override
    protected synchronized byte[] read(SocketChannel socket, SSLEngine eng) throws IOException {

    	if(debug) System.out.println("Will read from a client");
    	byte[] data = new byte[64100];
    	int bytes_read = 0;
        rcv_encryptedData.clear();
        int bytesRead = socket.read(rcv_encryptedData);
        if (bytesRead > 0) {
            rcv_encryptedData.flip();
            while (rcv_encryptedData.hasRemaining()) {
                rcv_plainData.clear();
                SSLEngineResult res = eng.unwrap(rcv_encryptedData, rcv_plainData);
                switch (res.getStatus()) {
                case OK:
                    rcv_plainData.flip();
                    //System.out.println("aquiiiiii");
                    //data = Arrays.copyOfRange(rcv_plainData.array(), 0, res.bytesProduced()); 
                    System.arraycopy(rcv_plainData.array(), 0, data, bytes_read, res.bytesProduced());
                    bytes_read +=res.bytesProduced();
                    //System.out.println("Message: " + new String(data));
                    break;
                case BUFFER_OVERFLOW:
                    rcv_plainData = enlargeApplicationBuffer(eng, rcv_plainData);
                    break;
                case BUFFER_UNDERFLOW:
                rcv_encryptedData = handleBufferUnderflow(eng, rcv_encryptedData);
                    break;
                case CLOSED:
                	if(debug) System.out.println("Client wants to close connection");
                    closeConnection(socket, eng);
                    if(debug) System.out.println("Connection closed");
                    return data;
                default:
                    throw new IllegalStateException("Status is not valid: " + res.getStatus());
                }
            }

            //write(sC, eng, "Hello! I am your server!");

        } else if (bytesRead < 0) {
            handleEndOfStream(socket, eng);
            if(debug) System.out.println("Goodbye client!");
            return null;
        }
        return Arrays.copyOfRange(data, 0, bytes_read);
    }

    public void write(SocketChannel sC, SSLEngine eng, String message) throws IOException {
        write(sC, eng, message.getBytes());
    }
    

    @Override
    protected synchronized  void write(SocketChannel sC, SSLEngine eng, byte[] msg) throws IOException {

    	if(debug) System.out.println("Will write to a client");

        send_plainData.clear();
        send_plainData.put(msg);
        send_plainData.flip();
        while (send_plainData.hasRemaining()) {
            send_encryptedData.clear();
            SSLEngineResult res = eng.wrap(send_plainData, send_encryptedData);
            switch (res.getStatus()) {
            case OK:
                send_encryptedData.flip();
                while (send_encryptedData.hasRemaining()) {
                    sC.write(send_encryptedData);
                }
                System.out.println("Message sent: " + new String(msg));
                break;
            case BUFFER_OVERFLOW:
            send_encryptedData = enlargePacketBuffer(eng, send_encryptedData);
                break;
            case BUFFER_UNDERFLOW:
                throw new SSLException("Buffer underflow occured.");
            case CLOSED:
                closeConnection(sC, eng);
                return;
            default:
                throw new IllegalStateException("Status is not valid: " + res.getStatus());
            }
        }
    }

    public void stop() {
    	if(debug) System.out.println("Server closing");
    	executor.shutdown();
    	selector.wakeup();
    	serverActive = false;
    }

    protected boolean isServerActive() {
        return serverActive;
    }

    protected void accept(SelectionKey k) throws Exception {

    	if(debug) System.out.println("Connection request!");

        SocketChannel socketChannel = ((ServerSocketChannel) k.channel()).accept();
        socketChannel.configureBlocking(false);

        SSLEngine engine = context.createSSLEngine();
        engine.setUseClientMode(false);
        engine.beginHandshake();

        if (doHandshake(socketChannel, engine)) {
            socketChannel.register(selector, SelectionKey.OP_READ, engine);
        } else {
            socketChannel.close();
            if(debug) System.out.println("Connection closed due to handshake failure.");
        }
    }

}