package Chord;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.spi.SelectorProvider;

import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLSession;
import javax.net.ssl.TrustManager;

import java.util.Iterator;



public class SSLServer extends SSLBase{
    private boolean serverActive;
    private SSLContext context =  SSLContext.getInstance("TLS");
    private Selector selector;

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

    	System.out.println("SSLServer initialized");

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
        
        System.out.println("Goodbye!");
    }

    @Override
    protected void read(SocketChannel sC, SSLEngine eng) throws IOException {

        System.out.println("Will read from a client");

        rcv_encryptedData.clear();
        int bytesRead = sC.read(rcv_encryptedData);
        if (bytesRead > 0) {
            rcv_encryptedData.flip();
            while (rcv_encryptedData.hasRemaining()) {
                rcv_plainData.rewind();
                SSLEngineResult res = eng.unwrap(rcv_encryptedData, rcv_plainData);
                switch (res.getStatus()) {
                case OK:
                    rcv_plainData.flip();
                    System.out.println("Message: " + new String(rcv_plainData.array()));
                    break;
                case BUFFER_OVERFLOW:
                    rcv_plainData = enlargeApplicationBuffer(eng, rcv_plainData);
                    break;
                case BUFFER_UNDERFLOW:
                rcv_encryptedData = handleBufferUnderflow(eng, rcv_encryptedData);
                    break;
                case CLOSED:
                    System.out.println("Client wants to close connection");
                    closeConnection(sC, eng);
                    System.out.println("Connection closed");
                    return;
                default:
                    throw new IllegalStateException("Status is not valid: " + res.getStatus());
                }
            }

            write(sC, eng, "Hello! I am your server!");

        } else if (bytesRead < 0) {
            handleEndOfStream(sC, eng);
            System.out.println("Goodbye client!");
        }
    }

    public void write(SocketChannel sC, SSLEngine eng, String message) throws IOException {
        write(sC, eng, message.getBytes());
    }
    

    @Override
    protected void write(SocketChannel sC, SSLEngine eng, byte[] msg) throws IOException {

        System.out.println("Will write to a client");

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
    	System.out.println("Server closing");
    	serverActive = false;
    	executor.shutdown();
    	selector.wakeup();
    }

    private boolean isServerActive() {
        return serverActive;
    }

    private void accept(SelectionKey k) throws Exception {

    	System.out.println("Connection request!");

        SocketChannel socketChannel = ((ServerSocketChannel) k.channel()).accept();
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

}