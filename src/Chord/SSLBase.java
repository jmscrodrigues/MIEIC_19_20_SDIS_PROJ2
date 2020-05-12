package Chord;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.security.KeyStore;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLEngineResult.HandshakeStatus;
import javax.net.ssl.SSLException;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;

public abstract class SSLBase {

    protected ByteBuffer send_plainData;

    protected ByteBuffer send_encryptedData;

    protected ByteBuffer rcv_plainData;

    protected ByteBuffer rcv_encryptedData;

    protected ExecutorService executor = Executors.newSingleThreadExecutor();

    protected abstract byte[] read(SocketChannel socketChannel, SSLEngine engine) throws Exception;

    protected abstract void write(SocketChannel socketChannel, SSLEngine engine, byte[] message) throws Exception;
    
    protected boolean debug = false;;
    
    public void setDebug(boolean b) {
    	this.debug = b;
    }

    protected boolean doHandshake(SocketChannel socketChannel, SSLEngine engine) throws IOException {

        if(debug) System.out.println("Going to do handshake...");

        SSLEngineResult result;
        HandshakeStatus handshakeStatus;

        int maxBufferSize = engine.getSession().getApplicationBufferSize();
        ByteBuffer send_plainData = ByteBuffer.allocate(maxBufferSize);
        ByteBuffer rcv_plainData = ByteBuffer.allocate(maxBufferSize);
        send_encryptedData.clear();
        rcv_encryptedData.clear();

        handshakeStatus = engine.getHandshakeStatus();
        while (handshakeStatus != SSLEngineResult.HandshakeStatus.FINISHED && handshakeStatus != SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING) {
            switch (handshakeStatus) {
            case NEED_UNWRAP:
                if (socketChannel.read(rcv_encryptedData) < 0) {
                    if (engine.isInboundDone() && engine.isOutboundDone()) {
                        return false;
                    }
                    try {
                        engine.closeInbound();
                    } catch (SSLException e) {
                    	if(debug) System.out.println("This engine was forced to close inbound, without having received the proper SSL/TLS close notification message from the peer, due to end of stream.");
                    }
                    engine.closeOutbound();
                    
                    handshakeStatus = engine.getHandshakeStatus();
                    break;
                }
                rcv_encryptedData.flip();
                try {
                    result = engine.unwrap(rcv_encryptedData, rcv_plainData);
                    rcv_encryptedData.compact();
                    handshakeStatus = result.getHandshakeStatus();
                } catch (SSLException sslException) {
                	if(debug) System.out.println("A problem was encountered while processing the data that caused the SSLEngine to abort. Will try to properly close connection...");
                    engine.closeOutbound();
                    handshakeStatus = engine.getHandshakeStatus();
                    break;
                }
                switch (result.getStatus()) {
                case OK:
                    break;
                case BUFFER_OVERFLOW:
                
                    rcv_plainData = enlargeApplicationBuffer(engine, rcv_plainData);
                    break;
                case BUFFER_UNDERFLOW:
                    
                    rcv_encryptedData = handleBufferUnderflow(engine, rcv_encryptedData);
                    break;
                case CLOSED:
                    if (engine.isOutboundDone()) {
                        return false;
                    } else {
                        engine.closeOutbound();
                        handshakeStatus = engine.getHandshakeStatus();
                        break;
                    }
                default:
                    throw new IllegalStateException("Invalid SSL status: " + result.getStatus());
                }
                break;
            case NEED_WRAP:
                send_encryptedData.clear();
                try {
                    result = engine.wrap(send_plainData, send_encryptedData);
                    handshakeStatus = result.getHandshakeStatus();
                } catch (SSLException sslException) {
                	if(debug) System.out.println("A problem was encountered while processing the data that caused the SSLEngine to abort. Will try to properly close connection...");
                    engine.closeOutbound();
                    handshakeStatus = engine.getHandshakeStatus();
                    break;
                }
                switch (result.getStatus()) {
                case OK :
                    send_encryptedData.flip();
                    while (send_encryptedData.hasRemaining()) {
                        socketChannel.write(send_encryptedData);
                    }
                    break;
                case BUFFER_OVERFLOW:
            
                    send_encryptedData = enlargePacketBuffer(engine, send_encryptedData);
                    break;
                case BUFFER_UNDERFLOW:
                    throw new SSLException("Buffer underflow occured after a wrap. I don't think we should ever get here.");
                case CLOSED:
                    try {
                        send_encryptedData.flip();
                        while (send_encryptedData.hasRemaining()) {
                            socketChannel.write(send_encryptedData);
                        }
                        
                        rcv_encryptedData.clear();
                    } catch (Exception e) {
                    	if(debug) System.out.println("Failed to send server's CLOSE message due to socket channel's failure.");
                        handshakeStatus = engine.getHandshakeStatus();
                    }
                    break;
                default:
                    throw new IllegalStateException("Invalid SSL status: " + result.getStatus());
                }
                break;
            case NEED_TASK:
                Runnable task;
                while ((task = engine.getDelegatedTask()) != null) {
                    executor.execute(task);
                }
                handshakeStatus = engine.getHandshakeStatus();
                break;
            case FINISHED:
                break;
            case NOT_HANDSHAKING:
                break;
            default:
                throw new IllegalStateException("Invalid SSL status: " + handshakeStatus);
            }
        }

        return true;

    }

    protected ByteBuffer enlargePacketBuffer(SSLEngine engine, ByteBuffer buffer) {
        return enlargeBuffer(buffer, engine.getSession().getPacketBufferSize());
    }

    protected ByteBuffer enlargeApplicationBuffer(SSLEngine engine, ByteBuffer buffer) {
        return enlargeBuffer(buffer, engine.getSession().getApplicationBufferSize());
    }


    protected ByteBuffer enlargeBuffer(ByteBuffer buffer, int sessionProposedCapacity) {
        if (sessionProposedCapacity > buffer.capacity()) {
            buffer = ByteBuffer.allocate(sessionProposedCapacity);
        } else {
            buffer = ByteBuffer.allocate(buffer.capacity() * 2);
        }
        return buffer;
    }

    protected ByteBuffer handleBufferUnderflow(SSLEngine engine, ByteBuffer buffer) {
        if (engine.getSession().getPacketBufferSize() < buffer.limit()) {
            return buffer;
        } else {
            ByteBuffer replaceBuffer = enlargePacketBuffer(engine, buffer);
            buffer.flip();
            replaceBuffer.put(buffer);
            return replaceBuffer;
        }
    }

    protected void closeConnection(SocketChannel socketChannel, SSLEngine engine) throws IOException  {
        engine.closeOutbound();
        doHandshake(socketChannel, engine);
        socketChannel.close();
    }

    protected void handleEndOfStream(SocketChannel socketChannel, SSLEngine engine) throws IOException  {
        try {
            engine.closeInbound();
        } catch (Exception e) {
        	if(debug) System.out.println("This engine was forced to close inbound, without having received the proper SSL/TLS close notification message from the peer, due to end of stream.");
        }
        closeConnection(socketChannel, engine);
    }

    protected KeyManager[] createKeyManagers(String filepath, String keystorePassword, String keyPassword) throws Exception {
        KeyStore keyStore = KeyStore.getInstance("JKS");
        InputStream keyStoreIS = new FileInputStream(filepath);
        try {
            keyStore.load(keyStoreIS, keystorePassword.toCharArray());
        } finally {
            if (keyStoreIS != null) {
                keyStoreIS.close();
            }
        }
        KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        kmf.init(keyStore, keyPassword.toCharArray());
        return kmf.getKeyManagers();
    }

    protected TrustManager[] createTrustManagers(String filepath, String keystorePassword) throws Exception {
        KeyStore trustStore = KeyStore.getInstance("JKS");
        InputStream trustStoreIS = new FileInputStream(filepath);
        try {
            trustStore.load(trustStoreIS, keystorePassword.toCharArray());
        } finally {
            if (trustStoreIS != null) {
                trustStoreIS.close();
            }
        }
        TrustManagerFactory trustFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        trustFactory.init(trustStore);
        return trustFactory.getTrustManagers();
    }

}
