package Chord;

import javax.net.ssl.*;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.KeyStore;

public class SSLMessage {
	
	static final String CRLF = "\r\n" ;
	
	private final String ip;
	private final int port;

    private SSLSocket sslSocket;
    
    public SSLMessage(InetSocketAddress address) {
    	this(address.getHostName(), address.getPort());
    }
    
    public SSLMessage(String ip, int port) {
    	this.ip = ip;
    	this.port = port;

        try {
            this.sslSocket = this.createSocket();
        } catch (Exception e) {
            System.err.print("Failed to create ssl socket\n");
            e.printStackTrace();
            return;
        }

        try {
            this.connect();
        } catch (Exception e) {
            System.err.print("Failed to connect to socket\n");
            e.printStackTrace();
        }
    }

    private void connect() throws Exception {
        //this.sslSocket.setEnabledProtocols(); TODO
        //this.sslSocket.setEnabledCipherSuites(); TODO

        this.sslSocket.startHandshake();
    }

    public void write(String message) {
        this.write(message.getBytes());
    }

    public void write(String d, byte [] body){
        String header = d + " " + CRLF + CRLF;

        byte[] headerB = header.getBytes();
        byte [] data = new byte[headerB.length + body.length];

        System.arraycopy(headerB, 0, data, 0, headerB.length);
        System.arraycopy(body, 0, data, headerB.length, body.length);

        this.write(data);
    }
    
    public void write(byte[] message) {
        try {
            this.sslSocket.getOutputStream().write(message);
        } catch (IOException e) {
            System.err.print("Failed to write to ssl socket\n");
            e.printStackTrace();
        }
    }

    public byte[] read() {
        try {
            return this.sslSocket.getInputStream().readAllBytes();
        } catch (IOException e) {
            System.err.print("Failed to write to ssl socket\n");
            e.printStackTrace();
            return null;
        }
    }

    public void close() {
        try {
            this.sslSocket.close();
        } catch (IOException e) {
            System.err.print("Failed to close ssl socket\n");
            e.printStackTrace();
        }
    }

    private SSLSocket createSocket() throws Exception {
        SSLContext context;
        KeyManagerFactory keyManagerFactory;
        KeyStore ks;
        char[] passphrase = "passphrase".toCharArray();

        context = SSLContext.getInstance("TLS");
        keyManagerFactory = KeyManagerFactory.getInstance("SunX509");
        ks = KeyStore.getInstance("JKS");

        ks.load(new FileInputStream("testkeys.jks"), passphrase);

        keyManagerFactory.init(ks, passphrase);

        context.init(keyManagerFactory.getKeyManagers(), null, null);

        SSLSocketFactory sslSocketFactory = context.getSocketFactory();

        return (SSLSocket) sslSocketFactory.createSocket(this.ip, this.port);
    }
}