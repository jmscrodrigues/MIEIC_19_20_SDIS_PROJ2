package Chord;

import javax.net.ssl.*;
import java.io.*;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.security.KeyStore;

public class SSLMessage {
	
	static final String CRLF = "\r\n" ;
	
	private final String ip;
	private final int port;

    private SSLSocket sslSocket;
    
    private boolean debug = false;
    
    public SSLMessage(InetSocketAddress address) throws ConnectException {
    	this(address.getHostName(), address.getPort());
    }
    
    public SSLMessage(String ip, int port) throws ConnectException  {
    	this.ip = ip;
    	this.port = port;

        try {
            this.sslSocket = this.createSocket();
        }catch(ConnectException e) {
        	System.err.print("Failed to connect ssl socket\n");
        	throw e;
        }
        catch (Exception e) {
            System.err.print("Failed to create ssl socket\n");
            e.printStackTrace();
            return;
        }


        try {
            this.connect();
        } catch (Exception e) {
            System.err.print("Failed to handshake to socket\n");
            e.printStackTrace();
        }

    }

    private void connect() throws Exception {

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
    	if(debug) System.out.println("mensagem para enviar: " + new String(message)+" - " + message.length);

        try {
        	OutputStream out = this.sslSocket.getOutputStream(); 
    	    DataOutputStream dos = new DataOutputStream(out);
    	    dos.writeInt(message.length);
    	    dos.write(message,0,message.length);

        } catch (IOException e) {
            System.err.print("Failed to write to ssl socket\n");
            e.printStackTrace();
        }
        if(debug) System.out.println("mensagem enviada");
    }

    public byte[] read() {
    	byte [] buf = null;
    	int len;
    	if(debug) System.out.println("a ler resposta");
        try {
        	DataInputStream dis = new DataInputStream(this.sslSocket.getInputStream());
			len = dis.readInt();
			buf = new byte[len];
			if (len > 0) dis.readFully(buf);
        } catch (IOException e) {
            System.err.print("Failed to read from ssl socket\n");
            e.printStackTrace();
            return null;
        }
        if(debug) System.out.println("resposta: " + new String(buf));
        return buf;
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
    	context = SSLContext.getInstance("SSL");
    	
    	KeyStore keyStore = KeyStore.getInstance("JKS");
        InputStream keyStoreIS = new FileInputStream("./client.jks");
        try {
            keyStore.load(keyStoreIS, "storepass".toCharArray());
        } finally {
            keyStoreIS.close();
        }
        KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        kmf.init(keyStore, "keypass".toCharArray());
        KeyManager[] keys = kmf.getKeyManagers();
        
        KeyStore trustStore = KeyStore.getInstance("JKS");
        InputStream trustStoreIS = new FileInputStream("./trustedCerts.jks");
        try {
            trustStore.load(trustStoreIS, "storepass".toCharArray());
        } finally {
            trustStoreIS.close();
        }
        TrustManagerFactory trustFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        trustFactory.init(trustStore);
        
        TrustManager[] trusts = trustFactory.getTrustManagers();

        context.init(keys, trusts, null); 
        
        SSLSocketFactory sslSocketFactory = context.getSocketFactory();

        return (SSLSocket) sslSocketFactory.createSocket(this.ip, this.port);
    }
}