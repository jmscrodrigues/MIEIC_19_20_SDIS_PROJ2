package Chord;

import javax.net.ServerSocketFactory;
import javax.net.ssl.*;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.KeyStore;

public class SSLServer {

    private final int port;
    private SSLServerSocket serverSocket;

    private boolean serverActive;

    public SSLServer(int port) {
        this.port = port;
        this.serverActive = true;

        try {
            this.serverSocket = this.createServerSocket();
        } catch (Exception e) {
            System.err.print("Failed to create server socket");
            e.printStackTrace();
        }

        this.serverSocket.setNeedClientAuth(true);

        System.out.print("Created ssl server socket\n");
    }

    public SSLSocket acceptConnection() throws IOException {
        return (SSLSocket) this.serverSocket.accept();
    }

    public boolean isServerActive() {
        return this.serverActive;
    }

    public void stop() {
        this.serverActive = false;
    }

    public boolean serverIsClosed() {
        return this.serverSocket.isClosed();
    }

    private SSLServerSocket createServerSocket() throws Exception {
        /*SSLContext context;
        KeyManagerFactory keyManagerFactory;
        KeyStore keyStore;
        char[] passphrase = "passphrase".toCharArray();

        context = SSLContext.getInstance("TLS");
        keyManagerFactory = KeyManagerFactory.getInstance("SunX509");
        keyStore = KeyStore.getInstance("JKS");

        keyStore.load(new FileInputStream("testkeys.jks"), passphrase);
        keyManagerFactory.init(keyStore, passphrase);
        context.init(keyManagerFactory.getKeyManagers(), null, null);

        ServerSocketFactory serverSocketFactory = context.getServerSocketFactory();*/
    	
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
        ServerSocketFactory serverSocketFactory = context.getServerSocketFactory();

        return (SSLServerSocket) serverSocketFactory.createServerSocket(this.port);
    }
}