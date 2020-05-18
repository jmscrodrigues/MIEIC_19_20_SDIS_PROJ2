package Chord;

import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import javax.net.ssl.SSLEngine;

public class ServerThread extends SSLServer implements Runnable {

	Chord chord;
	
	ServerThread(Chord c, String hostAddress, int port) throws Exception{
		super(hostAddress,port);
		this.chord = c;
		this.setDebug(false);
	}
	
	@Override
    public void start() throws Exception {
		
		ScheduledThreadPoolExecutor scheduler_executor = this.chord.getPeer().getExecutor();

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
                	SocketChannel sc = (SocketChannel) k.channel();
                	SSLEngine eng = (SSLEngine) k.attachment();
                    scheduler_executor.execute(new SSLMessageHandler(this,this.chord,sc,eng,read(sc, eng)));
                	//read(sc, eng);
                	//write(sc, eng, "Hello! I am your server!");
                }
            }
        }
        
        if(debug) System.out.println("Goodbye!");
    }
	
	@Override
	public void run() {
		try {
			this.start();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
}