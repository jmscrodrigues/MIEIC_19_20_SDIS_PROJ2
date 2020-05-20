package Chord;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class ChordMessage {

    public byte[] data;

    public byte[] header;
    public byte[] body;

    public String op;
    public int key;
    public int oldKey;
    public String ip;
    public int port;
    public int index;
    public int replication;
    

    public ChordMessage(byte[] buf) {
        this.data = buf;
        this.getHeaderAndBody();

        String message = new String(buf, StandardCharsets.UTF_8);
        System.out.println("TO HANDLE " + message);

        String[] parts = message.split(" ");

        this.op = parts[0];
        if (op.equals(ChordOps.DELETE_FINGER)) {
            this.key = Integer.parseInt(parts[1]);
            this.oldKey = Integer.parseInt(parts[2]);
            this.ip = parts[3];
            this.port = Integer.parseInt(parts[4]);
            return;
        } else if (this.opHasReplication()) {
            this.key = Integer.parseInt(parts[1]);
            this.replication = Integer.parseInt(parts[2]);
            return;
        }
        
        if (parts.length > 1)
            this.key = Integer.parseInt(parts[1].replaceAll("\\D", ""));
        
        if (parts.length > 2)
            this.ip = parts[2];
        
        if (parts.length > 3)
            this.port = Integer.parseInt(parts[3].replaceAll("\\D", ""));
        
        if (parts.length > 4)
            this.index = Integer.parseInt(parts[4]);

    }

    public void getHeaderAndBody() {
        byte[] buf = data;

        for (int i = 0; i <= buf.length - 4 ; ++i) {
            if (buf[i] == 0xD && buf[i+1] == 0xA && buf[i+2] == 0xD && buf[i+3] == 0xA) {
                this.header = Arrays.copyOf(buf, i);
                this.body = Arrays.copyOfRange(buf,i+4, buf.length);
                break;
            }
        }
    }

    private boolean opHasReplication() {
        switch (this.op) {
            case ChordOps.PUT:
            case ChordOps.GET:
            case ChordOps.REMOVE:
                return true;
            default:
                return false;
        }
    }
    
}
