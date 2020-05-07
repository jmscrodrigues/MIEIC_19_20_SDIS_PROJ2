package Chord;

import java.nio.charset.StandardCharsets;

public class ChordMessage {

    public byte[] buf;
    public String op;
    public int key;
    public int oldKey;
    public String ip;
    public int port;
    public int index;

    public ChordMessage(byte[] buf) {
        this.buf = buf;

        String message = new String(buf, StandardCharsets.UTF_8);
        System.out.println(message);

        String[] parts = message.split(" ");

        this.op = parts[0];

        if (op.equals(ChordOps.DELETE_FINGER)) {
            this.key = Integer.parseInt(parts[1]);
            this.oldKey = Integer.parseInt(parts[2]);
            this.ip = parts[3];
            this.port = Integer.parseInt(parts[4]);
            return;
        }

        if (parts.length > 1)
            this.key = Integer.parseInt(parts[1]);

        if (parts.length > 2)
            this.ip = parts[2];

        if (parts.length > 3)
            this.port = Integer.parseInt(parts[3]);

        if (parts.length > 4)
            this.index = Integer.parseInt(parts[4]);
    }
}
