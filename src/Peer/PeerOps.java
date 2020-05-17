package Peer;

public interface PeerOps {

    // To test Chord
    String PUT = "PUT";
    String GET = "GET";
    String REMOVE = "REMOVE";

    // ###### Protocols ######

    String PUT_CHUNK = "PUTCHUNK";
    String STORED = "STORED";

    String GET_CHUNK = "GETCHUNK";
    String CHUNK = "CHUNK";

    String DELETE = "DELETE";
}
