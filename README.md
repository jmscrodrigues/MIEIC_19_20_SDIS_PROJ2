### All of these commands should be run in the src folder

**In order to compile the program, you should run the following command:**

javac -d build */*.java

**To create the first Peer run the following command (remember that all the ports should be different):**

java -cp build/ Peer.Peer <peer_port> <chord_port>

**To create the other peers you should run the command:
(if all the peers are on your pc the first_peer_ip should be localhost)**

java -cp build/ Peer.Peer <peer_port> <chord_port> <first_peer_ip>:<first_peer_port>

**To run the operations you should run one of the following commands:
(the filename assumes the file is also in the src folder)**

* java -cp build/ Peer.TestApp <peer_ip>:<peer_port> BACKUP \<filename> <rep_degree>

* java -cp build/ Peer.TestApp <peer_ip>:<peer_port> DELETE \<filename>

* java -cp build/ Peer.TestApp <peer_ip>:<peer_port> RESTORE \<filename>

* java -cp build/ Peer.TestApp <peer_ip>:<peer_port> RECLAIM \<space>

* java -cp build/ Peer.TestApp <peer_ip>:<peer_port> STATUS