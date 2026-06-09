package pt.ua;

import java.nio.file.Path;

public class NodeServerMain {
    public static void main(String[] args) {
        if (args.length < 2) {
            System.out.println("Usage: NodeServerMain <nodeId> <port>");
            return;
        }

        String nodeId = args[0];
        int port = Integer.parseInt(args[1]);

        System.out.println("Starting node " + nodeId + " on port " + port);

        DiskSeriesStore store = new DiskSeriesStore(Path.of("data/" + nodeId));
        NodeState state = new NodeState(nodeId, store);

        NodeInfo self = new NodeInfo(nodeId, "localhost", port);
        PeerTable peers = new PeerTable();
        peers.addPeer(new NodeInfo("node-1", "localhost", 7878));
        peers.addPeer(new NodeInfo("node-2", "localhost", 7879));

        DhtNodeServer server = new DhtNodeServer(state, self, peers);
        server.start();
    }
}