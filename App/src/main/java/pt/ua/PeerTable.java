package pt.ua;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class PeerTable {
    private final List<NodeInfo> peers = new ArrayList<>();

    public synchronized void addPeer(NodeInfo peer) {
        if (peers.stream().noneMatch(p -> p.getNodeId().equals(peer.getNodeId()))) {
            peers.add(peer);
        }
    }

    public synchronized List<NodeInfo> allPeers() {
        return new ArrayList<>(peers);
    }

    public synchronized NodeInfo closestPeer(BigInteger targetId, NodeInfo self) {
        return peers.stream()
                .filter(p -> !p.getNodeId().equals(self.getNodeId()))
                .min(Comparator.comparing(p -> KeyHasher.xorDistance(
                        KeyHasher.hashToId(p.getNodeId()), targetId)))
                .orElse(null);
    }
}