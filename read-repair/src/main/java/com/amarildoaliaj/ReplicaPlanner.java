package com.amarildoaliaj;

import com.amarildoaliaj.dto.Key;
import com.amarildoaliaj.dto.NodeId;

import java.util.List;

public interface ReplicaPlanner {
    List<NodeId> replicasFor(Key key);

    /**
     * Dynamic snitch: choose the "best" replica for direct read (latency, load, etc.).
     */
    NodeId pickFastestReplica(List<NodeId> replicas);
}
