package com.amarildoaliaj;

import com.amarildoaliaj.dto.Key;
import com.amarildoaliaj.dto.NodeId;
import com.amarildoaliaj.dto.VersionedValue;
import lombok.NonNull;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ForkJoinPool;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class ReadRepairTest {

    @NonNull
    private Map<NodeId, Long> getNodeIdLongMap(NodeId n1, NodeId n2, NodeId n3) {
        return Map.of(
                n1, 30L,
                n2, 10L,
                n3, 20L
        );
    }

    @NonNull
    private ReplicaPlanner getReplicaPlanner(NodeId n1, NodeId n2, NodeId n3) {
        return new ReplicaPlanner() {
            @Override
            public List<NodeId> replicasFor(Key k) {
                return List.of(n1, n2, n3);
            }

            @Override
            public NodeId pickFastestReplica(List<NodeId> reps) {
                return n2; // n2 is the fastest
            }
        };
    }

    @Test
    @DisplayName("consistency level c1")
    void consistencyLevelC1() {
        NodeId n1 = new NodeId("n1");
        NodeId n2 = new NodeId("n2");
        NodeId n3 = new NodeId("n3");

        // n2 is the fastest -> is chosen as direct read
        Map<NodeId, Long> latency = getNodeIdLongMap(n1, n2, n3);
        InMemoryReplicaClient replicaClient = new InMemoryReplicaClient(latency);
        Key key = new Key("user:42", "profile");

        // divergence: n3 is the most up-to-date, but CL=ONE will only read n2 (direct) and will NOT do digest -> no repair
        replicaClient.put(n1, key, new VersionedValue("A", 100));
        replicaClient.put(n2, key, new VersionedValue("A", 100));
        replicaClient.put(n3, key, new VersionedValue("B", 200));

        ReplicaPlanner planner = getReplicaPlanner(n1, n2, n3);
        Coordinator coordinator = new Coordinator(planner, replicaClient, ForkJoinPool.commonPool(), Duration.ofSeconds(1));
        VersionedValue result = coordinator.read(key, ConsistencyLevel.ONE).join();

        // with CL=ONE, the coordinator does not need digests: it returns what it reads from direct replication.
        assertNotNull(result);
        assertEquals("A", result.value());
        assertEquals(100, result.timestamp());

        // no read repair expected: n2 remains unchanged, and n3 remains more up-to-date.
        VersionedValue n2After = replicaClient.directRead(n2, key).join().valueOrNull();
        VersionedValue n3After = replicaClient.directRead(n3, key).join().valueOrNull();

        assertEquals(new VersionedValue("A", 100), n2After);
        assertEquals(new VersionedValue("B", 200), n3After);
    }

    @Test
    @DisplayName("consistency level c2")
    void consistencyLevelC2() {
        NodeId n1 = new NodeId("n1");
        NodeId n2 = new NodeId("n2");
        NodeId n3 = new NodeId("n3");

        Map<NodeId, Long> latency = getNodeIdLongMap(n1, n2, n3);
        InMemoryReplicaClient replicaClient = new InMemoryReplicaClient(latency);
        Key key = new Key("user:42", "profile");

        // divergence: n3 is the most up-to-date
        replicaClient.put(n1, key, new VersionedValue("A", 100));
        replicaClient.put(n2, key, new VersionedValue("A", 100));
        replicaClient.put(n3, key, new VersionedValue("B", 200));

        ReplicaPlanner planner = getReplicaPlanner(n1, n2, n3);
        Coordinator coordinator = new Coordinator(planner, replicaClient, ForkJoinPool.commonPool(), Duration.ofSeconds(1));
        VersionedValue result = coordinator.read(key, ConsistencyLevel.TWO).join();

        // With CL=2: direct on n2, digest on n1 (as we choose "other replica" from the list). If the digest mismatch
        // occurs, escalate to direct on n1 as well, but n3 is not contacted. So the "winner" remains A@100 (n2 and n1
        // match) and nothing is repaired.
        assertNotNull(result);
        assertEquals("A", result.value());
        assertEquals(100, result.timestamp());

        // repair involves only read replicas; here n1 and n2 remain A@100, n3 remains B@200.
        VersionedValue n1After = replicaClient.directRead(n1, key).join().valueOrNull();
        VersionedValue n2After = replicaClient.directRead(n2, key).join().valueOrNull();
        VersionedValue n3After = replicaClient.directRead(n3, key).join().valueOrNull();

        assertEquals(new VersionedValue("A", 100), n1After);
        assertEquals(new VersionedValue("A", 100), n2After);
        assertEquals(new VersionedValue("B", 200), n3After);
    }

    @Test
    @DisplayName("consistency level c3")
    void consistencyLevelC3() {
        NodeId n1 = new NodeId("n1");
        NodeId n2 = new NodeId("n2");
        NodeId n3 = new NodeId("n3");

        // n2 is the fastest -> direct on n2; digest on n1 e n3 (to reach CL=3)
        Map<NodeId, Long> latency = getNodeIdLongMap(n1, n2, n3);
        InMemoryReplicaClient replicaClient = new InMemoryReplicaClient(latency);
        Key key = new Key("user:42", "profile");

        // Divergenza: n3 è la più aggiornata.
        replicaClient.put(n1, key, new VersionedValue("A", 100));
        replicaClient.put(n2, key, new VersionedValue("A", 100));
        replicaClient.put(n3, key, new VersionedValue("B", 200));

        ReplicaPlanner planner = getReplicaPlanner(n1, n2, n3);
        Coordinator coordinator = new Coordinator(planner, replicaClient, ForkJoinPool.commonPool(), Duration.ofSeconds(1));
        VersionedValue result = coordinator.read(key, ConsistencyLevel.THREE).join();

        // with CL=THREE, mismatch digest almost certain (n3 differs), escalation to direct read, winner n3.
        assertNotNull(result);
        assertEquals("B", result.value());
        assertEquals(200, result.timestamp());

        // Read repair: with this coordinator you repair all the replicas you have read (n2 + n1 + n3), so n1 and n2 must become "B@200".
        VersionedValue n1After = replicaClient.directRead(n1, key).join().valueOrNull();
        VersionedValue n2After = replicaClient.directRead(n2, key).join().valueOrNull();
        VersionedValue n3After = replicaClient.directRead(n3, key).join().valueOrNull();

        assertEquals(new VersionedValue("B", 200), n1After);
        assertEquals(new VersionedValue("B", 200), n2After);
        assertEquals(new VersionedValue("B", 200), n3After);
    }
}
