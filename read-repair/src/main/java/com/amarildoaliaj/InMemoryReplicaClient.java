package com.amarildoaliaj;

import com.amarildoaliaj.dto.DigestReadResponse;
import com.amarildoaliaj.dto.DirectReadResponse;
import com.amarildoaliaj.dto.Key;
import com.amarildoaliaj.dto.NodeId;
import com.amarildoaliaj.dto.VersionedValue;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

public final class InMemoryReplicaClient implements ReplicaClient {
    private final Map<NodeId, ConcurrentHashMap<Key, VersionedValue>> stores = new ConcurrentHashMap<>();
    private final Map<NodeId, Long> artificialLatencyMs;

    public InMemoryReplicaClient(Map<NodeId, Long> artificialLatencyMs) {
        this.artificialLatencyMs = artificialLatencyMs;
    }

    public void put(NodeId node, Key key, VersionedValue v) {
        stores.computeIfAbsent(node, n -> new ConcurrentHashMap<>()).put(key, v);
    }

    @Override
    public CompletableFuture<DirectReadResponse> directRead(NodeId replica, Key key) {
        return CompletableFuture.supplyAsync(() -> {
            sleep(replica);
            VersionedValue v = stores.getOrDefault(replica, new ConcurrentHashMap<>()).get(key);
            return new DirectReadResponse(replica, key, v);
        });
    }

    @Override
    public CompletableFuture<DigestReadResponse> digestRead(NodeId replica, Key key) {
        return CompletableFuture.supplyAsync(() -> {
            sleep(replica);
            VersionedValue v = stores.getOrDefault(replica, new ConcurrentHashMap<>()).get(key);
            byte[] digest = Coordinator.sha256(((v == null) ? "NULL" : (v.timestamp() + "|" + v.value()))
                    .getBytes(StandardCharsets.UTF_8));
            return new DigestReadResponse(replica, key, digest);
        });
    }

    @Override
    public CompletableFuture<Void> repairWrite(NodeId replica, Key key, VersionedValue correctValue) {
        return CompletableFuture.runAsync(() -> {
            sleep(replica);
            stores.computeIfAbsent(replica, n -> new ConcurrentHashMap<>()).put(key, correctValue);
        });
    }

    private void sleep(NodeId replica) {
        long ms = artificialLatencyMs.getOrDefault(replica, 0L);
        if (ms <= 0) return;
        try {
            Thread.sleep(ms);
        } catch (InterruptedException ignored) {
        }
    }
}
