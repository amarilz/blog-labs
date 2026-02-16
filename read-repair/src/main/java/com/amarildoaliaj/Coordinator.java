package com.amarildoaliaj;

import com.amarildoaliaj.dto.DigestReadResponse;
import com.amarildoaliaj.dto.DirectReadResponse;
import com.amarildoaliaj.dto.Key;
import com.amarildoaliaj.dto.NodeId;
import com.amarildoaliaj.dto.VersionedValue;
import lombok.AllArgsConstructor;
import lombok.NonNull;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

@AllArgsConstructor
public final class Coordinator {

    private final ReplicaPlanner planner;
    private final ReplicaClient client;
    private final Executor executor;
    private final Duration readTimeout;

    private static boolean equivalent(VersionedValue a, VersionedValue b) {
        if (a == null && b == null) return true;
        if (a == null || b == null) return false;
        return a.timestamp() == b.timestamp() && Objects.equals(a.value(), b.value());
    }

    private static VersionedValue selectWinnerLww(@NonNull List<DirectReadResponse> responses) {
        // Last-Write-Wins based on timestamp. If null, consider timestamp as -infinity.
        return responses.stream()
                .map(DirectReadResponse::valueOrNull)
                .filter(Objects::nonNull)
                .max(Comparator.comparingLong(VersionedValue::timestamp))
                .orElse(null);
    }

    private static List<NodeId> pickOtherReplicas(@NonNull List<NodeId> replicas, NodeId exclude, int count) {
        if (count <= 0) return List.of();

        List<NodeId> others = replicas.stream()
                .filter(r -> !r.equals(exclude))
                .toList();
        if (count > others.size()) {
            throw new IllegalArgumentException("Not enough replicas to satisfy CL");
        }
        return others.subList(0, count);
    }

    private static byte[] digestOf(VersionedValue v) {
        // Deterministic digest: timestamp + value
        if (v == null) {
            return sha256("NULL".getBytes(StandardCharsets.UTF_8));
        }
        String canon = v.timestamp() + "|" + v.value();
        return sha256(canon.getBytes(StandardCharsets.UTF_8));
    }

    public static byte[] sha256(byte[] data) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            return md.digest(data);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public CompletableFuture<VersionedValue> read(Key key, ConsistencyLevel cl) {
        List<NodeId> replicas = planner.replicasFor(key);
        if (replicas.isEmpty()) {
            return CompletableFuture.failedFuture(new IllegalStateException("No replicas for key"));
        }
        int required = cl.getRequiredAcks();
        if (required > replicas.size()) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("CL > RF"));
        }

        // 1. Choose direct target ("fastest" replica) and digest targets to satisfy Consistency Level (CL)
        NodeId directReplica = planner.pickFastestReplica(replicas);
        List<NodeId> digestReplicas = pickOtherReplicas(replicas, directReplica, required - 1);

        CompletableFuture<DirectReadResponse> directF =
                withTimeout(client.directRead(directReplica, key), readTimeout);

        List<CompletableFuture<DigestReadResponse>> digestFs = digestReplicas.stream()
                .map(r -> withTimeout(client.digestRead(r, key), readTimeout))
                .toList();

        // Wait for direct response + (CL-1) digest responses
        CompletableFuture<Void> digestsAll = CompletableFuture.allOf(digestFs.toArray(new CompletableFuture[0]));

        return directF.thenCombine(digestsAll, (direct, ignore) -> {
                    List<DigestReadResponse> digests = digestFs.stream()
                            .map(CompletableFuture::join)
                            .toList();
                    return new DirectAndDigests(direct, digests);
                })
                .thenCompose(pair -> verifyOrRepair(pair, key, directReplica, digestReplicas));
    }

    private CompletableFuture<VersionedValue> verifyOrRepair(
            @NonNull DirectAndDigests pair,
            Key key,
            NodeId directReplica,
            List<NodeId> digestReplicas
    ) {
        DirectReadResponse direct = pair.direct;
        VersionedValue directValue = direct.valueOrNull();

        byte[] directDigest = digestOf(directValue);

        boolean allMatch = pair.digests.stream()
                .allMatch(d -> Arrays.equals(d.digest(), directDigest));
        if (allMatch) {
            // Data is consistent across replicas; no read repair needed
            return CompletableFuture.completedFuture(directValue);
        }

        // Mismatch detected: escalate to direct reads for replicas that initially provided digests
        List<CompletableFuture<DirectReadResponse>> extraDirectFs = digestReplicas.stream()
                .map(r -> withTimeout(client.directRead(r, key), readTimeout))
                .toList();

        return CompletableFuture.allOf(extraDirectFs.toArray(new CompletableFuture[0]))
                .thenCompose(v -> {
                    List<DirectReadResponse> extra = extraDirectFs.stream()
                            .map(CompletableFuture::join)
                            .toList();

                    // Compare versions: include the original direct response in the resolution
                    List<DirectReadResponse> all = new ArrayList<>();
                    all.add(direct);
                    all.addAll(extra);

                    VersionedValue winner = selectWinnerLww(all);

                    // Trigger repair for stale replicas (digest mismatch implies potential divergence)
                    // Like Cassandra, this only repairs replicas involved in this specific read.
                    CompletableFuture<Void> repairs = triggerRepairs(key, winner, all);

                    // Respond to the client immediately without necessarily waiting for repairs to finish (typical behavior).
                    // To ensure repairs complete before responding, use `repairs.thenApply(...)`.
                    repairs.exceptionally(ex -> null);
                    return CompletableFuture.completedFuture(winner);
                });
    }

    @NonNull
    private <T> CompletableFuture<T> withTimeout(@NonNull CompletableFuture<T> cf, @NonNull Duration timeout) {
        return cf.orTimeout(timeout.toMillis(), TimeUnit.MILLISECONDS);
    }

    @NonNull
    private CompletableFuture<Void> triggerRepairs(Key key, VersionedValue winner, @NonNull List<DirectReadResponse> allRead) {
        List<CompletableFuture<Void>> repairFs = new ArrayList<>();
        for (DirectReadResponse r : allRead) {
            VersionedValue current = r.valueOrNull();
            if (!equivalent(current, winner)) {
                repairFs.add(client.repairWrite(r.node(), key, winner));
            }
        }
        return CompletableFuture.allOf(repairFs.toArray(new CompletableFuture[0]));
    }

    private record DirectAndDigests(
            DirectReadResponse direct,
            List<DigestReadResponse> digests
    ) {
    }
}
