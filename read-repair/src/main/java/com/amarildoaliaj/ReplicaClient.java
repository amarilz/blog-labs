package com.amarildoaliaj;

import com.amarildoaliaj.dto.DigestReadResponse;
import com.amarildoaliaj.dto.DirectReadResponse;
import com.amarildoaliaj.dto.Key;
import com.amarildoaliaj.dto.NodeId;
import com.amarildoaliaj.dto.VersionedValue;

import java.util.concurrent.CompletableFuture;

public interface ReplicaClient {
    CompletableFuture<DirectReadResponse> directRead(NodeId replica, Key key);

    CompletableFuture<DigestReadResponse> digestRead(NodeId replica, Key key);

    /**
     * Repair write: Updates the replica with the chosen version. In Cassandra, this is often asynchronous with respect
     * to the client response.
     */
    CompletableFuture<Void> repairWrite(NodeId replica, Key key, VersionedValue correctValue);
}
