package com.amarildoaliaj.dto;

public record Key(
        String partitionKey,
        String clusteringKey
) {
}
