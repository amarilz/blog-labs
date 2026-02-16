package com.amarildoaliaj.dto;

public record DirectReadResponse(
        NodeId node,
        Key key,
        VersionedValue valueOrNull
) {
}
