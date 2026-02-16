package com.amarildoaliaj.dto;

public record DigestReadResponse(
        NodeId node,
        Key key,
        byte[] digest
) {
}
