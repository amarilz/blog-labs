package com.amarildoaliaj;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public enum ConsistencyLevel {
    ONE(1),
    TWO(2),
    THREE(3);

    private final int requiredAcks;
}
