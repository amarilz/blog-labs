package com.amarildoaliaj;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MerkleTreeTest {

    @Test
    @DisplayName("same input -> same root")
    void sameInputSameRoot() {
        // given
        var items = List.of("a", "b", "c", "d", "e");

        // when
        var t1 = MerkleTree.fromStrings(items);
        var t2 = MerkleTree.fromStrings(items);

        // then
        assertArrayEquals(t1.getRoot(), t2.getRoot());
        assertEquals(t1.getRootHex(), t2.getRootHex());
    }

    @Test
    @DisplayName("changing one leaf -> changes root")
    void changingOneLeafChangesRoot() {
        // given
        var items1 = List.of("a", "b", "c", "d", "e");
        var items2 = List.of("a", "b", "X", "d", "e"); // change "c" -> "X"

        // when
        var t1 = MerkleTree.fromStrings(items1);
        var t2 = MerkleTree.fromStrings(items2);

        // then
        assertNotEquals(t1.getRootHex(), t2.getRootHex());
    }

    @Test
    @DisplayName("proof verifies for each leaf - even number of leaves")
    void proofVerifiesForEachLeafEvenNumberOfLeaves() {
        // given
        var items = List.of("a", "b", "c", "d");
        var tree = MerkleTree.fromStrings(items);

        for (int i = 0; i < items.size(); i++) {
            // when
            MerkleTree.MerkleProof proof = tree.getProof(i);
            byte[] leaf = items.get(i).getBytes(StandardCharsets.UTF_8);

            // then
            assertTrue(MerkleTree.verifyProof(leaf, proof, tree.getRoot()), "Proof should verify for leaf index " + i);
        }
    }

    @Test
    @DisplayName("proof verifies for each leaf - odd number of leaves")
    void proofVerifiesForEachLeafOddNumberOfLeaves() {
        // given
        var items = List.of("a", "b", "c", "d", "e");
        var tree = MerkleTree.fromStrings(items);

        for (int i = 0; i < items.size(); i++) {
            // when
            var proof = tree.getProof(i);
            byte[] leaf = items.get(i).getBytes(StandardCharsets.UTF_8);

            // then
            assertTrue(MerkleTree.verifyProof(leaf, proof, tree.getRoot()),
                    "Proof should verify for leaf index " + i);
        }
    }

    @Test
    @DisplayName("proof fails if leaf data is different")
    void proofFailsIfLeafDataIsDifferent() {
        // given
        var items = List.of("a", "b", "c", "d", "e");
        var tree = MerkleTree.fromStrings(items);

        // when
        int idx = 2; // "c"
        var proof = tree.getProof(idx);

        // then
        byte[] tamperedLeaf = "X".getBytes(StandardCharsets.UTF_8);
        assertFalse(MerkleTree.verifyProof(tamperedLeaf, proof, tree.getRoot()));
    }

    @Test
    @DisplayName("proof fails if root is different")
    void proofFailsIfRootIsDifferent() {
        // given
        var items = List.of("a", "b", "c", "d", "e");
        var tree = MerkleTree.fromStrings(items);

        // when
        int idx = 1; // "b"
        var proof = tree.getProof(idx);
        byte[] leaf = items.get(idx).getBytes(StandardCharsets.UTF_8);
        var otherTree = MerkleTree.fromStrings(List.of("a", "b", "c", "d", "X"));

        // then
        assertFalse(MerkleTree.verifyProof(leaf, proof, otherTree.getRoot()));
    }
}
