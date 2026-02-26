package com.amarildoaliaj;

import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@NullMarked
public final class MerkleTree {

    private final String hashAlgorithm;
    private final List<List<byte[]>> levels; // levels[0] = leafs, levels[last] = root (1 hash)
    private final byte[] root;

    private MerkleTree(String hashAlgorithm, List<List<byte[]>> levels) {
        this.hashAlgorithm = hashAlgorithm;
        this.levels = levels;
        this.root = copy(levels.getLast().getFirst());
    }

    public static MerkleTree fromStrings(List<String> items) {
        return fromStrings(items, "SHA-256");
    }

    public static MerkleTree fromStrings(List<@Nullable String> items, String hashAlgorithm) {
        List<byte[]> data = new ArrayList<>();
        for (String s : items) {
            byte[] content = s == null
                    ? new byte[0]
                    : s.getBytes(StandardCharsets.UTF_8);
            data.add(content);
        }
        return fromBytes(data, hashAlgorithm);
    }

    public static MerkleTree fromBytes(List<byte[]> items) {
        return fromBytes(items, "SHA-256");
    }

    public static MerkleTree fromBytes(List<byte[]> items, String hashAlgorithm) {
        if (items.isEmpty()) {
            // hash of empty string
            byte[] emptyRoot = hash(hashAlgorithm, new byte[0]);
            List<List<byte[]>> lvls = new ArrayList<>();
            lvls.add(List.of(emptyRoot));
            return new MerkleTree(hashAlgorithm, lvls);
        }

        // Level 0: leafs = hash(data)
        List<byte[]> leafLevel = new ArrayList<>();
        for (byte[] item : items) {
            byte[] data = item == null
                    ? new byte[0]
                    : item;
            leafLevel.add(hash(hashAlgorithm, data));
        }

        List<List<byte[]>> levels = new ArrayList<>();
        levels.add(leafLevel);

        // building higher levels
        List<byte[]> current = leafLevel;
        while (current.size() > 1) {
            List<byte[]> next = new ArrayList<>();

            for (int i = 0; i < current.size(); i += 2) {
                byte[] left = current.get(i);
                byte[] right = i + 1 < current.size() // duplicate the last one
                        ? current.get(i + 1)
                        : left;
                next.add(hashConcat(hashAlgorithm, left, right));
            }

            levels.add(next);
            current = next;
        }

        return new MerkleTree(hashAlgorithm, levels);
    }

    public static boolean verifyProof(byte[] leafData, MerkleProof proof, byte[] expectedRoot) {
        byte[] current = hash(proof.hashAlgorithm(), leafData);

        for (ProofStep step : proof.steps()) {
            if (step.side() == Side.LEFT) {
                // sibling on the left: hash(sibling || current)
                current = hashConcat(proof.hashAlgorithm(), step.hash(), current);
            } else {
                // sibling on the right: hash(current || sibling)
                current = hashConcat(proof.hashAlgorithm(), current, step.hash());
            }
        }

        return Arrays.equals(current, expectedRoot);
    }

    private static byte[] hashConcat(String algo, byte[] left, byte[] right) {
        byte[] combined = new byte[left.length + right.length];
        System.arraycopy(left, 0, combined, 0, left.length);
        System.arraycopy(right, 0, combined, left.length, right.length);
        return hash(algo, combined);
    }

    private static byte[] hash(String algo, byte[] data) {
        try {
            MessageDigest md = MessageDigest.getInstance(algo);
            md.update(data);
            return md.digest();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalArgumentException("Unsupported hash algorithm: " + algo, e);
        }
    }

    private static byte[] copy(byte[] in) {
        return Arrays.copyOf(in, in.length);
    }

    public static String toHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) {
            int v = b & 0xFF;
            sb.append(Character.forDigit(v >>> 4, 16));
            sb.append(Character.forDigit(v & 0x0F, 16));
        }
        return sb.toString();
    }

    public byte[] getRoot() {
        return copy(root);
    }

    public String getRootHex() {
        return toHex(root);
    }

    public int leafCount() {
        return levels.getFirst().size();
    }

    public MerkleProof getProof(int leafIndex) {
        if (leafIndex < 0 || leafIndex >= leafCount()) {
            throw new IndexOutOfBoundsException("leafIndex out of range: " + leafIndex);
        }

        List<ProofStep> steps = new ArrayList<>();
        int index = leafIndex;

        // from level 0 to the penultimate level
        for (int level = 0; level < levels.size() - 1; level++) {
            List<byte[]> hashes = levels.get(level);

            boolean isLeftNode = (index % 2 == 0);
            int siblingIndex = isLeftNode
                    ? index + 1
                    : index - 1;

            byte[] sibling;
            if (siblingIndex >= hashes.size()) {
                // if the brother is missing (odd level) it is a duplication of the node itself
                sibling = hashes.get(index);
            } else {
                sibling = hashes.get(siblingIndex);
            }

            // if i am on the left, the sibling is on the right; if i am on the right, the sibling is on the left
            Side side = isLeftNode
                    ? Side.RIGHT
                    : Side.LEFT;
            steps.add(new ProofStep(copy(sibling), side));

            // parent index
            index = index / 2;
        }

        return new MerkleProof(hashAlgorithm, steps);
    }

    public enum Side {LEFT, RIGHT}

    public record ProofStep(byte[] hash, Side side) {

        public byte[] hash() {
            return copy(hash);
        }
    }

    public record MerkleProof(String hashAlgorithm, List<ProofStep> steps) {
    }
}
