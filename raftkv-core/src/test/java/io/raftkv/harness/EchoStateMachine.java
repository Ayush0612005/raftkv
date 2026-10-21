package io.raftkv.harness;

import io.raftkv.raft.StateMachine;

/**
 * The default application for Raft-level tests: it returns each command as its result and has no state of its own
 * (what was applied is tracked by {@link SimCluster}, whose snapshots carry it).
 */
public final class EchoStateMachine implements StateMachine {

    @Override
    public byte[] apply(long index, byte[] command) {
        return command;
    }

    @Override
    public byte[] snapshot() {
        return new byte[0];
    }

    @Override
    public void restore(byte[] snapshot) {
        // stateless
    }
}
