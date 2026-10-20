package io.raftkv.kv;

import com.google.protobuf.InvalidProtocolBufferException;
import io.raftkv.proto.KvCommand;
import io.raftkv.proto.KvResponse;
import io.raftkv.proto.KvResult;
import io.raftkv.raft.NotLeaderException;
import io.raftkv.raft.Proposal;
import io.raftkv.raft.RaftNode;

import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Server side of the client API, independent of the transport: proposes the command to Raft and waits until it is
 * committed and applied. Used by the gRPC service and by the simulator.
 */
public final class KvService {

    private final RaftNode node;
    private final Duration commitTimeout;

    public KvService(RaftNode node, Duration commitTimeout) {
        this.node = node;
        this.commitTimeout = commitTimeout;
    }

    public KvResponse execute(KvCommand command) {
        Proposal proposal;
        try {
            proposal = node.propose(command.toByteArray());
        } catch (NotLeaderException e) {
            return notLeader(e.leaderHint());
        }
        try {
            byte[] result = proposal.result().get(commitTimeout.toMillis(), TimeUnit.MILLISECONDS);
            return KvResponse.newBuilder()
                    .setStatus(KvResponse.Status.OK)
                    .setResult(KvResult.parseFrom(result))
                    .build();
        } catch (ExecutionException e) {
            int hint = e.getCause() instanceof NotLeaderException nle ? nle.leaderHint() : RaftNode.NONE;
            return notLeader(hint);
        } catch (TimeoutException e) {
            // e.g. a leader cut off from the majority: the client should try elsewhere
            return KvResponse.newBuilder().setStatus(KvResponse.Status.TIMEOUT).setLeaderHint(RaftNode.NONE).build();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return KvResponse.newBuilder().setStatus(KvResponse.Status.TIMEOUT).setLeaderHint(RaftNode.NONE).build();
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalStateException("state machine returned a malformed result", e);
        }
    }

    private static KvResponse notLeader(int hint) {
        return KvResponse.newBuilder().setStatus(KvResponse.Status.NOT_LEADER).setLeaderHint(hint).build();
    }
}
