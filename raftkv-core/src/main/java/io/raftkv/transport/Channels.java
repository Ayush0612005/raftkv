package io.raftkv.transport;

import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;

import java.util.concurrent.TimeUnit;

/** gRPC channel helpers shared by the node-to-node transport and the client. */
public final class Channels {

    /** Large enough for a batch of log entries or (Phase 5) a whole snapshot. */
    public static final int MAX_MESSAGE_BYTES = 64 << 20;

    private Channels() {
    }

    public static ManagedChannel open(String address) {
        return Grpc.newChannelBuilder(address, InsecureChannelCredentials.create())
                .maxInboundMessageSize(MAX_MESSAGE_BYTES)
                .build();
    }

    public static void close(ManagedChannel channel) {
        channel.shutdownNow();
        try {
            channel.awaitTermination(1, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
