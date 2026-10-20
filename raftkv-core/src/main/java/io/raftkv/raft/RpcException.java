package io.raftkv.raft;

/** An RPC that did not produce a response: timeout, unreachable peer, dropped message. Always retryable. */
public final class RpcException extends Exception {
    public RpcException(String message) {
        super(message, null, false, false);
    }

    public RpcException(String message, Throwable cause) {
        super(message, cause);
    }
}
