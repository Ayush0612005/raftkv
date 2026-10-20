package io.raftkv.client;

/**
 * The client gave up before any server confirmed the operation. Its outcome is unknown: it may still be applied
 * later (exactly once). The client stays usable.
 */
public final class KvTimeoutException extends RuntimeException {
    public KvTimeoutException(String message) {
        super(message);
    }
}
