package io.raftkv.kv;

import io.raftkv.proto.KvCommand;
import io.raftkv.proto.KvResult;
import io.raftkv.proto.Op;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class KvStateMachineTest {

    private final KvStateMachine sm = new KvStateMachine();
    private long index;

    private KvResult apply(Op op, String key, String value, long client, long seq) throws Exception {
        KvCommand cmd = KvCommand.newBuilder().setOp(op).setKey(key).setValue(value)
                .setClientId(client).setSeq(seq).build();
        return KvResult.parseFrom(sm.apply(++index, cmd.toByteArray()));
    }

    @Test
    void getPutDelete() throws Exception {
        assertFalse(apply(Op.GET, "k", "", 1, 1).getFound());
        assertFalse(apply(Op.PUT, "k", "v1", 1, 2).getFound(), "key did not exist before");
        KvResult got = apply(Op.GET, "k", "", 1, 3);
        assertTrue(got.getFound());
        assertEquals("v1", got.getValue());
        assertTrue(apply(Op.PUT, "k", "v2", 1, 4).getFound(), "key existed before");
        assertEquals("v2", apply(Op.GET, "k", "", 1, 5).getValue());
        assertTrue(apply(Op.DELETE, "k", "", 1, 6).getFound());
        assertFalse(apply(Op.DELETE, "k", "", 1, 7).getFound());
        assertFalse(apply(Op.GET, "k", "", 1, 8).getFound());
    }

    @Test
    void retriedCommandIsExecutedOnce() throws Exception {
        apply(Op.PUT, "k", "v", 1, 1);
        KvResult first = apply(Op.DELETE, "k", "", 1, 2);
        assertTrue(first.getFound());
        // the same delete arrives again (client retry after a lost reply): same answer, no second effect
        KvResult retry = apply(Op.DELETE, "k", "", 1, 2);
        assertEquals(first, retry);
    }

    @Test
    void duplicateDoesNotUndoOtherClientsWrites() throws Exception {
        apply(Op.PUT, "k", "from-a", 1, 1);
        apply(Op.PUT, "k", "from-b", 2, 1);
        // a's put shows up in the log a second time, after b's: it must not overwrite b
        apply(Op.PUT, "k", "from-a", 1, 1);
        assertEquals("from-b", sm.peek("k"));
    }

    @Test
    void clientsHaveIndependentSequences() throws Exception {
        apply(Op.PUT, "x", "1", 1, 5);
        apply(Op.PUT, "y", "2", 2, 1); // a lower seq from another client is not a duplicate
        assertEquals("2", sm.peek("y"));
        assertEquals(2, sm.sessionCount());
    }

    @Test
    void malformedCommandIsANoOp() {
        byte[] result = sm.apply(1, new byte[] {(byte) 0xFF, 0x01, 0x02});
        assertEquals(0, result.length);
        assertEquals(0, sm.size());
    }
}
