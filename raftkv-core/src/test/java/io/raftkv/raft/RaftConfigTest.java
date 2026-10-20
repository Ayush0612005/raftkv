package io.raftkv.raft;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class RaftConfigTest {

    @Test
    void majorityCountsSelf() {
        assertEquals(1, RaftConfig.defaults(0, List.of()).majority());
        assertEquals(2, RaftConfig.defaults(0, List.of(1, 2)).majority());
        assertEquals(3, RaftConfig.defaults(0, List.of(1, 2, 3)).majority());
        assertEquals(3, RaftConfig.defaults(0, List.of(1, 2, 3, 4)).majority());
    }

    @Test
    void rejectsInvalidConfigs() {
        assertThrows(IllegalArgumentException.class, () -> RaftConfig.defaults(0, List.of(0, 1)));
        assertThrows(IllegalArgumentException.class, () -> RaftConfig.defaults(0, List.of(1, 1)));
        assertThrows(IllegalArgumentException.class, () -> new RaftConfig(0, List.of(1), 300, 150, 40));
        assertThrows(IllegalArgumentException.class, () -> new RaftConfig(0, List.of(1), 150, 300, 100));
    }
}
