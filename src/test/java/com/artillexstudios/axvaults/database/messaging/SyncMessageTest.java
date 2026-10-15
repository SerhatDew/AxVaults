package com.artillexstudios.axvaults.database.messaging;

import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

class SyncMessageTest {

    @Test
    void encodeDecodeRoundtrip() {
        UUID uuid = UUID.randomUUID();
        SyncMessage original = new SyncMessage(ChangeType.UPDATE, "srv-1", uuid, 42);

        String encoded = original.encode();
        assertEquals("UPDATE:srv-1:" + uuid + ":42", encoded);

        SyncMessage decoded = SyncMessage.decode(encoded);
        assertNotNull(decoded);
        assertEquals(original, decoded);
    }

    @Test
    void decodeRejectsMalformedPayloads() {
        assertNull(SyncMessage.decode(""));
        assertNull(SyncMessage.decode("NOT_ENOUGH_PARTS"));
        assertNull(SyncMessage.decode("UPDATE:srv-1:not-a-uuid:1"));
        assertNull(SyncMessage.decode("UPDATE:srv-1:" + UUID.randomUUID() + ":not-a-number"));
        assertNull(SyncMessage.decode("INVALID_TYPE:srv-1:" + UUID.randomUUID() + ":1"));
        assertNull(SyncMessage.decode("UPDATE:srv-1:" + UUID.randomUUID() + ":1:extra"));
    }

    @Test
    void changeTypeOrdinalsAreStable() {
        // SQL messaging persists event by ordinal in a TINYINT column
        assertEquals(0, ChangeType.DELETE.ordinal());
        assertEquals(1, ChangeType.UPDATE.ordinal());
        assertEquals(2, ChangeType.INSERT.ordinal());
        assertEquals(3, ChangeType.ENTRIES.length);
    }
}
