package com.artillexstudios.axvaults.database.messaging;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.UUID;

// format: TYPE:SERVER_ID:UUID:VAULT_ID
public record SyncMessage(@NotNull ChangeType type, @NotNull String serverId, @NotNull UUID uuid, int vaultId) {

    public String encode() {
        return type.name() + ':' + serverId + ':' + uuid + ':' + vaultId;
    }

    @Nullable
    public static SyncMessage decode(@NotNull String message) {
        String[] parts = message.split(":");
        if (parts.length != 4) return null;

        try {
            return new SyncMessage(ChangeType.valueOf(parts[0]), parts[1], UUID.fromString(parts[2]), Integer.parseInt(parts[3]));
        } catch (IllegalArgumentException ex) {
            return null;
        }
    }
}
