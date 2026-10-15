package com.artillexstudios.axvaults.database.messaging;

import org.jetbrains.annotations.NotNull;

import java.util.UUID;

public interface Messenger {

    void start();

    void stop();

    void sendChange(@NotNull ChangeType type, @NotNull UUID uuid, int vaultId);

    // the lock marks which server currently owns the vaults of an online player,
    // the next server only loads them after the previous one finished saving

    // true if this server holds the lock after the call
    default boolean tryLock(@NotNull UUID uuid) {
        return true;
    }

    default void unlock(@NotNull UUID uuid) {
    }
}
