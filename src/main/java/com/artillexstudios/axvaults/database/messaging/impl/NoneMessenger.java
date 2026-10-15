package com.artillexstudios.axvaults.database.messaging.impl;

import com.artillexstudios.axvaults.database.messaging.ChangeType;
import com.artillexstudios.axvaults.database.messaging.Messenger;
import org.jetbrains.annotations.NotNull;

import java.util.UUID;

public class NoneMessenger implements Messenger {

    @Override
    public void start() {}

    @Override
    public void stop() {}

    @Override
    public void sendChange(@NotNull ChangeType type, @NotNull UUID uuid, int vaultId) {}
}
