package com.artillexstudios.axvaults.database.messaging;

public enum ChangeType {
    DELETE,
    UPDATE,
    INSERT;

    public static final ChangeType[] ENTRIES = ChangeType.values();
}
