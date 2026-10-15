package com.artillexstudios.axvaults.utils;

import com.artillexstudios.axapi.serializers.Serializers;
import com.artillexstudios.axvaults.AxVaults;
import com.artillexstudios.axvaults.vaults.Vault;
import org.bukkit.Material;
import org.bukkit.inventory.ItemStack;

import java.util.concurrent.CompletableFuture;

import static com.artillexstudios.axvaults.AxVaults.CONFIG;

public final class VaultUtils {
    private static boolean asyncItemSerializer;
    private static boolean deleteEmptyVaults;

    private VaultUtils() {
    }

    public static void reload() {
        asyncItemSerializer = CONFIG.getBoolean("async-item-serializer", false);
        deleteEmptyVaults = CONFIG.getBoolean("delete-empty-vaults", true);
    }

    /**
     * Captures a vault on the main thread and submits the resulting immutable snapshot to the
     * datastore queue. The queue is the only place where a database write is performed, so writes
     * retain the order in which snapshots were captured.
     */
    public static CompletableFuture<Void> save(Vault vault) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        Runnable capture = () -> captureAndQueue(vault, result);

        try {
            // During shutdown ThreadUtils.runSync executes inline. This is intentional: waiting
            // for a main-thread snapshot from the disable thread would deadlock the server.
            ThreadUtils.runSync(capture);
        } catch (Throwable throwable) {
            result.completeExceptionally(throwable);
        }
        return result;
    }

    private static void captureAndQueue(Vault vault, CompletableFuture<Void> future) {
        if (future.isDone()) return;

        try {
            ItemStack[] snapshot = vault.getStorage().getContents();
            boolean empty = true;
            for (int i = 0; i < snapshot.length; i++) {
                ItemStack item = snapshot[i];
                if (item != null) {
                    if (!item.getType().isAir() && item.getAmount() > 0) empty = false;
                    snapshot[i] = item.clone();
                }
            }
            Material icon = vault.getRealIcon();
            boolean delete = deleteEmptyVaults && empty;
            boolean serializeAsync = asyncItemSerializer;
            Object serialized = delete ? Boolean.TRUE : null;
            if (!delete && !serializeAsync) {
                serialized = serializeSnapshot(snapshot);
            }

            // The icon and contents are both read before the write is queued. In particular, do
            // not let the provider read the live Vault after this point.
            Object capturedResult = serialized;
            AxVaults.getThreadedQueue().submit(() -> {
                try {
                    Object result = delete ? Boolean.TRUE : serializeAsync ? serializeSnapshot(snapshot) : capturedResult;
                    AxVaults.getDatabase().saveVault(vault, result, icon);
                    future.complete(null);
                } catch (Throwable throwable) {
                    future.completeExceptionally(throwable);
                }
            });
        } catch (Throwable throwable) {
            future.completeExceptionally(throwable);
        }
    }

    private static Object serializeSnapshot(ItemStack[] snapshot) {
        try {
            return Serializers.ITEM_ARRAY.serialize(snapshot);
        } catch (Throwable throwable) {
            throw new IllegalStateException("Could not serialize vault inventory", throwable);
        }
    }

    public static boolean isAsyncItemSerializer() {
        return asyncItemSerializer;
    }

    public static boolean isDeleteEmptyVaults() {
        return deleteEmptyVaults;
    }
}
