package com.artillexstudios.axvaults.database.messaging;

import com.artillexstudios.axapi.executor.ExceptionReportingScheduledThreadPool;
import com.artillexstudios.axvaults.AxVaults;
import com.artillexstudios.axvaults.database.impl.MySQL;
import com.artillexstudios.axvaults.database.impl.Redis;
import com.artillexstudios.axvaults.database.messaging.impl.NoneMessenger;
import com.artillexstudios.axvaults.database.messaging.impl.RedisMessenger;
import com.artillexstudios.axvaults.database.messaging.impl.SQLMessenger;
import com.artillexstudios.axvaults.vaults.VaultManager;
import com.artillexstudios.axvaults.vaults.VaultPlayer;
import org.jetbrains.annotations.NotNull;

import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.artillexstudios.axvaults.AxVaults.CONFIG;

public class Messaging {
    private static final long LOCK_TIMEOUT_MILLIS = 10_000L;
    private static final long LOCK_RETRY_MILLIS = 100L;
    private static final Set<UUID> heldLocks = ConcurrentHashMap.newKeySet();
    private static Messenger messenger = new NoneMessenger();
    // single thread, so lock and unlock calls are executed in order
    private static ScheduledExecutorService lockExecutor = null;
    private static volatile boolean stopping = true;

    public static void start() {
        String mode = CONFIG.getString("multi-server-support", "none").toLowerCase(Locale.ROOT);
        messenger = switch (mode) {
            case "redis" -> new RedisMessenger();
            case "sql" -> {
                if (AxVaults.getDatabase() instanceof MySQL mySQL) yield new SQLMessenger(mySQL);
                throw new IllegalStateException("SQL synchronization requires database.type: mysql");
            }
            case "none" -> new NoneMessenger();
            default -> throw new IllegalArgumentException("Unknown multi-server-support mode: " + mode);
        };
        if (isEnabled() && !(AxVaults.getDatabase() instanceof MySQL)
                && !(AxVaults.getDatabase() instanceof Redis)) {
            throw new IllegalStateException("Multi-server synchronization requires shared MySQL or Redis storage");
        }

        messenger.start();
        if (isEnabled()) lockExecutor = new ExceptionReportingScheduledThreadPool(1);
        stopping = false;
    }

    public static void stop(boolean releaseLocks) {
        stopping = true;
        boolean interrupted = false;
        if (lockExecutor != null) {
            lockExecutor.shutdown();
            while (!lockExecutor.isTerminated()) {
                try {
                    lockExecutor.awaitTermination(1, TimeUnit.SECONDS);
                } catch (InterruptedException ex) {
                    interrupted = true;
                }
            }
            lockExecutor = null;
        }

        if (releaseLocks) {
            for (UUID uuid : List.copyOf(heldLocks)) {
                messenger.unlock(uuid);
            }
        }
        if (interrupted) Thread.currentThread().interrupt();
        heldLocks.clear();

        messenger.stop();
        messenger = new NoneMessenger();
    }

    public static boolean isEnabled() {
        return !(messenger instanceof NoneMessenger);
    }

    public static void sendChange(@NotNull ChangeType type, @NotNull UUID uuid, int vaultId) {
        messenger.sendChange(type, uuid, vaultId);
    }

    // called by the messengers when another server changed a vault
    public static void handleChange(@NotNull ChangeType type, @NotNull UUID uuid, int vaultId) {
        if (stopping || AxVaults.isStopping()) return;
        VaultPlayer vaultPlayer = VaultManager.getPlayers().get(uuid);
        if (vaultPlayer == null || !vaultPlayer.isLoaded()) return;

        // The owning server is the only writer. Delayed notifications must not overwrite its inventory.
        if (holdsLock(uuid)) return;
        AxVaults.getThreadedQueue().submit(() -> AxVaults.getDatabase().updateVault(vaultPlayer, vaultId));
    }

    public static CompletableFuture<Void> acquireLock(@NotNull UUID uuid) {
        if (stopping || AxVaults.isStopping()) {
            return CompletableFuture.failedFuture(new IllegalStateException("Vault synchronization is stopping"));
        }
        if (!isEnabled() || heldLocks.contains(uuid)) return CompletableFuture.completedFuture(null);

        Messenger current = messenger;
        return LockAcquirer.acquire(lockExecutor, () -> {
            if (stopping || AxVaults.isStopping()) throw new IllegalStateException("Vault synchronization is stopping");
            boolean acquired = current.tryLock(uuid);
            if (acquired && (stopping || AxVaults.isStopping())) {
                current.unlock(uuid);
                throw new IllegalStateException("Vault synchronization stopped during lock acquisition");
            }
            return acquired;
        }, LOCK_TIMEOUT_MILLIS, LOCK_RETRY_MILLIS).thenRun(() -> heldLocks.add(uuid));
    }

    public static boolean holdsLock(@NotNull UUID uuid) {
        return heldLocks.contains(uuid);
    }

    public static void releaseLock(@NotNull UUID uuid) {
        if (!heldLocks.remove(uuid)) return;

        Messenger current = messenger;
        lockExecutor.execute(() -> current.unlock(uuid));
    }
}
