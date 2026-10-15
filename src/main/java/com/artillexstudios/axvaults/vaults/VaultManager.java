package com.artillexstudios.axvaults.vaults;

import com.artillexstudios.axvaults.AxVaults;
import com.artillexstudios.axvaults.database.messaging.Messaging;
import com.artillexstudios.axvaults.utils.ThreadUtils;
import com.artillexstudios.axvaults.utils.VaultUtils;
import org.bukkit.Bukkit;
import org.bukkit.OfflinePlayer;
import org.bukkit.inventory.Inventory;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class VaultManager {
    private static final ConcurrentHashMap<UUID, VaultPlayer> players = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<UUID, CompletableFuture<VaultPlayer>> loadingPlayers = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<UUID, CompletableFuture<Void>> evictingPlayers = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<UUID, CompletableFuture<Void>> pendingSaves = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<UUID, AtomicInteger> pendingOperations = new ConcurrentHashMap<>();
    private static final Set<UUID> failedSaves = ConcurrentHashMap.newKeySet();

    public static ConcurrentHashMap<UUID, VaultPlayer> getPlayers() {
        return players;
    }

    @ApiStatus.Internal
    public static ConcurrentHashMap<UUID, CompletableFuture<VaultPlayer>> getLoadingPlayers() {
        return loadingPlayers;
    }

    public static void loadPlayer(@NotNull OfflinePlayer offlinePlayer) {
        getPlayer(offlinePlayer).exceptionally(throwable -> {
            Bukkit.getLogger().warning("Failed to load vaults for " + offlinePlayer.getUniqueId() + ": " + cause(throwable));
            return null;
        });
    }

    // The future is completed on the main thread during normal operation.
    public static CompletableFuture<VaultPlayer> getPlayer(@NotNull OfflinePlayer offlinePlayer) {
        return getPlayer(offlinePlayer, CompletableFuture.completedFuture(null));
    }

    // Loading starts only after the "after" future completed.
    public static CompletableFuture<VaultPlayer> getPlayer(@NotNull OfflinePlayer offlinePlayer, @NotNull CompletableFuture<?> after) {
        UUID uuid = offlinePlayer.getUniqueId();
        if (AxVaults.isStopping()) return failedFuture(new IllegalStateException("AxVaults is stopping"));
        if (pendingOperations.containsKey(uuid)) {
            return failedFuture(new IllegalStateException("A vault operation is pending for " + uuid));
        }

        CompletableFuture<VaultPlayer> loading = loadingPlayers.get(uuid);
        if (loading != null) return loading;

        CompletableFuture<Void> eviction = evictingPlayers.get(uuid);
        if (eviction != null) return afterEviction(offlinePlayer, after, eviction);

        VaultPlayer cached = players.get(uuid);
        if (cached != null && cached.isLoaded()) {
            if (Messaging.isEnabled() && !Messaging.holdsLock(uuid)) {
                return afterEviction(offlinePlayer, after, beginStaleEviction(cached));
            }

            CompletableFuture<Void> pending = pendingSaves.get(uuid);
            if (pending != null) return afterPendingSave(offlinePlayer, after, pending);
            return completedOnMain(cached);
        }

        CompletableFuture<VaultPlayer> result = new CompletableFuture<>();
        loading = loadingPlayers.putIfAbsent(uuid, result);
        if (loading != null) return loading;

        VaultPlayer vaultPlayer = players.computeIfAbsent(uuid, VaultPlayer::new);
        startLoad(offlinePlayer, vaultPlayer, after, result);
        return result;
    }

    private static void startLoad(@NotNull OfflinePlayer offlinePlayer, @NotNull VaultPlayer vaultPlayer,
                                  @NotNull CompletableFuture<?> after, @NotNull CompletableFuture<VaultPlayer> result) {
        UUID uuid = vaultPlayer.getUUID();
        CompletableFuture<?> ready = after.thenCompose(ignored -> {
            if (AxVaults.isStopping()) return failedFuture(new IllegalStateException("AxVaults is stopping"));
            // Offline administrative loads must use exactly the same distributed lock as online loads.
            return Messaging.acquireLock(uuid);
        });

        ready.whenComplete((ignored, throwable) -> {
            if (throwable != null) {
                failLoad(uuid, vaultPlayer, result, throwable);
                return;
            }
            if (AxVaults.isStopping()) {
                failLoad(uuid, vaultPlayer, result, new IllegalStateException("AxVaults is stopping"));
                return;
            }

            try {
                AxVaults.getThreadedQueue().submit(() -> {
                    if (AxVaults.isStopping()) {
                        failLoad(uuid, vaultPlayer, result, new IllegalStateException("AxVaults is stopping"));
                        return;
                    }
                    try {
                        vaultPlayer.load();
                        if (AxVaults.isStopping()) {
                            throw new IllegalStateException("Player load completed while AxVaults was stopping");
                        }
                        completeOnMain(result, vaultPlayer);
                    } catch (Throwable loadError) {
                        failLoad(uuid, vaultPlayer, result, loadError);
                    }
                });
            } catch (Throwable submitError) {
                failLoad(uuid, vaultPlayer, result, submitError);
            }
        });

        result.whenComplete((loaded, throwable) -> {
            loadingPlayers.remove(uuid, result);
            if (throwable != null) {
                players.remove(uuid, vaultPlayer);
                vaultPlayer.getVaultMap().clear();
                // Lock acquisition failures do not own a lock, but a load can fail after acquisition.
                if (Messaging.holdsLock(uuid)) Messaging.releaseLock(uuid);
            }
        });
    }

    private static void failLoad(UUID uuid, VaultPlayer vaultPlayer, CompletableFuture<VaultPlayer> result, Throwable throwable) {
        completeExceptionallyOnMain(result, throwable);
    }

    private static CompletableFuture<VaultPlayer> afterEviction(@NotNull OfflinePlayer offlinePlayer,
                                                                 @NotNull CompletableFuture<?> after,
                                                                 @NotNull CompletableFuture<Void> eviction) {
        CompletableFuture<VaultPlayer> result = new CompletableFuture<>();
        eviction.whenComplete((ignored, throwable) -> {
            evictingPlayers.remove(offlinePlayer.getUniqueId(), eviction);
            if (throwable != null) {
                VaultPlayer cached = players.get(offlinePlayer.getUniqueId());
                if (cached != null && cached.isLoaded() && Messaging.holdsLock(cached.getUUID())) {
                    result.complete(cached);
                } else {
                    result.completeExceptionally(throwable);
                }
                return;
            }
            getPlayer(offlinePlayer, after).whenComplete((player, loadError) -> {
                if (loadError == null) result.complete(player);
                else result.completeExceptionally(loadError);
            });
        });
        return result;
    }

    private static CompletableFuture<VaultPlayer> afterPendingSave(@NotNull OfflinePlayer offlinePlayer,
                                                                    @NotNull CompletableFuture<?> after,
                                                                    @NotNull CompletableFuture<Void> pending) {
        CompletableFuture<VaultPlayer> result = new CompletableFuture<>();
        pending.whenComplete((ignored, throwable) -> {
            pendingSaves.remove(offlinePlayer.getUniqueId(), pending);
            getPlayer(offlinePlayer, after).whenComplete((player, loadError) -> {
                if (loadError == null) result.complete(player);
                else result.completeExceptionally(loadError);
            });
        });
        return result;
    }

    private static CompletableFuture<Void> beginStaleEviction(@NotNull VaultPlayer vaultPlayer) {
        UUID uuid = vaultPlayer.getUUID();
        CompletableFuture<Void> result = new CompletableFuture<>();
        CompletableFuture<Void> existing = evictingPlayers.putIfAbsent(uuid, result);
        if (existing != null) return existing;
        result.whenComplete((ignored, throwable) -> evictingPlayers.remove(uuid, result));

        try {
            ThreadUtils.runSync(() -> {
                try {
                    // Never write an unowned cache. If ownership was acquired while this was queued,
                    // leave the player in place and let getPlayer use the now-current owner.
                    if (players.get(uuid) == vaultPlayer && !Messaging.holdsLock(uuid)) {
                        vaultPlayer.closeInventories();
                        vaultPlayer.getVaultMap().clear();
                        players.remove(uuid, vaultPlayer);
                    }
                    result.complete(null);
                } catch (Throwable throwable) {
                    result.completeExceptionally(throwable);
                }
            });
        } catch (Throwable throwable) {
            result.completeExceptionally(throwable);
        }
        return result;
    }

    @Nullable
    public static VaultPlayer getPlayerOrNull(@NotNull OfflinePlayer offlinePlayer) {
        return players.get(offlinePlayer.getUniqueId());
    }

    public static CompletableFuture<Void> save(@NotNull VaultPlayer vaultPlayer) {
        CompletableFuture<Void> save = vaultPlayer.save();
        trackPendingSave(vaultPlayer.getUUID(), save);
        return save;
    }

    public static CompletableFuture<Void> saveVault(@NotNull Vault vault) {
        CompletableFuture<Void> save = VaultUtils.save(vault);
        trackPendingSave(vault.getUUID(), save);
        return save;
    }


    public static boolean isSavePending(@NotNull UUID uuid) {
        return pendingSaves.containsKey(uuid);
    }
    public static CompletableFuture<Void> pendingSaves() {
        CompletableFuture<?>[] saves = pendingSaves.values().toArray(CompletableFuture[]::new);
        return CompletableFuture.allOf(saves);
    }


    private static void trackPendingSave(UUID uuid, CompletableFuture<Void> save) {
        CompletableFuture<Void> aggregate;
        synchronized (pendingSaves) {
            CompletableFuture<Void> existing = pendingSaves.get(uuid);
            aggregate = existing == null ? save : CompletableFuture.allOf(existing, save);
            pendingSaves.put(uuid, aggregate);
        }
        aggregate.whenComplete((ignored, throwable) -> pendingSaves.remove(uuid, aggregate));
        aggregate.whenComplete((ignored, throwable) -> {
            if (throwable == null) failedSaves.remove(uuid);
            else failedSaves.add(uuid);
        });
    }

    public static void releaseIdleOfflinePlayers() {
        ThreadUtils.checkMain("releasing idle offline vault players");
        for (VaultPlayer vaultPlayer : List.copyOf(players.values())) {
            UUID uuid = vaultPlayer.getUUID();
            if (!vaultPlayer.isLoaded() || Bukkit.getPlayer(uuid) != null) continue;
            if (loadingPlayers.containsKey(uuid) || pendingSaves.containsKey(uuid)
                    || pendingOperations.containsKey(uuid) || evictingPlayers.containsKey(uuid)) continue;
            if (vaultPlayer.getVaultMap().values().stream().anyMatch(Vault::isOpened)) continue;

            beginIdleEviction(vaultPlayer);
        }
    }

    private static CompletableFuture<Void> beginIdleEviction(@NotNull VaultPlayer vaultPlayer) {
        UUID uuid = vaultPlayer.getUUID();
        CompletableFuture<Void> result = new CompletableFuture<>();
        CompletableFuture<Void> existing = evictingPlayers.putIfAbsent(uuid, result);
        if (existing != null) return existing;
        result.whenComplete((ignored, throwable) -> evictingPlayers.remove(uuid, result));

        try {
            ThreadUtils.runSync(() -> {
                if (players.get(uuid) != vaultPlayer || Bukkit.getPlayer(uuid) != null
                        || loadingPlayers.containsKey(uuid) || pendingSaves.containsKey(uuid)
                        || pendingOperations.containsKey(uuid)) {
                    result.complete(null);
                    return;
                }

                boolean dirty = vaultPlayer.getVaultMap().values().stream().anyMatch(vault -> vault.hasChanged().get());
                if (!dirty) {
                    removeAndRelease(vaultPlayer);
                    result.complete(null);
                    return;
                }
                if (failedSaves.contains(uuid)) {
                    result.complete(null);
                    return;
                }
                save(vaultPlayer).whenComplete((ignored, throwable) -> ThreadUtils.runSync(() -> {
                    if (throwable != null) {
                        Bukkit.getLogger().warning("Failed to save offline vaults for " + uuid + ": " + cause(throwable));
                        result.completeExceptionally(throwable);
                        return;
                    }
                    for (Vault vault : vaultPlayer.getVaultMap().values()) vault.hasChanged().set(false);
                    if (Bukkit.getPlayer(uuid) == null && players.get(uuid) == vaultPlayer) removeAndRelease(vaultPlayer);
                    result.complete(null);
                }));
            });
        } catch (Throwable throwable) {
            result.completeExceptionally(throwable);
        }
        return result;
    }

    public static void evictAfterSuccessfulSave(@NotNull VaultPlayer vaultPlayer) {
        ThreadUtils.runSync(() -> {
            UUID uuid = vaultPlayer.getUUID();
            if (players.get(uuid) != vaultPlayer || pendingOperations.containsKey(uuid)) return;
            removeAndRelease(vaultPlayer);
        });
    }

    private static void removeAndRelease(@NotNull VaultPlayer vaultPlayer) {
        UUID uuid = vaultPlayer.getUUID();
        vaultPlayer.closeInventories();
        vaultPlayer.getVaultMap().clear();
        if (!players.remove(uuid, vaultPlayer)) return;
        if (Messaging.holdsLock(uuid)) Messaging.releaseLock(uuid);
        failedSaves.remove(uuid);
    }

    public static void beginPersistenceOperation(@NotNull UUID uuid) {
        pendingOperations.compute(uuid, (ignored, count) -> {
            if (count == null) return new AtomicInteger(1);
            count.incrementAndGet();
            return count;
        });
    }

    public static void endPersistenceOperation(@NotNull UUID uuid) {
        pendingOperations.computeIfPresent(uuid, (ignored, count) -> {
            return count.decrementAndGet() == 0 ? null : count;
        });
    }
    public static boolean isPersistenceOperationPending(@NotNull UUID uuid) {
        return pendingOperations.containsKey(uuid);
    }


    public static List<Vault> getVaults() {
        ArrayList<Vault> vaults = new ArrayList<>();
        for (VaultPlayer vaultPlayer : players.values()) {
            vaults.addAll(vaultPlayer.getVaultMap().values());
        }
        return Collections.unmodifiableList(vaults);
    }

    public static void closeAllInventories() {
        ThreadUtils.checkMain("closing all vault inventories");
        for (VaultPlayer vaultPlayer : players.values()) {
            vaultPlayer.closeInventories();
        }
    }

    @Nullable
    public static Vault getVault(Inventory inventory) {
        for (VaultPlayer vaultPlayer : players.values()) {
            for (Vault vault : vaultPlayer.getVaultMap().values()) {
                if (inventory.equals(vault.getStorage())) return vault;
            }
        }
        return null;
    }

    public static boolean removeVault(@NotNull Vault vault) {
        VaultPlayer vaultPlayer = vault.getVaultPlayer();
        boolean success = vaultPlayer.removeVault(vault) != null;
        return success;
    }

    private static CompletableFuture<VaultPlayer> completedOnMain(VaultPlayer value) {
        CompletableFuture<VaultPlayer> result = new CompletableFuture<>();
        completeOnMain(result, value);
        return result;
    }

    private static void completeOnMain(CompletableFuture<VaultPlayer> result, VaultPlayer value) {
        ThreadUtils.runSync(() -> result.complete(value));
    }

    private static void completeExceptionallyOnMain(CompletableFuture<VaultPlayer> result, Throwable throwable) {
        ThreadUtils.runSync(() -> result.completeExceptionally(throwable));
    }

    private static <T> CompletableFuture<T> failedFuture(Throwable throwable) {
        CompletableFuture<T> result = new CompletableFuture<>();
        result.completeExceptionally(throwable);
        return result;
    }

    private static String cause(Throwable throwable) {
        Throwable current = throwable;
        while (current.getCause() != null) current = current.getCause();
        return current.toString();
    }
}
