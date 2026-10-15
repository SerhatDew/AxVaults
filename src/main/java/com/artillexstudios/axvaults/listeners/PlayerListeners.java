package com.artillexstudios.axvaults.listeners;

import com.artillexstudios.axapi.scheduler.Scheduler;
import com.artillexstudios.axvaults.AxVaults;
import com.artillexstudios.axvaults.database.messaging.Messaging;
import com.artillexstudios.axvaults.utils.ThreadUtils;
import com.artillexstudios.axvaults.vaults.VaultPlayer;
import com.artillexstudios.axvaults.vaults.VaultManager;
import org.bukkit.Bukkit;
import org.bukkit.entity.Player;
import org.bukkit.event.EventHandler;
import org.bukkit.event.Listener;
import org.bukkit.event.player.PlayerJoinEvent;
import org.bukkit.event.player.PlayerQuitEvent;
import org.jetbrains.annotations.NotNull;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

public class PlayerListeners implements Listener {
    private static final ConcurrentHashMap.KeySetView<UUID, Boolean> quitting = ConcurrentHashMap.newKeySet();

    public PlayerListeners() {
        for (Player player : Bukkit.getOnlinePlayers()) {
            VaultManager.loadPlayer(player);
        }
    }

    @EventHandler
    public void onJoin(@NotNull PlayerJoinEvent event) {
        UUID uuid = event.getPlayer().getUniqueId();
        quitting.remove(uuid);
        VaultManager.getPlayer(event.getPlayer()).exceptionally(throwable -> {
            Bukkit.getLogger().warning("Failed to load vaults for " + uuid + ": " + cause(throwable));
            return null;
        });
    }

    @EventHandler
    public void onQuit(@NotNull PlayerQuitEvent event) {
        UUID uuid = event.getPlayer().getUniqueId();
        quitting.add(uuid);

        VaultPlayer vaultPlayer = VaultManager.getPlayers().get(uuid);
        CompletableFuture<VaultPlayer> loading = VaultManager.getLoadingPlayers().get(uuid);
        if (vaultPlayer == null) {
            if (loading != null) {
                loading.whenComplete((loaded, throwable) -> {
                    if (throwable != null || loaded == null) {
                        quitting.remove(uuid);
                        return;
                    }
                    saveAfterQuit(loaded);
                });
            } else {
                quitting.remove(uuid);
                if (Messaging.holdsLock(uuid)) Messaging.releaseLock(uuid);
            }
            return;
        }

        if (loading != null) {
            loading.whenComplete((loaded, throwable) -> {
                if (throwable != null || loaded == null) {
                    quitting.remove(uuid);
                    return;
                }
                saveAfterQuit(loaded);
            });
            return;
        }
        saveAfterQuit(vaultPlayer);
    }

    private void saveAfterQuit(@NotNull VaultPlayer vaultPlayer) {
        UUID uuid = vaultPlayer.getUUID();
        // Always defer the handoff check: a fast save may complete inside PlayerQuitEvent,
        // before Bukkit removes the old player from its online-player registry.
        VaultManager.save(vaultPlayer).whenComplete((ignored, throwable) -> {
            if (AxVaults.isStopping()) {
                if (throwable != null) {
                    Bukkit.getLogger().warning("Failed to save vaults for quitting player " + uuid + ": " + cause(throwable));
                }
                return;
            }
            Scheduler.get().run(() -> {
                if (throwable != null) {
                    // Keep both cache and distributed ownership so another server cannot read stale data.
                    Bukkit.getLogger().warning("Failed to save vaults for quitting player " + uuid + ": " + cause(throwable));
                    return;
                }
                if (quitting.contains(uuid) && Bukkit.getPlayer(uuid) == null) {
                    for (var vault : vaultPlayer.getVaultMap().values()) {
                        vault.hasChanged().set(false);
                    }
                    VaultManager.evictAfterSuccessfulSave(vaultPlayer);
                    quitting.remove(uuid);
                } else {
                    // A reconnect won the handoff while the save was in flight; retain cache and lock.
                    quitting.remove(uuid);
                }
            });
        });
    }

    private static String cause(Throwable throwable) {
        Throwable current = throwable;
        while (current.getCause() != null) current = current.getCause();
        return current.toString();
    }
}
