package com.artillexstudios.axvaults.schedulers;

import com.artillexstudios.axapi.executor.ExceptionReportingScheduledThreadPool;
import com.artillexstudios.axapi.utils.mutable.MutableInteger;
import com.artillexstudios.axvaults.AxVaults;
import com.artillexstudios.axvaults.utils.ThreadUtils;
import com.artillexstudios.axvaults.vaults.Vault;
import com.artillexstudios.axvaults.vaults.VaultManager;
import org.bukkit.Bukkit;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static com.artillexstudios.axvaults.AxVaults.CONFIG;

public class AutoSaveScheduler {
    private static int autoSaveMinutes = 1;
    private static ExceptionReportingScheduledThreadPool pool = null;
    private static long lastSave = -1;
    private static long savedVaults = -1;

    private static CompletableFuture<Void> saveOnMain() {
        long saveStart = System.currentTimeMillis();
        MutableInteger saved = new MutableInteger();
        List<CompletableFuture<Void>> futures = new ArrayList<>();

        try {
            ThreadUtils.checkMain("autosaving vaults");
            List<Vault> dirtyVaults = new ArrayList<>();
            for (Vault vault : VaultManager.getVaults()) {
                // A quit/shutdown save owns the whole player snapshot. Do not race it with an
                // individual vault save or let a later callback clear its dirty state.
                if (vault.hasChanged().get() && !VaultManager.isSavePending(vault.getUUID())) {
                    dirtyVaults.add(vault);
                }
            }
            for (Vault vault : dirtyVaults) {
                boolean wasOpened = vault.isOpened();
                long lastOpen = vault.getLastOpen();
                var icon = vault.getRealIcon();
                CompletableFuture<Void> future = VaultManager.saveVault(vault);
                futures.add(future);
                saved.increment();
                future.whenComplete((ignored, throwable) -> {
                    if (AxVaults.isStopping()) return;
                    ThreadUtils.runSync(() -> {
                        if (throwable != null) {
                            Bukkit.getLogger().warning("Failed to autosave vault #" + vault.getId() + " for "
                                    + vault.getUUID() + ": " + cause(throwable));
                        } else if (!wasOpened && !vault.isOpened() && lastOpen == vault.getLastOpen()
                                && icon == vault.getRealIcon()) {
                            // Never clear dirty state before persistence succeeds.
                            vault.hasChanged().set(false);
                        }
                    });
                });
            }
        } catch (Throwable throwable) {
            Bukkit.getLogger().warning("Failed to start vault autosave: " + cause(throwable));
            return CompletableFuture.failedFuture(throwable);
        }

        return CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new)).whenComplete((ignored, throwable) -> {
            lastSave = System.currentTimeMillis() - saveStart;
            savedVaults = saved.get();
            if (throwable != null) {
                Bukkit.getLogger().warning("One or more vault autosaves failed: " + cause(throwable));
            }
        });
    }

    public static void start() {
        autoSaveMinutes = CONFIG.getInt("auto-save-minutes");
        if (pool != null) pool.shutdown();

        pool = new ExceptionReportingScheduledThreadPool(1);
        pool.scheduleAtFixedRate(AutoSaveScheduler::save, autoSaveMinutes, autoSaveMinutes, TimeUnit.MINUTES);
        pool.scheduleAtFixedRate(() -> ThreadUtils.runSync(VaultManager::releaseIdleOfflinePlayers),
                1, 1, TimeUnit.SECONDS);
    }

    public static void stop() {
        if (pool == null) return;
        pool.shutdown();
        pool = null;
    }

    public static long getLastSaveLength() {
        return lastSave;
    }

    public static long getSavedVaults() {
        return savedVaults;
    }

    public static CompletableFuture<Void> save() {
        CompletableFuture<Void> result = new CompletableFuture<>();
        ThreadUtils.runSync(() -> {
            if (AxVaults.isStopping()) {
                result.completeExceptionally(new IllegalStateException("AxVaults is stopping"));
                return;
            }
            saveOnMain().whenComplete((ignored, throwable) -> {
                if (throwable == null) result.complete(null);
                else result.completeExceptionally(throwable);
            });
        });
        return result;
    }

    private static String cause(Throwable throwable) {
        Throwable current = throwable;
        while (current.getCause() != null) current = current.getCause();
        return current.toString();
    }
}
