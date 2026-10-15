package com.artillexstudios.axvaults;

import com.artillexstudios.axapi.AxPlugin;
import com.artillexstudios.axapi.config.Config;
import com.artillexstudios.axapi.dependencies.DependencyManagerWrapper;
import com.artillexstudios.axapi.executor.ThreadedQueue;
import com.artillexstudios.axapi.libs.boostedyaml.dvs.versioning.BasicVersioning;
import com.artillexstudios.axapi.libs.boostedyaml.settings.dumper.DumperSettings;
import com.artillexstudios.axapi.libs.boostedyaml.settings.general.GeneralSettings;
import com.artillexstudios.axapi.libs.boostedyaml.settings.loader.LoaderSettings;
import com.artillexstudios.axapi.libs.boostedyaml.settings.updater.UpdaterSettings;
import com.artillexstudios.axapi.metrics.AxMetrics;
import com.artillexstudios.axapi.utils.MessageUtils;
import com.artillexstudios.axapi.utils.StringUtils;
import com.artillexstudios.axapi.utils.featureflags.FeatureFlags;
import com.artillexstudios.axapi.utils.logging.LoggerNameFormat;
import com.artillexstudios.axvaults.commands.CommandManager;
import com.artillexstudios.axvaults.database.Database;
import com.artillexstudios.axvaults.database.impl.H2;
import com.artillexstudios.axvaults.database.impl.MySQL;
import com.artillexstudios.axvaults.database.impl.Redis;
import com.artillexstudios.axvaults.database.impl.SQLite;
import com.artillexstudios.axvaults.database.messaging.Messaging;
import com.artillexstudios.axvaults.hooks.HookManager;
import com.artillexstudios.axvaults.libraries.Libraries;
import com.artillexstudios.axvaults.listeners.BlacklistListener;
import com.artillexstudios.axvaults.listeners.BlockBreakListener;
import com.artillexstudios.axvaults.listeners.InventoryClickListener;
import com.artillexstudios.axvaults.listeners.InventoryCloseListener;
import com.artillexstudios.axvaults.listeners.PlayerInteractListener;
import com.artillexstudios.axvaults.listeners.PlayerListeners;
import com.artillexstudios.axvaults.schedulers.AutoSaveScheduler;
import com.artillexstudios.axvaults.utils.DebugUtils;
import com.artillexstudios.axvaults.utils.UpdateNotifier;
import com.artillexstudios.axvaults.utils.VaultUtils;
import com.artillexstudios.axvaults.vaults.VaultManager;
import org.bstats.bukkit.Metrics;
import org.bukkit.Bukkit;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public final class AxVaults extends AxPlugin {
    private static boolean stopping = false;
    public static Config CONFIG;
    public static Config MESSAGES;
    public static MessageUtils MESSAGEUTILS;
    private static AxPlugin instance;
    private static ThreadedQueue<Runnable> threadedQueue;
    private static Database database;
    private static AxMetrics metrics;

    public static ThreadedQueue<Runnable> getThreadedQueue() {
        return threadedQueue;
    }

    public static AxPlugin getInstance() {
        return instance;
    }

    public static Database getDatabase() {
        return database;
    }

    public static boolean isStopping() {
        return stopping;
    }

    @Override
    public void dependencies(DependencyManagerWrapper manager) {
        instance = this;
        Libraries.load(instance, manager);
    }

    public void enable() {
        stopping = false;
        new Metrics(this, 20541);

        CONFIG = new Config(new File(getDataFolder(), "config.yml"), getResource("config.yml"), GeneralSettings.builder().setUseDefaults(false).build(), LoaderSettings.builder().setAutoUpdate(true).build(), DumperSettings.DEFAULT, UpdaterSettings.builder().setKeepAll(true).setVersioning(new BasicVersioning("version")).build());
        MESSAGES = new Config(new File(getDataFolder(), "messages.yml"), getResource("messages.yml"), GeneralSettings.builder().setUseDefaults(false).build(), LoaderSettings.builder().setAutoUpdate(true).build(), DumperSettings.DEFAULT, UpdaterSettings.builder().setKeepAll(true).setVersioning(new BasicVersioning("version")).build());

        MESSAGEUTILS = new MessageUtils(MESSAGES.getBackingDocument(), "prefix", CONFIG.getBackingDocument());

        threadedQueue = new ThreadedQueue<>("AxVaults-Datastore-thread");

        VaultUtils.reload();
        HookManager.setupHooks();
        DebugUtils.init(CONFIG);

        database = switch (CONFIG.getString("database.type", "h2").toLowerCase()) {
            case "sqlite" -> new SQLite();
            case "mysql" -> new MySQL();
            case "redis" -> new Redis();
            default -> new H2();
        };

        database.setup();

        threadedQueue.submit(() -> database.load());

        Messaging.start();

        getServer().getPluginManager().registerEvents(new PlayerListeners(), this);
        getServer().getPluginManager().registerEvents(new BlacklistListener(), this);
        getServer().getPluginManager().registerEvents(new PlayerInteractListener(), this);
        getServer().getPluginManager().registerEvents(new BlockBreakListener(), this);
        getServer().getPluginManager().registerEvents(new InventoryCloseListener(), this);
        getServer().getPluginManager().registerEvents(new InventoryClickListener(), this);

        CommandManager.load();

        AutoSaveScheduler.start();
        metrics = new AxMetrics(this, 3);
        metrics.start();

        Bukkit.getConsoleSender().sendMessage(StringUtils.formatToString("&#55ff00[AxVaults] Loaded plugin!"));

        UpdateNotifier.init(CONFIG, MESSAGES);
        if (CONFIG.getBoolean("update-notifier.enabled", true)) new UpdateNotifier();
    }

    public void disable() {
        stopping = true;
        if (metrics != null) metrics.cancel();
        AutoSaveScheduler.stop();

        // Drain work queued before shutdown (notably a delete restoration) before taking
        // final snapshots. Late completion callbacks are stopping-aware and do not touch Bukkit.
        boolean queueSucceeded = true;
        try {
            CompletableFuture<Void> barrier = new CompletableFuture<>();
            threadedQueue.submit(() -> barrier.complete(null));
            barrier.join();
        } catch (Throwable throwable) {
            queueSucceeded = false;
            Bukkit.getLogger().severe("Failed to drain the AxVaults datastore queue before shutdown snapshots: "
                    + cause(throwable));
        }

        // Closing is a Bukkit operation and must happen before any final persistence snapshot.
        VaultManager.closeAllInventories();
        List<CompletableFuture<Void>> saves = new ArrayList<>();
        for (var vaultPlayer : List.copyOf(VaultManager.getPlayers().values())) {
            if (vaultPlayer.isLoaded()) saves.add(VaultManager.save(vaultPlayer));
        }

        boolean savesSucceeded = true;
        try {
            saves.add(VaultManager.pendingSaves());
            CompletableFuture.allOf(saves.toArray(CompletableFuture[]::new)).join();
        } catch (Throwable throwable) {
            savesSucceeded = false;
            Bukkit.getLogger().severe("Failed to persist all vaults during shutdown: " + cause(throwable));
        }

        try {
            CompletableFuture<Void> barrier = new CompletableFuture<>();
            threadedQueue.submit(() -> barrier.complete(null));
            barrier.join();
        } catch (Throwable throwable) {
            queueSucceeded = false;
            Bukkit.getLogger().severe("Failed to drain the AxVaults datastore queue during shutdown: " + cause(throwable));
        }

        boolean releaseLocks = savesSucceeded && queueSucceeded;
        Messaging.stop(releaseLocks);
        database.disable();
        threadedQueue.stop();
    }

    public void updateFlags() {
        FeatureFlags.LOGGER_NAME_FORMAT.set(LoggerNameFormat.NAMEABLE);
    }

    private static String cause(Throwable throwable) {
        Throwable current = throwable;
        while (current.getCause() != null) current = current.getCause();
        return current.toString();
    }
}
