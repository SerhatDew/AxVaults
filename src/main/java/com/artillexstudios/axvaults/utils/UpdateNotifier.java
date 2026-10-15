package com.artillexstudios.axvaults.utils;

import com.artillexstudios.axapi.config.Config;
import com.artillexstudios.axapi.scheduler.Scheduler;
import com.artillexstudios.axapi.utils.StringUtils;
import com.artillexstudios.axvaults.AxVaults;
import org.bukkit.Bukkit;
import org.bukkit.event.EventHandler;
import org.bukkit.event.Listener;
import org.bukkit.event.player.PlayerJoinEvent;
import org.jetbrains.annotations.Nullable;
import org.json.JSONObject;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.HashMap;

import static java.time.temporal.ChronoUnit.SECONDS;

public class UpdateNotifier implements Listener {
    private static Config config;
    private static Config lang;
    private static boolean onJoin;
    private static String prefix;
    private static String updateNotifier;

    private final String current;
    private String latest = null;
    private boolean newest = true;

    public static void init(Config config, Config lang) {
        UpdateNotifier.config = config;
        UpdateNotifier.lang = lang;
        reload();
    }

    public static void reload() {
        onJoin = config.getBoolean("update-notifier.on-join", true);
        prefix = config.getString("prefix");
        updateNotifier = lang.getString("update-notifier");
    }

    public UpdateNotifier() {
        this.current = AxVaults.getInstance().getDescription().getVersion();

        AxVaults.getInstance().getServer().getPluginManager().registerEvents(this, AxVaults.getInstance());

        long time = 30L * 60L * 20L;
        Scheduler.get().runAsyncTimer(t -> {
            this.latest = readVersion();
            this.newest = !isOutdated(current);

            if (latest == null || newest) return;
            Scheduler.get().runLaterAsync(t2 -> {
                Bukkit.getConsoleSender().sendMessage(getMessage());
            }, 50L);
            t.cancel();
        }, 0, time);
    }

    @EventHandler
    public void onJoin(PlayerJoinEvent event) {
        if (latest == null || newest) return;
        if (!onJoin) return;
        if (!event.getPlayer().hasPermission(AxVaults.getInstance().getName().toLowerCase() + ".update-notify")) return;
        Scheduler.get().runLaterAsync(t -> {
            event.getPlayer().sendMessage(getMessage());
        }, 50L);
    }

    private String getMessage() {
        HashMap<String, String> map = new HashMap<>();
        map.put("%current%", current);
        map.put("%latest%", latest);
        return StringUtils.formatToString(String.format("%s %s", prefix, updateNotifier), map);
    }

    @Nullable
    private String readVersion() {
        try (HttpClient client = HttpClient.newHttpClient()) {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(new URI("https://api.github.com/repos/SerhatDew/AxVaults/releases/latest"))
                    .timeout(Duration.of(10, SECONDS))
                    .GET()
                    .build();

            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() != 200) return null;
            String tag = new JSONObject(response.body()).getString("tag_name");
            return tag.startsWith("v") ? tag.substring(1) : tag;
        } catch (Exception ex) {
            return null;
        }
    }

    public String getLatest() {
        return latest;
    }

    public boolean isOutdated(String current) {
        // GitHub's latest release is authoritative for this fork, including suffixed versions.
        return latest != null && !latest.equals(current);
    }
}