package com.artillexstudios.axvaults.database.impl;

import com.artillexstudios.axapi.serializers.Serializers;
import com.artillexstudios.axapi.utils.StringUtils;
import com.artillexstudios.axapi.utils.logging.LogUtils;
import com.artillexstudios.axvaults.database.Database;
import com.artillexstudios.axvaults.database.messaging.ChangeType;
import com.artillexstudios.axvaults.database.messaging.Messaging;
import com.artillexstudios.axvaults.placed.PlacedVaults;
import com.artillexstudios.axvaults.utils.ThreadUtils;
import com.artillexstudios.axvaults.vaults.Vault;
import com.artillexstudios.axvaults.vaults.VaultPlayer;
import org.bukkit.Bukkit;
import org.bukkit.Location;
import org.bukkit.Material;
import org.bukkit.inventory.ItemStack;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Transaction;

import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

public class Redis implements Database {
    private JedisPool jedisPool;

    private record LoadedVault(int id, Material icon, ItemStack[] items) {
    }

    private record LoadedBlock(Location location, Integer number) {
    }

    public Redis() {
        if (Bukkit.getServer() != null) {
            Bukkit.getConsoleSender().sendMessage(StringUtils.formatToString("&#55ff00[AxVaults] Loaded Redis database provider!"));
        }
    }

    @Override
    public String getType() {
        return "Redis";
    }

    public JedisPool getJedisPool() {
        return jedisPool;
    }

    @Override
    public void setup() {
        this.jedisPool = RedisConnection.createPool("database");
    }

    @Override
    public void saveVault(@NotNull Vault vault, Object result, @Nullable Material icon) {
        if (result instanceof Boolean bool && bool) {
            deleteVault(vault.getUUID(), vault.getId());
            return;
        }
        if (!(result instanceof byte[] bytes)) {
            throw new IllegalArgumentException("Vault save result must be serialized bytes or the empty-vault delete marker");
        }

        String key = dataKey(vault.getUUID(), vault.getId());
        try (Jedis jedis = jedisPool.getResource()) {
            Map<String, String> data = new HashMap<>();
            data.put("storage", Base64.getEncoder().encodeToString(bytes));
            data.put("icon", icon == null ? "" : icon.name());

            try (Transaction transaction = jedis.multi()) {
                var storageWrite = transaction.hset(key, data);
                var indexWrite = transaction.sadd(indexKey(vault.getUUID()), String.valueOf(vault.getId()));
                if (transaction.exec() == null) throw new IllegalStateException("Redis save transaction was discarded");
                storageWrite.get();
                indexWrite.get();
            }
        } catch (Exception ex) {
            throw redisFailure("Failed to save vault #" + vault.getId() + " of " + vault.getUUID() + " to Redis", ex);
        }
        Messaging.sendChange(ChangeType.UPDATE, vault.getUUID(), vault.getId());
    }

    @Override
    public void loadVaults(@NotNull VaultPlayer vaultPlayer) {
        List<LoadedVault> loaded = new ArrayList<>();
        try (Jedis jedis = jedisPool.getResource()) {
            Set<String> vaultIds = jedis.smembers(indexKey(vaultPlayer.getUUID()));
            if (vaultIds == null) throw new IllegalStateException("Redis returned no player vault index");
            for (String idString : vaultIds) {
                final int id;
                try {
                    id = Integer.parseInt(idString);
                } catch (NumberFormatException ex) {
                    throw new IllegalStateException("Redis contains an invalid vault id " + idString + " for " + vaultPlayer.getUUID(), ex);
                }
                Map<String, String> data = jedis.hgetAll(dataKey(vaultPlayer.getUUID(), id));
                if (data == null || data.isEmpty() || !data.containsKey("storage")) {
                    throw new IllegalStateException("Redis index references missing vault #" + id + " for " + vaultPlayer.getUUID());
                }
                try {
                    String storage = data.get("storage");
                    if (storage == null) throw new IllegalStateException("storage field is NULL");
                    ItemStack[] items = storage.isEmpty()
                            ? new ItemStack[0]
                            : Serializers.ITEM_ARRAY.deserialize(Base64.getDecoder().decode(storage));
                    String iconString = data.get("icon");
                    Material icon = iconString == null || iconString.isEmpty() ? null : Material.valueOf(iconString);
                    loaded.add(new LoadedVault(id, icon, items));
                } catch (Exception ex) {
                    throw new IllegalStateException("Failed to decode vault #" + id + " for " + vaultPlayer.getUUID() + " from Redis", ex);
                }
            }
        } catch (Exception ex) {
            throw redisFailure("Failed to load vaults from Redis for " + vaultPlayer.getUUID(), ex);
        }
        for (LoadedVault vault : loaded) new Vault(vaultPlayer, vault.id(), vault.icon(), vault.items());
    }

    @Override
    public boolean isVault(@NotNull Location location) {
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.hexists("axvaults:blocks", Serializers.LOCATION.serialize(location));
        } catch (Exception ex) {
            throw redisFailure("Failed to check whether a location is a vault in Redis", ex);
        }
    }

    @Override
    public void setVault(@NotNull Location location, @Nullable Integer num) {
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.hset("axvaults:blocks", Serializers.LOCATION.serialize(location), num == null ? "" : String.valueOf(num));
        } catch (Exception ex) {
            throw redisFailure("Failed to set vault block in Redis", ex);
        }
        PlacedVaults.addVault(location, num);
    }

    @Override
    public void removeVault(@NotNull Location location) {
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.hdel("axvaults:blocks", Serializers.LOCATION.serialize(location));
        } catch (Exception ex) {
            throw redisFailure("Failed to remove vault block in Redis", ex);
        }
    }

    @Override
    public void deleteVault(@NotNull UUID uuid, int num) {
        try (Jedis jedis = jedisPool.getResource()) {
            try (Transaction transaction = jedis.multi()) {
                var storageDelete = transaction.del(dataKey(uuid, num));
                var indexDelete = transaction.srem(indexKey(uuid), String.valueOf(num));
                if (transaction.exec() == null) throw new IllegalStateException("Redis delete transaction was discarded");
                storageDelete.get();
                indexDelete.get();
            }
        } catch (Exception ex) {
            // Do not publish DELETE when either half of the atomic delete failed.
            throw redisFailure("Failed to delete vault #" + num + " of " + uuid + " from Redis", ex);
        }
        Messaging.sendChange(ChangeType.DELETE, uuid, num);
    }

    @Override
    public void load() {
        List<LoadedBlock> loaded = new ArrayList<>();
        try (Jedis jedis = jedisPool.getResource()) {
            Map<String, String> blocks = jedis.hgetAll("axvaults:blocks");
            if (blocks == null) throw new IllegalStateException("Redis returned no placed-vault map");
            for (Map.Entry<String, String> entry : blocks.entrySet()) {
                try {
                    String value = entry.getValue();
                    loaded.add(new LoadedBlock(Serializers.LOCATION.deserialize(entry.getKey()), value == null || value.isEmpty() ? null : Integer.valueOf(value)));
                } catch (Exception ex) {
                    throw new IllegalStateException("Failed to decode a placed vault block from Redis", ex);
                }
            }
        } catch (Exception ex) {
            throw redisFailure("Failed to load placed vaults from Redis", ex);
        }
        for (LoadedBlock block : loaded) PlacedVaults.addVault(block.location(), block.number());
    }

    @Override
    public void updateVault(@NotNull VaultPlayer vaultPlayer, int id) {
        try (Jedis jedis = jedisPool.getResource()) {
            Map<String, String> data = jedis.hgetAll(dataKey(vaultPlayer.getUUID(), id));
            if (data == null || data.isEmpty() || !data.containsKey("storage")) {
                ThreadUtils.runSync(() -> vaultPlayer.applyRemoteChange(id, null, null));
                return;
            }
            try {
                String storage = data.get("storage");
                if (storage == null) throw new IllegalStateException("storage field is NULL");
                ItemStack[] items = storage.isEmpty()
                        ? new ItemStack[0]
                        : Serializers.ITEM_ARRAY.deserialize(Base64.getDecoder().decode(storage));
                String iconString = data.get("icon");
                Material icon = iconString == null || iconString.isEmpty() ? null : Material.valueOf(iconString);
                ThreadUtils.runSync(() -> vaultPlayer.applyRemoteChange(id, items, icon));
            } catch (Exception ex) {
                throw new IllegalStateException("Failed to decode updated vault #" + id + " for " + vaultPlayer.getUUID() + " from Redis", ex);
            }
        } catch (Exception ex) {
            throw redisFailure("Failed to update vault #" + id + " from Redis for " + vaultPlayer.getUUID(), ex);
        }
    }

    private String dataKey(UUID uuid, int id) {
        return "axvaults:data:" + uuid + ":" + id;
    }

    private String indexKey(UUID uuid) {
        return "axvaults:player_vaults:" + uuid;
    }

    private IllegalStateException redisFailure(String message, Exception cause) {
        LogUtils.error(message, cause);
        return new IllegalStateException(message, cause);
    }

    @Override
    public void disable() {
        if (jedisPool != null && !jedisPool.isClosed()) {
            try {
                jedisPool.close();
            } catch (Exception ex) {
                LogUtils.error("Error closing Redis JedisPool", ex);
            }
        }
    }
}
