package com.artillexstudios.axvaults.database.messaging.impl;

import com.artillexstudios.axapi.utils.StringUtils;
import com.artillexstudios.axapi.utils.logging.LogUtils;
import com.artillexstudios.axvaults.AxVaults;
import com.artillexstudios.axvaults.database.impl.Redis;
import com.artillexstudios.axvaults.database.impl.RedisConnection;
import com.artillexstudios.axvaults.database.messaging.ChangeType;
import com.artillexstudios.axvaults.database.messaging.Messaging;
import com.artillexstudios.axvaults.database.messaging.Messenger;
import com.artillexstudios.axvaults.database.messaging.SyncMessage;
import org.bukkit.Bukkit;
import org.jetbrains.annotations.NotNull;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPubSub;
import redis.clients.jedis.params.SetParams;

import java.util.Collections;
import java.util.UUID;

import static com.artillexstudios.axvaults.AxVaults.CONFIG;

public class RedisMessenger implements Messenger {
    private static final String LOCK_PREFIX = "axvaults:lock:";
    private static final String UNLOCK_LUA =
            "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end";

    private final String serverId = UUID.randomUUID().toString();
    private volatile boolean running = false;
    private JedisPool jedisPool;
    private boolean ownsPool = true;
    private volatile JedisPubSub currentPubSub;
    private volatile Jedis subscriberConnection;
    private Thread subThread;
    private String channel;

    @Override
    public void start() {
        this.channel = CONFIG != null ? CONFIG.getString("redis.channel", "axvaults:sync") : "axvaults:sync";

        if (AxVaults.getDatabase() instanceof Redis redisDb && redisDb.getJedisPool() != null) {
            this.jedisPool = redisDb.getJedisPool();
            this.ownsPool = false;
        } else {
            this.jedisPool = RedisConnection.createPool("messenger");
            this.ownsPool = true;
        }

        if (Bukkit.getServer() != null) {
            Bukkit.getConsoleSender().sendMessage(StringUtils.formatToString("&#55ff00[AxVaults] Connected to Redis for multi-server synchronization!"));
        }

        this.running = true;
        this.subThread = new Thread(() -> {
            while (running && !Thread.currentThread().isInterrupted()) {
                JedisPubSub pubSub = new JedisPubSub() {
                    @Override
                    public void onMessage(String ch, String message) {
                        if (!running || !ch.equals(channel)) return;
                        handleIncomingMessage(message);
                    }
                };
                this.currentPubSub = pubSub;

                try (Jedis jedis = jedisPool.getResource()) {
                    subscriberConnection = jedis;
                    if (running) jedis.subscribe(pubSub, channel);
                } catch (Exception ex) {
                    if (running) {
                        LogUtils.warn("Redis pub/sub subscriber disconnected, reconnecting in 2s...");
                        try {
                            Thread.sleep(2000);
                        } catch (InterruptedException ignored) {
                            break;
                        }
                    }
                } finally {
                    subscriberConnection = null;
                }
            }
        }, "AxVaults-Redis-Subscriber");
        this.subThread.setDaemon(true);
        this.subThread.start();
    }

    private void handleIncomingMessage(String rawMessage) {
        SyncMessage message = SyncMessage.decode(rawMessage);
        if (message == null || serverId.equals(message.serverId())) return;

        Messaging.handleChange(message.type(), message.uuid(), message.vaultId());
    }

    @Override
    public void sendChange(@NotNull ChangeType type, @NotNull UUID uuid, int vaultId) {
        if (!running || jedisPool == null || jedisPool.isClosed()) return;

        AxVaults.getThreadedQueue().submit(() -> {
            try (Jedis jedis = jedisPool.getResource()) {
                jedis.publish(channel, new SyncMessage(type, serverId, uuid, vaultId).encode());
            } catch (Exception ex) {
                LogUtils.error("Failed to publish Redis sync message for vault #{}", vaultId, ex);
            }
        });
    }

    @Override
    public boolean tryLock(@NotNull UUID uuid) {
        if (!running || jedisPool == null || jedisPool.isClosed()) {
            throw new IllegalStateException("Redis synchronization is not running");
        }

        String key = LOCK_PREFIX + uuid;
        try (Jedis jedis = jedisPool.getResource()) {
            String current = jedis.get(key);
            if (serverId.equals(current)) return true;

            String result = jedis.set(key, serverId, SetParams.setParams().nx());
            return "OK".equalsIgnoreCase(result);
        } catch (Exception ex) {
            throw new IllegalStateException("Failed to acquire Redis lock for " + uuid, ex);
        }
    }

    @Override
    public void unlock(@NotNull UUID uuid) {
        if (jedisPool == null || jedisPool.isClosed()) return;

        try (Jedis jedis = jedisPool.getResource()) {
            jedis.eval(UNLOCK_LUA, Collections.singletonList(LOCK_PREFIX + uuid), Collections.singletonList(serverId));
        } catch (Exception ex) {
            LogUtils.error("Failed to unlock {} in Redis", uuid, ex);
        }
    }

    @Override
    public void stop() {
        running = false;
        if (currentPubSub != null && currentPubSub.isSubscribed()) {
            try {
                currentPubSub.unsubscribe();
            } catch (Exception ignored) {}
        }
        Jedis subscriber = subscriberConnection;
        if (subscriber != null) subscriber.getClient().disconnect();
        if (subThread != null) {
            subThread.interrupt();
            boolean interrupted = false;
            while (subThread.isAlive()) {
                try {
                    subThread.join();
                } catch (InterruptedException ex) {
                    interrupted = true;
                }
            }
            if (interrupted) Thread.currentThread().interrupt();
        }
        if (ownsPool && jedisPool != null && !jedisPool.isClosed()) {
            try {
                jedisPool.close();
            } catch (Exception ignored) {}
        }
    }
}
