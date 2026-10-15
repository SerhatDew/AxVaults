package com.artillexstudios.axvaults.database.impl;

import org.jetbrains.annotations.NotNull;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.time.Duration;
import static com.artillexstudios.axvaults.AxVaults.CONFIG;

public final class RedisConnection {

    private RedisConnection() {
    }

    @NotNull
    public static JedisPool createPool(@NotNull String poolName) {
        final JedisPoolConfig poolConfig = new JedisPoolConfig();
        int timeout = CONFIG != null ? CONFIG.getInt("redis.timeout", 2000) : 2000;
        if (timeout <= 0) timeout = 2000;

        int maxTotal = CONFIG != null ? CONFIG.getInt("redis.pool.max-total", 16) : 16;
        int maxIdle = CONFIG != null ? CONFIG.getInt("redis.pool.max-idle", 8) : 8;
        int minIdle = CONFIG != null ? CONFIG.getInt("redis.pool.min-idle", 2) : 2;

        // A subscriber reserves one resource for its lifetime; keep one resource available for
        // ordinary commands even when the configured value is one.
        maxTotal = Math.max(2, maxTotal);
        maxIdle = Math.max(1, Math.min(maxIdle, maxTotal));
        minIdle = Math.max(0, Math.min(minIdle, maxIdle));

        poolConfig.setMaxTotal(maxTotal);
        poolConfig.setMaxIdle(maxIdle);
        poolConfig.setMinIdle(minIdle);
        poolConfig.setBlockWhenExhausted(true);
        poolConfig.setMaxWait(Duration.ofMillis(timeout));
        poolConfig.setTestOnBorrow(true);
        poolConfig.setTestOnReturn(false);

        String host = CONFIG != null ? CONFIG.getString("redis.host", "localhost") : "localhost";
        int port = CONFIG != null ? CONFIG.getInt("redis.port", 6379) : 6379;
        String password = CONFIG != null ? CONFIG.getString("redis.password", null) : null;
        if (password != null && (password.isEmpty() || password.isBlank())) password = null;
        String user = CONFIG != null ? CONFIG.getString("redis.user", null) : null;
        if (user != null && (user.isEmpty() || user.isBlank())) user = null;
        int database = CONFIG != null ? CONFIG.getInt("redis.database", 0) : 0;
        boolean ssl = CONFIG != null && CONFIG.getBoolean("redis.ssl", false);

        JedisPool pool;
        if (user != null && password != null) {
            pool = new JedisPool(poolConfig, host, port, timeout, user, password, database, ssl);
        } else if (password != null) {
            pool = new JedisPool(poolConfig, host, port, timeout, password, database, ssl);
        } else {
            pool = new JedisPool(poolConfig, host, port, timeout, null, database, ssl);
        }

        try (Jedis jedis = pool.getResource()) {
            jedis.ping();
        } catch (Exception ex) {
            try {
                pool.close();
            } catch (Exception ignored) {}
            throw new RuntimeException("Could not connect to Redis at " + host + ":" + port + " (" + poolName + ")", ex);
        }

        return pool;
    }
}
