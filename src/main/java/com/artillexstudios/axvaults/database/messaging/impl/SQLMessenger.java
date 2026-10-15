package com.artillexstudios.axvaults.database.messaging.impl;

import com.artillexstudios.axapi.executor.ExceptionReportingScheduledThreadPool;
import com.artillexstudios.axapi.utils.logging.LogUtils;
import com.artillexstudios.axvaults.AxVaults;
import com.artillexstudios.axvaults.database.impl.MySQL;
import com.artillexstudios.axvaults.database.messaging.ChangeType;
import com.artillexstudios.axvaults.database.messaging.Messaging;
import com.artillexstudios.axvaults.database.messaging.Messenger;
import org.jetbrains.annotations.NotNull;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class SQLMessenger implements Messenger {
    private final MySQL mySQL;
    private final String serverId = UUID.randomUUID().toString();
    private final ConcurrentHashMap<Integer, Long> acknowledged = new ConcurrentHashMap<>();
    private ScheduledExecutorService executor = null;

    public SQLMessenger(@NotNull MySQL mySQL) {
        this.mySQL = mySQL;
    }

    @Override
    public void start() {
        if (executor != null) executor.shutdown();
        executor = new ExceptionReportingScheduledThreadPool(1);
        executor.scheduleAtFixedRate(this::checkForChanges, 3, 3, TimeUnit.SECONDS);
        executor.scheduleAtFixedRate(this::removeOldChanges, 10, 10, TimeUnit.SECONDS);
    }

    @Override
    public void sendChange(@NotNull ChangeType type, @NotNull UUID uuid, int vaultId) {
        AxVaults.getThreadedQueue().submit(() -> {
            final String sql = "INSERT INTO `axvaults_messages` (`event`, `vault_id`, `uuid`, `server`, `date`) VALUES (?, ?, ?, ?, ?);";
            try (Connection conn = mySQL.getConnection(); PreparedStatement stmt = conn.prepareStatement(sql)) {
                stmt.setShort(1, (short) type.ordinal());
                stmt.setInt(2, vaultId);
                stmt.setString(3, uuid.toString());
                stmt.setString(4, serverId);
                stmt.setLong(5, System.currentTimeMillis());
                stmt.executeUpdate();
            } catch (SQLException ex) {
                LogUtils.error("Failed to send SQL message for vault #{}", vaultId, ex);
            }
        });
    }

    @Override
    public boolean tryLock(@NotNull UUID uuid) {
        final String insertSql = "INSERT IGNORE INTO `axvaults_locks` (`uuid`, `server`, `date`) VALUES (?, ?, ?);";
        try (Connection conn = mySQL.getConnection(); PreparedStatement stmt = conn.prepareStatement(insertSql)) {
            stmt.setString(1, uuid.toString());
            stmt.setString(2, serverId);
            stmt.setLong(3, System.currentTimeMillis());
            if (stmt.executeUpdate() > 0) return true;
        } catch (SQLException ex) {
            throw new IllegalStateException("Failed to insert SQL lock for " + uuid, ex);
        }

        final String checkSql = "SELECT `server` FROM `axvaults_locks` WHERE `uuid` = ?;";
        try (Connection conn = mySQL.getConnection(); PreparedStatement stmt = conn.prepareStatement(checkSql)) {
            stmt.setString(1, uuid.toString());
            try (ResultSet rs = stmt.executeQuery()) {
                return rs.next() && serverId.equals(rs.getString(1));
            }
        } catch (SQLException ex) {
            throw new IllegalStateException("Failed to check SQL lock for " + uuid, ex);
        }
    }

    @Override
    public void unlock(@NotNull UUID uuid) {
        final String sql = "DELETE FROM `axvaults_locks` WHERE `uuid` = ? AND `server` = ?;";
        try (Connection conn = mySQL.getConnection(); PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, uuid.toString());
            stmt.setString(2, serverId);
            stmt.executeUpdate();
        } catch (SQLException ex) {
            LogUtils.error("Failed to unlock {} in SQL", uuid, ex);
        }
    }

    private void checkForChanges() {
        // Scan retained messages: auto-increment IDs can commit out of order on different connections.
        final String sql = "SELECT `id`, `event`, `vault_id`, `uuid` FROM `axvaults_messages` WHERE `server` != ? ORDER BY `id`;";
        try (Connection conn = mySQL.getConnection(); PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, serverId);
            try (ResultSet rs = stmt.executeQuery()) {
                long now = System.currentTimeMillis();
                while (rs.next()) {
                    int id = rs.getInt(1);
                    if (acknowledged.putIfAbsent(id, now) != null) continue;

                    int eventOrdinal = rs.getInt(2);
                    if (eventOrdinal < 0 || eventOrdinal >= ChangeType.ENTRIES.length) continue;
                    ChangeType changeType = ChangeType.ENTRIES[eventOrdinal];
                    int vaultId = rs.getInt(3);
                    try {
                        UUID uuid = UUID.fromString(rs.getString(4));
                        Messaging.handleChange(changeType, uuid, vaultId);
                    } catch (IllegalArgumentException ex) {
                        LogUtils.warn("Ignoring malformed SQL sync message #{}", id);
                    }
                }
            }
        } catch (SQLException ex) {
            LogUtils.error("Failed to check for SQL messaging changes!", ex);
        }
    }

    private void removeOldChanges() {
        long now = System.currentTimeMillis();
        acknowledged.entrySet().removeIf(entry -> entry.getValue() < now - 60_000L);

        final String sql = "DELETE FROM `axvaults_messages` WHERE `date` < ? - 15000;";
        try (Connection conn = mySQL.getConnection(); PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setLong(1, now);
            stmt.executeUpdate();
        } catch (SQLException ex) {
            LogUtils.error("Failed to remove old SQL messages!", ex);
        }
    }

    @Override
    public void stop() {
        if (executor == null) return;
        executor.shutdown();
        boolean interrupted = false;
        while (!executor.isTerminated()) {
            try {
                executor.awaitTermination(1, TimeUnit.SECONDS);
            } catch (InterruptedException ex) {
                interrupted = true;
            }
        }
        executor = null;
        if (interrupted) Thread.currentThread().interrupt();
    }
}
