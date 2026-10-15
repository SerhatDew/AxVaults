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
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.bukkit.Bukkit;
import org.bukkit.Location;
import org.bukkit.Material;
import org.bukkit.inventory.ItemStack;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.UUID;

import static com.artillexstudios.axvaults.AxVaults.CONFIG;

public class MySQL implements Database {
    private HikariDataSource dataSource;

    private record LoadedVault(int id, Material icon, ItemStack[] items) {
    }

    private record LoadedBlock(Location location, Integer number) {
    }

    public MySQL() {
        if (Bukkit.getServer() != null) {
            Bukkit.getConsoleSender().sendMessage(StringUtils.formatToString("&#55ff00[AxVaults] Loaded MySQL database provider!"));
        }
    }

    @Override
    public String getType() {
        return "MySQL";
    }

    public Connection getConnection() throws SQLException {
        if (dataSource == null || dataSource.isClosed()) {
            throw new SQLException("HikariDataSource is closed or not initialized!");
        }
        return dataSource.getConnection();
    }

    @Override
    public void setup() {
        final HikariConfig hConfig = new HikariConfig();
        hConfig.setPoolName("axvaults-mysql-pool");

        int maxPool = CONFIG.getInt("database.pool.maximum-pool-size", 10);
        if (maxPool <= 0) maxPool = 10;
        hConfig.setMaximumPoolSize(maxPool);

        int minIdle = CONFIG.getInt("database.pool.minimum-idle", 5);
        if (minIdle < 0) minIdle = 5;
        hConfig.setMinimumIdle(Math.min(minIdle, maxPool));

        int maxLifetime = CONFIG.getInt("database.pool.maximum-lifetime", 1800000);
        if (maxLifetime <= 0) maxLifetime = 1800000;
        hConfig.setMaxLifetime(maxLifetime);

        int keepalive = CONFIG.getInt("database.pool.keepalive-time", 30000);
        if (keepalive <= 0) keepalive = 30000;
        hConfig.setKeepaliveTime(keepalive);

        int timeout = CONFIG.getInt("database.pool.connection-timeout", 30000);
        if (timeout <= 0) timeout = 30000;
        hConfig.setConnectionTimeout(timeout);

        String address = CONFIG.getString("database.address", "localhost");
        int port = CONFIG.getInt("database.port", 3306);
        String dbName = CONFIG.getString("database.database", "axvaults");
        String username = CONFIG.getString("database.username", "root");
        String password = CONFIG.getString("database.password", "");
        boolean ssl = CONFIG.getBoolean("database.ssl", false);

        hConfig.setDriverClassName("com.mysql.cj.jdbc.Driver");
        hConfig.setJdbcUrl("jdbc:mysql://" + address + ":" + port + "/" + dbName + "?useSSL=" + ssl + "&characterEncoding=utf8");
        hConfig.setUsername(username);
        hConfig.setPassword(password);

        hConfig.addDataSourceProperty("cachePrepStmts", "true");
        hConfig.addDataSourceProperty("prepStmtCacheSize", "250");
        hConfig.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
        hConfig.addDataSourceProperty("useServerPrepStmts", "true");

        this.dataSource = new HikariDataSource(hConfig);
        try (Connection conn = dataSource.getConnection(); Statement migrationLock = conn.createStatement()) {
            try (ResultSet result = migrationLock.executeQuery("SELECT GET_LOCK('axvaults-schema', 30)")) {
                if (!result.next() || result.getInt(1) != 1) throw new SQLException("Another AxVaults schema migration is still running");
            }
            try {
                createAndValidateSchema(conn);
            } finally {
                migrationLock.executeQuery("SELECT RELEASE_LOCK('axvaults-schema')").close();
            }
        } catch (SQLException | RuntimeException ex) {
            disable();
            throw new IllegalStateException("Could not initialize the AxVaults MySQL schema. "
                    + "Back up the database and resolve the reported schema/data conflict before starting AxVaults again.", ex);
        }
    }

    private void createAndValidateSchema(Connection conn) throws SQLException {
        execute(conn, """
                CREATE TABLE IF NOT EXISTS `axvaults_data` (
                  `id` INT NOT NULL,
                  `uuid` VARCHAR(36) NOT NULL,
                  `storage` LONGBLOB,
                  `icon` VARCHAR(128),
                  PRIMARY KEY (`uuid`, `id`)
                )
                """);
        ensureColumns(conn, "axvaults_data", "id", "uuid", "storage", "icon");
        ensurePrimaryKey(conn, "axvaults_data", List.of("uuid", "id"), this::deduplicateData);

        execute(conn, """
                CREATE TABLE IF NOT EXISTS `axvaults_blocks` (
                  `location` VARCHAR(255) NOT NULL,
                  `number` INT,
                  PRIMARY KEY (`location`)
                )
                """);
        ensureColumns(conn, "axvaults_blocks", "location", "number");
        ensurePrimaryKey(conn, "axvaults_blocks", List.of("location"), this::deduplicateBlocks);

        execute(conn, """
                CREATE TABLE IF NOT EXISTS `axvaults_messages` (
                  `id` INT NOT NULL AUTO_INCREMENT,
                  `event` TINYINT NOT NULL,
                  `vault_id` INT NOT NULL,
                  `uuid` VARCHAR(36) NOT NULL,
                  `server` VARCHAR(36) NOT NULL,
                  `date` BIGINT NOT NULL,
                  PRIMARY KEY (`id`),
                  INDEX `idx_date` (`date`)
                )
                """);
        addColumnIfMissing(conn, "axvaults_messages", "server", "`server` VARCHAR(36) NOT NULL DEFAULT ''");
        ensureColumns(conn, "axvaults_messages", "id", "event", "vault_id", "uuid", "server", "date");
        ensurePrimaryKey(conn, "axvaults_messages", List.of("id"), null);

        execute(conn, """
                CREATE TABLE IF NOT EXISTS `axvaults_locks` (
                  `uuid` VARCHAR(36) NOT NULL,
                  `server` VARCHAR(36) NOT NULL,
                  `date` BIGINT NOT NULL,
                  PRIMARY KEY (`uuid`)
                )
                """);
        ensureColumns(conn, "axvaults_locks", "uuid", "server", "date");
        ensurePrimaryKey(conn, "axvaults_locks", List.of("uuid"), null);
    }

    private void execute(Connection conn, String sql) throws SQLException {
        try (Statement statement = conn.createStatement()) {
            statement.executeUpdate(sql);
        }
    }

    private Set<String> showColumns(Connection conn, String table) throws SQLException {
        Set<String> columns = new HashSet<>();
        try (Statement statement = conn.createStatement(); ResultSet result = statement.executeQuery("SHOW COLUMNS FROM `" + table + "`")) {
            while (result.next()) columns.add(result.getString("Field").toLowerCase(Locale.ROOT));
        }
        return columns;
    }

    private void ensureColumns(Connection conn, String table, String... required) throws SQLException {
        Set<String> columns = showColumns(conn, table);
        for (String column : required) {
            if (!columns.contains(column.toLowerCase(Locale.ROOT))) {
                throw new SQLException("Table " + table + " is missing required column " + column);
            }
        }
    }

    private void addColumnIfMissing(Connection conn, String table, String column, String definition) throws SQLException {
        if (!showColumns(conn, table).contains(column.toLowerCase(Locale.ROOT))) {
            execute(conn, "ALTER TABLE `" + table + "` ADD COLUMN " + definition);
        }
    }

    private void ensurePrimaryKey(Connection conn, String table, List<String> expected, DuplicateCleaner cleaner) throws SQLException {
        List<String> actual = new ArrayList<>();
        try (Statement statement = conn.createStatement(); ResultSet result = statement.executeQuery("SHOW INDEX FROM `" + table + "` WHERE Key_name = 'PRIMARY'")) {
            while (result.next()) {
                int index = result.getInt("Seq_in_index") - 1;
                while (actual.size() <= index) actual.add(null);
                actual.set(index, result.getString("Column_name").toLowerCase(Locale.ROOT));
            }
        }
        if (actual.equals(expected)) return;
        if (!actual.isEmpty()) {
            throw new SQLException("Table " + table + " has an unexpected primary key " + actual + "; expected " + expected);
        }
        if (cleaner == null) {
            throw new SQLException("Table " + table + " has no required primary key");
        }
        cleaner.clean(conn);
        execute(conn, "ALTER TABLE `" + table + "` ADD PRIMARY KEY (`" + String.join("`, `", expected) + "`)");
    }

    @FunctionalInterface
    private interface DuplicateCleaner {
        void clean(Connection connection) throws SQLException;
    }

    private void deduplicateData(Connection conn) throws SQLException {
        // Legacy rows are ordered by stable stored values. Equal rows are interchangeable; any
        // differing storage/icon values are ambiguous and therefore abort startup.
        String groupsSql = "SELECT `uuid`, `id`, COUNT(*) AS `count` FROM `axvaults_data` GROUP BY `uuid`, `id` HAVING COUNT(*) > 1";
        try (Statement statement = conn.createStatement(); ResultSet groups = statement.executeQuery(groupsSql)) {
            while (groups.next()) {
                String uuid = groups.getString("uuid");
                int id = groups.getInt("id");
                int count = groups.getInt("count");
                byte[] storage = null;
                String icon = null;
                boolean initialized = false;
                try (PreparedStatement rows = conn.prepareStatement("SELECT `storage`, `icon` FROM `axvaults_data` WHERE `uuid` = ? AND `id` = ? ORDER BY COALESCE(`icon`, ''), OCTET_LENGTH(`storage`), `storage`")) {
                    rows.setString(1, uuid);
                    rows.setInt(2, id);
                    try (ResultSet result = rows.executeQuery()) {
                        while (result.next()) {
                            byte[] currentStorage = result.getBytes("storage");
                            String currentIcon = result.getString("icon");
                            if (!initialized) {
                                storage = currentStorage;
                                icon = currentIcon;
                                initialized = true;
                            } else if (!Arrays.equals(storage, currentStorage) || !java.util.Objects.equals(icon, currentIcon)) {
                                throw new SQLException("Ambiguous duplicate inventory for uuid=" + uuid + ", id=" + id
                                        + "; rows contain different storage/icon values. Back up and clean this group manually.");
                            }
                        }
                    }
                }
                try (PreparedStatement delete = conn.prepareStatement("DELETE FROM `axvaults_data` WHERE `uuid` = ? AND `id` = ? ORDER BY COALESCE(`icon`, ''), OCTET_LENGTH(`storage`), `storage` LIMIT ?")) {
                    delete.setString(1, uuid);
                    delete.setInt(2, id);
                    delete.setInt(3, count - 1);
                    if (delete.executeUpdate() != count - 1) {
                        throw new SQLException("Could not deterministically deduplicate axvaults_data for uuid=" + uuid + ", id=" + id);
                    }
                }
            }
        }
    }

    private void deduplicateBlocks(Connection conn) throws SQLException {
        // A placed block is deduplicated only when every legacy row has the same number.
        String groupsSql = "SELECT `location`, COUNT(*) AS `count` FROM `axvaults_blocks` GROUP BY `location` HAVING COUNT(*) > 1";
        try (Statement statement = conn.createStatement(); ResultSet groups = statement.executeQuery(groupsSql)) {
            while (groups.next()) {
                String location = groups.getString("location");
                int count = groups.getInt("count");
                Integer number = null;
                boolean initialized = false;
                try (PreparedStatement rows = conn.prepareStatement("SELECT `number` FROM `axvaults_blocks` WHERE `location` = ? ORDER BY `number`")) {
                    rows.setString(1, location);
                    try (ResultSet result = rows.executeQuery()) {
                        while (result.next()) {
                            Integer current = (Integer) result.getObject("number");
                            if (!initialized) {
                                number = current;
                                initialized = true;
                            } else if (!java.util.Objects.equals(number, current)) {
                                throw new SQLException("Ambiguous duplicate placed block for location=" + location
                                        + "; rows contain different vault numbers. Back up and clean this location manually.");
                            }
                        }
                    }
                }
                try (PreparedStatement delete = conn.prepareStatement("DELETE FROM `axvaults_blocks` WHERE `location` = ? ORDER BY `number` LIMIT ?")) {
                    delete.setString(1, location);
                    delete.setInt(2, count - 1);
                    if (delete.executeUpdate() != count - 1) {
                        throw new SQLException("Could not deterministically deduplicate axvaults_blocks for location=" + location);
                    }
                }
            }
        }
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

        String sql = """
                INSERT INTO `axvaults_data` (`id`, `uuid`, `storage`, `icon`)
                VALUES (?, ?, ?, ?)
                ON DUPLICATE KEY UPDATE `storage` = VALUES(`storage`), `icon` = VALUES(`icon`)
                """;
        try (Connection conn = dataSource.getConnection(); PreparedStatement statement = conn.prepareStatement(sql)) {
            statement.setInt(1, vault.getId());
            statement.setString(2, vault.getUUID().toString());
            statement.setBytes(3, bytes);
            statement.setString(4, icon == null ? null : icon.name());
            statement.executeUpdate();
        } catch (SQLException ex) {
            throw databaseFailure("Failed to save vault #" + vault.getId() + " of " + vault.getUUID(), ex);
        }
        Messaging.sendChange(ChangeType.UPDATE, vault.getUUID(), vault.getId());
    }

    @Override
    public void loadVaults(@NotNull VaultPlayer vaultPlayer) {
        List<LoadedVault> loaded = new ArrayList<>();
        String sql = "SELECT `id`, `storage`, `icon` FROM `axvaults_data` WHERE `uuid` = ?";
        try (Connection conn = dataSource.getConnection(); PreparedStatement statement = conn.prepareStatement(sql)) {
            statement.setString(1, vaultPlayer.getUUID().toString());
            try (ResultSet result = statement.executeQuery()) {
                while (result.next()) {
                    int id = result.getInt("id");
                    try {
                        byte[] storage = result.getBytes("storage");
                        if (storage == null) throw new IllegalStateException("storage column is NULL");
                        ItemStack[] items = Serializers.ITEM_ARRAY.deserialize(storage);
                        String iconString = result.getString("icon");
                        Material icon = iconString == null ? null : Material.valueOf(iconString);
                        loaded.add(new LoadedVault(id, icon, items));
                    } catch (Exception ex) {
                        throw new IllegalStateException("Failed to decode vault #" + id + " of " + vaultPlayer.getUUID(), ex);
                    }
                }
            }
        } catch (SQLException ex) {
            throw databaseFailure("Failed to load vaults for " + vaultPlayer.getUUID(), ex);
        }
        for (LoadedVault vault : loaded) new Vault(vaultPlayer, vault.id(), vault.icon(), vault.items());
    }

    @Override
    public boolean isVault(@NotNull Location location) {
        String sql = "SELECT 1 FROM `axvaults_blocks` WHERE `location` = ?";
        try (Connection conn = dataSource.getConnection(); PreparedStatement statement = conn.prepareStatement(sql)) {
            statement.setString(1, Serializers.LOCATION.serialize(location));
            try (ResultSet result = statement.executeQuery()) {
                return result.next();
            }
        } catch (SQLException ex) {
            throw databaseFailure("Failed to check whether a location is a vault", ex);
        }
    }

    @Override
    public void setVault(@NotNull Location location, @Nullable Integer num) {
        String sql = """
                INSERT INTO `axvaults_blocks` (`location`, `number`) VALUES (?, ?)
                ON DUPLICATE KEY UPDATE `number` = VALUES(`number`)
                """;
        try (Connection conn = dataSource.getConnection(); PreparedStatement statement = conn.prepareStatement(sql)) {
            statement.setString(1, Serializers.LOCATION.serialize(location));
            if (num == null) statement.setNull(2, java.sql.Types.INTEGER);
            else statement.setInt(2, num);
            statement.executeUpdate();
        } catch (SQLException ex) {
            throw databaseFailure("Failed to set vault block", ex);
        }
        PlacedVaults.addVault(location, num);
    }

    @Override
    public void removeVault(@NotNull Location location) {
        try (Connection conn = dataSource.getConnection(); PreparedStatement statement = conn.prepareStatement("DELETE FROM `axvaults_blocks` WHERE `location` = ?")) {
            statement.setString(1, Serializers.LOCATION.serialize(location));
            statement.executeUpdate();
        } catch (SQLException ex) {
            throw databaseFailure("Failed to remove vault block", ex);
        }
    }

    @Override
    public void deleteVault(@NotNull UUID uuid, int num) {
        try (Connection conn = dataSource.getConnection(); PreparedStatement statement = conn.prepareStatement("DELETE FROM `axvaults_data` WHERE `uuid` = ? AND `id` = ?")) {
            statement.setString(1, uuid.toString());
            statement.setInt(2, num);
            statement.executeUpdate();
        } catch (SQLException ex) {
            throw databaseFailure("Failed to delete vault #" + num + " of " + uuid, ex);
        }
        Messaging.sendChange(ChangeType.DELETE, uuid, num);
    }

    @Override
    public void load() {
        List<LoadedBlock> loaded = new ArrayList<>();
        try (Connection conn = dataSource.getConnection(); PreparedStatement statement = conn.prepareStatement("SELECT `location`, `number` FROM `axvaults_blocks`")) {
            try (ResultSet result = statement.executeQuery()) {
                while (result.next()) {
                    try {
                        String number = result.getString("number");
                        loaded.add(new LoadedBlock(Serializers.LOCATION.deserialize(result.getString("location")), number == null ? null : Integer.valueOf(number)));
                    } catch (Exception ex) {
                        throw new IllegalStateException("Failed to decode a placed vault block", ex);
                    }
                }
            }
        } catch (SQLException ex) {
            throw databaseFailure("Failed to load placed vaults from MySQL", ex);
        }
        for (LoadedBlock block : loaded) PlacedVaults.addVault(block.location(), block.number());
    }

    @Override
    public void updateVault(@NotNull VaultPlayer vaultPlayer, int id) {
        String sql = "SELECT `storage`, `icon` FROM `axvaults_data` WHERE `uuid` = ? AND `id` = ?";
        try (Connection conn = dataSource.getConnection(); PreparedStatement statement = conn.prepareStatement(sql)) {
            statement.setString(1, vaultPlayer.getUUID().toString());
            statement.setInt(2, id);
            try (ResultSet result = statement.executeQuery()) {
                if (!result.next()) {
                    ThreadUtils.runSync(() -> vaultPlayer.applyRemoteChange(id, null, null));
                    return;
                }
                try {
                    byte[] storage = result.getBytes("storage");
                    if (storage == null) throw new IllegalStateException("storage column is NULL");
                    ItemStack[] items = Serializers.ITEM_ARRAY.deserialize(storage);
                    String iconString = result.getString("icon");
                    Material icon = iconString == null ? null : Material.valueOf(iconString);
                    ThreadUtils.runSync(() -> vaultPlayer.applyRemoteChange(id, items, icon));
                } catch (Exception ex) {
                    throw new IllegalStateException("Failed to decode updated vault #" + id + " of " + vaultPlayer.getUUID(), ex);
                }
            }
        } catch (SQLException ex) {
            throw databaseFailure("Failed to fetch vault update #" + id + " for " + vaultPlayer.getUUID(), ex);
        }
    }

    private IllegalStateException databaseFailure(String message, SQLException cause) {
        LogUtils.error(message, cause);
        return new IllegalStateException(message, cause);
    }

    @Override
    public void disable() {
        if (dataSource != null && !dataSource.isClosed()) {
            try {
                dataSource.close();
            } catch (Exception ex) {
                LogUtils.error("Error closing MySQL HikariDataSource", ex);
            }
        }
    }
}
