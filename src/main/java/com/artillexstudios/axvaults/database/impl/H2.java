package com.artillexstudios.axvaults.database.impl;

import com.artillexstudios.axapi.serializers.Serializers;
import com.artillexstudios.axvaults.AxVaults;
import com.artillexstudios.axvaults.database.Database;
import com.artillexstudios.axvaults.placed.PlacedVaults;
import com.artillexstudios.axvaults.utils.ThreadUtils;
import com.artillexstudios.axvaults.vaults.Vault;
import com.artillexstudios.axvaults.vaults.VaultPlayer;
import org.bukkit.Location;
import org.bukkit.Material;
import org.bukkit.inventory.ItemStack;
import org.h2.jdbc.JdbcConnection;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

public class H2 implements Database {
    private JdbcConnection conn;

    private record LoadedVault(int id, Material icon, ItemStack[] items) {
    }

    private record LoadedBlock(Location location, Integer number) {
    }

    @Override
    public String getType() {
        return "H2";
    }

    @Override
    public void setup() {
        try {
            conn = new JdbcConnection("jdbc:h2:./" + AxVaults.getInstance().getDataFolder() + "/data;mode=MySQL", new Properties(), null, null, false);
            conn.setAutoCommit(true);
        } catch (Exception ex) {
            throw new IllegalStateException("Could not connect to H2", ex);
        }

        String createDataTable = """
                CREATE TABLE IF NOT EXISTS `axvaults_data` (
                  `id` INT(128) NOT NULL,
                  `uuid` VARCHAR(36) NOT NULL,
                  `storage` LONGBLOB,
                  `icon` VARCHAR(128)
                )
                """;
        try (PreparedStatement statement = conn.prepareStatement(createDataTable)) {
            statement.executeUpdate();
        } catch (SQLException ex) {
            throw new IllegalStateException("Failed to create axvaults_data table", ex);
        }

        String createBlocksTable = """
                CREATE TABLE IF NOT EXISTS `axvaults_blocks` (
                  `location` VARCHAR(255) NOT NULL,
                  `number` INT,
                  PRIMARY KEY (`location`)
                )
                """;
        try (PreparedStatement statement = conn.prepareStatement(createBlocksTable)) {
            statement.executeUpdate();
        } catch (SQLException ex) {
            throw new IllegalStateException("Failed to create axvaults_blocks table", ex);
        }
    }

    @Override
    public void saveVault(@NotNull Vault vault, Object result, @Nullable Material icon) {
        if (result instanceof Boolean bool && bool) {
            try (PreparedStatement statement = conn.prepareStatement("DELETE FROM axvaults_data WHERE uuid = ? AND id = ?")) {
                statement.setString(1, vault.getUUID().toString());
                statement.setInt(2, vault.getId());
                statement.executeUpdate();
            } catch (SQLException ex) {
                throw databaseFailure("Failed to delete empty vault #" + vault.getId() + " of " + vault.getUUID(), ex);
            }
            return;
        }
        if (!(result instanceof byte[] bytes)) {
            throw new IllegalArgumentException("Vault save result must be serialized bytes or the empty-vault delete marker");
        }

        String select = "SELECT * FROM axvaults_data WHERE uuid = ? AND id = ?";
        try (PreparedStatement statement = conn.prepareStatement(select)) {
            statement.setString(1, vault.getUUID().toString());
            statement.setInt(2, vault.getId());
            try (ResultSet resultSet = statement.executeQuery()) {
                if (resultSet.next()) {
                    try (PreparedStatement update = conn.prepareStatement("UPDATE axvaults_data SET storage = ?, icon = ? WHERE uuid = ? AND id = ?")) {
                        update.setBytes(1, bytes);
                        update.setString(2, icon == null ? null : icon.name());
                        update.setString(3, vault.getUUID().toString());
                        update.setInt(4, vault.getId());
                        update.executeUpdate();
                    }
                } else {
                    try (PreparedStatement insert = conn.prepareStatement("INSERT INTO axvaults_data(id, uuid, storage, icon) VALUES (?, ?, ?, ?)")) {
                        insert.setInt(1, vault.getId());
                        insert.setString(2, vault.getUUID().toString());
                        insert.setBytes(3, bytes);
                        insert.setString(4, icon == null ? null : icon.name());
                        insert.executeUpdate();
                    }
                }
            }
        } catch (SQLException ex) {
            throw databaseFailure("Failed to save vault #" + vault.getId() + " of " + vault.getUUID(), ex);
        }
    }

    @Override
    public void loadVaults(@NotNull VaultPlayer vaultPlayer) {
        List<LoadedVault> loaded = new ArrayList<>();
        try (PreparedStatement statement = conn.prepareStatement("SELECT * FROM axvaults_data WHERE uuid = ?")) {
            statement.setString(1, vaultPlayer.getUUID().toString());
            try (ResultSet result = statement.executeQuery()) {
                while (result.next()) {
                    int id = result.getInt(1);
                    try {
                        byte[] storage = result.getBytes(3);
                        if (storage == null) throw new IllegalStateException("storage column is NULL");
                        ItemStack[] items = Serializers.ITEM_ARRAY.deserialize(storage);
                        String iconString = result.getString(4);
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
        for (LoadedVault vault : loaded) ThreadUtils.runSync(() -> new Vault(vaultPlayer, vault.id(), vault.icon(), vault.items()));
    }

    @Override
    public boolean isVault(@NotNull Location location) {
        try (PreparedStatement statement = conn.prepareStatement("SELECT * FROM axvaults_blocks WHERE location = ?")) {
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
        try (PreparedStatement statement = conn.prepareStatement("INSERT INTO axvaults_blocks(`location`, `number`) VALUES (?, ?)")) {
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
        try (PreparedStatement statement = conn.prepareStatement("DELETE FROM axvaults_blocks WHERE location = ?")) {
            statement.setString(1, Serializers.LOCATION.serialize(location));
            statement.executeUpdate();
        } catch (SQLException ex) {
            throw databaseFailure("Failed to remove vault block", ex);
        }
    }

    @Override
    public void deleteVault(@NotNull UUID uuid, int num) {
        try (PreparedStatement statement = conn.prepareStatement("DELETE FROM axvaults_data WHERE uuid = ? AND id = ?")) {
            statement.setString(1, uuid.toString());
            statement.setInt(2, num);
            statement.executeUpdate();
        } catch (SQLException ex) {
            throw databaseFailure("Failed to delete vault #" + num + " of " + uuid, ex);
        }
    }

    @Override
    public void load() {
        List<LoadedBlock> loaded = new ArrayList<>();
        try (PreparedStatement statement = conn.prepareStatement("SELECT * FROM axvaults_blocks")) {
            try (ResultSet result = statement.executeQuery()) {
                while (result.next()) {
                    try {
                        String number = result.getString(2);
                        loaded.add(new LoadedBlock(Serializers.LOCATION.deserialize(result.getString(1)), number == null ? null : Integer.valueOf(number)));
                    } catch (Exception ex) {
                        throw new IllegalStateException("Failed to decode a placed vault block", ex);
                    }
                }
            }
        } catch (SQLException ex) {
            throw databaseFailure("Failed to load placed vaults from H2", ex);
        }
        for (LoadedBlock block : loaded) PlacedVaults.addVault(block.location(), block.number());
    }

    private IllegalStateException databaseFailure(String message, SQLException cause) {
        return new IllegalStateException(message, cause);
    }

    @Override
    public void disable() {
        if (conn == null) return;
        try {
            conn.close();
        } catch (Exception ex) {
            throw new IllegalStateException("Failed to close H2 database", ex);
        }
    }
}
