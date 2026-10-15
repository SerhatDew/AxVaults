package com.artillexstudios.axvaults.commands.subcommands;

import com.artillexstudios.axapi.utils.logging.LogUtils;
import com.artillexstudios.axvaults.AxVaults;
import com.artillexstudios.axvaults.utils.ThreadUtils;
import com.artillexstudios.axvaults.vaults.Vault;
import com.artillexstudios.axvaults.vaults.VaultManager;
import org.bukkit.OfflinePlayer;
import org.bukkit.command.CommandSender;
import org.bukkit.entity.HumanEntity;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static com.artillexstudios.axvaults.AxVaults.MESSAGEUTILS;

public enum Delete {
    INSTANCE;

    public void execute(CommandSender sender, OfflinePlayer player, int number) {
        Map<String, String> replacements = new HashMap<>();
        replacements.put("%player%", player.getName());
        replacements.put("%num%", "" + number);

        VaultManager.getPlayer(player).thenAccept(vaultPlayer -> {
            final Vault vault = vaultPlayer.getVault(number);
            if (vault == null) {
                MESSAGEUTILS.sendLang(sender, "view.not-found", replacements);
                return;
            }
            for (HumanEntity viewer : new ArrayList<>(vault.getStorage().getViewers())) {
                viewer.closeInventory();
            }
            VaultManager.beginPersistenceOperation(player.getUniqueId());
            VaultManager.removeVault(vault);
            AxVaults.getThreadedQueue().submit(() -> {
                Throwable failure = null;
                try {
                    AxVaults.getDatabase().deleteVault(player.getUniqueId(), number);
                } catch (Exception ex) {
                    failure = ex;
                }
                Throwable error = failure;
                ThreadUtils.runSync(() -> {
                    if (error != null) {
                        vaultPlayer.addVault(vault);
                        vault.hasChanged().set(true);
                        LogUtils.error("Failed to delete vault #{} of {}", number, player.getUniqueId(), error);
                    }
                    VaultManager.endPersistenceOperation(player.getUniqueId());
                    if (!AxVaults.isStopping()) {
                        MESSAGEUTILS.sendLang(sender, error == null ? "delete" : "vault.unavailable", replacements);
                    }
                });
            });
        }).exceptionally(error -> {
            MESSAGEUTILS.sendLang(sender, "vault.unavailable");
            return null;
        });
    }
}
