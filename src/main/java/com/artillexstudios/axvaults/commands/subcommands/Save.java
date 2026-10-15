package com.artillexstudios.axvaults.commands.subcommands;

import com.artillexstudios.axapi.utils.logging.LogUtils;
import com.artillexstudios.axvaults.schedulers.AutoSaveScheduler;
import com.artillexstudios.axvaults.utils.ThreadUtils;
import com.artillexstudios.axvaults.vaults.Vault;
import com.artillexstudios.axvaults.vaults.VaultManager;
import org.bukkit.command.CommandSender;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static com.artillexstudios.axvaults.AxVaults.MESSAGEUTILS;

public enum Save {
    INSTANCE;

    public void execute(CommandSender sender, boolean autosave) {
        long time = System.currentTimeMillis();
        CompletableFuture<Void> save;
        if (autosave) {
            save = AutoSaveScheduler.save();
        } else {
            List<CompletableFuture<Void>> futures = new ArrayList<>();
            for (Vault vault : VaultManager.getVaults()) futures.add(VaultManager.saveVault(vault));
            save = CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new));
        }
        save.whenComplete((ignored, error) -> ThreadUtils.runSync(() -> {
            if (error != null) LogUtils.error("Manual vault save failed", error);
            MESSAGEUTILS.sendLang(sender, error == null ? "save.manual" : "vault.unavailable",
                    Map.of("%time%", Long.toString(System.currentTimeMillis() - time)));
        }));
    }
}
