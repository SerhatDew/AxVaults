package com.artillexstudios.axvaults.listeners;

import com.artillexstudios.axapi.utils.PaperUtils;
import com.artillexstudios.axvaults.utils.BlacklistUtils;
import com.artillexstudios.axvaults.vaults.Vault;
import org.bukkit.event.EventHandler;
import org.bukkit.event.Listener;
import org.bukkit.event.inventory.ClickType;
import org.bukkit.event.inventory.InventoryAction;
import org.bukkit.event.inventory.InventoryClickEvent;
import org.bukkit.event.inventory.InventoryDragEvent;
import org.bukkit.inventory.Inventory;
import org.bukkit.inventory.ItemStack;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static com.artillexstudios.axvaults.AxVaults.MESSAGEUTILS;

public class BlacklistListener implements Listener {

    @EventHandler
    public void onClick(@NotNull InventoryClickEvent event) {
        if (!(PaperUtils.getHolder(event.getInventory(), false) instanceof Vault)) {
            return;
        }

        if (BlacklistUtils.isBlacklisted(this.getItem(event))) {
            event.setCancelled(true);
            MESSAGEUTILS.sendLang(event.getWhoClicked(), "banned-item");
        }
    }

    @EventHandler
    public void onDrag(@NotNull InventoryDragEvent event) {
        if (!(PaperUtils.getHolder(event.getInventory(), false) instanceof Vault)) return;
        if (!BlacklistUtils.isBlacklisted(event.getOldCursor())) return;
        int vaultSize = event.getView().getTopInventory().getSize();
        for (int slot : event.getRawSlots()) {
            if (slot >= vaultSize) continue;
            event.setCancelled(true);
            MESSAGEUTILS.sendLang(event.getWhoClicked(), "banned-item");
            return;
        }
    }

    // Only inspect transfers into the vault; existing blacklisted items can still be removed.
    @Nullable
    private ItemStack getItem(InventoryClickEvent event) {
        Inventory clicked = event.getClickedInventory();
        if (clicked == null) {
            return null;
        }

        boolean clickedVault = clicked.equals(event.getView().getTopInventory());
        if (clickedVault) {
            return switch (event.getAction()) {
                case HOTBAR_SWAP, HOTBAR_MOVE_AND_READD -> event.getClick() == ClickType.SWAP_OFFHAND
                        ? event.getWhoClicked().getInventory().getItemInOffHand()
                        : event.getView().getBottomInventory().getItem(event.getHotbarButton());
                case PLACE_ALL, PLACE_ONE, PLACE_SOME, SWAP_WITH_CURSOR -> event.getCursor();
                default -> null;
            };
        }

        // clicked the player inventory: only shift clicking moves the item into the vault
        if (event.getAction() == InventoryAction.MOVE_TO_OTHER_INVENTORY) {
            return event.getCurrentItem();
        }
        return null;
    }
}
