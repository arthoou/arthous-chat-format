package com.arthou.chatformat;

import com.arthou.chatformat.client.ChatFormatModListDescription;
import com.arthou.chatformat.mc.ApiServer;
import net.minecraft.server.level.ServerPlayer;
import net.minecraftforge.api.distmarker.Dist;
import net.minecraftforge.common.MinecraftForge;
import net.minecraftforge.event.entity.player.PlayerEvent;
import net.minecraftforge.event.server.ServerStartingEvent;
import net.minecraftforge.event.server.ServerStoppingEvent;
import net.minecraftforge.fml.common.Mod;
import net.minecraftforge.fml.loading.FMLEnvironment;

/**
 * The DUSMP chat format as a standalone mod, and nothing else: "[+] name" and
 * "[-] name" instead of the vanilla join/leave lines, and "Name » message" for
 * chat. Server-side; clients do not need it installed.
 */
@Mod(ChatFormatMod.MOD_ID)
public final class ChatFormatMod {
    public static final String MOD_ID = "arthous_chat_format";
    private ApiServer api;

    public ChatFormatMod() {
        MinecraftForge.EVENT_BUS.addListener(ChatFormatHandler::onPlayerLoggedIn);
        MinecraftForge.EVENT_BUS.addListener(ChatFormatHandler::onPlayerLoggedOut);
        MinecraftForge.EVENT_BUS.addListener(ChatFormatHandler::onServerChat);
        if (FMLEnvironment.dist == Dist.DEDICATED_SERVER) {
            MinecraftForge.EVENT_BUS.addListener(this::onServerStarting);
            MinecraftForge.EVENT_BUS.addListener(this::onServerStopping);
            MinecraftForge.EVENT_BUS.addListener(this::onPlayerLoggedIn);
            MinecraftForge.EVENT_BUS.addListener(this::onPlayerLoggedOut);
        }

        if (FMLEnvironment.dist == Dist.CLIENT) {
            ChatFormatModListDescription.register();
        }
    }

    private void onServerStarting(ServerStartingEvent event) {
        if (!event.getServer().isDedicatedServer()) {
            return;
        }
        try {
            api = new ApiServer(event.getServer());
            api.start();
        } catch (Exception ignored) {
        }
    }

    private void onServerStopping(ServerStoppingEvent event) {
        if (api != null) {
            api.stop();
            api = null;
        }
    }

    private void onPlayerLoggedIn(PlayerEvent.PlayerLoggedInEvent event) {
        if (event.getEntity() instanceof ServerPlayer player) {
            ApiServer.installLanguageTracker(player);
        }
    }

    private void onPlayerLoggedOut(PlayerEvent.PlayerLoggedOutEvent event) {
        if (event.getEntity() instanceof ServerPlayer player) {
            ApiServer.forgetLanguage(player);
        }
    }
}
