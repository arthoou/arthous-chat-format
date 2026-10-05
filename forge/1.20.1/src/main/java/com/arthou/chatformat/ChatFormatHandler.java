package com.arthou.chatformat;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import net.minecraft.ChatFormatting;
import net.minecraft.network.Connection;
import net.minecraft.network.chat.Component;
import net.minecraft.network.chat.MutableComponent;
import net.minecraft.network.chat.contents.TranslatableContents;
import net.minecraft.network.protocol.game.ClientboundDisguisedChatPacket;
import net.minecraft.network.protocol.game.ClientboundSystemChatPacket;
import net.minecraft.server.level.ServerPlayer;
import net.minecraftforge.event.ServerChatEvent;
import net.minecraftforge.event.entity.player.PlayerEvent;

/**
 * Join, leave and chat messages in the DUSMP format:
 * <pre>
 * [+] name
 * [-] name
 * Name » message
 * </pre>
 *
 * Vanilla broadcasts its own yellow "joined/left the game" lines with no event
 * to cancel them, so every player gets a small outbound filter on their
 * connection that drops exactly those translatable messages. Chat is cancelled
 * and re-broadcast as a system message in the new format, which also lands in
 * the server console.
 */
public final class ChatFormatHandler {
    private static final String FILTER_NAME = "arthous_chat_format_filter";

    private ChatFormatHandler() {
    }

    public static void onPlayerLoggedIn(PlayerEvent.PlayerLoggedInEvent event) {
        if (!(event.getEntity() instanceof ServerPlayer player)) {
            return;
        }

        installFilter(player);
        player.server.getPlayerList().broadcastSystemMessage(joinMessage(player.getGameProfile().getName()), false);
    }

    public static void onPlayerLoggedOut(PlayerEvent.PlayerLoggedOutEvent event) {
        if (!(event.getEntity() instanceof ServerPlayer player)) {
            return;
        }

        player.server.getPlayerList().broadcastSystemMessage(quitMessage(player.getGameProfile().getName()), false);
    }

    public static void onServerChat(ServerChatEvent event) {
        event.setCanceled(true);
        ServerPlayer player = event.getPlayer();
        player.server.getPlayerList().broadcastSystemMessage(chatMessage(player, event.getRawText()), false);
    }

    private static void installFilter(ServerPlayer player) {
        Connection connection = player.connection.connection;
        if (connection.channel() == null || connection.channel().pipeline().get(FILTER_NAME) != null) {
            return;
        }

        connection.channel().pipeline().addBefore("packet_handler", FILTER_NAME, new ChannelDuplexHandler() {
            @Override
            public void write(ChannelHandlerContext context, Object message, ChannelPromise promise) throws Exception {
                if (message instanceof ClientboundSystemChatPacket packet && isVanillaJoinOrLeave(packet.content())) {
                    promise.setSuccess();
                    return;
                }

                if (message instanceof ClientboundDisguisedChatPacket packet && isVanillaJoinOrLeave(packet.message())) {
                    promise.setSuccess();
                    return;
                }

                super.write(context, message, promise);
            }
        });
    }

    private static boolean isVanillaJoinOrLeave(Component message) {
        if (!(message.getContents() instanceof TranslatableContents contents)) {
            return false;
        }

        String key = contents.getKey();
        return key.equals("multiplayer.player.left")
            || key.equals("multiplayer.player.joined")
            || key.equals("multiplayer.player.joined.renamed");
    }

    private static Component joinMessage(String username) {
        return Component.empty()
            .append(Component.literal("[").withStyle(ChatFormatting.GRAY))
            .append(Component.literal("+").withStyle(ChatFormatting.GREEN))
            .append(Component.literal("] ").withStyle(ChatFormatting.GRAY))
            .append(Component.literal(username).withStyle(ChatFormatting.WHITE));
    }

    private static Component quitMessage(String username) {
        return Component.empty()
            .append(Component.literal("[").withStyle(ChatFormatting.GRAY))
            .append(Component.literal("-").withStyle(ChatFormatting.RED))
            .append(Component.literal("] ").withStyle(ChatFormatting.GRAY))
            .append(Component.literal(username).withStyle(ChatFormatting.WHITE));
    }

    private static Component chatMessage(ServerPlayer player, String rawText) {
        MutableComponent message = player.getDisplayName().copy().withStyle(ChatFormatting.YELLOW);
        message.append(Component.literal(" » ").withStyle(ChatFormatting.DARK_GRAY));
        message.append(Component.literal(rawText).withStyle(ChatFormatting.WHITE));
        return message;
    }
}
