package com.arthou.chatformat.client;

import com.arthou.chatformat.ChatFormatMod;
import com.mojang.logging.LogUtils;
import net.minecraft.ChatFormatting;
import net.minecraft.client.Minecraft;
import net.minecraft.client.gui.Font;
import net.minecraft.network.chat.ClickEvent;
import net.minecraft.network.chat.Component;
import net.minecraft.network.chat.HoverEvent;
import net.minecraft.network.chat.MutableComponent;
import net.minecraft.network.chat.Style;
import net.minecraft.network.chat.TextColor;
import net.minecraft.util.FormattedCharSequence;
import net.minecraftforge.client.event.ScreenEvent;
import net.minecraftforge.client.gui.ModListScreen;
import net.minecraftforge.client.gui.widget.ModListWidget;
import net.minecraftforge.client.gui.widget.ScrollPanel;
import net.minecraftforge.common.MinecraftForge;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

/**
 * Makes the Chat Format entry in Forge's Mods screen show the same masked links the
 * launcher does. The only client-side code in this mod.
 *
 * The description in mods.toml is HTML, because that is what PrismLauncher
 * renders (with "arthou", "Discord" and "Documentation" as real links). Forge's own info panel only links bare URLs and would print the tags
 * as-is, so while this is the selected mod its description lines are swapped for
 * styled text with click events. Clicking them goes through the screen's normal
 * link handling.
 *
 * The panel keeps its lines in private fields of Forge's own classes (whose
 * names are not remapped), so they are reached by reflection. If that ever
 * fails, the hook switches itself off and the raw description stays.
 */
public final class ChatFormatModListDescription {
    private static final String AUTHOR_URL = "https://discord.com/users/990003604725321808";
    private static final String DISCORD_URL = "https://discord.gg/sSpj3pQw5Y";
    private static final String DOCUMENTATION_URL = "https://chat.arthou.xyz";

    /** The raw HTML description opens with the author link. */
    private static final String DESCRIPTION_START = "By <a href=";
    private static final int LINK_COLOR = 0x5BC8FF;
    /** Same inset Forge's InfoPanel wraps its text to. */
    private static final int PANEL_TEXT_INSET = 12;

    private static Field selectedField;
    private static Field modInfoField;
    private static Field linesField;
    private static Field panelWidthField;
    private static boolean disabled;
    private static List<FormattedCharSequence> injected;

    private ChatFormatModListDescription() {
    }

    public static void register() {
        MinecraftForge.EVENT_BUS.addListener(ChatFormatModListDescription::onRenderPre);
    }

    @SuppressWarnings("unchecked")
    private static void onRenderPre(ScreenEvent.Render.Pre event) {
        if (disabled || !(event.getScreen() instanceof ModListScreen screen)) {
            return;
        }

        try {
            resolveFields();
            if (!(selectedField.get(screen) instanceof ModListWidget.ModEntry entry)
                || !ChatFormatMod.MOD_ID.equals(entry.getInfo().getModId())) {
                return;
            }

            Object panel = modInfoField.get(screen);
            if (panel == null) {
                return;
            }

            // Forge rebuilds the list whenever the selection changes or the
            // window resizes; only rewrite a list we have not produced ourselves.
            List<FormattedCharSequence> lines = (List<FormattedCharSequence>) linesField.get(panel);
            if (lines == injected || lines.isEmpty()) {
                return;
            }

            int start = findDescriptionStart(lines);
            if (start < 0) {
                return;
            }

            Font font = Minecraft.getInstance().font;
            int wrapWidth = panelWidthField.getInt(panel) - PANEL_TEXT_INSET;
            List<FormattedCharSequence> rewritten = new ArrayList<>(lines.subList(0, start));
            for (Component line : descriptionLines()) {
                if (wrapWidth > 0) {
                    rewritten.addAll(font.split(line, wrapWidth));
                } else {
                    rewritten.add(line.getVisualOrderText());
                }
            }

            injected = rewritten;
            linesField.set(panel, rewritten);
        } catch (ReflectiveOperationException | RuntimeException e) {
            disabled = true;
            LogUtils.getLogger().warn("Arthou's Chat Format could not restyle its Mods screen description; leaving it as is", e);
        }
    }

    private static List<Component> descriptionLines() {
        return List.of(
            Component.literal("By ").append(link("arthou", AUTHOR_URL)),
            Component.literal("Chat format inspired by Essentials Chat, made for DUSMP."),
            link("Discord", DISCORD_URL)
                .append(Component.literal("  \u2022  ").withStyle(ChatFormatting.GRAY))
                .append(link("Documentation", DOCUMENTATION_URL))
        );
    }

    private static MutableComponent link(String text, String url) {
        return Component.literal(text).withStyle(Style.EMPTY
            .withColor(TextColor.fromRgb(LINK_COLOR))
            .withUnderlined(true)
            .withClickEvent(new ClickEvent(ClickEvent.Action.OPEN_URL, url))
            .withHoverEvent(new HoverEvent(HoverEvent.Action.SHOW_TEXT, Component.literal(url))));
    }

    /** The HTML description is the last block in the panel. */
    private static int findDescriptionStart(List<FormattedCharSequence> lines) {
        for (int i = 0; i < lines.size(); i++) {
            FormattedCharSequence line = lines.get(i);
            if (line != null && plainText(line).startsWith(DESCRIPTION_START)) {
                return i;
            }
        }

        return -1;
    }

    private static String plainText(FormattedCharSequence line) {
        StringBuilder text = new StringBuilder();
        line.accept((index, style, codePoint) -> {
            text.appendCodePoint(codePoint);
            return true;
        });
        return text.toString();
    }

    private static void resolveFields() throws ReflectiveOperationException {
        if (selectedField != null) {
            return;
        }

        Field selected = ModListScreen.class.getDeclaredField("selected");
        Field modInfo = ModListScreen.class.getDeclaredField("modInfo");
        Field lines = modInfo.getType().getDeclaredField("lines");
        Field width = ScrollPanel.class.getDeclaredField("width");
        selected.setAccessible(true);
        modInfo.setAccessible(true);
        lines.setAccessible(true);
        width.setAccessible(true);

        modInfoField = modInfo;
        linesField = lines;
        panelWidthField = width;
        selectedField = selected;
    }
}
