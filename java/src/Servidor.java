import java.io.File;
import java.net.InetAddress;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

public final class Servidor {
    private Servidor() {}


    static final class Referencia {
        private final ZContext ctx;
        private final String endpoint;
        private final String nome;
        private ZMQ.Socket sock;

        Referencia(ZContext ctx, String endpoint, String nome) {
            this.ctx = ctx;
            this.endpoint = endpoint;
            this.nome = nome;
            abrir();
        }

        private void abrir() {
            sock = ctx.createSocket(SocketType.REQ);
            sock.setLinger(0);
            sock.setReceiveTimeOut(3000);
            sock.connect(endpoint);
        }

        Protocolo.Mensagem requisitar(String tipo, Map<String, Object> dados) throws Exception {
            return requisitar(tipo, dados, null);
        }

        Protocolo.Mensagem requisitar(String tipo, Map<String, Object> dados, Integer maxTentativas) throws Exception {
            int tentativa = 0;
            while (true) {
                tentativa++;
                Protocolo.Mensagem pedido = Protocolo.criarMensagem(tipo, nome, dados);
                sock.send(Protocolo.serializar(pedido), 0);
                Protocolo.Log.log(Protocolo.formatarLog("servidor " + nome + " -> referência", "ENVIADO  >>", pedido));
                byte[] raw = sock.recv(0);
                if (raw != null) {
                    Protocolo.Mensagem resposta = Protocolo.desserializar(raw);
                    Protocolo.receberRelogioLogico(resposta.logicalClock());
                    Protocolo.Log.log(Protocolo.formatarLog("servidor " + nome + " -> referência", "RECEBIDO <<", resposta));
                    return resposta;
                }
                ctx.destroySocket(sock);
                if (maxTentativas != null && tentativa >= maxTentativas) {
                    Protocolo.Log.log("[servidor " + nome + "] referência sem resposta após " + tentativa + " tentativa(s)");
                    abrir();
                    return null;
                }
                Thread.sleep(1000);
                abrir();
            }
        }

        void fechar() { ctx.destroySocket(sock); }
    }

    static final String CLOCK = "clock";
    static final String CLOCK_REPLY = "clock_reply";
    static final String ELECTION = "election";
    static final String ELECTION_REPLY = "election_reply";
    static final String COORDINATOR = "coordinator";
    static final String TOPICO_SERVERS = "servers";
    static final String ERR_NOT_COORDINATOR = "NOT_COORDINATOR";
    static final int INTERVALO_SYNC = 15;
    static final long ESPERA_ELEICAO_MS = 5000;

    static long monotonicoMs() {
        return System.nanoTime() / 1_000_000L;
    }

    static Protocolo.Mensagem requisicaoUnica(ZContext ctx, String endpoint, String tipo, String remetente,
                                              Map<String, Object> dados, int timeoutMs) {
        String quem = "servidor " + remetente + " -> " + endpoint;
        ZMQ.Socket s = null;
        try {
            s = ctx.createSocket(SocketType.REQ);
            s.setLinger(0);
            s.setReceiveTimeOut(timeoutMs);
            s.setSendTimeOut(timeoutMs);
            s.connect(endpoint);
            Protocolo.Mensagem pedido = Protocolo.criarMensagem(tipo, remetente, dados);
            if (!s.send(Protocolo.serializar(pedido), 0)) {
                Protocolo.Log.log("[" + quem + "] sem resposta (" + tipo + ")");
                return null;
            }
            Protocolo.Log.log(Protocolo.formatarLog(quem, "ENVIADO  >>", pedido));
            byte[] raw = s.recv(0);
            if (raw == null) {
                Protocolo.Log.log("[" + quem + "] sem resposta (" + tipo + ")");
                return null;
            }
            Protocolo.Mensagem resposta = Protocolo.desserializar(raw);
            Protocolo.receberRelogioLogico(resposta.logicalClock());
            Protocolo.Log.log(Protocolo.formatarLog(quem, "RECEBIDO <<", resposta));
            return resposta;
        } catch (Exception e) {
            Protocolo.Log.log("[" + quem + "] sem resposta (" + tipo + "): " + e.getMessage());
            return null;
        } finally {
            if (s != null) {
                ctx.destroySocket(s);
            }
        }
    }

    static final class Coordenacao {
        private final ZContext ctx;
        private final String nome;
        private final String quem;
        private final Referencia referencia;
        private final ZMQ.Socket pub;
        private final String porta;
        private final int timeoutParMs;
        long rank;
        String coordenador = null;
        boolean eleicaoPendente = false;
        private long aguardandoAteMs = 0;
        private List<Object> lista = new ArrayList<>();

        Coordenacao(ZContext ctx, String nome, long rank, Referencia referencia, ZMQ.Socket pub,
                    String porta, int timeoutParMs) {
            this.ctx = ctx;
            this.nome = nome;
            this.rank = rank;
            this.referencia = referencia;
            this.pub = pub;
            this.porta = porta;
            this.timeoutParMs = timeoutParMs;
            this.quem = "servidor " + nome;
        }

        private String endpoint(String servidor) {
            return "tcp://" + servidor + ":" + porta;
        }

        private void atualizarLista() throws Exception {
            Protocolo.Mensagem resp = referencia.requisitar("list", new java.util.LinkedHashMap<>(), 2);
            if (resp != null && Protocolo.OK.equals(resp.data().get("status"))
                    && resp.data().get("servers") instanceof List<?> servidores) {
                lista = new ArrayList<>(servidores);
            }
        }

        boolean precisaEleger() {
            return eleicaoPendente || (coordenador == null && monotonicoMs() >= aguardandoAteMs);
        }

        void eleicao() throws Exception {
            eleicaoPendente = false;
            atualizarLista();
            List<String> maiores = new ArrayList<>();
            for (Object item : lista) {
                if (item instanceof Map<?, ?> m && m.get("rank") instanceof Number r
                        && r.longValue() > rank && m.get("name") instanceof String n && !n.equals(nome)) {
                    maiores.add(n);
                }
            }
            Protocolo.Log.log("[" + quem + "] iniciando eleição (rank=" + rank
                    + "); servidores de rank maior: " + maiores);
            boolean alguemRespondeu = false;
            for (String par : maiores) {
                Map<String, Object> dados = new java.util.LinkedHashMap<>();
                dados.put("rank", rank);
                Protocolo.Mensagem resp = requisicaoUnica(ctx, endpoint(par), ELECTION, nome, dados, timeoutParMs);
                if (resp != null && Protocolo.OK.equals(resp.data().get("status"))) {
                    alguemRespondeu = true;
                }
            }
            if (alguemRespondeu) {
                aguardandoAteMs = monotonicoMs() + ESPERA_ELEICAO_MS;
                Protocolo.Log.log("[" + quem + "] um servidor de rank maior respondeu OK; aguardando o anúncio do coordenador");
            } else {
                assumir();
            }
        }

        void assumir() {
            coordenador = nome;
            Map<String, Object> dados = new java.util.LinkedHashMap<>();
            dados.put("coordinator", nome);
            dados.put("rank", rank);
            Protocolo.Mensagem anuncio = Protocolo.criarMensagem(COORDINATOR, nome, dados);
            pub.sendMore(TOPICO_SERVERS);
            pub.send(Protocolo.serializar(anuncio), 0);
            Protocolo.Log.log(Protocolo.formatarLog(quem,
                    "PUBLICADO >> [" + TOPICO_SERVERS + "] (sou o novo coordenador)", anuncio));
        }

        void sincronizar() throws Exception {
            if (coordenador == null) {
                return;
            }
            if (coordenador.equals(nome)) {
                Protocolo.Log.log("[" + quem + "] sou o coordenador: relógio de referência = " + Protocolo.agoraMs());
                return;
            }
            Protocolo.Mensagem resp = requisicaoUnica(ctx, endpoint(coordenador), CLOCK, nome,
                    new java.util.LinkedHashMap<>(), timeoutParMs);
            if (resp != null && Protocolo.OK.equals(resp.data().get("status"))
                    && resp.data().get("time") instanceof Long hora) {
                long antes = Protocolo.agoraMs();
                Protocolo.ajustarRelogioFisico(hora);
                Protocolo.Log.log("[" + quem + "] Berkeley: relógio " + antes + " -> " + Protocolo.agoraMs()
                        + " (coordenador " + coordenador + " informou " + hora + "; ajuste " + (hora - antes) + " ms)");
            } else {
                Protocolo.Log.log("[" + quem + "] coordenador '" + coordenador
                        + "' indisponível; iniciando eleição");
                coordenador = null;
                eleicao();
            }
        }

        Map.Entry<String, Map<String, Object>> tratarPar(Protocolo.Mensagem msg) {
            String tipo = msg.type();
            if (CLOCK.equals(tipo)) {
                if (nome.equals(coordenador)) {
                    return new SimpleEntry<>(CLOCK_REPLY,
                            Protocolo.dadosOk("time", Protocolo.agoraMs(), "coordinator", nome));
                }
                return new SimpleEntry<>(CLOCK_REPLY,
                        Protocolo.dadosErro(ERR_NOT_COORDINATOR, "este servidor não é o coordenador"));
            }
            if (ELECTION.equals(tipo)) {
                if (msg.data().get("rank") instanceof Long r && r < rank) {
                    eleicaoPendente = true; // Bully: quem tem rank maior assume a eleição
                }
                return new SimpleEntry<>(ELECTION_REPLY, Protocolo.dadosOk());
            }
            return new SimpleEntry<>(Protocolo.ERROR_REPLY,
                    Protocolo.dadosErro(Protocolo.ERR_INVALID_REQUEST, "tipo desconhecido: " + tipo));
        }

        void tratarAnuncio(String topico, byte[] payload) {
            try {
                Protocolo.Mensagem msg = Protocolo.desserializar(payload);
                Protocolo.receberRelogioLogico(msg.logicalClock());
                if (!COORDINATOR.equals(msg.type()) || !TOPICO_SERVERS.equals(topico)
                        || !(msg.data().get("coordinator") instanceof String novo)) {
                    return;
                }
                aguardandoAteMs = 0;
                if (!novo.equals(coordenador)) {
                    Protocolo.Log.log("[" + quem + "] RECEBIDO << [" + TOPICO_SERVERS + "] novo coordenador: "
                            + novo + " (anterior: " + coordenador + ")");
                }
                coordenador = novo;
            } catch (Protocolo.ProtocoloException e) {
                Protocolo.Log.log("[" + quem + "] anúncio descartado (inválido): " + e.getMessage());
            }
        }
    }

    static class Banco implements AutoCloseable {
        private final Connection con;

        public Banco(String caminho) throws SQLException {
            File pasta = new File(caminho).getAbsoluteFile().getParentFile();
            if (pasta != null) {
                pasta.mkdirs();
            }
            try {
                Class.forName("org.sqlite.JDBC");
            } catch (ClassNotFoundException e) {
                throw new SQLException("driver SQLite não encontrado", e);
            }
            con = DriverManager.getConnection("jdbc:sqlite:" + caminho);
            try (Statement st = con.createStatement()) {
                st.executeUpdate(
                        "CREATE TABLE IF NOT EXISTS logins ("
                                + "id INTEGER PRIMARY KEY AUTOINCREMENT, "
                                + "usuario TEXT NOT NULL, "
                                + "timestamp_ms INTEGER NOT NULL)");
                st.executeUpdate(
                        "CREATE TABLE IF NOT EXISTS canais ("
                                + "nome TEXT PRIMARY KEY, "
                                + "criado_por TEXT NOT NULL, "
                                + "timestamp_ms INTEGER NOT NULL)");
                st.executeUpdate(
                        "CREATE TABLE IF NOT EXISTS publicacoes ("
                                + "id INTEGER PRIMARY KEY AUTOINCREMENT, "
                                + "msg_id TEXT NOT NULL UNIQUE, "
                                + "canal TEXT NOT NULL, "
                                + "usuario TEXT NOT NULL, "
                                + "mensagem TEXT NOT NULL, "
                                + "ts_cliente_ms INTEGER NOT NULL, "
                                + "ts_publicacao_ms INTEGER NOT NULL, "
                                + "servidor TEXT NOT NULL)");
            }
        }

        public void registrarLogin(String usuario, long ts) throws SQLException {
            try (PreparedStatement ps =
                    con.prepareStatement("INSERT INTO logins (usuario, timestamp_ms) VALUES (?, ?)")) {
                ps.setString(1, usuario);
                ps.setLong(2, ts);
                ps.executeUpdate();
            }
        }

        public boolean criarCanal(String nome, String criadoPor, long ts) throws SQLException {
            try (PreparedStatement ps =
                    con.prepareStatement(
                            "INSERT OR IGNORE INTO canais (nome, criado_por, timestamp_ms) VALUES (?, ?, ?)")) {
                ps.setString(1, nome);
                ps.setString(2, criadoPor);
                ps.setLong(3, ts);
                return ps.executeUpdate() == 1;
            }
        }

        public List<String> listarCanais() throws SQLException {
            List<String> canais = new ArrayList<>();
            try (Statement st = con.createStatement();
                    ResultSet rs = st.executeQuery("SELECT nome FROM canais ORDER BY nome")) {
                while (rs.next()) {
                    canais.add(rs.getString(1));
                }
            }
            return canais;
        }

        public boolean registrarPublicacao(String msgId, String canal, String usuario, String mensagem,
                                           long tsCliente, long tsPublicacao, String servidor) throws SQLException {
            try (PreparedStatement ps = con.prepareStatement(
                    "INSERT OR IGNORE INTO publicacoes "
                            + "(msg_id, canal, usuario, mensagem, ts_cliente_ms, ts_publicacao_ms, servidor) "
                            + "VALUES (?, ?, ?, ?, ?, ?, ?)")) {
                ps.setString(1, msgId);
                ps.setString(2, canal);
                ps.setString(3, usuario);
                ps.setString(4, mensagem);
                ps.setLong(5, tsCliente);
                ps.setLong(6, tsPublicacao);
                ps.setString(7, servidor);
                return ps.executeUpdate() == 1;
            }
        }

        public void removerPublicacao(String msgId) throws SQLException {
            try (PreparedStatement ps = con.prepareStatement("DELETE FROM publicacoes WHERE msg_id = ?")) {
                ps.setString(1, msgId);
                ps.executeUpdate();
            }
        }

        @Override
        public void close() throws SQLException {
            con.close();
        }
    }

    static Map.Entry<String, Map<String, Object>> tratar(Protocolo.Mensagem msg, Banco banco,
                                                           ZMQ.Socket pub, String nome) throws Exception {
        String tipo = msg.type();
        Map<String, Object> dados = msg.data();
        long agora = Protocolo.agoraMs();

        switch (tipo) {
            case Protocolo.LOGIN: {
                Object usuario = dados.get("user");
                if (!(usuario instanceof String u) || !Protocolo.USER_RE.matcher(u).matches()) {
                    return new SimpleEntry<>(
                            Protocolo.LOGIN_REPLY,
                            Protocolo.dadosErro(
                                    Protocolo.ERR_INVALID_NAME,
                                    "usuário deve ter 3 a 20 caracteres: letras, números, '_' ou '-'"));
                }
                banco.registrarLogin(u, agora);
                return new SimpleEntry<>(Protocolo.LOGIN_REPLY, Protocolo.dadosOk("user", u));
            }
            case Protocolo.CREATE_CHANNEL: {
                Object canal = dados.get("channel");
                if (!(canal instanceof String c) || !Protocolo.CHANNEL_RE.matcher(c).matches()) {
                    return new SimpleEntry<>(
                            Protocolo.CREATE_CHANNEL_REPLY,
                            Protocolo.dadosErro(
                                    Protocolo.ERR_INVALID_NAME,
                                    "canal deve ter 3 a 30 caracteres: letras minúsculas, números, '_' ou '-'"));
                }
                if (!banco.criarCanal(c, msg.sender(), agora)) {
                    return new SimpleEntry<>(
                            Protocolo.CREATE_CHANNEL_REPLY,
                            Protocolo.dadosErro(Protocolo.ERR_ALREADY_EXISTS, "o canal '" + c + "' já existe"));
                }
                return new SimpleEntry<>(Protocolo.CREATE_CHANNEL_REPLY, Protocolo.dadosOk("channel", c));
            }
            case Protocolo.LIST_CHANNELS:
                return new SimpleEntry<>(
                        Protocolo.LIST_CHANNELS_REPLY, Protocolo.dadosOk("channels", banco.listarCanais()));
            case Protocolo.PUBLISH:
                return new SimpleEntry<>(Protocolo.PUBLISH_REPLY, publicar(msg, banco, pub, nome));
            default:
                return new SimpleEntry<>(
                        Protocolo.ERROR_REPLY,
                        Protocolo.dadosErro(
                                Protocolo.ERR_INVALID_REQUEST, "tipo de mensagem desconhecido: " + tipo));
        }
    }

    private static Map<String, Object> publicar(Protocolo.Mensagem pedido, Banco banco,
                                                  ZMQ.Socket pub, String nome) throws Exception {
        Map<String, Object> dados = pedido.data();
        Object canalObj = dados.get("channel");
        Object textoObj = dados.get("message");
        Object idObj = dados.get("msg_id");

        if (!(canalObj instanceof String canal) || !Protocolo.CHANNEL_RE.matcher(canal).matches()) {
            return Protocolo.dadosErro(Protocolo.ERR_INVALID_NAME,
                    "canal deve ter 3 a 30 caracteres: letras minúsculas, números, '_' ou '-'");
        }
        if (!(textoObj instanceof String texto) || texto.isBlank() || texto.length() > Protocolo.MAX_MENSAGEM) {
            return Protocolo.dadosErro(Protocolo.ERR_INVALID_REQUEST,
                    "mensagem deve ter de 1 a " + Protocolo.MAX_MENSAGEM + " caracteres");
        }
        if (!(idObj instanceof String msgId) || msgId.isBlank() || msgId.length() > Protocolo.MAX_MSG_ID) {
            return Protocolo.dadosErro(Protocolo.ERR_INVALID_REQUEST,
                    "msg_id deve ter de 1 a " + Protocolo.MAX_MSG_ID + " caracteres");
        }

        Protocolo.Mensagem publicacao = Protocolo.criarMensagem(Protocolo.PUBLICATION, nome,
                Map.of("channel", canal, "message", texto, "user", pedido.sender(), "msg_id", msgId));
        boolean nova = banco.registrarPublicacao(msgId, canal, pedido.sender(), texto,
                pedido.timestamp(), publicacao.timestamp(), nome);
        if (!nova) {
            Protocolo.Log.log("[servidor " + nome + "] msg_id " + msgId + " já registrado, não será publicado de novo");
            return Protocolo.dadosOk("msg_id", msgId);
        }

        try {
            if (!pub.sendMore(canal) || !pub.send(Protocolo.serializar(publicacao), 0)) {
                throw new IllegalStateException("não foi possível enviar a publicação ao proxy");
            }
        } catch (Exception e) {
            banco.removerPublicacao(msgId);
            throw e;
        }
        Protocolo.Log.log(Protocolo.formatarLog("servidor " + nome, "PUBLICADO >> [" + canal + "]", publicacao));
        return Protocolo.dadosOk("msg_id", msgId);
    }

    public static void executar() throws Exception {
        String nome = System.getenv("NOME_SERVIDOR");
        if (nome == null || nome.isBlank()) {
            nome = InetAddress.getLocalHost().getHostName();
        }
        String host = System.getenv().getOrDefault("BROKER_HOST", "broker");
        String porta = System.getenv().getOrDefault("BROKER_BACKEND_PORT", "5556");
        String proxyHost = System.getenv().getOrDefault("PROXY_HOST", "proxy");
        String portaXsub = System.getenv().getOrDefault("PROXY_XSUB_PORT", "5557");
        String portaXpub = System.getenv().getOrDefault("PROXY_XPUB_PORT", "5558");
        String caminhoDb = System.getenv().getOrDefault("DB_PATH", "/data/" + nome + ".db");
        String referenciaHost = System.getenv().getOrDefault("REFERENCE_HOST", "referencia");
        String referenciaPorta = System.getenv().getOrDefault("REFERENCE_PORT", "5559");
        String portaServidores = System.getenv().getOrDefault("SERVER_PORT", "5560");
        String bindServidores = System.getenv().getOrDefault("SERVER_BIND_HOST", "*");
        int timeoutParMs = Integer.parseInt(System.getenv().getOrDefault("PEER_TIMEOUT_MS", "1500"));
        long desvioInicialMs = Long.parseLong(System.getenv().getOrDefault("CLOCK_OFFSET_MS", "0"));
        String quem = "servidor " + nome;

        try (Banco banco = new Banco(caminhoDb);
                ZContext ctx = new ZContext()) {
            ZMQ.Socket sock = ctx.createSocket(SocketType.REP);
            sock.connect("tcp://" + host + ":" + porta);
            ZMQ.Socket pub = ctx.createSocket(SocketType.PUB);
            pub.connect("tcp://" + proxyHost + ":" + portaXsub);
            ZMQ.Socket sub = ctx.createSocket(SocketType.SUB);
            sub.connect("tcp://" + proxyHost + ":" + portaXpub);
            sub.subscribe(TOPICO_SERVERS.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            ZMQ.Socket par = ctx.createSocket(SocketType.REP);
            par.setLinger(0);
            par.bind("tcp://" + bindServidores + ":" + portaServidores);
            Referencia referencia = new Referencia(ctx, "tcp://" + referenciaHost + ":" + referenciaPorta, nome);
            Protocolo.Log.log("[" + quem + "] conectado ao broker em " + host + ":" + porta
                    + ", ao proxy em " + proxyHost + ":" + portaXsub + "/" + portaXpub
                    + " e à referência em " + referenciaHost + ":" + referenciaPorta
                    + " | porta servidor-servidor: " + portaServidores + " | banco: " + caminhoDb);

            if (desvioInicialMs != 0) {
                Protocolo.ajustarRelogioFisico(System.currentTimeMillis() + desvioInicialMs);
                Protocolo.Log.log("[" + quem + "] relógio começa com desvio simulado de " + desvioInicialMs
                        + " ms: " + Protocolo.agoraMs());
            }

            Map<String, Object> dadosRank = new java.util.LinkedHashMap<>();
            dadosRank.put("name", nome);
            Protocolo.Mensagem cadastro = referencia.requisitar("rank", dadosRank);
            long rank = ((Number) cadastro.data().get("rank")).longValue();
            Protocolo.Log.log("[" + quem + "] rank atribuído pela referência: " + rank);
            Coordenacao coord = new Coordenacao(ctx, nome, rank, referencia, pub, portaServidores, timeoutParMs);
            int recebidas = 0;

            ZMQ.Poller poller = ctx.createPoller(3);
            poller.register(sock, ZMQ.Poller.POLLIN);
            poller.register(par, ZMQ.Poller.POLLIN);
            poller.register(sub, ZMQ.Poller.POLLIN);

            while (!Thread.currentThread().isInterrupted()) {
                if (poller.poll(1000) < 0) {
                    break;
                }

                if (poller.pollin(1)) {
                    byte[] raw = par.recv(0);
                    if (raw == null) {
                        break;
                    }
                    String tipoResp;
                    Map<String, Object> dados;
                    try {
                        Protocolo.Mensagem msg = Protocolo.desserializar(raw);
                        Protocolo.receberRelogioLogico(msg.logicalClock());
                        Protocolo.Log.log(Protocolo.formatarLog(quem, "RECEBIDO << (servidor)", msg));
                        Map.Entry<String, Map<String, Object>> resultado = coord.tratarPar(msg);
                        tipoResp = resultado.getKey();
                        dados = resultado.getValue();
                    } catch (Protocolo.ProtocoloException e) {
                        tipoResp = Protocolo.ERROR_REPLY;
                        dados = Protocolo.dadosErro(Protocolo.ERR_INVALID_REQUEST, e.getMessage());
                    }
                    Protocolo.Mensagem resposta = Protocolo.criarMensagem(tipoResp, nome, dados);
                    par.send(Protocolo.serializar(resposta), 0);
                    Protocolo.Log.log(Protocolo.formatarLog(quem, "ENVIADO  >> (servidor)", resposta));
                }

                if (poller.pollin(2)) {
                    String topico = sub.recvStr(0);
                    byte[] payload = sub.hasReceiveMore() ? sub.recv(0) : null;
                    while (sub.hasReceiveMore()) {
                        sub.recv(0);
                    }
                    if (topico != null && payload != null) {
                        coord.tratarAnuncio(topico, payload);
                    }
                }

                if (poller.pollin(0)) {
                    byte[] raw = sock.recv(0);
                    if (raw == null) {
                        break;
                    }

                    String tipoResp;
                    Map<String, Object> dados;
                    try {
                        Protocolo.Mensagem msg = Protocolo.desserializar(raw);
                        Protocolo.receberRelogioLogico(msg.logicalClock());
                        Protocolo.Log.log(Protocolo.formatarLog(quem, "RECEBIDO <<", msg));
                        Map.Entry<String, Map<String, Object>> resultado = tratar(msg, banco, pub, nome);
                        tipoResp = resultado.getKey();
                        dados = resultado.getValue();
                    } catch (Protocolo.ProtocoloException e) {
                        tipoResp = Protocolo.ERROR_REPLY;
                        dados = Protocolo.dadosErro(Protocolo.ERR_INVALID_REQUEST, e.getMessage());
                    } catch (Exception e) {
                        tipoResp = Protocolo.ERROR_REPLY;
                        dados = Protocolo.dadosErro(Protocolo.ERR_INTERNAL, String.valueOf(e.getMessage()));
                    }

                    Protocolo.Mensagem resposta = Protocolo.criarMensagem(tipoResp, nome, dados);
                    sock.send(Protocolo.serializar(resposta), 0);
                    Protocolo.Log.log(Protocolo.formatarLog(quem, "ENVIADO  >>", resposta));
                    recebidas++;
                    if (recebidas % 10 == 0) {
                        Map<String, Object> heartbeat = new java.util.LinkedHashMap<>();
                        heartbeat.put("name", nome);
                        Protocolo.Mensagem hb = referencia.requisitar("heartbeat", heartbeat, 2);
                        if (hb != null && !Protocolo.OK.equals(hb.data().get("status"))) {
                            Protocolo.Mensagem registro = referencia.requisitar("rank", heartbeat, 2);
                            if (registro != null && Protocolo.OK.equals(registro.data().get("status"))) {
                                rank = ((Number) registro.data().get("rank")).longValue();
                                coord.rank = rank;
                                hb = referencia.requisitar("heartbeat", heartbeat, 2);
                            }
                        }
                        if (hb != null && Protocolo.OK.equals(hb.data().get("status"))) {
                            Protocolo.Log.log("[" + quem + "] heartbeat OK | rank=" + rank);
                        } else {
                            Protocolo.Log.log("[" + quem + "] heartbeat não confirmado; seguindo sem a referência");
                        }
                    }
                    if (recebidas % INTERVALO_SYNC == 0) {
                        coord.sincronizar();
                    }
                }

                if (coord.precisaEleger()) {
                    coord.eleicao();
                }
            }
            referencia.fechar();
            ctx.destroySocket(pub);
            ctx.destroySocket(sub);
            ctx.destroySocket(par);
            ctx.destroySocket(sock);
        }
    }
}
