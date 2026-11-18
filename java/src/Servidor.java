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
import java.util.concurrent.ThreadLocalRandom;
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
    static final String REPLICATION = "replication";
    static final String SYNC_EVENTOS = "sync_events";
    static final String SYNC_EVENTOS_REPLY = "sync_events_reply";
    static final String TOPICO_REPLICACAO = "replicacao:eventos";
    static final int LOTE_SYNC = 200;
    static final long INTERVALO_REPLICACAO_MS = 30_000;

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

        List<String> pares() throws Exception {
            atualizarLista();
            List<String> nomes = new ArrayList<>();
            for (Object item : lista) {
                if (item instanceof Map<?, ?> m && m.get("name") instanceof String n && !n.equals(nome)) {
                    nomes.add(n);
                }
            }
            return nomes;
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

    static final class Replicacao {
        private final Banco banco;
        private final Coordenacao coord;
        private final String origem;
        private long seq = 0;
        long proximaMs = Long.MIN_VALUE;

        Replicacao(Banco banco, Coordenacao coord) throws SQLException {
            this.banco = banco;
            this.coord = coord;
            this.origem = coord.nome + "#" + String.format("%08x", ThreadLocalRandom.current().nextInt());
            try (Statement st = banco.con.createStatement()) {
                st.execute("PRAGMA synchronous = OFF");
                st.executeUpdate(
                        "CREATE TABLE IF NOT EXISTS replicacao ("
                                + "origem TEXT NOT NULL, seq INTEGER NOT NULL, dados BLOB NOT NULL, "
                                + "PRIMARY KEY (origem, seq))");
            }
        }

        private Map<String, Object> corpo(Map<?, ?> evento) {
            Map<String, Object> corpo = new java.util.LinkedHashMap<>();
            corpo.put("events", List.of(evento));
            return corpo;
        }

        private void guardar(Map<?, ?> evento) throws SQLException {
            try (PreparedStatement ps = banco.con.prepareStatement(
                    "INSERT INTO replicacao (origem, seq, dados) VALUES (?, ?, ?)")) {
                ps.setString(1, (String) evento.get("origin"));
                ps.setLong(2, (Long) evento.get("seq"));
                ps.setBytes(3, Protocolo.serializar(
                        new Protocolo.Mensagem(REPLICATION, coord.nome, Protocolo.agoraMs(), 0, corpo(evento))));
                ps.executeUpdate();
            }
        }

        void registrar(String tipo, Map<String, Object> dados) throws SQLException {
            Map<String, Object> evento = new java.util.LinkedHashMap<>();
            evento.put("origin", origem);
            evento.put("seq", ++seq);
            evento.put("kind", tipo);
            evento.put("data", dados);
            guardar(evento);
            Protocolo.Mensagem msg = Protocolo.criarMensagem(REPLICATION, coord.nome, corpo(evento));
            coord.pub.sendMore(TOPICO_REPLICACAO);
            coord.pub.send(Protocolo.serializar(msg), 0);
            Protocolo.Log.log(Protocolo.formatarLog(coord.quem, "PUBLICADO >> [" + TOPICO_REPLICACAO + "]", msg));
        }

        private long ultimoSeq(String origemEvento) throws SQLException {
            try (PreparedStatement ps = banco.con.prepareStatement(
                    "SELECT COALESCE(MAX(seq), 0) FROM replicacao WHERE origem = ?")) {
                ps.setString(1, origemEvento);
                try (ResultSet rs = ps.executeQuery()) {
                    rs.next();
                    return rs.getLong(1);
                }
            }
        }

        private void reconciliar(String sql, Object... parametros) throws SQLException {
            try (PreparedStatement ps = banco.con.prepareStatement(sql)) {
                for (int i = 0; i < parametros.length; i++) {
                    ps.setObject(i + 1, parametros[i]);
                }
                ps.executeUpdate();
            }
        }

        private void executar(String tipo, Map<?, ?> d) throws SQLException {
            switch (tipo) {
                case "login":
                    banco.registrarLogin((String) d.get("usuario"), (Long) d.get("ts"));
                    break;
                case "channel":
                    banco.criarCanal((String) d.get("nome"), (String) d.get("criado_por"), (Long) d.get("ts"));
                    reconciliar("UPDATE canais SET criado_por = ?, timestamp_ms = ? WHERE nome = ? "
                            + "AND (? < timestamp_ms OR (? = timestamp_ms AND ? < criado_por))",
                            d.get("criado_por"), d.get("ts"), d.get("nome"), d.get("ts"), d.get("ts"),
                            d.get("criado_por"));
                    break;
                case "publication":
                    banco.registrarPublicacao((String) d.get("msg_id"), (String) d.get("canal"),
                            (String) d.get("usuario"), (String) d.get("mensagem"), (Long) d.get("ts_cliente"),
                            (Long) d.get("ts_publicacao"), (String) d.get("servidor"));
                    reconciliar("UPDATE publicacoes SET canal = ?, usuario = ?, mensagem = ?, ts_cliente_ms = ?, "
                            + "ts_publicacao_ms = ?, servidor = ? WHERE msg_id = ? AND (? < ts_publicacao_ms "
                            + "OR (? = ts_publicacao_ms AND ? < servidor))",
                            d.get("canal"), d.get("usuario"), d.get("mensagem"), d.get("ts_cliente"),
                            d.get("ts_publicacao"), d.get("servidor"), d.get("msg_id"), d.get("ts_publicacao"),
                            d.get("ts_publicacao"), d.get("servidor"));
                    break;
                default:
                    throw new IllegalArgumentException("tipo de evento desconhecido: " + tipo);
            }
        }

        int aplicar(List<?> eventos) {
            int aplicados = 0;
            for (Object item : eventos) {
                try {
                    Map<?, ?> ev = (Map<?, ?>) item;
                    long ultimo = ultimoSeq((String) ev.get("origin"));
                    long numero = (Long) ev.get("seq");
                    if (numero > ultimo + 1) {
                        proximaMs = Long.MIN_VALUE; // lacuna na sequência: sincroniza já
                    } else if (numero == ultimo + 1) {
                        executar((String) ev.get("kind"), (Map<?, ?>) ev.get("data"));
                        guardar(ev);
                        aplicados++;
                    }
                } catch (Exception e) { // evento inválido: não derruba o servidor
                    Protocolo.Log.log("[" + coord.quem + "] evento de replicação descartado: " + e);
                }
            }
            if (aplicados > 0) {
                Protocolo.Log.log("[" + coord.quem + "] replicação: " + aplicados + " evento(s) aplicado(s)");
            }
            return aplicados;
        }

        void tratarPublicacao(byte[] payload) {
            try {
                Protocolo.Mensagem msg = Protocolo.desserializar(payload);
                Protocolo.receberRelogioLogico(msg.logicalClock());
                List<Object> eventos = new ArrayList<>();
                for (Object ev : (List<?>) msg.data().get("events")) {
                    if (!origem.equals(((Map<?, ?>) ev).get("origin"))) {
                        eventos.add(ev);
                    }
                }
                if (!eventos.isEmpty()) {
                    Protocolo.Log.log(Protocolo.formatarLog(coord.quem,
                            "RECEBIDO << [" + TOPICO_REPLICACAO + "]", msg));
                    aplicar(eventos);
                }
            } catch (Exception e) {
                Protocolo.Log.log("[" + coord.quem + "] evento de replicação descartado: " + e);
            }
        }

        Map.Entry<String, Map<String, Object>> tratarSync(Protocolo.Mensagem msg) {
            try {
                Map<?, ?> vetor = (Map<?, ?>) msg.data().get("vector");
                List<Object> eventos = new ArrayList<>();
                for (String o : vetor().keySet()) {
                    try (PreparedStatement ps = banco.con.prepareStatement(
                            "SELECT dados FROM replicacao WHERE origem = ? AND seq > ? ORDER BY seq LIMIT ?")) {
                        ps.setString(1, o);
                        ps.setLong(2, vetor.get(o) instanceof Long n ? n : 0L);
                        ps.setInt(3, LOTE_SYNC - eventos.size());
                        try (ResultSet rs = ps.executeQuery()) {
                            while (rs.next()) {
                                Protocolo.Mensagem guardada = Protocolo.desserializar(rs.getBytes(1));
                                eventos.add(((List<?>) guardada.data().get("events")).get(0));
                            }
                        }
                    }
                }
                return new SimpleEntry<>(SYNC_EVENTOS_REPLY, Protocolo.dadosOk("events", eventos));
            } catch (Exception e) {
                return new SimpleEntry<>(SYNC_EVENTOS_REPLY,
                        Protocolo.dadosErro(Protocolo.ERR_INTERNAL, String.valueOf(e.getMessage())));
            }
        }

        private Map<String, Long> vetor() throws SQLException {
            Map<String, Long> vetor = new java.util.LinkedHashMap<>();
            try (Statement st = banco.con.createStatement();
                    ResultSet rs = st.executeQuery("SELECT origem, MAX(seq) FROM replicacao GROUP BY origem")) {
                while (rs.next()) {
                    vetor.put(rs.getString(1), rs.getLong(2));
                }
            }
            return vetor;
        }

        void sincronizar() throws Exception {
            proximaMs = monotonicoMs()
                    + (long) (INTERVALO_REPLICACAO_MS * (0.7 + 0.6 * ThreadLocalRandom.current().nextDouble()));
            List<String> pares = coord.pares();
            Protocolo.Log.log("[" + coord.quem + "] sincronização com: " + pares);
            for (String par : pares) {
                while (true) {
                    Map<String, Object> pedido = new java.util.LinkedHashMap<>();
                    pedido.put("vector", vetor());
                    Protocolo.Mensagem resp = requisicaoUnica(coord.ctx, "tcp://" + par + ":" + coord.porta,
                            SYNC_EVENTOS, coord.nome, pedido, coord.timeoutParMs);
                    List<?> eventos = resp != null && resp.data().get("events") instanceof List<?> l ? l : List.of();
                    if (aplicar(eventos) == 0 || eventos.size() < LOTE_SYNC) {
                        break;
                    }
                }
            }
        }
    }

    static class Banco implements AutoCloseable {
        final Connection con;

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

        public List<Map<String, Object>> historico(String canal, int limite) throws SQLException {
            List<Map<String, Object>> mensagens = new ArrayList<>();
            try (PreparedStatement ps = con.prepareStatement(
                    "SELECT msg_id, usuario, mensagem, ts_publicacao_ms, servidor FROM publicacoes "
                            + "WHERE canal = ? ORDER BY ts_publicacao_ms DESC, msg_id DESC LIMIT ?")) {
                ps.setString(1, canal);
                ps.setInt(2, limite);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        Map<String, Object> m = new java.util.LinkedHashMap<>();
                        m.put("msg_id", rs.getString(1));
                        m.put("user", rs.getString(2));
                        m.put("message", rs.getString(3));
                        m.put("timestamp", rs.getLong(4));
                        m.put("server", rs.getString(5));
                        mensagens.add(0, m);
                    }
                }
            }
            return mensagens;
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
                                                           ZMQ.Socket pub, String nome,
                                                           Replicacao repl) throws Exception {
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
                repl.registrar("login", Map.of("usuario", u, "ts", agora));
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
                repl.registrar("channel", Map.of("nome", c, "criado_por", msg.sender(), "ts", agora));
                return new SimpleEntry<>(Protocolo.CREATE_CHANNEL_REPLY, Protocolo.dadosOk("channel", c));
            }
            case Protocolo.LIST_CHANNELS:
                return new SimpleEntry<>(
                        Protocolo.LIST_CHANNELS_REPLY, Protocolo.dadosOk("channels", banco.listarCanais()));
            case Protocolo.PUBLISH:
                return new SimpleEntry<>(Protocolo.PUBLISH_REPLY, publicar(msg, banco, pub, nome, repl));
            case Protocolo.HISTORY: {
                Object canal = dados.get("channel");
                Object limite = dados.getOrDefault("limit", 20L);
                if (!(canal instanceof String c) || !Protocolo.CHANNEL_RE.matcher(c).matches()) {
                    return new SimpleEntry<>(
                            Protocolo.HISTORY_REPLY,
                            Protocolo.dadosErro(
                                    Protocolo.ERR_INVALID_NAME,
                                    "canal deve ter 3 a 30 caracteres: letras minúsculas, números, '_' ou '-'"));
                }
                if (!(limite instanceof Long l) || l < 1 || l > 100) {
                    return new SimpleEntry<>(
                            Protocolo.HISTORY_REPLY,
                            Protocolo.dadosErro(Protocolo.ERR_INVALID_REQUEST, "limit deve ser um inteiro de 1 a 100"));
                }
                return new SimpleEntry<>(
                        Protocolo.HISTORY_REPLY,
                        Protocolo.dadosOk("channel", c, "messages", banco.historico(c, l.intValue())));
            }
            default:
                return new SimpleEntry<>(
                        Protocolo.ERROR_REPLY,
                        Protocolo.dadosErro(
                                Protocolo.ERR_INVALID_REQUEST, "tipo de mensagem desconhecido: " + tipo));
        }
    }

    private static Map<String, Object> publicar(Protocolo.Mensagem pedido, Banco banco,
                                                  ZMQ.Socket pub, String nome,
                                                  Replicacao repl) throws Exception {
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
        repl.registrar("publication", Map.of("msg_id", msgId, "canal", canal, "usuario", pedido.sender(),
                "mensagem", texto, "ts_cliente", pedido.timestamp(), "ts_publicacao", publicacao.timestamp(),
                "servidor", nome));
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
            ZMQ.Socket pub = ctx.createSocket(SocketType.PUB);
            pub.connect("tcp://" + proxyHost + ":" + portaXsub);
            ZMQ.Socket sub = ctx.createSocket(SocketType.SUB);
            sub.connect("tcp://" + proxyHost + ":" + portaXpub);
            sub.subscribe(TOPICO_SERVERS.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            sub.subscribe(TOPICO_REPLICACAO.getBytes(java.nio.charset.StandardCharsets.UTF_8));
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
            Replicacao repl = new Replicacao(banco, coord);
            repl.sincronizar();
            sock.connect("tcp://" + host + ":" + porta);
            Protocolo.Log.log("[" + quem + "] sincronizado; atendendo os clientes pelo broker");
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
                        Map.Entry<String, Map<String, Object>> resultado = SYNC_EVENTOS.equals(msg.type())
                                ? repl.tratarSync(msg)
                                : coord.tratarPar(msg);
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
                        if (TOPICO_REPLICACAO.equals(topico)) {
                            repl.tratarPublicacao(payload);
                        } else {
                            coord.tratarAnuncio(topico, payload);
                        }
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
                        Map.Entry<String, Map<String, Object>> resultado = tratar(msg, banco, pub, nome, repl);
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

                if (monotonicoMs() >= repl.proximaMs) {
                    repl.sincronizar();
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
