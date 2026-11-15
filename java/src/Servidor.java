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
            while (true) {
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
                Thread.sleep(1000);
                abrir();
            }
        }

        void fechar() { ctx.destroySocket(sock); }
    }

    static class Banco implements AutoCloseable {
        private final Connection con;

        public Banco(String caminho) throws SQLException {
            File pasta = new File(caminho).getAbsoluteFile().getParentFile();
            if (pasta != null) {
                pasta.mkdirs();
            }
            try {
                Class.forName("org.sqlite.JDBC"); // garante o registro do driver no jar único
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
                return ps.executeUpdate() == 1; // 0 linhas = chave primária duplicada
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
        String caminhoDb = System.getenv().getOrDefault("DB_PATH", "/data/" + nome + ".db");
        String referenciaHost = System.getenv().getOrDefault("REFERENCE_HOST", "referencia");
        String referenciaPorta = System.getenv().getOrDefault("REFERENCE_PORT", "5559");
        String quem = "servidor " + nome;

        try (Banco banco = new Banco(caminhoDb);
                ZContext ctx = new ZContext()) {
            ZMQ.Socket sock = ctx.createSocket(SocketType.REP);
            sock.connect("tcp://" + host + ":" + porta);
            ZMQ.Socket pub = ctx.createSocket(SocketType.PUB);
            Referencia referencia = new Referencia(ctx, "tcp://" + referenciaHost + ":" + referenciaPorta, nome);
            pub.connect("tcp://" + proxyHost + ":" + portaXsub);
            Protocolo.Log.log("[" + quem + "] conectado ao broker em " + host + ":" + porta
                    + " e ao proxy em " + proxyHost + ":" + portaXsub + " e à referência em "
                    + referenciaHost + ":" + referenciaPorta + " | banco: " + caminhoDb);

            Map<String, Object> dadosRank = new java.util.LinkedHashMap<>();
            dadosRank.put("name", nome);
            Protocolo.Mensagem cadastro = referencia.requisitar("rank", dadosRank);
            Object rank = cadastro.data().get("rank");
            Protocolo.Log.log("[" + quem + "] rank atribuído pela referência: " + rank);
            Protocolo.Mensagem lista = referencia.requisitar("list", new java.util.LinkedHashMap<>());
            Protocolo.Log.log("[" + quem + "] servidores disponíveis: " + lista.data().get("servers"));
            int recebidas = 0;

            while (!Thread.currentThread().isInterrupted()) {
                byte[] raw = sock.recv(0);
                if (raw == null) {
                    break; // contexto encerrado
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
                } catch (Exception e) { // erro inesperado: não derruba o servidor
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
                    Protocolo.Mensagem hb = referencia.requisitar("heartbeat", heartbeat);
                    if (!Protocolo.OK.equals(hb.data().get("status"))) {
                        Map<String, Object> novoCadastro = new java.util.LinkedHashMap<>();
                        novoCadastro.put("name", nome);
                        Protocolo.Mensagem registro = referencia.requisitar("rank", novoCadastro);
                        rank = registro.data().get("rank");
                        hb = referencia.requisitar("heartbeat", heartbeat);
                    }
                    if (Protocolo.OK.equals(hb.data().get("status"))) {
                        Protocolo.ajustarRelogioFisico(hb.timestamp());
                        Protocolo.Log.log("[" + quem + "] heartbeat OK | hora sincronizada: "
                                + Protocolo.agoraMs() + " | rank=" + rank);
                    }
                }
            }
            referencia.fechar();
            ctx.destroySocket(pub);
            ctx.destroySocket(sock);
        }
    }
}