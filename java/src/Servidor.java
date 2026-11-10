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

        @Override
        public void close() throws SQLException {
            con.close();
        }
    }

    static Map.Entry<String, Map<String, Object>> tratar(Protocolo.Mensagem msg, Banco banco) throws Exception {
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
            default:
                return new SimpleEntry<>(
                        Protocolo.ERROR_REPLY,
                        Protocolo.dadosErro(
                                Protocolo.ERR_INVALID_REQUEST, "tipo de mensagem desconhecido: " + tipo));
        }
    }

    public static void executar() throws Exception {
        String nome = System.getenv("NOME_SERVIDOR");
        if (nome == null || nome.isBlank()) {
            nome = InetAddress.getLocalHost().getHostName();
        }
        String host = System.getenv().getOrDefault("BROKER_HOST", "broker");
        String porta = System.getenv().getOrDefault("BROKER_BACKEND_PORT", "5556");
        String caminhoDb = System.getenv().getOrDefault("DB_PATH", "/data/" + nome + ".db");
        String quem = "servidor " + nome;

        try (Banco banco = new Banco(caminhoDb);
                ZContext ctx = new ZContext()) {
            ZMQ.Socket sock = ctx.createSocket(SocketType.REP);
            sock.connect("tcp://" + host + ":" + porta);
            Protocolo.Log.log("[" + quem + "] conectado ao broker em " + host + ":" + porta + " | banco: " + caminhoDb);

            while (!Thread.currentThread().isInterrupted()) {
                byte[] raw = sock.recv(0);
                if (raw == null) {
                    break; // contexto encerrado
                }

                String tipoResp;
                Map<String, Object> dados;
                try {
                    Protocolo.Mensagem msg = Protocolo.desserializar(raw);
                    Protocolo.Log.log(Protocolo.formatarLog(quem, "RECEBIDO <<", msg));
                    Map.Entry<String, Map<String, Object>> resultado = tratar(msg, banco);
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
            }
        }
    }
}