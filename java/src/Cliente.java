import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

public final class Cliente {
    private static final int MIN_CANAIS = 5;
    private static final int TIMEOUT_MS = 3000;
    private static final int INTERVALO_LISTAGEM_MS = 5000;
    private static final SecureRandom RANDOM = new SecureRandom();

    private Cliente() {}

    static final class Conexao {
        private final ZContext ctx;
        private final String endpoint;
        String nome;
        private ZMQ.Socket sock;

        Conexao(ZContext ctx, String endpoint, String nome) {
            this.ctx = ctx;
            this.endpoint = endpoint;
            this.nome = nome;
            this.sock = abrir();
        }

        private ZMQ.Socket abrir() {
            ZMQ.Socket s = ctx.createSocket(SocketType.REQ);
            s.setLinger(0);
            s.setReceiveTimeOut(TIMEOUT_MS);
            s.connect(endpoint);
            return s;
        }

        Protocolo.Mensagem requisitar(String tipo, Map<String, Object> dados) throws Protocolo.ProtocoloException {
            String quem = "cliente " + nome;
            while (true) {
                Protocolo.Mensagem msg = Protocolo.criarMensagem(tipo, nome, dados);
                sock.send(Protocolo.serializar(msg), 0);
                Protocolo.Log.log(Protocolo.formatarLog(quem, "ENVIADO  >>", msg));

                byte[] raw = sock.recv(0); // null se estourar o timeout
                if (raw != null) {
                    Protocolo.Mensagem resposta = Protocolo.desserializar(raw);
                    Protocolo.Log.log(Protocolo.formatarLog(quem, "RECEBIDO <<", resposta));
                    return resposta;
                }
                Protocolo.Log.log("[" + quem + "] sem resposta em " + TIMEOUT_MS + " ms, tentando de novo...");
                ctx.destroySocket(sock);
                sock = abrir();
            }
        }

        void fechar() {
            ctx.destroySocket(sock);
        }
    }

    static String nomeAleatorio() {
        return String.format("bot_%04x", RANDOM.nextInt(0x10000));
    }

    static String sufixoAleatorio() {
        return String.format("%06x", RANDOM.nextInt(0x1000000));
    }

    static void fazerLogin(Conexao con) throws Exception {
        while (true) {
            Map<String, Object> pedido = new LinkedHashMap<>();
            pedido.put("user", con.nome);
            Protocolo.Mensagem resposta = con.requisitar(Protocolo.LOGIN, pedido);
            Map<String, Object> dados = resposta.data();
            if (Protocolo.OK.equals(dados.get("status"))) {
                Protocolo.Log.log("[cliente " + con.nome + "] login realizado com sucesso");
                return;
            }
            Protocolo.Log.log("[cliente " + con.nome + "] login falhou: " + dados.get("error") + " - " + dados.get("message"));
            if (Protocolo.ERR_INVALID_NAME.equals(dados.get("error"))) {
                con.nome = nomeAleatorio();
                Protocolo.Log.log("[cliente " + con.nome + "] usando novo nome de usuário: " + con.nome);
            }
            Thread.sleep(2000);
        }
    }

    static List<String> listarCanais(Conexao con) throws Exception {
        Map<String, Object> dados = con.requisitar(Protocolo.LIST_CHANNELS, new LinkedHashMap<>()).data();
        List<String> canais = new ArrayList<>();
        if (Protocolo.OK.equals(dados.get("status")) && dados.get("channels") instanceof List<?> lista) {
            for (Object item : lista) {
                canais.add(String.valueOf(item));
            }
        }
        return canais;
    }

    static boolean criarCanal(Conexao con, String nomeCanal) throws Exception {
        Map<String, Object> pedido = new LinkedHashMap<>();
        pedido.put("channel", nomeCanal);
        Map<String, Object> dados = con.requisitar(Protocolo.CREATE_CHANNEL, pedido).data();
        if (Protocolo.OK.equals(dados.get("status"))) {
            return true;
        }
        Protocolo.Log.log("[cliente " + con.nome + "] canal '" + nomeCanal + "' não criado: " + dados.get("error"));
        return false;
    }

    public static void executar() throws Exception {
        String nome = System.getenv("NOME_USUARIO");
        if (nome == null || nome.isBlank()) {
            nome = nomeAleatorio();
        }
        String host = System.getenv().getOrDefault("BROKER_HOST", "broker");
        String porta = System.getenv().getOrDefault("BROKER_FRONTEND_PORT", "5555");

        try (ZContext ctx = new ZContext()) {
            Conexao con = new Conexao(ctx, "tcp://" + host + ":" + porta, nome);
            try {
                fazerLogin(con);

                List<String> canais = listarCanais(con);
                if (!canais.contains("geral")) {
                    criarCanal(con, "geral");
                }
                canais = listarCanais(con);
                if (canais.size() < MIN_CANAIS) {
                    criarCanal(con, "canal-" + sufixoAleatorio());
                }

                while (!Thread.currentThread().isInterrupted()) {
                    listarCanais(con);
                    Thread.sleep(INTERVALO_LISTAGEM_MS);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                con.fechar();
            }
        }
    }
}
