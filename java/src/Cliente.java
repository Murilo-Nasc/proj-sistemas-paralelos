import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

public final class Cliente {
    private static final int MIN_CANAIS = 5;
    private static final int MIN_ASSINATURAS = 3;
    private static final int MENSAGENS_POR_RODADA = 10;
    private static final int TIMEOUT_MS = 3000;
    private static final int INTERVALO_MENSAGENS_MS = 1000;
    private static final SecureRandom RANDOM = new SecureRandom();

    private static final String[] FRASES = {
        "Bom dia, pessoal!", "Alguém viu o resultado do jogo?",
        "Estou testando o sistema distribuído", "ZeroMQ é bem mais simples do que parece",
        "Quem está online por aqui?", "Mais uma mensagem aleatória",
        "O broker está distribuindo bem as requisições", "Pub/Sub funcionando por aqui",
        "Vamos ver se essa chega a todo mundo", "Até já, volto em alguns segundos"
    };

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
                    Protocolo.receberRelogioLogico(resposta.logicalClock());
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

    static final class Assinante {
        private final ZMQ.Socket sock;
        private String nome;
        private final Set<String> canais = new LinkedHashSet<>();

        Assinante(ZContext ctx, String endpoint, String nome) {
            this.nome = nome;
            sock = ctx.createSocket(SocketType.SUB);
            sock.setLinger(0);
            sock.setReceiveTimeOut(100);
            sock.connect(endpoint);
        }

        void assinar(String canal) {
            if (canais.add(canal)) {
                sock.subscribe(canal.getBytes(StandardCharsets.UTF_8));
                Protocolo.Log.log("[cliente " + nome + "] inscrito no canal '" + canal
                        + "' (total: " + canais.size() + ")");
            }
        }

        int quantidade() {
            return canais.size();
        }

        boolean contem(String canal) {
            return canais.contains(canal);
        }

        void esperar(int milissegundos) throws InterruptedException {
            long limite = System.currentTimeMillis() + milissegundos;
            while (System.currentTimeMillis() < limite) {
                String topico = sock.recvStr(0);
                if (topico == null) {
                    continue;
                }
                byte[] raw = sock.recv(0);
                if (raw == null) {
                    continue;
                }
                long recebido = Protocolo.agoraMs();
                try {
                    Protocolo.Mensagem msg = Protocolo.desserializar(raw);
                    Protocolo.receberRelogioLogico(msg.logicalClock());
                    if (canais.contains(topico) && Protocolo.PUBLICATION.equals(msg.type())
                            && topico.equals(msg.data().get("channel"))) {
                        Protocolo.Log.log(Protocolo.formatarPublicacaoRecebida(
                                "cliente " + nome, topico, msg, recebido));
                    } else {
                        Protocolo.Log.log("[cliente " + nome + "] publicação descartada: tipo/canal inconsistente");
                    }
                } catch (Protocolo.ProtocoloException e) {
                    Protocolo.Log.log("[cliente " + nome + "] publicação descartada (inválida): " + e.getMessage());
                }
            }
        }

        void fechar() {
            sock.close();
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

    static void historico(Conexao con, String canal) throws Exception {
        Map<String, Object> pedido = new LinkedHashMap<>();
        pedido.put("channel", canal);
        pedido.put("limit", 5L);
        Map<String, Object> dados = con.requisitar(Protocolo.HISTORY, pedido).data();
        int total = Protocolo.OK.equals(dados.get("status")) && dados.get("messages") instanceof List<?> m ? m.size() : 0;
        Protocolo.Log.log("[cliente " + con.nome + "] histórico do canal '" + canal + "': " + total
                + " mensagem(ns) recuperada(s)");
    }

    static boolean publicar(Conexao con, String canal, String texto) throws Exception {
        Map<String, Object> pedido = new LinkedHashMap<>();
        pedido.put("channel", canal);
        pedido.put("message", texto);
        pedido.put("msg_id", UUID.randomUUID().toString());
        Map<String, Object> dados = con.requisitar(Protocolo.PUBLISH, pedido).data();
        if (Protocolo.OK.equals(dados.get("status"))) {
            return true;
        }
        Protocolo.Log.log("[cliente " + con.nome + "] publicação em '" + canal + "' falhou: "
                + dados.get("error") + " - " + dados.get("message"));
        return false;
    }

    static String textoAleatorio() {
        return FRASES[RANDOM.nextInt(FRASES.length)] + " [" + sufixoAleatorio().substring(0, 4) + "]";
    }

    static void rodada(Conexao con, Assinante assinante) throws Exception {
        List<String> canais = listarCanais(con);

        if (canais.size() < MIN_CANAIS) {
            String novo = "canal-" + sufixoAleatorio();
            if (criarCanal(con, novo)) {
                canais.add(novo);
            }
        }

        if (assinante.quantidade() < MIN_ASSINATURAS) {
            List<String> candidatos = new ArrayList<>();
            for (String canal : canais) {
                if (!assinante.contem(canal)) {
                    candidatos.add(canal);
                }
            }
            if (!candidatos.isEmpty()) {
                String escolhido = candidatos.get(RANDOM.nextInt(candidatos.size()));
                assinante.assinar(escolhido);
                historico(con, escolhido);
            }
        }

        if (canais.isEmpty()) {
            Protocolo.Log.log("[cliente " + con.nome + "] nenhum canal disponível, tentando novamente...");
            assinante.esperar(INTERVALO_MENSAGENS_MS);
            return;
        }

        String canal = canais.get(RANDOM.nextInt(canais.size()));
        Protocolo.Log.log("[cliente " + con.nome + "] publicando " + MENSAGENS_POR_RODADA
                + " mensagens no canal '" + canal + "'");
        for (int i = 0; i < MENSAGENS_POR_RODADA; i++) {
            publicar(con, canal, textoAleatorio());
            assinante.esperar(INTERVALO_MENSAGENS_MS);
        }
    }

    public static void executar() throws Exception {
        String nome = System.getenv("NOME_USUARIO");
        if (nome == null || nome.isBlank()) {
            nome = nomeAleatorio();
        }
        String host = System.getenv().getOrDefault("BROKER_HOST", "broker");
        String porta = System.getenv().getOrDefault("BROKER_FRONTEND_PORT", "5555");
        String proxyHost = System.getenv().getOrDefault("PROXY_HOST", "proxy");
        String portaXpub = System.getenv().getOrDefault("PROXY_XPUB_PORT", "5558");

        try (ZContext ctx = new ZContext()) {
            Conexao con = new Conexao(ctx, "tcp://" + host + ":" + porta, nome);
            Assinante assinante = new Assinante(ctx, "tcp://" + proxyHost + ":" + portaXpub, nome);
            try {
                fazerLogin(con);
                assinante.nome = con.nome;

                while (!Thread.currentThread().isInterrupted()) {
                    rodada(con, assinante);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                con.fechar();
                assinante.fechar();
            }
        }
    }
}
