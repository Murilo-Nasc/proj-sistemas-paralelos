import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import org.msgpack.core.MessageBufferPacker;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageUnpacker;
import org.msgpack.value.Value;

public final class Protocolo {
    private Protocolo() {}

    public record Mensagem(String type, String sender, long timestamp, long logicalClock, Map<String, Object> data) {}

    private static long relogioLogico = 0;
    private static long desvioRelogioMs = 0;

    public static class ProtocoloException extends Exception {
        private static final long serialVersionUID = 1L;

        public ProtocoloException(String mensagem) {
            super(mensagem);
        }

        public ProtocoloException(String mensagem, Throwable causa) {
            super(mensagem, causa);
        }
    }

    public static final class Log {
        private static final PrintStream OUT =
                new PrintStream(new FileOutputStream(FileDescriptor.out), true, StandardCharsets.UTF_8);

        private Log() {}

        public static synchronized void log(String texto) {
            OUT.println(texto);
            OUT.flush();
        }
    }

    public static final String LOGIN = "login";
    public static final String LOGIN_REPLY = "login_reply";
    public static final String CREATE_CHANNEL = "create_channel";
    public static final String CREATE_CHANNEL_REPLY = "create_channel_reply";
    public static final String LIST_CHANNELS = "list_channels";
    public static final String LIST_CHANNELS_REPLY = "list_channels_reply";
    public static final String PUBLISH = "publish";
    public static final String PUBLISH_REPLY = "publish_reply";
    public static final String PUBLICATION = "publication";
    public static final String HISTORY = "history";
    public static final String HISTORY_REPLY = "history_reply";
    public static final String ERROR_REPLY = "error_reply";

    public static final String OK = "ok";
    public static final String ERROR = "error";
    public static final String ERR_INVALID_NAME = "INVALID_NAME";
    public static final String ERR_ALREADY_EXISTS = "ALREADY_EXISTS";
    public static final String ERR_INVALID_REQUEST = "INVALID_REQUEST";
    public static final String ERR_INTERNAL = "INTERNAL_ERROR";

    public static final Pattern USER_RE = Pattern.compile("^[A-Za-z0-9_-]{3,20}$");
    public static final Pattern CHANNEL_RE = Pattern.compile("^[a-z0-9_-]{3,30}$");
    public static final int MAX_MENSAGEM = 500;
    public static final int MAX_MSG_ID = 64;

    private static final DateTimeFormatter HORA = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");

    public static long agoraMs() {
        return System.currentTimeMillis() + desvioRelogioMs;
    }

    public static synchronized void receberRelogioLogico(long recebido) {
        relogioLogico = Math.max(relogioLogico, recebido);
    }

    public static synchronized void ajustarRelogioFisico(long horaReferenciaMs) {
        desvioRelogioMs = horaReferenciaMs - System.currentTimeMillis();
    }

    public static synchronized Mensagem criarMensagem(String tipo, String remetente, Map<String, Object> dados) {
        relogioLogico++;
        return new Mensagem(tipo, remetente, agoraMs(), relogioLogico,
                dados == null ? new LinkedHashMap<>() : dados);
    }

    public static Map<String, Object> dadosOk(Object... paresChaveValor) {
        Map<String, Object> dados = new LinkedHashMap<>();
        dados.put("status", OK);
        for (int i = 0; i + 1 < paresChaveValor.length; i += 2) {
            dados.put((String) paresChaveValor[i], paresChaveValor[i + 1]);
        }
        return dados;
    }

    public static Map<String, Object> dadosErro(String codigo, String mensagem) {
        Map<String, Object> dados = new LinkedHashMap<>();
        dados.put("status", ERROR);
        dados.put("error", codigo);
        dados.put("message", mensagem);
        return dados;
    }

    public static byte[] serializar(Mensagem msg) {
        try (MessageBufferPacker packer = MessagePack.newDefaultBufferPacker()) {
            packer.packMapHeader(5);
            packer.packString("type");
            packer.packString(msg.type());
            packer.packString("sender");
            packer.packString(msg.sender());
            packer.packString("timestamp");
            packer.packLong(msg.timestamp());
            packer.packString("logical_clock");
            packer.packLong(msg.logicalClock());
            packer.packString("data");
            empacotar(packer, msg.data());
            return packer.toByteArray();
        } catch (Exception e) {
            throw new IllegalStateException("falha ao serializar a mensagem", e);
        }
    }

    private static void empacotar(MessageBufferPacker packer, Object valor) throws java.io.IOException {
        if (valor == null) {
            packer.packNil();
        } else if (valor instanceof String s) {
            packer.packString(s);
        } else if (valor instanceof Boolean b) {
            packer.packBoolean(b);
        } else if (valor instanceof Integer || valor instanceof Long || valor instanceof Short) {
            packer.packLong(((Number) valor).longValue());
        } else if (valor instanceof Double || valor instanceof Float) {
            packer.packDouble(((Number) valor).doubleValue());
        } else if (valor instanceof Map<?, ?> mapa) {
            packer.packMapHeader(mapa.size());
            for (Map.Entry<?, ?> e : mapa.entrySet()) {
                packer.packString(String.valueOf(e.getKey()));
                empacotar(packer, e.getValue());
            }
        } else if (valor instanceof List<?> lista) {
            packer.packArrayHeader(lista.size());
            for (Object item : lista) {
                empacotar(packer, item);
            }
        } else {
            throw new IllegalArgumentException("tipo não suportado: " + valor.getClass());
        }
    }

    public static Mensagem desserializar(byte[] raw) throws ProtocoloException {
        Object obj;
        try (MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(raw)) {
            Value valor = unpacker.unpackValue();
            if (unpacker.hasNext()) {
                throw new ProtocoloException("bytes extras após o envelope");
            }
            obj = converter(valor);
        } catch (ProtocoloException e) {
            throw e;
        } catch (Exception e) {
            throw new ProtocoloException("MessagePack inválido: " + e.getMessage(), e);
        }

        if (!(obj instanceof Map<?, ?> mapa)) {
            throw new ProtocoloException("a mensagem deve ser um mapa");
        }
        for (String campo : new String[] {"type", "sender", "timestamp", "logical_clock", "data"}) {
            if (!mapa.containsKey(campo)) {
                throw new ProtocoloException("campo obrigatório ausente: " + campo);
            }
        }
        if (!(mapa.get("type") instanceof String tipo) || !(mapa.get("sender") instanceof String remetente)) {
            throw new ProtocoloException("'type' e 'sender' devem ser strings");
        }
        if (!(mapa.get("timestamp") instanceof Long timestamp)) {
            throw new ProtocoloException("'timestamp' deve ser inteiro (ms)");
        }
        if (!(mapa.get("logical_clock") instanceof Long logicalClock)) {
            throw new ProtocoloException("'logical_clock' deve ser inteiro");
        }
        if (!(mapa.get("data") instanceof Map<?, ?> dadosBrutos)) {
            throw new ProtocoloException("'data' deve ser um mapa");
        }
        Map<String, Object> dados = new LinkedHashMap<>();
        for (Map.Entry<?, ?> e : dadosBrutos.entrySet()) {
            dados.put((String) e.getKey(), e.getValue());
        }
        return new Mensagem(tipo, remetente, timestamp, logicalClock, dados);
    }

    private static Object converter(Value v) throws ProtocoloException {
        switch (v.getValueType()) {
            case NIL:
                return null;
            case BOOLEAN:
                return v.asBooleanValue().getBoolean();
            case INTEGER:
                return v.asIntegerValue().toLong();
            case FLOAT:
                return v.asFloatValue().toDouble();
            case STRING:
                return v.asStringValue().asString();
            case BINARY:
                return v.asBinaryValue().asByteArray();
            case ARRAY: {
                List<Object> lista = new ArrayList<>();
                for (Value item : v.asArrayValue()) {
                    lista.add(converter(item));
                }
                return lista;
            }
            case MAP: {
                Map<String, Object> mapa = new LinkedHashMap<>();
                for (Map.Entry<Value, Value> e : v.asMapValue().entrySet()) {
                    if (!e.getKey().isStringValue()) {
                        throw new ProtocoloException("chaves do mapa devem ser strings");
                    }
                    mapa.put(e.getKey().asStringValue().asString(), converter(e.getValue()));
                }
                return mapa;
            }
            default:
                throw new ProtocoloException("tipo MessagePack não suportado: " + v.getValueType());
        }
    }

    public static String formatarLog(String quem, String direcao, Mensagem msg) {
        return "[" + LocalTime.now().format(HORA) + "] [" + quem + "] " + direcao + " " + msg.type()
                + " | de=" + msg.sender() + " | ts=" + msg.timestamp() + " | logical_clock=" + msg.logicalClock() + " | dados=" + repr(msg.data());
    }

    public static String formatarPublicacaoRecebida(String quem, String topico, Mensagem msg, long recebidoMs) {
        Map<String, Object> dados = msg.data();
        return "[" + LocalTime.now().format(HORA) + "] [" + quem + "] RECEBIDO << PUBLICAÇÃO canal=" + topico
                + " | mensagem=" + repr(dados.get("message")) + " | autor=" + dados.get("user")
                + " | servidor=" + msg.sender() + " | enviado_ts=" + msg.timestamp() + " | logical_clock=" + msg.logicalClock()
                + " | recebido_ts=" + recebidoMs + " | atraso=" + (recebidoMs - msg.timestamp())
                + " ms | msg_id=" + dados.get("msg_id");
    }

    static String repr(Object valor) {
        if (valor == null) {
            return "None";
        }
        if (valor instanceof String s) {
            return "'" + s.replace("\\", "\\\\").replace("'", "\\'") + "'";
        }
        if (valor instanceof Boolean b) {
            return b ? "True" : "False";
        }
        if (valor instanceof Map<?, ?> mapa) {
            StringBuilder sb = new StringBuilder("{");
            boolean primeiro = true;
            for (Map.Entry<?, ?> e : mapa.entrySet()) {
                if (!primeiro) {
                    sb.append(", ");
                }
                primeiro = false;
                sb.append(repr(e.getKey())).append(": ").append(repr(e.getValue()));
            }
            return sb.append("}").toString();
        }
        if (valor instanceof List<?> lista) {
            StringBuilder sb = new StringBuilder("[");
            for (int i = 0; i < lista.size(); i++) {
                if (i > 0) {
                    sb.append(", ");
                }
                sb.append(repr(lista.get(i)));
            }
            return sb.append("]").toString();
        }
        return String.valueOf(valor);
    }
}
