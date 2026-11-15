import os
import signal
import socket as pysocket
import sqlite3
import sys
from time import sleep

import zmq

import protocolo as p
from protocolo import log


class Banco:
    def __init__(self, caminho: str) -> None:
        pasta = os.path.dirname(caminho)
        if pasta:
            os.makedirs(pasta, exist_ok=True)
        self.con = sqlite3.connect(caminho)
        self.con.executescript(
            """
            CREATE TABLE IF NOT EXISTS logins (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                usuario      TEXT    NOT NULL,
                timestamp_ms INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS canais (
                nome         TEXT    PRIMARY KEY,
                criado_por   TEXT    NOT NULL,
                timestamp_ms INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS publicacoes (
                id               INTEGER PRIMARY KEY AUTOINCREMENT,
                msg_id           TEXT    NOT NULL UNIQUE,
                canal            TEXT    NOT NULL,
                usuario          TEXT    NOT NULL,
                mensagem         TEXT    NOT NULL,
                ts_cliente_ms    INTEGER NOT NULL,
                ts_publicacao_ms INTEGER NOT NULL,
                servidor         TEXT    NOT NULL
            );
            """
        )
        self.con.commit()

    def registrar_login(self, usuario: str, ts: int) -> None:
        self.con.execute(
            "INSERT INTO logins (usuario, timestamp_ms) VALUES (?, ?)", (usuario, ts)
        )
        self.con.commit()

    def criar_canal(self, nome: str, criado_por: str, ts: int) -> bool:
        """Retorna False se o canal já existir."""
        try:
            self.con.execute(
                "INSERT INTO canais (nome, criado_por, timestamp_ms) VALUES (?, ?, ?)",
                (nome, criado_por, ts),
            )
            self.con.commit()
            return True
        except sqlite3.IntegrityError:
            return False

    def listar_canais(self) -> list[str]:
        linhas = self.con.execute("SELECT nome FROM canais ORDER BY nome").fetchall()
        return [linha[0] for linha in linhas]

    def registrar_publicacao(
        self,
        msg_id: str,
        canal: str,
        usuario: str,
        mensagem: str,
        ts_cliente: int,
        ts_publicacao: int,
        servidor: str,
    ) -> bool:
        cur = self.con.execute(
            "INSERT OR IGNORE INTO publicacoes "
            "(msg_id, canal, usuario, mensagem, ts_cliente_ms, ts_publicacao_ms, servidor) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (msg_id, canal, usuario, mensagem, ts_cliente, ts_publicacao, servidor),
        )
        self.con.commit()
        return cur.rowcount == 1

    def remover_publicacao(self, msg_id: str) -> None:
        self.con.execute("DELETE FROM publicacoes WHERE msg_id = ?", (msg_id,))
        self.con.commit()



class Referencia:
    """Cliente REQ do serviço de referência, independente do broker."""
    def __init__(self, ctx: zmq.Context, endpoint: str, nome: str) -> None:
        self.ctx = ctx
        self.endpoint = endpoint
        self.nome = nome
        self.sock = self._abrir()

    def _abrir(self):
        sock = self.ctx.socket(zmq.REQ)
        sock.setsockopt(zmq.LINGER, 0)
        sock.setsockopt(zmq.RCVTIMEO, 3000)
        sock.connect(self.endpoint)
        return sock

    def requisitar(self, tipo: str, dados: dict) -> dict:
        while True:
            msg = p.criar_mensagem(tipo, self.nome, dados)
            self.sock.send(p.serializar(msg))
            log(p.formatar_log(f"servidor {self.nome} -> referência", "ENVIADO  >>", msg))
            try:
                resposta = p.desserializar(self.sock.recv())
                p.receber_relogio_logico(resposta["logical_clock"])
                log(p.formatar_log(f"servidor {self.nome} -> referência", "RECEBIDO <<", resposta))
                return resposta
            except zmq.Again:
                self.sock.close(linger=0)
                self.sock = self._abrir()
                sleep(1)

    def fechar(self) -> None:
        self.sock.close(linger=0)


def publicar(msg: dict, banco: Banco, pub: zmq.Socket, nome: str) -> dict:
    dados = msg["data"]
    canal = dados.get("channel")
    texto = dados.get("message")
    msg_id = dados.get("msg_id")

    if not isinstance(canal, str) or not p.CHANNEL_RE.match(canal):
        return p.dados_erro(
            p.ERR_INVALID_NAME,
            "canal deve ter 3 a 30 caracteres: letras minúsculas, números, '_' ou '-'",
        )
    if not isinstance(texto, str) or not texto.strip() or len(texto) > p.MAX_MENSAGEM:
        return p.dados_erro(
            p.ERR_INVALID_REQUEST, f"mensagem deve ter de 1 a {p.MAX_MENSAGEM} caracteres"
        )
    if not isinstance(msg_id, str) or not 1 <= len(msg_id) <= p.MAX_MSG_ID:
        return p.dados_erro(
            p.ERR_INVALID_REQUEST, f"msg_id deve ter de 1 a {p.MAX_MSG_ID} caracteres"
        )

    publicacao = p.criar_mensagem(
        p.PUBLICATION,
        nome,
        {"channel": canal, "message": texto, "user": msg["sender"], "msg_id": msg_id},
    )
    novo = banco.registrar_publicacao(
        msg_id, canal, msg["sender"], texto, msg["timestamp"], publicacao["timestamp"], nome
    )
    quem = f"servidor {nome}"
    if not novo:
        log(f"[{quem}] msg_id {msg_id} já registrado, não será publicado de novo")
        return p.dados_ok(msg_id=msg_id)

    try:
        pub.send_multipart([canal.encode("utf-8"), p.serializar(publicacao)])
    except zmq.ZMQError:
        banco.remover_publicacao(msg_id)
        raise
    log(p.formatar_log(quem, f"PUBLICADO >> [{canal}]", publicacao))
    return p.dados_ok(msg_id=msg_id)


def tratar(msg: dict, banco: Banco, pub: zmq.Socket, nome: str) -> tuple[str, dict]:
    tipo = msg["type"]
    dados = msg["data"]
    agora = p.agora_ms()

    if tipo == p.LOGIN:
        usuario = dados.get("user")
        if not isinstance(usuario, str) or not p.USER_RE.match(usuario):
            return p.LOGIN_REPLY, p.dados_erro(
                p.ERR_INVALID_NAME,
                "usuário deve ter 3 a 20 caracteres: letras, números, '_' ou '-'",
            )
        banco.registrar_login(usuario, agora)
        return p.LOGIN_REPLY, p.dados_ok(user=usuario)

    if tipo == p.CREATE_CHANNEL:
        canal = dados.get("channel")
        if not isinstance(canal, str) or not p.CHANNEL_RE.match(canal):
            return p.CREATE_CHANNEL_REPLY, p.dados_erro(
                p.ERR_INVALID_NAME,
                "canal deve ter 3 a 30 caracteres: letras minúsculas, números, '_' ou '-'",
            )
        if not banco.criar_canal(canal, msg["sender"], agora):
            return p.CREATE_CHANNEL_REPLY, p.dados_erro(
                p.ERR_ALREADY_EXISTS, f"o canal '{canal}' já existe"
            )
        return p.CREATE_CHANNEL_REPLY, p.dados_ok(channel=canal)

    if tipo == p.LIST_CHANNELS:
        return p.LIST_CHANNELS_REPLY, p.dados_ok(channels=banco.listar_canais())

    if tipo == p.PUBLISH:
        return p.PUBLISH_REPLY, publicar(msg, banco, pub, nome)

    return p.ERROR_REPLY, p.dados_erro(
        p.ERR_INVALID_REQUEST, f"tipo de mensagem desconhecido: {tipo}"
    )


def main() -> None:
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))

    nome = os.getenv("NOME_SERVIDOR") or pysocket.gethostname()
    host = os.getenv("BROKER_HOST", "broker")
    porta = os.getenv("BROKER_BACKEND_PORT", "5556")
    proxy_host = os.getenv("PROXY_HOST", "proxy")
    porta_xsub = os.getenv("PROXY_XSUB_PORT", "5557")
    caminho_db = os.getenv("DB_PATH", f"/data/{nome}.db")
    ref_host = os.getenv("REFERENCE_HOST", "referencia")
    ref_porta = os.getenv("REFERENCE_PORT", "5559")

    banco = Banco(caminho_db)

    ctx = zmq.Context()
    sock = ctx.socket(zmq.REP)
    sock.connect(f"tcp://{host}:{porta}")
    pub = ctx.socket(zmq.PUB)
    referencia = Referencia(ctx, f"tcp://{ref_host}:{ref_porta}", nome)
    pub.connect(f"tcp://{proxy_host}:{porta_xsub}")
    quem = f"servidor {nome}"
    log(
        f"[{quem}] conectado ao broker em {host}:{porta} e ao proxy em "
        f"{proxy_host}:{porta_xsub} e à referência em {ref_host}:{ref_porta} | banco: {caminho_db}"
    )

    cadastro = referencia.requisitar("rank", {"name": nome})
    rank = cadastro.get("data", {}).get("rank")
    log(f"[{quem}] rank atribuído pela referência: {rank}")
    lista = referencia.requisitar("list", {})
    log(f"[{quem}] servidores disponíveis: {lista.get('data', {}).get('servers', [])}")
    recebidas = 0

    try:
        while True:
            raw = sock.recv()
            try:
                msg = p.desserializar(raw)
                p.receber_relogio_logico(msg["logical_clock"])
                log(p.formatar_log(quem, "RECEBIDO <<", msg))
                tipo_resp, dados = tratar(msg, banco, pub, nome)
            except ValueError as e:
                tipo_resp, dados = p.ERROR_REPLY, p.dados_erro(p.ERR_INVALID_REQUEST, str(e))
            except Exception as e:  # erro inesperado: não derruba o servidor
                tipo_resp, dados = p.ERROR_REPLY, p.dados_erro(p.ERR_INTERNAL, str(e))

            resposta = p.criar_mensagem(tipo_resp, nome, dados)
            sock.send(p.serializar(resposta))
            log(p.formatar_log(quem, "ENVIADO  >>", resposta))
            recebidas += 1
            if recebidas % 10 == 0:
                hb = referencia.requisitar("heartbeat", {"name": nome})
                if hb.get("data", {}).get("status") != p.OK:
                    cadastro = referencia.requisitar("rank", {"name": nome})
                    rank = cadastro.get("data", {}).get("rank", rank)
                    hb = referencia.requisitar("heartbeat", {"name": nome})
                if hb.get("data", {}).get("status") == p.OK:
                    hora_ref = hb.get("timestamp")
                    p.ajustar_relogio_fisico(hora_ref)
                    log(f"[{quem}] heartbeat OK | hora sincronizada: {p.agora_ms()} | rank={rank}")
    except (KeyboardInterrupt, SystemExit):
        pass
    finally:
        sock.close(linger=0)
        pub.close(linger=0)
        referencia.fechar()
        ctx.term()
        banco.con.close()


if __name__ == "__main__":
    main()
