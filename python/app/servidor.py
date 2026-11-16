import os
import signal
import socket as pysocket
import sqlite3
import sys
import time
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

    def requisitar(self, tipo: str, dados: dict, max_tentativas: int | None = None) -> dict | None:
        tentativa = 0
        while True:
            tentativa += 1
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
                if max_tentativas is not None and tentativa >= max_tentativas:
                    log(f"[servidor {self.nome}] referência sem resposta após {tentativa} tentativa(s)")
                    return None
                sleep(1)

    def fechar(self) -> None:
        self.sock.close(linger=0)


CLOCK = "clock"
CLOCK_REPLY = "clock_reply"
ELECTION = "election"
ELECTION_REPLY = "election_reply"
COORDINATOR = "coordinator"
TOPICO_SERVERS = "servers"
ERR_NOT_COORDINATOR = "NOT_COORDINATOR"
INTERVALO_SYNC = 15
ESPERA_ELEICAO_S = 5.0


def requisicao_unica(ctx: zmq.Context, endpoint: str, tipo: str, remetente: str,
                     dados: dict, timeout_ms: int) -> dict | None:
    sock = ctx.socket(zmq.REQ)
    sock.setsockopt(zmq.LINGER, 0)
    sock.setsockopt(zmq.RCVTIMEO, timeout_ms)
    sock.setsockopt(zmq.SNDTIMEO, timeout_ms)
    quem = f"servidor {remetente} -> {endpoint}"
    try:
        sock.connect(endpoint)
        msg = p.criar_mensagem(tipo, remetente, dados)
        sock.send(p.serializar(msg))
        log(p.formatar_log(quem, "ENVIADO  >>", msg))
        resposta = p.desserializar(sock.recv())
        p.receber_relogio_logico(resposta["logical_clock"])
        log(p.formatar_log(quem, "RECEBIDO <<", resposta))
        return resposta
    except (zmq.Again, zmq.ZMQError, ValueError):
        log(f"[{quem}] sem resposta ({tipo})")
        return None
    finally:
        sock.close(linger=0)


class Coordenacao:
    def __init__(self, ctx, nome, rank, referencia, pub, porta_servidores, timeout_par_ms):
        self.ctx = ctx
        self.nome = nome
        self.rank = rank
        self.referencia = referencia
        self.pub = pub
        self.porta = porta_servidores
        self.timeout_par_ms = timeout_par_ms
        self.coordenador: str | None = None
        self.eleicao_pendente = False
        self.aguardando_ate = 0.0
        self.lista: list[dict] = []
        self.quem = f"servidor {nome}"

    def _endpoint(self, nome: str) -> str:
        return f"tcp://{nome}:{self.porta}"

    def _atualizar_lista(self) -> None:
        resp = self.referencia.requisitar("list", {}, max_tentativas=2)
        if resp and resp.get("data", {}).get("status") == p.OK:
            self.lista = resp["data"].get("servers", [])

    def precisa_eleger(self) -> bool:
        return self.eleicao_pendente or (
            self.coordenador is None and time.monotonic() >= self.aguardando_ate)

    def eleicao(self) -> None:
        self.eleicao_pendente = False
        self._atualizar_lista()
        maiores = [s for s in self.lista if s["rank"] > self.rank and s["name"] != self.nome]
        log(f"[{self.quem}] iniciando eleição (rank={self.rank}); servidores de rank maior: "
            f"{[s['name'] for s in maiores]}")
        alguem_respondeu = False
        for par in maiores:
            resp = requisicao_unica(self.ctx, self._endpoint(par["name"]), ELECTION, self.nome,
                                    {"rank": self.rank}, self.timeout_par_ms)
            if resp and resp.get("data", {}).get("status") == p.OK:
                alguem_respondeu = True
        if alguem_respondeu:
            self.aguardando_ate = time.monotonic() + ESPERA_ELEICAO_S
            log(f"[{self.quem}] um servidor de rank maior respondeu OK; aguardando o anúncio do coordenador")
        else:
            self.assumir()

    def assumir(self) -> None:
        self.coordenador = self.nome
        anuncio = p.criar_mensagem(COORDINATOR, self.nome,
                                   {"coordinator": self.nome, "rank": self.rank})
        self.pub.send_multipart([TOPICO_SERVERS.encode("utf-8"), p.serializar(anuncio)])
        log(p.formatar_log(self.quem, f"PUBLICADO >> [{TOPICO_SERVERS}] (sou o novo coordenador)", anuncio))

    def sincronizar(self) -> None:
        if self.coordenador is None:
            return
        if self.coordenador == self.nome:
            log(f"[{self.quem}] sou o coordenador: relógio de referência = {p.agora_ms()}")
            return
        resp = requisicao_unica(self.ctx, self._endpoint(self.coordenador), CLOCK, self.nome,
                                {}, self.timeout_par_ms)
        hora = resp.get("data", {}).get("time") if resp else None
        if resp and resp["data"].get("status") == p.OK and isinstance(hora, int):
            antes = p.agora_ms()
            p.ajustar_relogio_fisico(hora)
            log(f"[{self.quem}] Berkeley: relógio {antes} -> {p.agora_ms()} "
                f"(coordenador {self.coordenador} informou {hora}; ajuste {hora - antes} ms)")
        else:
            log(f"[{self.quem}] coordenador '{self.coordenador}' indisponível; iniciando eleição")
            self.coordenador = None
            self.eleicao()

    def tratar_par(self, msg: dict) -> tuple[str, dict]:
        tipo = msg["type"]
        if tipo == CLOCK:
            if self.coordenador == self.nome:
                return CLOCK_REPLY, p.dados_ok(time=p.agora_ms(), coordinator=self.nome)
            return CLOCK_REPLY, p.dados_erro(ERR_NOT_COORDINATOR, "este servidor não é o coordenador")
        if tipo == ELECTION:
            rank_pedido = msg["data"].get("rank")
            if isinstance(rank_pedido, int) and rank_pedido < self.rank:
                self.eleicao_pendente = True   # Bully: quem tem rank maior assume a eleição
            return ELECTION_REPLY, p.dados_ok()
        return p.ERROR_REPLY, p.dados_erro(p.ERR_INVALID_REQUEST, f"tipo desconhecido: {tipo}")

    def tratar_anuncio(self, topico: bytes, payload: bytes) -> None:
        try:
            msg = p.desserializar(payload)
        except ValueError as e:
            log(f"[{self.quem}] anúncio descartado (inválido): {e}")
            return
        p.receber_relogio_logico(msg["logical_clock"])
        novo = msg["data"].get("coordinator")
        if msg["type"] != COORDINATOR or topico.decode("utf-8", "replace") != TOPICO_SERVERS \
                or not isinstance(novo, str):
            return
        self.aguardando_ate = 0.0
        if novo != self.coordenador:
            log(f"[{self.quem}] RECEBIDO << [{TOPICO_SERVERS}] novo coordenador: {novo} "
                f"(anterior: {self.coordenador})")
        self.coordenador = novo


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
    porta_xpub = os.getenv("PROXY_XPUB_PORT", "5558")
    caminho_db = os.getenv("DB_PATH", f"/data/{nome}.db")
    ref_host = os.getenv("REFERENCE_HOST", "referencia")
    ref_porta = os.getenv("REFERENCE_PORT", "5559")
    porta_servidores = os.getenv("SERVER_PORT", "5560")
    bind_servidores = os.getenv("SERVER_BIND_HOST", "*")
    timeout_par_ms = int(os.getenv("PEER_TIMEOUT_MS", "1500"))
    desvio_inicial_ms = int(os.getenv("CLOCK_OFFSET_MS", "0"))

    banco = Banco(caminho_db)

    ctx = zmq.Context()
    sock = ctx.socket(zmq.REP)
    sock.connect(f"tcp://{host}:{porta}")
    pub = ctx.socket(zmq.PUB)
    pub.connect(f"tcp://{proxy_host}:{porta_xsub}")
    sub = ctx.socket(zmq.SUB)
    sub.connect(f"tcp://{proxy_host}:{porta_xpub}")
    sub.setsockopt(zmq.SUBSCRIBE, TOPICO_SERVERS.encode("utf-8"))
    par = ctx.socket(zmq.REP)               # comunicação servidor <-> servidor
    par.setsockopt(zmq.LINGER, 0)
    par.bind(f"tcp://{bind_servidores}:{porta_servidores}")
    referencia = Referencia(ctx, f"tcp://{ref_host}:{ref_porta}", nome)
    quem = f"servidor {nome}"
    log(
        f"[{quem}] conectado ao broker em {host}:{porta}, ao proxy em "
        f"{proxy_host}:{porta_xsub}/{porta_xpub} e à referência em {ref_host}:{ref_porta} | "
        f"porta servidor-servidor: {porta_servidores} | banco: {caminho_db}"
    )

    if desvio_inicial_ms:
        p.ajustar_relogio_fisico(time.time_ns() // 1_000_000 + desvio_inicial_ms)
        log(f"[{quem}] relógio começa com desvio simulado de {desvio_inicial_ms} ms: {p.agora_ms()}")

    cadastro = referencia.requisitar("rank", {"name": nome})
    rank = cadastro.get("data", {}).get("rank")
    log(f"[{quem}] rank atribuído pela referência: {rank}")
    coord = Coordenacao(ctx, nome, rank, referencia, pub, porta_servidores, timeout_par_ms)
    recebidas = 0

    poller = zmq.Poller()
    poller.register(sock, zmq.POLLIN)
    poller.register(par, zmq.POLLIN)
    poller.register(sub, zmq.POLLIN)

    try:
        while True:
            eventos = dict(poller.poll(1000))

            if par in eventos:
                try:
                    msg = p.desserializar(par.recv())
                    p.receber_relogio_logico(msg["logical_clock"])
                    log(p.formatar_log(quem, "RECEBIDO << (servidor)", msg))
                    tipo_resp, dados = coord.tratar_par(msg)
                except ValueError as e:
                    tipo_resp, dados = p.ERROR_REPLY, p.dados_erro(p.ERR_INVALID_REQUEST, str(e))
                resposta = p.criar_mensagem(tipo_resp, nome, dados)
                par.send(p.serializar(resposta))
                log(p.formatar_log(quem, "ENVIADO  >> (servidor)", resposta))

            if sub in eventos:
                partes = sub.recv_multipart()
                if len(partes) == 2:
                    coord.tratar_anuncio(partes[0], partes[1])

            if sock in eventos:
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
                    hb = referencia.requisitar("heartbeat", {"name": nome}, max_tentativas=2)
                    if hb and hb.get("data", {}).get("status") != p.OK:
                        cadastro = referencia.requisitar("rank", {"name": nome}, max_tentativas=2)
                        if cadastro and cadastro.get("data", {}).get("status") == p.OK:
                            rank = cadastro["data"].get("rank", rank)
                            coord.rank = rank
                            hb = referencia.requisitar("heartbeat", {"name": nome}, max_tentativas=2)
                    if hb and hb.get("data", {}).get("status") == p.OK:
                        log(f"[{quem}] heartbeat OK | rank={rank}")
                    else:
                        log(f"[{quem}] heartbeat não confirmado; seguindo sem a referência")
                if recebidas % INTERVALO_SYNC == 0:
                    coord.sincronizar()

            if coord.precisa_eleger():
                coord.eleicao()
    except (KeyboardInterrupt, SystemExit):
        pass
    finally:
        sock.close(linger=0)
        pub.close(linger=0)
        sub.close(linger=0)
        par.close(linger=0)
        referencia.fechar()
        ctx.term()
        banco.con.close()


if __name__ == "__main__":
    main()
