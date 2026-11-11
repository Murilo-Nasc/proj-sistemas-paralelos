import os
import random
import secrets
import signal
import sys
import uuid
from time import monotonic, sleep

import zmq

import protocolo as p
from protocolo import log

MIN_CANAIS = 5
MIN_ASSINATURAS = 3
MENSAGENS_POR_RODADA = 10
INTERVALO_MENSAGENS_S = 1
TIMEOUT_MS = 3000

FRASES = [
    "Bom dia, pessoal!",
    "Alguém viu o resultado do jogo?",
    "Estou testando o sistema distribuído",
    "ZeroMQ é bem mais simples do que parece",
    "Quem está online por aqui?",
    "Mais uma mensagem aleatória",
    "O broker está distribuindo bem as requisições",
    "Pub/Sub funcionando por aqui",
    "Vamos ver se essa chega a todo mundo",
    "Até já, volto em alguns segundos",
]


class Conexao:
    def __init__(self, ctx: zmq.Context, endpoint: str, nome: str) -> None:
        self.ctx = ctx
        self.endpoint = endpoint
        self.nome = nome
        self.sock = self._abrir()

    def _abrir(self) -> zmq.Socket:
        sock = self.ctx.socket(zmq.REQ)
        sock.connect(self.endpoint)
        return sock

    def requisitar(self, tipo: str, dados: dict) -> dict:
        while True:
            msg = p.criar_mensagem(tipo, self.nome, dados)
            self.sock.send(p.serializar(msg))
            log(p.formatar_log(f"cliente {self.nome}", "ENVIADO  >>", msg))
            if self.sock.poll(TIMEOUT_MS):
                resposta = p.desserializar(self.sock.recv())
                log(p.formatar_log(f"cliente {self.nome}", "RECEBIDO <<", resposta))
                return resposta
            log(f"[cliente {self.nome}] sem resposta em {TIMEOUT_MS} ms, tentando de novo...")
            self.sock.close(linger=0)
            self.sock = self._abrir()


class Assinante:
    def __init__(self, ctx: zmq.Context, endpoint: str, nome: str) -> None:
        self.nome = nome
        self.canais: set[str] = set()
        self.sock = ctx.socket(zmq.SUB)
        self.sock.connect(endpoint)

    def assinar(self, canal: str) -> None:
        self.sock.setsockopt(zmq.SUBSCRIBE, canal.encode("utf-8"))
        self.canais.add(canal)
        log(f"[cliente {self.nome}] inscrito no canal '{canal}' (total: {len(self.canais)})")

    def _processar(self, frames: list[bytes]) -> None:
        recebido_ms = p.agora_ms()
        if len(frames) != 2:
            log(f"[cliente {self.nome}] mensagem descartada: esperava 2 frames, veio {len(frames)}")
            return
        topico = frames[0].decode("utf-8", errors="replace")
        if topico not in self.canais:
            return
        try:
            msg = p.desserializar(frames[1])
        except ValueError as e:
            log(f"[cliente {self.nome}] publicação descartada (inválida): {e}")
            return
        if msg["type"] != p.PUBLICATION or msg["data"].get("channel") != topico:
            log(f"[cliente {self.nome}] publicação descartada: tipo/canal inconsistente")
            return
        log(p.formatar_publicacao_recebida(f"cliente {self.nome}", topico, msg, recebido_ms))

    def esperar(self, segundos: float) -> None:
        """Equivale a sleep(), mas imprime as publicações que chegarem enquanto espera."""
        limite = monotonic() + segundos
        while True:
            restante_ms = max(0, int((limite - monotonic()) * 1000))
            if not self.sock.poll(restante_ms):
                break
            self._processar(self.sock.recv_multipart())


def nome_aleatorio() -> str:
    return f"bot_{secrets.token_hex(2)}"


def fazer_login(con: Conexao) -> None:
    while True:
        resposta = con.requisitar(p.LOGIN, {"user": con.nome})
        dados = resposta["data"]
        if dados.get("status") == p.OK:
            log(f"[cliente {con.nome}] login realizado com sucesso")
            return
        log(f"[cliente {con.nome}] login falhou: {dados.get('error')} - {dados.get('message')}")
        if dados.get("error") == p.ERR_INVALID_NAME:
            con.nome = nome_aleatorio()
            log(f"[cliente {con.nome}] usando novo nome de usuário: {con.nome}")
        sleep(2)


def listar_canais(con: Conexao) -> list[str]:
    dados = con.requisitar(p.LIST_CHANNELS, {})["data"]
    return list(dados.get("channels", [])) if dados.get("status") == p.OK else []


def criar_canal(con: Conexao, nome_canal: str) -> bool:
    dados = con.requisitar(p.CREATE_CHANNEL, {"channel": nome_canal})["data"]
    if dados.get("status") == p.OK:
        return True
    log(f"[cliente {con.nome}] canal '{nome_canal}' não criado: {dados.get('error')}")
    return False


def publicar(con: Conexao, canal: str, texto: str) -> bool:
    pedido = {"channel": canal, "message": texto, "msg_id": str(uuid.uuid4())}
    dados = con.requisitar(p.PUBLISH, pedido)["data"]
    if dados.get("status") == p.OK:
        return True
    log(
        f"[cliente {con.nome}] publicação em '{canal}' falhou: "
        f"{dados.get('error')} - {dados.get('message')}"
    )
    return False


def texto_aleatorio() -> str:
    return f"{random.choice(FRASES)} [{secrets.token_hex(2)}]"


def rodada(con: Conexao, assinante: Assinante) -> None:
    canais = listar_canais(con)

    if len(canais) < MIN_CANAIS:
        novo = f"canal-{secrets.token_hex(3)}"
        if criar_canal(con, novo):
            canais.append(novo)

    if len(assinante.canais) < MIN_ASSINATURAS:
        candidatos = [c for c in canais if c not in assinante.canais]
        if candidatos:
            assinante.assinar(random.choice(candidatos))

    if not canais:
        log(f"[cliente {con.nome}] nenhum canal disponível, tentando de novo...")
        assinante.esperar(INTERVALO_MENSAGENS_S)
        return

    canal = random.choice(canais)
    log(f"[cliente {con.nome}] publicando {MENSAGENS_POR_RODADA} mensagens no canal '{canal}'")
    for _ in range(MENSAGENS_POR_RODADA):
        publicar(con, canal, texto_aleatorio())
        assinante.esperar(INTERVALO_MENSAGENS_S)


def main() -> None:
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))

    nome = os.getenv("NOME_USUARIO") or nome_aleatorio()
    host = os.getenv("BROKER_HOST", "broker")
    porta = os.getenv("BROKER_FRONTEND_PORT", "5555")
    proxy_host = os.getenv("PROXY_HOST", "proxy")
    porta_xpub = os.getenv("PROXY_XPUB_PORT", "5558")

    ctx = zmq.Context()
    con = Conexao(ctx, f"tcp://{host}:{porta}", nome)
    assinante = Assinante(ctx, f"tcp://{proxy_host}:{porta_xpub}", nome)

    try:
        fazer_login(con)
        assinante.nome = con.nome
        while True:
            rodada(con, assinante)
    except (KeyboardInterrupt, SystemExit):
        pass
    finally:
        con.sock.close(linger=0)
        assinante.sock.close(linger=0)
        ctx.term()


if __name__ == "__main__":
    main()
