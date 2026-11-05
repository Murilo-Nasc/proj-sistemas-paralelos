import os
import secrets
import signal
import sys
from time import sleep

import zmq

import protocolo as p
from protocolo import log

MIN_CANAIS = 5
TIMEOUT_MS = 3000
INTERVALO_LISTAGEM_S = 5


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
    return dados.get("channels", []) if dados.get("status") == p.OK else []


def criar_canal(con: Conexao, nome_canal: str) -> bool:
    """ALREADY_EXISTS não é uma falha grave: o canal já está disponível."""
    dados = con.requisitar(p.CREATE_CHANNEL, {"channel": nome_canal})["data"]
    if dados.get("status") == p.OK:
        return True
    log(f"[cliente {con.nome}] canal '{nome_canal}' não criado: {dados.get('error')}")
    return False


def main() -> None:
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))

    nome = os.getenv("NOME_USUARIO") or nome_aleatorio()
    host = os.getenv("BROKER_HOST", "broker")
    porta = os.getenv("BROKER_FRONTEND_PORT", "5555")

    ctx = zmq.Context()
    con = Conexao(ctx, f"tcp://{host}:{porta}", nome)

    try:
        fazer_login(con)

        canais = listar_canais(con)
        if "geral" not in canais:
            criar_canal(con, "geral")
        canais = listar_canais(con)
        if len(canais) < MIN_CANAIS:
            criar_canal(con, f"canal-{secrets.token_hex(3)}")

        while True:
            listar_canais(con)
            sleep(INTERVALO_LISTAGEM_S)
    except (KeyboardInterrupt, SystemExit):
        pass
    finally:
        con.sock.close(linger=0)
        ctx.term()


if __name__ == "__main__":
    main()
