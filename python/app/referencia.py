import os
import signal
import sys
import zmq
import protocolo as p
from protocolo import log

TIMEOUT_HEARTBEAT_MS = int(os.getenv("HEARTBEAT_TIMEOUT_MS", "60000"))

def main() -> None:
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))
    porta = os.getenv("REFERENCE_PORT", "5559")
    ctx = zmq.Context()
    sock = ctx.socket(zmq.REP)
    sock.setsockopt(zmq.LINGER, 0)
    sock.bind(f"tcp://*:{porta}")
    poller = zmq.Poller()
    poller.register(sock, zmq.POLLIN)

    servidores: dict[str, dict[str, int]] = {}
    proximo_rank = 1
    log(f"[referência] serviço iniciado na porta {porta}; timeout heartbeat={TIMEOUT_HEARTBEAT_MS} ms")

    try:
        while True:
            eventos = dict(poller.poll(1000))
            agora = p.agora_ms()
            expirados = [nome for nome, info in servidores.items()
                         if agora - info["last_heartbeat_ms"] > TIMEOUT_HEARTBEAT_MS]
            for nome in expirados:
                info = servidores.pop(nome)
                log(f"[referência] servidor removido por ausência de heartbeat: {nome} (rank={info['rank']})")

            if sock not in eventos:
                continue

            raw = sock.recv()
            try:
                msg = p.desserializar(raw)
                p.receber_relogio_logico(msg["logical_clock"])
                log(p.formatar_log("referência", "RECEBIDO <<", msg))
                tipo = msg["type"]
                dados = msg["data"]
                nome = dados.get("name", msg["sender"])

                if tipo == "rank":
                    if not isinstance(nome, str) or not nome.strip():
                        resposta_dados = p.dados_erro(p.ERR_INVALID_REQUEST, "nome do servidor inválido")
                    else:
                        if nome not in servidores:
                            servidores[nome] = {"rank": proximo_rank, "last_heartbeat_ms": agora}
                            proximo_rank += 1
                            log(f"[referência] servidor cadastrado: {nome} | rank={servidores[nome]['rank']}")
                        else:
                            servidores[nome]["last_heartbeat_ms"] = agora
                        resposta_dados = p.dados_ok(rank=servidores[nome]["rank"])
                    tipo_resposta = "rank_reply"
                elif tipo == "list":
                    lista = [{"name": n, "rank": info["rank"]}
                             for n, info in sorted(servidores.items(), key=lambda item: item[1]["rank"])]
                    resposta_dados = p.dados_ok(servers=lista)
                    tipo_resposta = "list_reply"
                elif tipo == "heartbeat":
                    if nome not in servidores:
                        resposta_dados = p.dados_erro("UNKNOWN_SERVER", "servidor não cadastrado; solicite rank novamente")
                    else:
                        servidores[nome]["last_heartbeat_ms"] = agora
                        resposta_dados = p.dados_ok()
                    tipo_resposta = "heartbeat_reply"
                else:
                    tipo_resposta = "error_reply"
                    resposta_dados = p.dados_erro(p.ERR_INVALID_REQUEST, f"operação desconhecida: {tipo}")
            except Exception as exc:
                tipo_resposta = "error_reply"
                resposta_dados = p.dados_erro(p.ERR_INVALID_REQUEST, str(exc))

            resposta = p.criar_mensagem(tipo_resposta, "referencia", resposta_dados)
            sock.send(p.serializar(resposta))
            log(p.formatar_log("referência", "ENVIADO  >>", resposta))
    except (KeyboardInterrupt, SystemExit):
        pass
    finally:
        sock.close(linger=0)
        ctx.term()

if __name__ == "__main__":
    main()
