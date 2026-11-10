import os
import signal
import sys

import zmq

from protocolo import desserializar, formatar_log, log


def _registrar(direcao: str, frames: list[bytes]) -> None:
    try:
        log(formatar_log("broker", direcao, desserializar(frames[-1])))
    except ValueError:
        log(f"[broker] {direcao} mensagem inválida ({len(frames[-1])} bytes)")


def main() -> None:
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))

    porta_front = os.getenv("BROKER_FRONTEND_PORT", "5555")
    porta_back = os.getenv("BROKER_BACKEND_PORT", "5556")

    ctx = zmq.Context()
    frontend = ctx.socket(zmq.ROUTER)
    frontend.bind(f"tcp://*:{porta_front}")
    backend = ctx.socket(zmq.DEALER)
    backend.bind(f"tcp://*:{porta_back}")

    poller = zmq.Poller()
    poller.register(frontend, zmq.POLLIN)
    poller.register(backend, zmq.POLLIN)

    log(f"[broker] iniciado: clientes na porta {porta_front}, servidores na porta {porta_back}")

    try:
        while True:
            eventos = dict(poller.poll())
            if frontend in eventos:
                frames = frontend.recv_multipart()
                _registrar("CLIENTE  >> SERVIDOR", frames)
                backend.send_multipart(frames)
            if backend in eventos:
                frames = backend.recv_multipart()
                _registrar("SERVIDOR >> CLIENTE ", frames)
                frontend.send_multipart(frames)
    except (KeyboardInterrupt, SystemExit):
        pass
    finally:
        frontend.close(linger=0)
        backend.close(linger=0)
        ctx.term()


if __name__ == "__main__":
    main()
