import os
import signal
import sys

import zmq

from protocolo import desserializar, formatar_log, log


def _registrar_publicacao(frames: list[bytes]) -> None:
    topico = frames[0].decode("utf-8", errors="replace")
    try:
        log(formatar_log("proxy", f"REPASSANDO [{topico}]", desserializar(frames[-1])))
    except ValueError:
        log(f"[proxy] REPASSANDO [{topico}] mensagem inválida ({len(frames[-1])} bytes)")


def _registrar_assinatura(frame: bytes) -> None:
    acao = "ASSINATURA" if frame[:1] == b"\x01" else "CANCELAMENTO"
    topico = frame[1:].decode("utf-8", errors="replace")
    log(f"[proxy] {acao} do canal '{topico}'")


def main() -> None:
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))

    porta_xsub = os.getenv("PROXY_XSUB_PORT", "5557")
    porta_xpub = os.getenv("PROXY_XPUB_PORT", "5558")

    ctx = zmq.Context()
    xsub = ctx.socket(zmq.XSUB)
    xsub.bind(f"tcp://*:{porta_xsub}")
    xpub = ctx.socket(zmq.XPUB)
    xpub.setsockopt(zmq.XPUB_VERBOSE, 1)
    xpub.bind(f"tcp://*:{porta_xpub}")

    poller = zmq.Poller()
    poller.register(xsub, zmq.POLLIN)
    poller.register(xpub, zmq.POLLIN)

    log(f"[proxy] iniciado: servidores (XSUB) na porta {porta_xsub}, clientes (XPUB) na porta {porta_xpub}")

    try:
        while True:
            eventos = dict(poller.poll())
            if xsub in eventos:
                frames = xsub.recv_multipart()
                _registrar_publicacao(frames)
                xpub.send_multipart(frames)
            if xpub in eventos:
                frames = xpub.recv_multipart()
                _registrar_assinatura(frames[0])
                xsub.send_multipart(frames)
    except (KeyboardInterrupt, SystemExit):
        pass
    finally:
        xsub.close(linger=0)
        xpub.close(linger=0)
        ctx.term()


if __name__ == "__main__":
    main()
