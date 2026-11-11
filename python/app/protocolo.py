import re
import time
from datetime import datetime

import msgpack

LOGIN = "login"
LOGIN_REPLY = "login_reply"
CREATE_CHANNEL = "create_channel"
CREATE_CHANNEL_REPLY = "create_channel_reply"
LIST_CHANNELS = "list_channels"
LIST_CHANNELS_REPLY = "list_channels_reply"
PUBLISH = "publish"
PUBLISH_REPLY = "publish_reply"
PUBLICATION = "publication"
ERROR_REPLY = "error_reply"

OK = "ok"
ERROR = "error"
ERR_INVALID_NAME = "INVALID_NAME"
ERR_ALREADY_EXISTS = "ALREADY_EXISTS"
ERR_INVALID_REQUEST = "INVALID_REQUEST"
ERR_INTERNAL = "INTERNAL_ERROR"

USER_RE = re.compile(r"^[A-Za-z0-9_-]{3,20}$")
CHANNEL_RE = re.compile(r"^[a-z0-9_-]{3,30}$")
MAX_MENSAGEM = 500
MAX_MSG_ID = 64


def agora_ms() -> int:
    return time.time_ns() // 1_000_000


def criar_mensagem(tipo: str, remetente: str, dados: dict | None = None) -> dict:
    return {
        "type": tipo,
        "sender": remetente,
        "timestamp": agora_ms(),
        "data": dados or {},
    }


def dados_ok(**extras) -> dict:
    return {"status": OK, **extras}


def dados_erro(codigo: str, mensagem: str) -> dict:
    return {"status": ERROR, "error": codigo, "message": mensagem}


def serializar(msg: dict) -> bytes:
    return msgpack.packb(msg, use_bin_type=True)


def desserializar(raw: bytes) -> dict:
    try:
        msg = msgpack.unpackb(raw, raw=False)
    except Exception as e:
        raise ValueError(f"MessagePack inválido: {e}") from e
    if not isinstance(msg, dict):
        raise ValueError("a mensagem deve ser um mapa")
    for campo in ("type", "sender", "timestamp", "data"):
        if campo not in msg:
            raise ValueError(f"campo obrigatório ausente: {campo}")
    if not isinstance(msg["type"], str) or not isinstance(msg["sender"], str):
        raise ValueError("'type' e 'sender' devem ser strings")
    if not isinstance(msg["timestamp"], int):
        raise ValueError("'timestamp' deve ser inteiro (ms)")
    if not isinstance(msg["data"], dict):
        raise ValueError("'data' deve ser um mapa")
    return msg


def log(texto: str) -> None:
    print(texto, flush=True)

def _hora() -> str:
    return datetime.now().strftime("%H:%M:%S.%f")[:-3]


def formatar_log(quem: str, direcao: str, msg: dict) -> str:
    return (
        f"[{_hora()}] [{quem}] {direcao} {msg['type']} | de={msg['sender']} "
        f"| ts={msg['timestamp']} | dados={msg['data']}"
    )


def formatar_publicacao_recebida(quem: str, topico: str, msg: dict, recebido_ms: int) -> str:
    dados = msg["data"]
    return (
        f"[{_hora()}] [{quem}] RECEBIDO << PUBLICAÇÃO canal={topico} "
        f"| mensagem={dados.get('message')!r} | autor={dados.get('user')} "
        f"| servidor={msg['sender']} | enviado_ts={msg['timestamp']} "
        f"| recebido_ts={recebido_ms} | atraso={recebido_ms - msg['timestamp']} ms "
        f"| msg_id={dados.get('msg_id')}"
    )
