#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import re
import sys
import json
import time
import math
import shlex
import signal
import logging
import asyncio
import subprocess
from pathlib import Path
from typing import List, Optional, Tuple

# ========= util: carregar .env mesmo sem python-dotenv =========
def _load_env_file(dotenv_path: Path):
    if not dotenv_path.exists():
        return
    try:
        from dotenv import load_dotenv
        load_dotenv(dotenv_path)
        return
    except Exception:
        pass  # cai para o parser simples abaixo

    for line in dotenv_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "#" in line:
            # permite comentários ao fim da linha
            line = re.split(r"\s+#", line, 1)[0].strip()
        if "=" not in line:
            continue
        k, v = line.split("=", 1)
        k = k.strip()
        v = v.strip().strip('"').strip("'")
        os.environ.setdefault(k, v)

ROOT = Path(__file__).resolve().parent
_load_env_file(ROOT / ".env")

# ========= helpers de env =========
def env_bool(name: str, default: bool=False) -> bool:
    return str(os.environ.get(name, "1" if default else "0")).strip() in ("1","true","True","yes","on","Y")

def env_int(name: str, default: int=0) -> int:
    try:
        return int(str(os.environ.get(name, str(default))).strip())
    except Exception:
        return default

def env_float(name: str, default: float=0.0) -> float:
    try:
        return float(str(os.environ.get(name, str(default))).strip())
    except Exception:
        return default

def human_size(n: int) -> str:
    units = ["B","KB","MB","GB","TB"]
    s = float(n)
    for u in units:
        if s < 1024 or u == units[-1]:
            return f"{s:.2f}{u}"
        s /= 1024

# ========= config =========
DOWNLOAD_DIR   = Path(os.environ.get("DOWNLOAD_DIR", str(ROOT / "download")))
LOG_DIR        = Path(os.environ.get("LOG_DIR", str(ROOT / "logs")))
QUARANTINE_DIR = Path(os.environ.get("QUARANTINE_DIR", str(DOWNLOAD_DIR / "_bad")))

TELEGRAM_TOKEN   = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID_ENV = os.environ.get("TELEGRAM_CHAT_ID", "")

WATCH            = env_bool("WATCH", True)
WATCH_INTERVAL   = env_int("WATCH_INTERVAL", 10)

# Atenção: no .env novo o nome é FILE_STABLE_AGE
FILE_STABLE_AGE  = env_int("FILE_STABLE_AGE", 20)

DELETE_AFTER_SEND = env_bool("DELETE_AFTER_SEND", True)
MIN_FILE_MB       = env_int("MIN_FILE_MB", 5)
MAX_FILE_GB       = env_float("MAX_FILE_GB", 0.0)  # 0 = sem limite
EXTENSIONS        = [e.strip().lower() for e in os.environ.get("EXTENSIONS", ".mp4,.mkv,.mov,.m4v").split(",") if e.strip()]

CAPTION_TEMPLATE  = os.environ.get("CAPTION_TEMPLATE", "{folder_tag} {filename}").strip()

# Bot API: limite prático — coloque algo conservador; 176MB já deu 413 no seu host
BOT_MAX_MB        = env_int("BOT_MAX_MB", 190)  # qualquer arquivo > isso vai direto para Telethon

# Telethon / MTProto
ENABLE_MTPROTO    = env_bool("ENABLE_MTPROTO", False)
TG_API_ID         = env_int("TG_API_ID", 0)
TG_API_HASH       = os.environ.get("TG_API_HASH", "")
TELETHON_SESSION  = os.environ.get("TELETHON_SESSION", str(ROOT / "telethon.session"))
MT_PART_KB        = env_int("MT_PART_KB", 1024)

# Split (fallback) – opcional
SPLIT_ON_TELETHON_FAIL = env_bool("SPLIT_ON_TELETHON_FAIL", False)
SEGMENT_SECONDS        = env_int("SEGMENT_SECONDS", 1226)  # ~20 min; só usado no split

# ========= logging =========
LOG_DIR.mkdir(parents=True, exist_ok=True)
log_path = LOG_DIR / "uploader.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout), logging.FileHandler(log_path, encoding="utf-8")]
)
logging.info("Iniciando uploader…")

# ========= misc =========
def ensure_dirs():
    DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
    QUARANTINE_DIR.mkdir(parents=True, exist_ok=True)

def is_video_file(p: Path) -> bool:
    name = p.name.lower()
    return any(name.endswith(ext) for ext in EXTENSIONS)

def is_stable(p: Path, age_seconds: int) -> bool:
    try:
        st = p.stat()
    except FileNotFoundError:
        return False
    idle = time.time() - st.st_mtime
    return idle >= age_seconds

def list_ready_files(root: Path) -> List[Path]:
    candidates: List[Path] = []
    for ext in EXTENSIONS:
        for p in root.rglob(f"*{ext}"):
            if p.name.endswith(".part") or p.name.endswith(".__tmp__.mp4"):
                continue
            try:
                size = p.stat().st_size
            except FileNotFoundError:
                continue
            if size < MIN_FILE_MB * 1024 * 1024:
                continue
            if not is_stable(p, FILE_STABLE_AGE):
                continue
            if MAX_FILE_GB > 0 and size > MAX_FILE_GB * (1024 ** 3):
                logging.warning("Ignorando (muito grande, > %.2fGB): %s (%s)", MAX_FILE_GB, p.name, human_size(size))
                continue
            candidates.append(p)
    candidates.sort(key=lambda x: x.stat().st_mtime)  # mais antigos primeiro
    return candidates

def build_caption(path: Path) -> str:
    folder_tag = f"[{path.parent.name}]"
    return CAPTION_TEMPLATE.format(
        folder_tag=folder_tag,
        filename=path.name,
        stem=path.stem
    ).strip()

def parse_peer(chat_id_env: str | int):
    """
    Se TELEGRAM_CHAT_ID for numérico (ex.: -1001812...), converte para int.
    Isso evita Telethon tentar tratar como telefone (erro bot/phone number).
    """
    if isinstance(chat_id_env, int):
        return chat_id_env
    s = str(chat_id_env).strip()
    if re.fullmatch(r"-?\d+", s):
        try:
            return int(s)
        except Exception:
            pass
    return s  # username tipo @seucanal

def bot_limit_bytes() -> int:
    return BOT_MAX_MB * 1024 * 1024

# ========= Bot API =========
import requests

def upload_via_bot(p: Path, caption: str) -> Tuple[bool, Optional[str]]:
    """
    Retorna (ok, err). err contém mensagem/objeto JSON em caso de falha.
    Envia SEMPRE como vídeo (sendVideo).
    """
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendVideo"
    data = {
        "chat_id": TELEGRAM_CHAT_ID_ENV,
        "caption": caption,
        "supports_streaming": True,
        "disable_notification": True,
    }

    # stream de arquivo (requests faz multipart)
    with p.open("rb") as fh:
        files = {"video": (p.name, fh, "video/mp4")}
        try:
            resp = requests.post(url, data=data, files=files, timeout=600)
        except Exception as e:
            return False, f"requests error: {e}"

    try:
        js = resp.json()
    except Exception:
        js = {"status_code": resp.status_code, "text": resp.text}

    if resp.ok and isinstance(js, dict) and js.get("ok") is True:
        return True, None

    # 413: muito grande via Bot API → sinaliza fallback
    if resp.status_code == 413:
        logging.error("BOT falhou [413]: %s", json.dumps(js, ensure_ascii=False))
        return False, "413"

    return False, json.dumps(js, ensure_ascii=False)

# ========= Telethon (MTProto) =========
_telethon_import_ok = True
try:
    from telethon import TelegramClient
except Exception as e:
    _telethon_import_ok = False
    if ENABLE_MTPROTO:
        logging.warning("Telethon não disponível (%s). Instale com: pip install telethon", e)

async def _send_with_telethon(file_path: Path, caption: str) -> None:
    """
    Envia arquivo grande como VÍDEO via MTProto.
    """
    if not _telethon_import_ok:
        raise RuntimeError("Telethon não instalado")

    if not ENABLE_MTPROTO:
        raise RuntimeError("ENABLE_MTPROTO=0")

    if not TG_API_ID or not TG_API_HASH:
        raise RuntimeError("TG_API_ID/TG_API_HASH ausentes")

    session_path = TELETHON_SESSION
    client = TelegramClient(session_path, TG_API_ID, TG_API_HASH)

    await client.start(bot_token=TELEGRAM_TOKEN)  # login como bot (com privilégios do bot)

    try:
        peer = parse_peer(TELEGRAM_CHAT_ID_ENV)
        # force_document=False + supports_streaming=True → garante vídeo no player
        await client.send_file(
            peer,
            file=str(file_path),
            caption=caption,
            force_document=False,
            supports_streaming=True,
            part_size_kb=max(32, MT_PART_KB),  # mínimo sensato
        )
    finally:
        await client.disconnect()

def send_via_telethon(file_path: Path, caption: str) -> Tuple[bool, Optional[str]]:
    try:
        asyncio.run(_send_with_telethon(file_path, caption))
        return True, None
    except Exception as e:
        logging.error("MTProto falhou: %s", e, exc_info=True)
        return False, str(e)

# ========= Split (opcional) =========
def ffmpeg_split(file_path: Path) -> List[Path]:
    """
    Segmenta o vídeo 'no-copy' em blocos por tempo. Retorna a lista de partes criadas.
    """
    out_pat = file_path.with_suffix("").as_posix() + ".p%03d.mp4"
    cmd = f'ffmpeg -hide_banner -loglevel error -i {shlex.quote(str(file_path))} -c copy -map 0 -f segment -segment_time {SEGMENT_SECONDS} -reset_timestamps 1 {shlex.quote(out_pat)}'
    logging.info("Split ffmpeg: %s", cmd)
    try:
        subprocess.run(cmd, shell=True, check=True)
    except subprocess.CalledProcessError as e:
        logging.error("ffmpeg split falhou: %s", e)
        return []

    parts = sorted(file_path.parent.glob(file_path.stem + ".p" + "[0-9][0-9][0-9].mp4"))
    for p in parts:
        try:
            logging.info("Criado segmento: %s (%s)", p.name, human_size(p.stat().st_size))
        except Exception:
            pass
    return parts

# ========= fluxo de envio =========
def should_use_bot(size_bytes: int) -> bool:
    return size_bytes <= bot_limit_bytes()

def send_one(p: Path) -> bool:
    caption = build_caption(p)
    size = p.stat().st_size
    logging.info("Preparando envio: %s (%s)", p.name, human_size(size))

    # 1) tenta Bot API se couber no limite configurado
    if TELEGRAM_TOKEN and should_use_bot(size):
        ok, err = upload_via_bot(p, caption)
        if ok:
            logging.info("BOT ok: %s", p.name)
            if DELETE_AFTER_SEND:
                try:
                    p.unlink()
                    logging.info("Apagado: %s", p.name)
                except Exception as e:
                    logging.warning("Falhou ao apagar %s: %s", p.name, e)
            return True
        if err == "413":
            logging.info("Arquivo grande p/ Bot. Fallback para MTProto…")
        else:
            logging.warning("BOT falhou: %s", err)

    # 2) Telethon (MTProto) para tamanhos grandes ou fallback
    if ENABLE_MTPROTO:
        ok, err = send_via_telethon(p, caption)
        if ok:
            logging.info("MTProto ok: %s", p.name)
            if DELETE_AFTER_SEND:
                try:
                    p.unlink()
                    logging.info("Apagado: %s", p.name)
                except Exception as e:
                    logging.warning("Falhou ao apagar %s: %s", p.name, e)
            return True

        # 3) Split opcional se Telethon falhar
        if SPLIT_ON_TELETHON_FAIL:
            logging.info("Tentando split por ffmpeg e envio em partes (Telethon)…")
            parts = ffmpeg_split(p)
            success_all = True
            for idx, part in enumerate(parts, 1):
                part_caption = f"{caption} • parte {idx}/{len(parts)}"
                ok2, err2 = send_via_telethon(part, part_caption)
                if not ok2:
                    logging.error("Falhou parte %s: %s", part.name, err2)
                    success_all = False
                else:
                    if DELETE_AFTER_SEND:
                        with contextlib.suppress(Exception):
                            part.unlink()
            if success_all:
                if DELETE_AFTER_SEND:
                    with contextlib.suppress(Exception):
                        p.unlink()
                return True
            return False

        if err:
            logging.error("Envio falhou: %s", p.name)
        return False

    logging.error("Sem MTProto habilitado e Bot API falhou/ficou fora do limite.")
    return False

# ========= loop principal =========
_SHOULD_STOP = False
def _sig_handler(signum, frame):
    global _SHOULD_STOP
    logging.warning("Sinal recebido (%s). Encerrando após o ciclo atual...", signum)
    _SHOULD_STOP = True

for sig in (signal.SIGINT, signal.SIGTERM):
    signal.signal(sig, _sig_handler)

def run_once() -> None:
    ensure_dirs()
    files = list_ready_files(DOWNLOAD_DIR)
    if not files:
        logging.debug("Nada pronto para enviar.")
        return
    for p in files:
        try:
            send_one(p)
        except Exception as e:
            logging.error("Erro ao enviar %s: %s", p, e, exc_info=True)

def main():
    logging.info("Uploader iniciado. Pasta detectada: %s | WATCH=%s", str(DOWNLOAD_DIR), WATCH)
    if not TELEGRAM_TOKEN:
        logging.error("TELEGRAM_TOKEN não definido.")
    if not TELEGRAM_CHAT_ID_ENV:
        logging.error("TELEGRAM_CHAT_ID não definido.")
    if ENABLE_MTPROTO and (not TG_API_ID or not TG_API_HASH):
        logging.error("ENABLE_MTPROTO=1 mas TG_API_ID/TG_API_HASH não estão preenchidos.")

    if not WATCH:
        run_once()
        return

    while not _SHOULD_STOP:
        run_once()
        for _ in range(WATCH_INTERVAL):
            if _SHOULD_STOP:
                break
            time.sleep(1)

if __name__ == "__main__":
    # Permite override rápido por variável de ambiente (ex.: DOWNLOAD_DIR=/tmp WATCH=0 python3 telegram.py)
    try:
        if "DOWNLOAD_DIR" in os.environ:
            DOWNLOAD_DIR = Path(os.environ["DOWNLOAD_DIR"])
        if "WATCH" in os.environ:
            WATCH = env_bool("WATCH", WATCH)
    except Exception:
        pass
    # contextlib usado lá em cima
    import contextlib  # noqa: E402
    main()
