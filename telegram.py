#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
telegram.py — uploader de vídeos para Telegram com "watch mode".

• Varre uma pasta (DOWNLOAD_DIR) atrás de arquivos de vídeo finalizados (.mp4, .mkv, .mov, .m4v).
• Considera "pronto" apenas o arquivo que está estável (sem alteração de mtime por STABLE_AGE segundos).
• Envia cada arquivo para um chat/canal via:
    1) Bot API (padrão) — requer TELEGRAM_TOKEN e TELEGRAM_CHAT_ID
    2) (Opcional) Telethon (MTProto) — se TG_API_ID/TG_API_HASH forem fornecidos e ENABLE_MTPROTO=1
• Após envio bem-sucedido, apaga o arquivo se DELETE_AFTER_SEND=1.

Ambiente (ENV):
  DOWNLOAD_DIR       -> pasta para varrer (default: ./download)
  TELEGRAM_TOKEN     -> token do bot (obrigatório para Bot API)
  TELEGRAM_CHAT_ID   -> chat alvo (ex.: -1001234567890 para canal)
  CAPTION_TEMPLATE   -> legenda opcional; suporta {filename} e {folder_tag}
  ENABLE_MTPROTO     -> "1" para tentar MTProto com Telethon (default "0")
  TG_API_ID          -> necessário se ENABLE_MTPROTO=1
  TG_API_HASH        -> necessário se ENABLE_MTPROTO=1
  DELETE_AFTER_SEND  -> "1" apaga após enviar (default "1")
  WATCH              -> "1" loop contínuo (default "1")
  WATCH_INTERVAL     -> segundos entre varreduras (default "10")
  STABLE_AGE         -> arquivo é considerado estável após N s sem tocar (default "20")
  MAX_FILE_GB        -> limite de tamanho para enviar (default "0" = sem limite)
  EXTENSIONS         -> extensões separadas por vírgula (default ".mp4,.mkv,.mov,.m4v")

Requer:
  pip install requests requests-toolbelt
  (opcional) pip install telethon
  (opcional) FFmpeg no PATH para extrair duração (ffprobe)
"""

import os
import re
import time
import json
import math
import logging
import subprocess
import shutil
from dataclasses import dataclass
from typing import Optional, List
from pathlib import Path

# ---------------- Logging ----------------
def setup_logging():
    level = os.getenv("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=getattr(logging, level, logging.INFO),
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

# ---------------- Config ----------------
def getenv_bool(key: str, default: str = "0") -> bool:
    v = os.getenv(key, default).strip().lower()
    return v not in ("0", "false", "no", "", "off")

DOWNLOAD_DIR = Path(os.getenv("DOWNLOAD_DIR", "download")).resolve()
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()
CAPTION_TEMPLATE = os.getenv("CAPTION_TEMPLATE", "{folder_tag} {filename}")
ENABLE_MTPROTO = getenv_bool("ENABLE_MTPROTO", "0")
TG_API_ID = os.getenv("TG_API_ID", "").strip()
TG_API_HASH = os.getenv("TG_API_HASH", "").strip()
DELETE_AFTER_SEND = getenv_bool("DELETE_AFTER_SEND", "1")
WATCH = getenv_bool("WATCH", "1")
WATCH_INTERVAL = int(os.getenv("WATCH_INTERVAL", "10"))
STABLE_AGE = int(os.getenv("STABLE_AGE", "20"))
MAX_FILE_GB = float(os.getenv("MAX_FILE_GB", "0"))  # 0 = sem limite
EXTENSIONS = [e if e.startswith(".") else "." + e for e in os.getenv("EXTENSIONS", ".mp4,.mkv,.mov,.m4v").split(",")]

# ---------------- Utils ----------------
def sanitize_hashtag(name: str) -> str:
    base = name.replace(" ", "_")
    base = re.sub(r"[^A-Za-z0-9_]", "", base)
    return f"#{base or 'SemPasta'}"

def human_size(n: int) -> str:
    units = ["B","KB","MB","GB","TB"]
    i = 0
    f = float(n)
    while f >= 1024 and i < len(units)-1:
        f /= 1024
        i += 1
    return f"{f:.2f}{units[i]}"

def which_ffprobe() -> Optional[str]:
    return shutil.which("ffprobe")

def probe_duration_seconds(path: Path) -> Optional[float]:
    try:
        ffprobe = shutil.which("ffprobe")
        if not ffprobe:
            return None
        out = subprocess.check_output(
            [ffprobe, "-v","error","-select_streams","v:0","-show_entries","stream=duration","-of","default=nk=1:nw=1", str(path)],
            stderr=subprocess.STDOUT,
        ).decode("utf-8","ignore").strip()
        if out and out.upper() != "N/A":
            return float(out)
    except Exception:
        return None
    return None

def is_stable(p: Path, min_age_sec: int) -> bool:
    try:
        if not p.exists():
            return False
        return (time.time() - p.stat().st_mtime) >= min_age_sec
    except Exception:
        return False

def list_ready_videos(root: Path, exts: List[str], min_age_sec: int) -> List[Path]:
    files = [p for p in root.rglob("*") if p.is_file() and p.suffix.lower() in exts and is_stable(p, min_age_sec)]
    files.sort(key=lambda x: x.stat().st_mtime)  # mais antigos primeiro
    return files

# ---------------- Uploaders ----------------
# Bot API
def upload_via_bot(token: str, chat_id: str, path: Path, caption: str) -> bool:
    import requests
    from requests_toolbelt.multipart.encoder import MultipartEncoder, MultipartEncoderMonitor

    url = f"https://api.telegram.org/bot{token}/sendDocument"
    size = path.stat().st_size
    filename = path.name

    fields = {
        "chat_id": chat_id,
        "caption": caption,
        "document": (filename, open(path, "rb"), "application/octet-stream"),
        "disable_notification": "true",
    }

    enc = MultipartEncoder(fields=fields)
    sent = {"n": 0}

    def cb(m: MultipartEncoderMonitor):
        sent["n"] = m.bytes_read
        pct = (sent["n"]/size*100) if size else 0
        logging.info("BOT upload %s: %.1f%% (%s/%s)", filename, pct, human_size(sent["n"]), human_size(size))

    mon = MultipartEncoderMonitor(enc, cb)
    headers = {"Content-Type": mon.content_type}

    try:
        r = requests.post(url, data=mon, headers=headers, timeout=60*60)  # até 1h
        if r.ok:
            logging.info("BOT enviado: %s", filename)
            return True
        else:
            logging.error("BOT falhou [%s]: %s", r.status_code, r.text[:300])
            return False
    except Exception as e:
        logging.exception("BOT exceção ao enviar %s: %s", filename, e)
        return False

# MTProto (Telethon) — opcional
def upload_via_telethon(api_id: int, api_hash: str, chat_id: str, path: Path, caption: str) -> bool:
    try:
        from telethon.sync import TelegramClient
    except Exception:
        logging.error("Telethon não instalado. pip install telethon")
        return False

    session = Path(os.getenv("TELETHON_SESSION", "tg_session")).resolve()
    filename = path.name

    try:
        with TelegramClient(str(session), api_id, api_hash) as client:
            logging.info("MTP: conectando…")
            entity = chat_id
            try:
                entity = int(chat_id)
            except Exception:
                pass
            try:
                client.send_file(
                    entity,
                    file=str(path),
                    caption=caption,
                    force_document=True,
                    part_size_kb=int(os.getenv("MT_PART_KB","1024"))
                )
                logging.info("MTP enviado: %s", filename)
                return True
            except Exception as e:
                logging.error("MTP erro ao enviar %s: %s", filename, e)
                return False
    except Exception as e:
        logging.error("MTP não foi possível iniciar sessão: %s", e)
        return False

# ---------------- Core ----------------
def make_caption(path: Path) -> str:
    parent = path.parent.name or "SemPasta"
    folder_tag = sanitize_hashtag(parent)
    filename = path.stem
    tpl = CAPTION_TEMPLATE or "{folder_tag} {filename}"
    try:
        return tpl.format(folder_tag=folder_tag, filename=filename)
    except Exception:
        return f"{folder_tag} {filename}"

@dataclass
class UploaderConfig:
    use_mtproto: bool
    token: str
    chat_id: str
    api_id: Optional[int]
    api_hash: Optional[str]

def build_uploader_config() -> UploaderConfig:
    use_mtproto = ENABLE_MTPROTO and TG_API_ID and TG_API_HASH
    api_id = int(TG_API_ID) if TG_API_ID else None
    api_hash = TG_API_HASH if TG_API_HASH else None
    return UploaderConfig(
        use_mtproto=use_mtproto,
        token=TELEGRAM_TOKEN,
        chat_id=TELEGRAM_CHAT_ID,
        api_id=api_id,
        api_hash=api_hash
    )

def send_one(cfg: UploaderConfig, path: Path) -> bool:
    if MAX_FILE_GB > 0 and path.stat().st_size > MAX_FILE_GB * (1024**3):
        logging.warning("Pulando %s (tamanho %s > %sGB)", path.name, human_size(path.stat().st_size), MAX_FILE_GB)
        return False
    caption = make_caption(path)
    if cfg.use_mtproto:
        logging.info("Enviando via MTProto: %s", path.name)
        ok = upload_via_telethon(cfg.api_id, cfg.api_hash, cfg.chat_id, path, caption)
    else:
        if not cfg.token or not cfg.chat_id:
            logging.error("Faltam TELEGRAM_TOKEN/TELEGRAM_CHAT_ID para Bot API.")
            return False
        logging.info("Enviando via Bot API: %s", path.name)
        ok = upload_via_bot(cfg.token, cfg.chat_id, path, caption)
    if ok and DELETE_AFTER_SEND:
        try:
            path.unlink()
            logging.info("Apagado: %s", path.name)
        except Exception as e:
            logging.error("Não foi possível apagar %s: %s", path.name, e)
    return ok

def run_once():
    if not DOWNLOAD_DIR.exists():
        logging.warning("DOWNLOAD_DIR não existe: %s", DOWNLOAD_DIR)
        return
    cfg = build_uploader_config
    cfg = build_uploader_config()
    files = list_ready_videos(DOWNLOAD_DIR, EXTENSIONS, STABLE_AGE)
    if not files:
        logging.info("Nenhum arquivo pronto para envio.")
        return
    for p in files:
        send_one(cfg, p)

def main():
    setup_logging()
    logging.info("Uploader iniciado. Pasta: %s | WATCH=%s", DOWNLOAD_DIR, WATCH)
    if WATCH:
        while True:
            try:
                run_once()
            except Exception as e:
                logging.exception("Erro no loop principal: %s", e)
            time.sleep(WATCH_INTERVAL)
    else:
        run_once()

if __name__ == "__main__":
    main()
