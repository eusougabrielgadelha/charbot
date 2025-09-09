#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
telegram.py — uploader com:
  • detecção dinâmica do diretório de download (lista de candidatos)
  • watch mode (varre periodicamente)
  • recuperação de .part (remux → fallback transcode com AR preservada)
  • deleção após envio

Requisitos:
  pip install requests requests-toolbelt
  (opcional) pip install telethon
  ffmpeg no sistema para remux/transcode/ffprobe
"""

import os
import re
import time
import json
import math
import logging
import subprocess
import shutil
import contextlib
from dataclasses import dataclass
from typing import Optional, List
from pathlib import Path

# ========== Logging ==========
def setup_logging():
    level = os.getenv("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=getattr(logging, level, logging.INFO),
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

# ========== Helpers básicos ==========
def getenv_bool(key: str, default: str = "0") -> bool:
    v = os.getenv(key, default).strip().lower()
    return v not in ("0", "false", "no", "", "off")

def human_size(n: int) -> str:
    units = ["B","KB","MB","GB","TB"]
    i = 0
    f = float(n)
    while f >= 1024 and i < len(units)-1:
        f /= 1024
        i += 1
    return f"{f:.2f}{units[i]}"

def sanitize_hashtag(name: str) -> str:
    base = name.replace(" ", "_")
    base = re.sub(r"[^A-Za-z0-9_]", "", base)
    return f"#{base or 'SemPasta'}"

def which_ffmpeg() -> Optional[str]:
    return shutil.which("ffmpeg")

def which_ffprobe() -> Optional[str]:
    return shutil.which("ffprobe")

def is_stable(p: Path, min_age_sec: int) -> bool:
    try:
        if not p.exists():
            return False
        return (time.time() - p.stat().st_mtime) >= min_age_sec
    except Exception:
        return False

def list_ready_videos(root: Path, exts: List[str], min_age_sec: int) -> List[Path]:
    files = [p for p in root.rglob("*") if p.is_file() and p.suffix.lower() in exts and is_stable(p, min_age_sec)]
    files.sort(key=lambda x: x.stat().st_mtime)
    return files

# ========== DETECÇÃO DINÂMICA DO DIRETÓRIO ==========
def candidate_roots() -> List[Path]:
    cands: List[Path] = []

    # 1) Se o usuário definiu DOWNLOAD_DIR, priorize
    env_dir = os.getenv("DOWNLOAD_DIR", "").strip()
    if env_dir:
        cands.append(Path(env_dir).expanduser().resolve())

    # 2) Diretório local padrão: ./download
    cands.append((Path.cwd() / "download").resolve())

    # 3) $HOME/charbot/download
    try:
        cands.append((Path.home() / "charbot" / "download").resolve())
    except Exception:
        pass

    # 4) Caminhos comuns de VPS
    cands.append(Path("/root/charbot/download"))
    # 5) Usuários em /home/*/charbot/download
    home_base = Path("/home")
    if home_base.exists():
        for p in home_base.glob("*/charbot/download"):
            cands.append(p.resolve())

    # Deduplicar mantendo ordem
    seen = set()
    uniq: List[Path] = []
    for p in cands:
        key = str(p)
        if key not in seen:
            seen.add(key)
            uniq.append(p)
    return uniq

def find_download_root(create_if_missing: bool = True) -> Path:
    for p in candidate_roots():
        if p.exists():
            return p
    # se nenhum existir: crie o primeiro candidato “padrão”
    default_target = candidate_roots()[0]
    if create_if_missing:
        try:
            default_target.mkdir(parents=True, exist_ok=True)
        except Exception:
            pass
    return default_target

# ========== Config (com defaults seguros) ==========
CAPTION_TEMPLATE = os.getenv("CAPTION_TEMPLATE", "{folder_tag} {filename}")

# Telegram
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()

# Modo de varredura/estabilidade
WATCH = getenv_bool("WATCH", "1")
WATCH_INTERVAL = int(os.getenv("WATCH_INTERVAL", "10"))
STABLE_AGE = int(os.getenv("STABLE_AGE", "20"))
PART_STABLE_AGE = int(os.getenv("PART_STABLE_AGE", "60"))

# Limites e extensões
MAX_FILE_GB = float(os.getenv("MAX_FILE_GB", "0"))  # 0 = sem limite
EXTENSIONS = [e if e.startswith(".") else "." + e for e in os.getenv("EXTENSIONS", ".mp4,.mkv,.mov,.m4v").split(",")]

# MTProto opcional
ENABLE_MTPROTO = getenv_bool("ENABLE_MTPROTO", "0")
TG_API_ID = os.getenv("TG_API_ID", "").strip()
TG_API_HASH = os.getenv("TG_API_HASH", "").strip()

DELETE_AFTER_SEND = getenv_bool("DELETE_AFTER_SEND", "1")

# Diretório raiz (dinâmico)
DOWNLOAD_DIR = find_download_root(create_if_missing=True)

# ========== ffmpeg helpers (finalização .part) ==========
def remux_copy_to_mp4(src: Path, dst: Path) -> bool:
    ffmpeg = which_ffmpeg()
    if not ffmpeg:
        return False
    try:
        subprocess.check_call([
            ffmpeg, "-y", "-i", str(src),
            "-c", "copy", "-movflags", "+faststart",
            str(dst)
        ], stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)
        return dst.exists()
    except Exception:
        with contextlib.suppress(Exception):
            if dst.exists(): dst.unlink()
        return False

def transcode_preserve_ar_to_mp4(src: Path, dst: Path) -> bool:
    """Reencode mantendo razão de aspecto (nunca 1:1 “quadrado”)."""
    ffmpeg = which_ffmpeg()
    if not ffmpeg:
        return False
    # Bounding box 1280x720, preservando AR pelo 'a' (iw/ih); SAR normalizado
    scale_expr = "scale='if(gt(a,16/9),1280,-2)':'if(gt(a,16/9),-2,720)',setsar=1"
    try:
        subprocess.check_call([
            ffmpeg, "-y", "-i", str(src),
            "-c:v", "libx264", "-preset", "veryfast", "-crf", "23",
            "-vf", scale_expr,
            "-c:a", "aac", "-b:a", "128k",
            "-movflags", "+faststart",
            str(dst)
        ], stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)
        return dst.exists()
    except Exception:
        with contextlib.suppress(Exception):
            if dst.exists(): dst.unlink()
        return False

def finalize_part_file(p: Path) -> Optional[Path]:
    """Tenta finalizar um .part → .mp4 (remux, depois fallback transcode)."""
    base = p.with_suffix("")
    dst = base if base.suffix.lower() == ".mp4" else base.with_suffix(".mp4")
    if dst.exists():
        dst = dst.with_stem(dst.stem + f"__fixed_{int(time.time())}")
    if remux_copy_to_mp4(p, dst):
        return dst
    if transcode_preserve_ar_to_mp4(p, dst):
        return dst
    return None

def recover_partials(root: Path) -> List[Path]:
    parts = [pp for pp in root.rglob("*.part") if pp.is_file() and is_stable(pp, PART_STABLE_AGE)]
    parts.sort(key=lambda x: x.stat().st_mtime)
    recovered: List[Path] = []
    for pf in parts:
        logging.info("Recuperando parcial: %s", pf.name)
        mp4 = finalize_part_file(pf)
        if mp4 and mp4.exists():
            with contextlib.suppress(Exception):
                pf.unlink(missing_ok=True)
            recovered.append(mp4)
            logging.info("Parcial finalizado → %s", mp4.name)
        else:
            logging.warning("Falha ao finalizar parcial: %s", pf.name)
    return recovered

# ========== Bot API / MTProto ==========
def telegram_api(method: str) -> str:
    return f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/{method}"

def upload_via_bot(chat_id: str, path: Path, caption: str) -> bool:
    import requests
    from requests_toolbelt.multipart.encoder import MultipartEncoder, MultipartEncoderMonitor

    url = telegram_api("sendDocument")
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
        r = requests.post(url, data=mon, headers=headers, timeout=60*60)
        if r.ok:
            logging.info("BOT enviado: %s", filename)
            return True
        else:
            logging.error("BOT falhou [%s]: %s", r.status_code, r.text[:300])
            return False
    except Exception as e:
        logging.exception("BOT exceção ao enviar %s: %s", filename, e)
        return False

def upload_via_telethon(api_id: int, api_hash: str, chat_id: str, path: Path, caption: str) -> bool:
    try:
        from telethon.sync import TelegramClient
    except Exception:
        logging.error("Telethon não instalado. pip install telethon")
        return False
    session = Path(os.getenv("TELETHON_SESSION", "tg_session")).resolve()
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
                logging.info("MTP enviado: %s", path.name)
                return True
            except Exception as e:
                logging.error("MTP erro ao enviar %s: %s", path.name, e)
                return False
    except Exception as e:
        logging.error("MTP não foi possível iniciar sessão: %s", e)
        return False

# ========== Núcleo ==========
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
        if not (cfg.api_id and cfg.api_hash):
            logging.error("Faltam TG_API_ID/TG_API_HASH para MTProto.")
            return False
        ok = upload_via_telethon(cfg.api_id, cfg.api_hash, cfg.chat_id, path, caption)
    else:
        if not cfg.token or not cfg.chat_id:
            logging.error("Faltam TELEGRAM_TOKEN/TELEGRAM_CHAT_ID para Bot API.")
            return False
        ok = upload_via_bot(cfg.chat_id, path, caption)
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

    # 1) Recuperar .part estáveis
    recovered = recover_partials(DOWNLOAD_DIR)

    # 2) Enviar prontos
    cfg = build_uploader_config()
    files = list_ready_videos(DOWNLOAD_DIR, EXTENSIONS, STABLE_AGE)

    # priorizar os recuperados
    for mp4 in reversed(recovered):
        if mp4 not in files:
            files.insert(0, mp4)

    if not files:
        logging.info("Nenhum arquivo pronto para envio.")
        return

    for p in files:
        send_one(cfg, p)

def main():
    setup_logging()
    logging.info("Uploader iniciado. Pasta detectada: %s | WATCH=%s", DOWNLOAD_DIR, WATCH)
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
