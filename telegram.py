#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import time
import json
import math
import shlex
import signal
import logging
import mimetypes
import subprocess
from pathlib import Path
from dataclasses import dataclass
from typing import List, Optional, Tuple

from datetime import datetime
from dotenv import load_dotenv
import requests

# ---------- Carrega .env ----------
load_dotenv(override=True)

# ---------- Logging ----------
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(message)s",
)

# ---------- Utilidades ----------
def human_size(b: int) -> str:
    if b is None:
        return "0B"
    units = ["B", "KB", "MB", "GB", "TB"]
    i = 0
    f = float(b)
    while f >= 1024 and i < len(units) - 1:
        f /= 1024.0
        i += 1
    return f"{f:.2f}{units[i]}"

def run_cmd(cmd: List[str]) -> Tuple[int, str, str]:
    """Executa comando e retorna (returncode, stdout, stderr)."""
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    out, err = proc.communicate()
    return proc.returncode, out, err

def ffprobe_probe(path: Path) -> dict:
    """Obtém metadados via ffprobe (duração, largura, altura)."""
    cmd = [
        "ffprobe", "-v", "error",
        "-select_streams", "v:0",
        "-show_entries", "format=duration:stream=width,height",
        "-of", "json", str(path)
    ]
    rc, out, err = run_cmd(cmd)
    if rc != 0:
        return {}
    try:
        data = json.loads(out)
    except json.JSONDecodeError:
        return {}
    info = {}
    if "format" in data and "duration" in data["format"]:
        try:
            info["duration"] = float(data["format"]["duration"])
        except Exception:
            pass
    if "streams" in data and data["streams"]:
        st = data["streams"][0]
        if "width" in st and "height" in st:
            info["width"] = int(st["width"])
            info["height"] = int(st["height"])
    return info

# ---------- Config ----------
def getenv_bool(name: str, default: int = 0) -> bool:
    v = os.getenv(name, str(default)).strip()
    return v in ("1", "true", "yes", "on", "True", "TRUE")

@dataclass
class Cfg:
    # Pastas
    base_dir: Path
    download_dir: Path
    log_dir: Path
    quarantine_dir: Optional[Path]
    move_bad_parts: bool

    # Watch / fluxo
    watch: bool
    watch_interval: int
    file_stable_age: int
    delete_after_send: bool

    # Seleção de arquivos
    extensions: List[str]
    min_file_mb: int
    max_file_gb: float  # 0 = sem limite

    # Telegram Bot API
    bot_token: Optional[str]
    chat_id: Optional[str]
    bot_small_limit_mb: int  # limite para usar Bot API (evita 413)
    use_bot_api: bool

    # Telethon / MTProto
    enable_mtproto: bool
    tg_api_id: Optional[int]
    tg_api_hash: Optional[str]
    premium: bool  # 4 GB se True, caso contrário 2 GB
    mt_part_kb: int

    # Split
    enable_split: bool
    split_piece_gb: float  # tamanho alvo de cada parte
    keep_original_after_split: bool

    # Legenda
    caption_template: str

def load_cfg() -> Cfg:
    base_dir = Path(os.getenv("BASE_DIR", str(Path.cwd()))).resolve()

    download_dir = Path(os.getenv("DOWNLOAD_DIR", str(base_dir / "download"))).resolve()
    log_dir = Path(os.getenv("LOG_DIR", str(base_dir / "logs"))).resolve()

    quarantine_raw = os.getenv("QUARANTINE_DIR", "").strip()
    quarantine_dir = Path(quarantine_raw).resolve() if quarantine_raw else None

    extensions = [e.strip().lower() for e in os.getenv("EXTENSIONS", ".mp4,.mkv,.mov,.m4v").split(",") if e.strip()]
    if not extensions:
        extensions = [".mp4"]

    cfg = Cfg(
        base_dir=base_dir,
        download_dir=download_dir,
        log_dir=log_dir,
        quarantine_dir=quarantine_dir,
        move_bad_parts=getenv_bool("MOVE_BAD_PARTS", 1),

        watch=getenv_bool("WATCH", 1),
        watch_interval=int(os.getenv("WATCH_INTERVAL", "10")),
        file_stable_age=int(os.getenv("STABLE_AGE", "20")),
        delete_after_send=getenv_bool("DELETE_AFTER_SEND", 1),

        extensions=extensions,
        min_file_mb=int(os.getenv("MIN_FILE_MB", "1")),
        max_file_gb=float(os.getenv("MAX_FILE_GB", "0")),

        bot_token=os.getenv("TELEGRAM_TOKEN", "").strip() or None,
        chat_id=os.getenv("TELEGRAM_CHAT_ID", "").strip() or None,
        bot_small_limit_mb=int(os.getenv("BOT_SMALL_LIMIT_MB", "45")),  # margem < 50MB
        use_bot_api=getenv_bool("USE_BOT_API", 1),

        enable_mtproto=getenv_bool("ENABLE_MTPROTO", 0),
        tg_api_id=int(os.getenv("TG_API_ID", "0")) or None,
        tg_api_hash=os.getenv("TG_API_HASH", "").strip() or None,
        premium=getenv_bool("TELEGRAM_PREMIUM", 0),
        mt_part_kb=int(os.getenv("MT_PART_KB", "1024")),

        enable_split=getenv_bool("ENABLE_SPLIT", 1),
        split_piece_gb=float(os.getenv("SPLIT_PIECE_GB", "1.95")),  # “seguro” p/ 2GB
        keep_original_after_split=getenv_bool("KEEP_ORIGINAL_AFTER_SPLIT", 0),

        caption_template=os.getenv("CAPTION_TEMPLATE", "{folder_tag} {filename}").strip(),
    )
    return cfg

# ---------- Estabilidade / seleção ----------
def is_stable(p: Path, stable_age: int) -> bool:
    try:
        mtime = p.stat().st_mtime
    except FileNotFoundError:
        return False
    return (time.time() - mtime) >= stable_age

def list_ready_files(root: Path, cfg: Cfg) -> List[Path]:
    cand: List[Path] = []
    for ext in cfg.extensions:
        for p in root.rglob(f"*{ext}"):
            if p.name.endswith(".part"):
                continue
            try:
                size = p.stat().st_size
            except FileNotFoundError:
                continue
            if size < cfg.min_file_mb * 1024 * 1024:
                continue
            if not is_stable(p, cfg.file_stable_age):
                continue
            # NÃO descarte os grandes aqui — deixe para o split/MTProto decidir.
            cand.append(p)
    cand.sort(key=lambda x: x.stat().st_mtime)  # mais antigos primeiro
    return cand

# ---------- Recuperação de parciais ----------
def try_finalize_part(p: Path) -> Optional[Path]:
    """
    Heurística leve para fechar .part: se o .mp4 "final" já existe com mesmo prefixo,
    preferir o final; caso contrário, se for .part e estiver estável, renomeia removendo .part.
    """
    if not p.name.endswith(".part"):
        return None

    candidate = p.with_suffix("")  # remove .part
    if candidate.exists():
        # Já existe arquivo final com o mesmo nome-base: apague o .part “antigo”
        try:
            p.unlink(missing_ok=True)
            logging.info("Descartando parcial redundante: %s", p.name)
        except Exception:
            pass
        return candidate

    # Se estável, tentar “promover” o .part.
    if is_stable(p, 60):
        try:
            p.rename(candidate)
            logging.info("Promovido .part -> final: %s", candidate.name)
            return candidate
        except Exception as e:
            logging.warning("Falha ao renomear .part: %s (%s)", p.name, e)
    return None

def recover_partials(root: Path, cfg: Cfg):
    parts = list(root.rglob("*.part"))
    for pp in sorted(parts, key=lambda x: x.stat().st_mtime):
        out = try_finalize_part(pp)
        if out is None and cfg.move_bad_parts and cfg.quarantine_dir:
            try:
                cfg.quarantine_dir.mkdir(parents=True, exist_ok=True)
                dest = cfg.quarantine_dir / pp.name
                pp.rename(dest)
                logging.info("Parcial movido para quarentena: %s", dest)
            except Exception as e:
                logging.warning("Falha ao mover para quarentena: %s (%s)", pp.name, e)

# ---------- Envio Bot API (arquivos pequenos) ----------
def upload_via_bot_api(chat_id: str, path: Path, caption: str) -> bool:
    url = f"https://api.telegram.org/bot{os.environ['TELEGRAM_TOKEN']}/sendVideo"
    size = path.stat().st_size
    logging.info("BOT-API enviando: %s (%s)", path.name, human_size(size))

    def iter_file(fp, chunk=1024*512):
        sent = 0
        while True:
            data = fp.read(chunk)
            if not data:
                break
            sent += len(data)
            pct = (sent / size) * 100.0
            logging.info("BOT upload %s: %.1f%% (%s/%s)", path.name, pct, human_size(sent), human_size(size))
            yield data

    with path.open("rb") as f:
        files = {"video": (path.name, iter_file(f), mimetypes.guess_type(path.name)[0] or "video/mp4")}
        data = {
            "chat_id": chat_id,
            "caption": caption,
            "supports_streaming": "true",
            "disable_notification": "true",
        }
        try:
            r = requests.post(url, data=data, files=files, timeout=60*60)
            if r.status_code != 200:
                logging.error("BOT falhou [%s]: %s", r.status_code, r.text)
                return False
            logging.info("BOT enviado: %s", path.name)
            return True
        except Exception as e:
            logging.exception("BOT erro ao enviar: %s", e)
            return False

# ---------- Envio via Telethon (MTProto) ----------
_telethon_client = None
def get_telethon_client(cfg: Cfg):
    global _telethon_client
    if _telethon_client is not None:
        return _telethon_client
    from telethon import TelegramClient
    session_name = os.getenv("TELETHON_SESSION_NAME", "bot")  # cria bot.session
    client = TelegramClient(session_name, cfg.tg_api_id, cfg.tg_api_hash)
    client.start(bot_token=cfg.bot_token)
    _telethon_client = client
    return client

def upload_via_mtproto(chat_id: str, path: Path, caption: str) -> bool:
    from telethon.tl.types import DocumentAttributeVideo
    client = get_telethon_client(cfg)

    meta = ffprobe_probe(path)
    width = meta.get("width", 0) or None
    height = meta.get("height", 0) or None
    duration = meta.get("duration", 0.0) or None

    # Progress callback
    size = path.stat().st_size
    last_log = {"t": 0}

    def on_progress(sent, total):
        now = time.time()
        if now - last_log["t"] >= 0.25:
            pct = (sent / total) * 100.0 if total else 0.0
            logging.info("MTProto upload %s: %.1f%% (%s/%s)",
                         path.name, pct, human_size(sent), human_size(total))
            last_log["t"] = now

    attrs = []
    if duration or width or height:
        try:
            attrs.append(DocumentAttributeVideo(
                duration=int(duration) if duration else 0,
                w=int(width) if width else 0,
                h=int(height) if height else 0,
                supports_streaming=True
            ))
        except Exception:
            pass

    async def _send():
        entity = chat_id
        try:
            await client.send_file(
                entity,
                file=str(path),
                caption=caption,
                attributes=attrs if attrs else None,
                force_document=False,   # **garante como vídeo**
                video=True,             # dica adicional
                part_size_kb=cfg.mt_part_kb,
                progress_callback=on_progress
            )
            return True
        except Exception as e:
            logging.exception("MTProto falhou: %s", e)
            return False

    # roda o async
    try:
        ok = client.loop.run_until_complete(_send())
        if ok:
            logging.info("MTProto enviado: %s", path.name)
        return ok
    except KeyboardInterrupt:
        raise
    except Exception as e:
        logging.exception("MTProto erro: %s", e)
        return False

# ---------- Split por tamanho (sem reencode) ----------
def compute_piece_duration_seconds(path: Path, target_bytes: int) -> Optional[int]:
    meta = ffprobe_probe(path)
    if not meta or "duration" not in meta:
        return None
    total_bytes = path.stat().st_size
    total_sec = float(meta["duration"])
    # proporção aproximada: duração * (target_bytes / total_bytes)
    piece = max(60.0, total_sec * (target_bytes / max(1.0, float(total_bytes))))
    return int(piece)

def split_video_copy(path: Path, piece_bytes: int) -> List[Path]:
    """
    Particiona sem re-encode, por tempo aproximado, preservando streaming.
    Gera arquivos: <basename>.p001.mp4, .p002.mp4, ...
    """
    piece_dur = compute_piece_duration_seconds(path, piece_bytes)
    if not piece_dur:
        # fallback: ~15 minutos
        piece_dur = 15 * 60

    base = path.with_suffix("")  # remove .mp4
    out_pattern = f"{base.name}.p%03d.mp4"
    out_dir = path.parent

    cmd = [
        "ffmpeg", "-hide_banner", "-loglevel", "error",
        "-i", str(path),
        "-c", "copy",
        "-map", "0",
        "-f", "segment",
        "-segment_time", str(piece_dur),
        "-reset_timestamps", "1",
        str(out_dir / out_pattern)
    ]
    logging.info("Split ffmpeg: %s", " ".join(shlex.quote(c) for c in cmd))
    rc, out, err = run_cmd(cmd)
    if rc != 0:
        logging.error("ffmpeg split falhou: %s", err.strip())
        return []

    parts = sorted(out_dir.glob(f"{base.name}.p*.mp4"))
    # Filtra vazios/corrompidos
    parts = [p for p in parts if p.stat().st_size > 1024 * 1024]
    for p in parts:
        logging.info("Criado segmento: %s (%s)", p.name, human_size(p.stat().st_size))
    return parts

# ---------- Envio orquestrado ----------
def build_caption(cfg: Cfg, path: Path, part_idx: Optional[int] = None, part_total: Optional[int] = None) -> str:
    # folder_tag = nome da subpasta (modelo)
    try:
        folder_tag = path.parent.name
    except Exception:
        folder_tag = ""
    base_caption = cfg.caption_template.format(folder_tag=f"[{folder_tag}]" if folder_tag else "", filename=path.name)
    if part_idx is not None and part_total is not None and part_total > 1:
        return f"{base_caption} • Parte {part_idx}/{part_total}"
    return base_caption

def send_one(cfg: Cfg, path: Path) -> bool:
    size = path.stat().st_size
    size_mb = size / (1024**2)
    size_gb = size / (1024**3)

    # 1) pequenos: Bot API (evita 413)
    if cfg.use_bot_api and size_mb <= cfg.bot_small_limit_mb:
        ok = upload_via_bot_api(cfg.chat_id, path, build_caption(cfg, path))
        if ok and cfg.delete_after_send:
            try:
                path.unlink()
                logging.info("Apagado: %s", path.name)
            except Exception:
                pass
        return ok

    # 2) MTProto direto (<= limite do cliente)
    mt_limit_gb = 4.0 if cfg.premium else 2.0
    if cfg.enable_mtproto and size_gb <= mt_limit_gb:
        ok = upload_via_mtproto(cfg.chat_id, path, build_caption(cfg, path))
        if ok and cfg.delete_after_send:
            try:
                path.unlink()
                logging.info("Apagado: %s", path.name)
            except Exception:
                pass
        return ok

    # 3) Maior que o limite -> split
    if cfg.enable_split and cfg.enable_mtproto:
        target_gb = min(cfg.split_piece_gb, mt_limit_gb - 0.05)  # margem de segurança
        target_bytes = int(target_gb * (1024**3))
        parts = split_video_copy(path, target_bytes)
        if not parts:
            logging.error("Sem partes geradas; abortando envio de %s", path.name)
            return False

        total = len(parts)
        ok_all = True
        for i, part in enumerate(parts, start=1):
            cap = build_caption(cfg, part, i, total)
            ok = upload_via_mtproto(cfg.chat_id, part, cap)
            if ok:
                try:
                    part.unlink()
                    logging.info("Apagado segmento: %s", part.name)
                except Exception:
                    pass
            else:
                ok_all = False
                # opcional: parar nos erros
                logging.error("Falha ao enviar segmento %s", part.name)
                break

        # remove original no fim (se configurado)
        if ok_all and cfg.delete_after_send and not cfg.keep_original_after_split:
            try:
                path.unlink()
                logging.info("Apagado original após split: %s", path.name)
            except Exception:
                pass
        return ok_all

    logging.error(
        "Arquivo %s (%s) excede o limite (%s GB) e split/MTProto está desabilitado.",
        path.name, human_size(size), mt_limit_gb
    )
    return False

# ---------- Loop principal ----------
_stop = False
def _sig_handler(signum, frame):
    global _stop
    logging.warning("Sinal recebido (%s). Encerrando após o ciclo atual...", signum)
    _stop = True

signal.signal(signal.SIGINT, _sig_handler)
signal.signal(signal.SIGTERM, _sig_handler)

def run_once(cfg: Cfg):
    # Recupera parciais primeiro
    recover_partials(cfg.download_dir, cfg)
    # Lista prontos
    files = list_ready_files(cfg.download_dir, cfg)
    if not files:
        return
    for p in files:
        try:
            logging.info("Preparando envio: %s (%s)", p.name, human_size(p.stat().st_size))
            ok = send_one(cfg, p)
            if not ok:
                logging.error("Envio falhou: %s", p.name)
        except Exception as e:
            logging.exception("Erro ao processar %s: %s", p.name, e)

def main():
    global cfg
    cfg = load_cfg()

    # Garante pastas
    cfg.download_dir.mkdir(parents=True, exist_ok=True)
    cfg.log_dir.mkdir(parents=True, exist_ok=True)
    if cfg.quarantine_dir:
        cfg.quarantine_dir.mkdir(parents=True, exist_ok=True)

    logging.info(
        "Uploader iniciado. Pasta detectada: %s | WATCH=%s",
        str(cfg.download_dir), int(cfg.watch)
    )

    # Avisos de configuração
    if not cfg.chat_id:
        logging.error("TELEGRAM_CHAT_ID não definido no .env")
    if not cfg.bot_token:
        logging.error("TELEGRAM_TOKEN não definido no .env")
    if cfg.enable_mtproto and (not cfg.tg_api_id or not cfg.tg_api_hash):
        logging.error("ENABLE_MTPROTO=1 mas TG_API_ID/TG_API_HASH não estão definidos.")

    # Loop
    if getenv_bool("WATCH", 1):
        while not _stop:
            run_once(cfg)
            time.sleep(cfg.watch_interval)
    else:
        run_once(cfg)

if __name__ == "__main__":
    main()
