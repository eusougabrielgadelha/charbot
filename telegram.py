#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import re
import sys
import json
import time
import shlex
import signal
import logging
import asyncio
import subprocess
from pathlib import Path
from typing import List, Optional, Tuple, Dict, Any

# ========= util: carregar .env (sem exigir python-dotenv) =========
def _load_env_file(dotenv_path: Path):
    if not dotenv_path.exists():
        return
    try:
        from dotenv import load_dotenv
        load_dotenv(dotenv_path)
        return
    except Exception:
        pass
    for line in dotenv_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "#" in line:
            line = re.split(r"\s+#", line, 1)[0].strip()
        if "=" not in line:
            continue
        k, v = line.split("=", 1)
        k = k.strip()
        v = v.strip().strip('"').strip("'")
        os.environ.setdefault(k, v)

ROOT = Path(__file__).resolve().parent
_load_env_file(ROOT / ".env")

# ========= helpers =========
def env_bool(name: str, default: bool=False) -> bool:
    return str(os.environ.get(name, "1" if default else "0")).strip().lower() in ("1","true","yes","on","y")

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

def run(cmd: str, check: bool=True) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=check)

# ========= config =========
DOWNLOAD_DIR   = Path(os.environ.get("DOWNLOAD_DIR", str(ROOT / "download")))
LOG_DIR        = Path(os.environ.get("LOG_DIR", str(ROOT / "logs")))
QUARANTINE_DIR = Path(os.environ.get("QUARANTINE_DIR", str(DOWNLOAD_DIR / "_bad")))

TELEGRAM_TOKEN      = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID_ENV= os.environ.get("TELEGRAM_CHAT_ID", "")

WATCH            = env_bool("WATCH", True)
WATCH_INTERVAL   = env_int("WATCH_INTERVAL", 10)
FILE_STABLE_AGE  = env_int("FILE_STABLE_AGE", 20)

DELETE_AFTER_SEND = env_bool("DELETE_AFTER_SEND", True)
MIN_FILE_MB       = env_int("MIN_FILE_MB", 5)
MAX_FILE_GB       = env_float("MAX_FILE_GB", 0.0)  # 0 = sem limite
EXTENSIONS        = [e.strip().lower() for e in os.environ.get("EXTENSIONS", ".mp4,.mkv,.mov,.m4v").split(",") if e.strip()]

CAPTION_TEMPLATE  = os.environ.get("CAPTION_TEMPLATE", "{folder_tag} {filename}").strip()

# Bot API: abaixo disso vai pelo Bot; acima, Telethon
BOT_MAX_MB        = env_int("BOT_MAX_MB", 190)

# Telethon
ENABLE_MTPROTO    = env_bool("ENABLE_MTPROTO", False)
TG_API_ID         = env_int("TG_API_ID", 0)
TG_API_HASH       = os.environ.get("TG_API_HASH", "")
TELETHON_SESSION  = os.environ.get("TELETHON_SESSION", str(ROOT / "telethon.session"))
MT_PART_KB        = env_int("MT_PART_KB", 1024)

# Split opcional se Telethon falhar
SPLIT_ON_TELETHON_FAIL = env_bool("SPLIT_ON_TELETHON_FAIL", False)
SEGMENT_SECONDS        = env_int("SEGMENT_SECONDS", 1226)

# Normalização de vídeo (1280×720)
FORCE_720P            = env_bool("FORCE_720P", True)
VIDEO_W               = env_int("VIDEO_WIDTH", 1280)
VIDEO_H               = env_int("VIDEO_HEIGHT", 720)
VIDEO_CRF             = env_int("VIDEO_CRF", 23)
VIDEO_PRESET          = os.environ.get("VIDEO_PRESET", "veryfast").strip()
VIDEO_MAX_FPS         = env_int("VIDEO_MAX_FPS", 30)  # 0 = manter
REMUX_ONLY_WHEN_OK    = env_bool("REMUX_ONLY_WHEN_OK", True)  # se já estiver 1280x720 h264/aac → só faststart

# ========= logging =========
LOG_DIR.mkdir(parents=True, exist_ok=True)
log_path = LOG_DIR / "uploader.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout), logging.FileHandler(log_path, encoding="utf-8")]
)

# ========= misc =========
def ensure_dirs():
    DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
    QUARANTINE_DIR.mkdir(parents=True, exist_ok=True)

def is_stable(p: Path, age_seconds: int) -> bool:
    try:
        st = p.stat()
    except FileNotFoundError:
        return False
    return (time.time() - st.st_mtime) >= age_seconds

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
    candidates.sort(key=lambda x: x.stat().st_mtime)
    return candidates

def build_caption(path: Path) -> str:
    folder_tag = f"[{path.parent.name}]"
    return CAPTION_TEMPLATE.format(folder_tag=folder_tag, filename=path.name, stem=path.stem).strip()

def parse_peer(chat_id_env: str | int):
    if isinstance(chat_id_env, int):
        return chat_id_env
    s = str(chat_id_env).strip()
    if re.fullmatch(r"-?\d+", s):
        try:
            return int(s)
        except Exception:
            pass
    return s  # ex.: @canal

def bot_limit_bytes() -> int:
    return BOT_MAX_MB * 1024 * 1024

# ========= FFprobe helpers =========
def probe_media(p: Path) -> Dict[str, Any]:
    cmd = f"ffprobe -v error -select_streams v:0 -show_entries stream=width,height,codec_name,avg_frame_rate,bit_rate -show_entries format=duration -of json {shlex.quote(str(p))}"
    try:
        cp = run(cmd, check=True)
        return json.loads(cp.stdout)
    except Exception as e:
        logging.warning("ffprobe falhou (%s)", e)
        return {}

def _probe_dims(p: Path) -> Tuple[Optional[int], Optional[int], Optional[float]]:
    data = probe_media(p)
    w = h = None
    dur = None
    if "streams" in data and data["streams"]:
        w = data["streams"][0].get("width")
        h = data["streams"][0].get("height")
    if "format" in data and "duration" in data["format"]:
        try:
            dur = float(data["format"]["duration"])
        except Exception:
            dur = None
    return w, h, dur

# ========= Preparação do arquivo (remux / encode 720p) =========
def ensure_mp4_faststart(src: Path) -> Path:
    """Remuxa para .mp4 com faststart, sem reencoder (copia codecs)."""
    out = src.with_suffix(".tg.mp4") if src.suffix.lower() != ".mp4" else src.with_name(src.stem + ".__tmp__.mp4")
    cmd = f"ffmpeg -hide_banner -loglevel error -y -i {shlex.quote(str(src))} -c copy -movflags +faststart {shlex.quote(str(out))}"
    run(cmd, check=True)
    if out.name.endswith(".__tmp__.mp4"):
        final = src  # mesmo nome, só realocar moov
        src.unlink(missing_ok=True)
        out.replace(final)
        return final
    return out

def transcode_to_720p(src: Path) -> Path:
    """Reencoda garantindo exatamente 1280x720 com letterbox (sem crop)."""
    out = src.with_suffix(".tg.mp4")
    vf = [
        f"scale={VIDEO_W}:{VIDEO_H}:force_original_aspect_ratio=decrease",
        f"pad={VIDEO_W}:{VIDEO_H}:(ow-iw)/2:(oh-ih)/2:color=black",
        "setsar=1",
        "setdar=16/9",
    ]
    fps_flag = f"-r {VIDEO_MAX_FPS}" if VIDEO_MAX_FPS > 0 else ""
    cmd = (
        f"ffmpeg -hide_banner -loglevel error -y -i {shlex.quote(str(src))} "
        f"-vf \"{','.join(vf)}\" {fps_flag} "
        f"-c:v libx264 -profile:v high -level 4.1 -pix_fmt yuv420p -preset {shlex.quote(VIDEO_PRESET)} -crf {VIDEO_CRF} "
        f"-c:a aac -b:a 128k -movflags +faststart {shlex.quote(str(out))}"
    )
    run(cmd, check=True)
    return out

def prepare_for_telegram(src: Path) -> Tuple[Path, Dict[str, Any], bool]:
    """
    Retorna: (arquivo_pronto, meta{width,height,duration}, created_temp)
    created_temp=True se criamos um novo arquivo (p/ apagar depois).
    """
    w, h, dur = _probe_dims(src)
    meta = {"width": w or VIDEO_W, "height": h or VIDEO_H, "duration": int(dur) if dur else None}

    try:
        if FORCE_720P:
            # Se já está perfeito e REMUX_ONLY_WHEN_OK, só faststart
            if REMUX_ONLY_WHEN_OK and w == VIDEO_W and h == VIDEO_H:
                ok = ensure_mp4_faststart(src)
                if ok == src:
                    return src, meta, False
                else:
                    nw, nh, nd = _probe_dims(ok)
                    meta.update(width=nw or VIDEO_W, height=nh or VIDEO_H, duration=int(nd) if nd else meta["duration"])
                    return ok, meta, True
            # Reencode para 1280x720
            out = transcode_to_720p(src)
            nw, nh, nd = _probe_dims(out)
            meta.update(width=nw or VIDEO_W, height=nh or VIDEO_H, duration=int(nd) if nd else meta["duration"])
            return out, meta, True
        else:
            # Não forçar 720p: apenas garantir MP4 + faststart
            out = ensure_mp4_faststart(src)
            if out == src:
                return src, meta, False
            nw, nh, nd = _probe_dims(out)
            meta.update(width=nw or w, height=nh or h, duration=int(nd) if nd else meta["duration"])
            return out, meta, True
    except subprocess.CalledProcessError as e:
        logging.error("Falha na preparação (ffmpeg): %s", e)
        # se falhar, tenta ao menos faststart
        try:
            out = ensure_mp4_faststart(src)
            nw, nh, nd = _probe_dims(out)
            meta.update(width=nw or w, height=nh or h, duration=int(nd) if nd else meta["duration"])
            return out, meta, (out != src)
        except Exception as e2:
            logging.error("Falhou faststart também: %s", e2)
            return src, meta, False

# ========= Bot API =========
import requests

def upload_via_bot(p: Path, caption: str, meta: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendVideo"
    data = {
        "chat_id": TELEGRAM_CHAT_ID_ENV,
        "caption": caption,
        "supports_streaming": True,
        "disable_notification": True,
    }
    # ajuda o Telegram a entender o player
    if meta.get("width"): data["width"] = int(meta["width"])
    if meta.get("height"): data["height"] = int(meta["height"])
    if meta.get("duration"): data["duration"] = int(meta["duration"])

    with p.open("rb") as fh:
        files = {"video": (p.name, fh, "video/mp4")}
        try:
            resp = requests.post(url, data=data, files=files, timeout=1800)
        except Exception as e:
            return False, f"requests error: {e}"

    try:
        js = resp.json()
    except Exception:
        js = {"status_code": resp.status_code, "text": resp.text}

    if resp.ok and isinstance(js, dict) and js.get("ok") is True:
        return True, None

    if resp.status_code == 413:
        logging.error("BOT falhou [413]: %s", json.dumps(js, ensure_ascii=False))
        return False, "413"

    return False, json.dumps(js, ensure_ascii=False)

# ========= Telethon =========
_telethon_import_ok = True
try:
    from telethon import TelegramClient
except Exception as e:
    _telethon_import_ok = False
    if ENABLE_MTPROTO:
        logging.warning("Telethon não disponível (%s). Instale com: pip install telethon", e)

async def _send_with_telethon(file_path: Path, caption: str) -> None:
    if not _telethon_import_ok:
        raise RuntimeError("Telethon não instalado")
    if not ENABLE_MTPROTO:
        raise RuntimeError("ENABLE_MTPROTO=0")
    if not TG_API_ID or not TG_API_HASH:
        raise RuntimeError("TG_API_ID/TG_API_HASH ausentes")

    client = TelegramClient(TELETHON_SESSION, TG_API_ID, TG_API_HASH)
    await client.start(bot_token=TELEGRAM_TOKEN)

    try:
        peer = parse_peer(TELEGRAM_CHAT_ID_ENV)
        await client.send_file(
            entity=peer,
            file=str(file_path),
            caption=caption,
            force_document=False,      # <<< garante como vídeo
            supports_streaming=True,   # <<< player
            part_size_kb=max(32, MT_PART_KB),
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

# ========= Split opcional =========
def ffmpeg_split(file_path: Path) -> List[Path]:
    out_pat = file_path.with_suffix("").as_posix() + ".p%03d.mp4"
    cmd = (
        f'ffmpeg -hide_banner -loglevel error -i {shlex.quote(str(file_path))} '
        f'-c copy -map 0 -f segment -segment_time {SEGMENT_SECONDS} -reset_timestamps 1 {shlex.quote(out_pat)}'
    )
    logging.info("Split ffmpeg: %s", cmd)
    try:
        run(cmd, check=True)
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

# ========= Envio =========
def should_use_bot(size_bytes: int) -> bool:
    return size_bytes <= bot_limit_bytes()

def send_one(src: Path) -> bool:
    caption = build_caption(src)
    size = src.stat().st_size
    logging.info("Preparando envio: %s (%s)", src.name, human_size(size))

    # 0) preparar arquivo (mp4 + faststart + 1280x720 se configurado)
    prepared, meta, created_temp = prepare_for_telegram(src)

    # 1) tenta Bot API se couber
    ok = False
    err: Optional[str] = None
    if TELEGRAM_TOKEN and should_use_bot(prepared.stat().st_size):
        ok, err = upload_via_bot(prepared, caption, meta)
        if ok:
            logging.info("BOT ok: %s", prepared.name)
    if not ok:
        if err == "413":
            logging.info("Grande p/ Bot → fallback Telethon…")
        elif err:
            logging.warning("BOT falhou: %s", err)

        if ENABLE_MTPROTO:
            ok, err = send_via_telethon(prepared, caption)
            if ok:
                logging.info("MTProto ok: %s", prepared.name)
            else:
                if SPLIT_ON_TELETHON_FAIL:
                    logging.info("Tentando split + Telethon…")
                    parts = ffmpeg_split(prepared)
                    ok = True
                    for i, part in enumerate(parts, 1):
                        part_caption = f"{caption} • parte {i}/{len(parts)}"
                        ok2, _ = send_via_telethon(part, part_caption)
                        ok = ok and ok2
                        if ok2 and DELETE_AFTER_SEND:
                            part.unlink(missing_ok=True)
        else:
            logging.error("Sem MTProto habilitado e Bot API não enviou.")

    # 2) limpeza
    if ok and DELETE_AFTER_SEND:
        try:
            src.unlink(missing_ok=True)
        except Exception as e:
            logging.warning("Falhou ao apagar original %s: %s", src, e)
    if created_temp:
        try:
            prepared.unlink(missing_ok=True)
        except Exception:
            pass

    return ok

# ========= Loop =========
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
    # Overrides rápidos por env
    try:
        if "DOWNLOAD_DIR" in os.environ:
            DOWNLOAD_DIR = Path(os.environ["DOWNLOAD_DIR"])
        if "WATCH" in os.environ:
            WATCH = env_bool("WATCH", WATCH)
    except Exception:
        pass
    main()
