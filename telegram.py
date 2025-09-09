#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import time
import json
import shutil
import signal
import logging
import contextlib
import subprocess
from pathlib import Path
from typing import Optional, List, Tuple

# =========================
# Helpers básicos
# =========================

def getenv_bool(name: str, default: str = "0") -> bool:
    return str(os.getenv(name, default)).strip().lower() in ("1", "true", "yes", "y", "on")

def human_size(n: int) -> str:
    units = ["B", "KB", "MB", "GB", "TB"]
    f = float(n)
    for u in units:
        if f < 1024 or u == units[-1]:
            return f"{f:.2f}{u}"
        f /= 1024

def which_ffmpeg() -> Optional[str]:
    return shutil.which("ffmpeg")

def which_ffprobe() -> Optional[str]:
    return shutil.which("ffprobe")

SCRIPT_DIR = Path(__file__).resolve().parent

# =========================
# Carrega .env local (se existir)
# =========================
# Isso garante que as variáveis do .env sejam lidas mesmo quando o PM2 não injeta env.
try:
    from dotenv import load_dotenv
    _dot = SCRIPT_DIR / ".env"
    if _dot.exists():
        load_dotenv(dotenv_path=_dot, override=True)
except Exception:
    pass

# =========================
# Descoberta de diretórios (dinâmica)
# =========================

def detect_download_dir() -> Path:
    """
    1) DOWNLOAD_DIR do env (se existir)
    2) /root/charbot/download
    3) SCRIPT_DIR/download
    4) CWD/download
    Cria a pasta escolhida, se não existir.
    """
    cands: List[Path] = []

    env_dir = os.getenv("DOWNLOAD_DIR", "").strip()
    if env_dir:
        cands.append(Path(env_dir).expanduser())

    cands.append(Path("/root/charbot/download"))
    cands.append(SCRIPT_DIR / "download")
    cands.append(Path.cwd() / "download")

    for c in cands:
        try:
            c.mkdir(parents=True, exist_ok=True)
            return c.resolve()
        except Exception:
            continue

    # fallback duro
    fallback = SCRIPT_DIR / "download"
    fallback.mkdir(parents=True, exist_ok=True)
    return fallback.resolve()

def detect_quarantine_dir(download_dir: Path) -> Path:
    env_q = os.getenv("QUARANTINE_DIR", "").strip()
    q = Path(env_q) if env_q else (download_dir / "_bad")
    q.mkdir(parents=True, exist_ok=True)
    return q.resolve()

# =========================
# Variáveis (após descobrir pastas)
# =========================

DOWNLOAD_DIR = detect_download_dir()
QUARANTINE_DIR = detect_quarantine_dir(DOWNLOAD_DIR)

# Bot
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()

# Envio
DELETE_AFTER_SEND = getenv_bool("DELETE_AFTER_SEND", "1")   # apaga o arquivo original após enviar
USER_AGENT = os.getenv(
    "USER_AGENT",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
)

# Watch / Loop
WATCH = getenv_bool("WATCH", "1")
WATCH_INTERVAL = int(os.getenv("WATCH_INTERVAL", "10"))

# Extensões finais aceitas (sem .part)
EXTENSIONS = [e if e.startswith(".") else "." + e for e in os.getenv("EXTENSIONS", ".mp4,.mkv,.mov,.m4v").split(",")]
EXTENSIONS = [e.strip().lower() for e in EXTENSIONS if e.strip()]

# Tamanhos e estabilidade
MIN_FILE_MB = int(os.getenv("MIN_FILE_MB", "5"))           # ignora arquivos finais < 5MB
MAX_FILE_GB = float(os.getenv("MAX_FILE_GB", "0"))         # 0 = sem limite
FILE_STABLE_AGE = int(os.getenv("FILE_STABLE_AGE", os.getenv("STABLE_AGE", "30")))  # compat

# Recuperação de .part
RECOVER_PARTS = getenv_bool("RECOVER_PARTS", "1")
PART_STABLE_AGE = int(os.getenv("PART_STABLE_AGE", "180")) # .part precisa "parar de crescer" por N seg
PART_MIN_MB = int(os.getenv("PART_MIN_MB", "5"))           # ignora .part < 5MB
MOVE_BAD_PARTS = getenv_bool("MOVE_BAD_PARTS", "1")

# Log
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

# =========================
# Logging
# =========================

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(message)s",
)

# =========================
# ffprobe / ffmpeg helpers
# =========================

def ffprobe_json(path: Path) -> dict:
    ffprobe = which_ffprobe()
    if not ffprobe:
        return {}
    try:
        out = subprocess.check_output([
            ffprobe, "-v", "error",
            "-print_format", "json",
            "-show_streams", "-show_format",
            str(path)
        ], stderr=subprocess.STDOUT)
        return json.loads(out.decode("utf-8", "ignore"))
    except Exception:
        return {}

def probe_video_params(path: Path) -> Tuple[int, int, int]:
    """
    Retorna (width, height, duration_seg) se conseguir; senão (0,0,0).
    """
    info = ffprobe_json(path)
    width = height = dur = 0
    for s in info.get("streams", []):
        if s.get("codec_type") == "video":
            try:
                width = int(float(s.get("width", 0) or 0))
                height = int(float(s.get("height", 0) or 0))
            except Exception:
                width = height = 0
            ds = s.get("duration")
            if ds:
                try:
                    dur = int(float(ds))
                except Exception:
                    pass
            break
    if dur == 0:
        df = info.get("format", {}).get("duration")
        if df:
            try:
                dur = int(float(df))
            except Exception:
                pass
    return width, height, dur

def is_mp4_h264_aac(path: Path) -> bool:
    if path.suffix.lower() != ".mp4":
        return False
    info = ffprobe_json(path)
    v_ok = a_ok = False
    for s in info.get("streams", []):
        if s.get("codec_type") == "video" and s.get("codec_name") in ("h264", "avc1"):
            v_ok = True
        if s.get("codec_type") == "audio" and s.get("codec_name") in ("aac",):
            a_ok = True
    return v_ok and a_ok

def remux_copy(src: Path, dst: Path, container: str = "mp4") -> bool:
    """
    Remux cópia (sem reencode) para MP4 ou MKV com flags tolerantes.
    """
    ffmpeg = which_ffmpeg()
    if not ffmpeg:
        return False
    out = dst.with_suffix(f".{container}")
    args = [
        ffmpeg, "-y",
        "-fflags", "+genpts", "-err_detect", "ignore_err",
        "-i", str(src),
        "-c", "copy",
    ]
    if container == "mp4":
        args += ["-movflags", "+faststart"]
    args += [str(out)]
    try:
        subprocess.check_call(args, stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)
        return out.exists()
    except Exception:
        with contextlib.suppress(Exception):
            if out.exists(): out.unlink()
        return False

def transcode_preserve_ar(src: Path, dst: Path, container: str = "mp4") -> bool:
    """
    Transcode preservando AR (nada de quadrado), 1280x720 bounding box e setsar=1.
    """
    ffmpeg = which_ffmpeg()
    if not ffmpeg:
        return False
    out = dst.with_suffix(f".{container}")
    scale_expr = "scale='if(gt(a,16/9),1280,-2)':'if(gt(a,16/9),-2,720)',setsar=1"
    try:
        subprocess.check_call([
            ffmpeg, "-y",
            "-fflags", "+genpts", "-err_detect", "ignore_err",
            "-analyzeduration", "100M", "-probesize", "100M",
            "-i", str(src),
            "-c:v", "libx264", "-preset", "veryfast", "-crf", "23",
            "-vf", scale_expr,
            "-c:a", "aac", "-b:a", "128k",
            "-movflags", "+faststart" if container == "mp4" else "frag_keyframe",
            str(out)
        ], stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)
        return out.exists()
    except Exception:
        with contextlib.suppress(Exception):
            if out.exists(): out.unlink()
        return False

def remux_to_mp4_copy(src: Path, dst: Path) -> bool:
    ffmpeg = which_ffmpeg()
    if not ffmpeg:
        return False
    try:
        subprocess.check_call([
            ffmpeg, "-y",
            "-fflags", "+genpts", "-err_detect", "ignore_err",
            "-i", str(src),
            "-c", "copy",
            "-movflags", "+faststart",
            str(dst)
        ], stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)
        return dst.exists()
    except Exception:
        with contextlib.suppress(Exception):
            if dst.exists(): dst.unlink()
        return False

def remux_to_mp4_copy_v_copy_aac(src: Path, dst: Path) -> bool:
    """
    Copia vídeo; força áudio AAC se necessário.
    """
    ffmpeg = which_ffmpeg()
    if not ffmpeg:
        return False
    try:
        subprocess.check_call([
            ffmpeg, "-y",
            "-fflags", "+genpts", "-err_detect", "ignore_err",
            "-i", str(src),
            "-c:v", "copy",
            "-c:a", "aac", "-b:a", "128k",
            "-movflags", "+faststart",
            str(dst)
        ], stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)
        return dst.exists()
    except Exception:
        with contextlib.suppress(Exception):
            if dst.exists(): dst.unlink()
        return False

def ensure_streamable_mp4(src: Path) -> Tuple[Path, List[Path]]:
    """
    Garante um .mp4 “streamável”:
      1) se já for MP4 H.264/AAC → retorna src
      2) remux copy → .mp4
      3) copy de vídeo + AAC no áudio → .mp4
      4) transcode completo (preserva AR) → .mp4
    Retorna (mp4_path, temporarios_gerados_para_limpar)
    """
    temps: List[Path] = []
    if is_mp4_h264_aac(src):
        return src, temps

    base = src.with_suffix("")  # remove .part se houver
    target = base.with_suffix(".mp4")
    if target.exists():
        target = target.with_stem(target.stem + f"__tg_{int(time.time())}")

    # 1) remux copy puro
    if remux_to_mp4_copy(src, target):
        temps.append(target)
        return target, temps

    # 2) copia vídeo + AAC
    if remux_to_mp4_copy_v_copy_aac(src, target):
        temps.append(target)
        return target, temps

    # 3) transcode completo
    if transcode_preserve_ar(src, base, container="mp4"):
        target = base.with_suffix(".mp4")
        temps.append(target)
        return target, temps

    # se nada deu certo, volta original
    return src, temps

# =========================
# Recuperação de .part
# =========================

def finalize_part_file(p: Path) -> Optional[Path]:
    """
    Estratégia em cascata para recuperar .part:
      1) remux → MP4
      2) remux → MKV
      3) transcode → MP4 (AR preservada)
      4) transcode → MKV
    Retorna caminho final gerado, se houver.
    """
    base_no_part = p.with_suffix("")
    out_base = base_no_part if base_no_part.suffix.lower() in (".mp4", ".mkv") else base_no_part.with_suffix(".mp4")

    # 1) remux MP4
    if remux_copy(p, out_base, container="mp4"):
        return out_base.with_suffix(".mp4")

    # 2) remux MKV (mais tolerante)
    if remux_copy(p, out_base, container="mkv"):
        return out_base.with_suffix(".mkv")

    # 3) transcode MP4
    if transcode_preserve_ar(p, out_base, container="mp4"):
        return out_base.with_suffix(".mp4")

    # 4) transcode MKV
    if transcode_preserve_ar(p, out_base, container="mkv"):
        return out_base.with_suffix(".mkv")

    return None

def recover_partials(root: Path) -> List[Path]:
    """
    Busca .part estáveis e tenta finalizar (AGORA RECURSIVO).
    Move para quarentena se falhar (opcional).
    """
    recovered: List[Path] = []
    now = time.time()
    for pf in sorted(root.rglob("*.part")):  # <-- rglob para achar em subpastas
        try:
            age = now - pf.stat().st_mtime
            size = pf.stat().st_size
        except FileNotFoundError:
            continue

        if age < PART_STABLE_AGE:
            logging.debug("Parcial jovem (%.0fs < %ds): %s", age, PART_STABLE_AGE, pf)
            continue
        if size < PART_MIN_MB * 1024 * 1024:
            logging.warning("Parcial muito pequeno, pulando: %s (%s)", pf.name, human_size(size))
            continue

        logging.info("Recuperando parcial: %s", pf)
        out = finalize_part_file(pf)
        if out and out.exists():
            with contextlib.suppress(Exception):
                pf.unlink()
            logging.info("Parcial finalizado ➜ %s (%s)", out, human_size(out.stat().st_size))
            recovered.append(out)
        else:
            logging.warning("Falha ao finalizar parcial: %s", pf)
            if MOVE_BAD_PARTS:
                try:
                    QUARANTINE_DIR.mkdir(parents=True, exist_ok=True)
                    dest = QUARANTINE_DIR / pf.name
                    pf.rename(dest)
                    logging.info("Movido para quarentena: %s", dest)
                except Exception as e:
                    logging.error("Não foi possível mover para quarentena: %s -> %s (%s)", pf, QUARANTINE_DIR, e)
    return recovered


# =========================
# Seleção de arquivos prontos
# =========================

def is_stable(path: Path, min_age: int) -> bool:
    try:
        age = time.time() - path.stat().st_mtime
        return age >= min_age
    except FileNotFoundError:
        return False

def list_ready_files(root: Path) -> List[Path]:
    """
    Lista arquivos finalizados, recorrendo subpastas e explicando no log
    por que cada um foi ignorado (tamanho, idade, extensão ou limite).
    Também ignora temporários tipo '.__tmp__.mp4'.
    Permite 'FORCE_SEND_ALL=1' para ignorar idade mínima.
    """
    force = str(os.getenv("FORCE_SEND_ALL", "0")).strip() in ("1", "true", "yes", "on")
    candidates: List[Path] = []
    exts = set(EXTENSIONS)  # p.ex.: {".mp4",".mkv",".mov",".m4v"}

    for p in root.rglob("*"):
        if not p.is_file():
            continue

        suf = p.suffix.lower()
        if suf not in exts:
            continue

        name = p.name.lower()

        # ignora parciais/temporários
        if name.endswith(".part"):
            logging.debug("Ignorando .part: %s", p)
            continue
        if "__tmp__" in name:
            logging.debug("Ignorando temporário (__tmp__): %s", p)
            continue

        # tamanho
        try:
            size = p.stat().st_size
        except FileNotFoundError:
            continue

        if size < MIN_FILE_MB * 1024 * 1024:
            logging.debug("Ignorando (pequeno < %dMB): %s (%s)", MIN_FILE_MB, p, human_size(size))
            continue

        # limite de tamanho (se configurado)
        if MAX_FILE_GB > 0 and size > MAX_FILE_GB * (1024 ** 3):
            logging.warning("Ignorando (muito grande, > %.2fGB): %s (%s)", MAX_FILE_GB, p, human_size(size))
            continue

        # estabilidade (a não ser que FORCE_SEND_ALL)
        if not force:
            try:
                age = time.time() - p.stat().st_mtime
            except FileNotFoundError:
                continue
            if age < FILE_STABLE_AGE:
                logging.debug("Ignorando (ainda mexendo, %.0fs < %ds): %s", age, FILE_STABLE_AGE, p)
                continue

        candidates.append(p)

    # mais antigos primeiro (quem está “parado” há mais tempo sai antes)
    def mtime_safe(x: Path) -> float:
        try:
            return x.stat().st_mtime
        except FileNotFoundError:
            return time.time()

    candidates.sort(key=mtime_safe)
    logging.info("Prontos para envio (%d): %s", len(candidates), ", ".join(str(c) for c in candidates[:10]) + ("..." if len(candidates) > 10 else ""))
    return candidates

def build_caption(path: Path) -> str:
    try:
        size = human_size(path.stat().st_size)
    except FileNotFoundError:
        size = "?"
    # Você pode personalizar com tags de pasta, etc:
    return f"{path.name} | {size}"

# =========================
# Envio p/ Telegram (sempre VÍDEO)
# =========================

def upload_via_bot(chat_id: str, path: Path, caption: str) -> bool:
    """
    Envia sempre com sendVideo (vídeo, não documento),
    garantindo MP4 streamável antes do envio.
    """
    try:
        import requests
    except Exception:
        logging.error("Biblioteca 'requests' não instalada.")
        return False

    # Encoder com barra de progresso (opcional)
    try:
        from requests_toolbelt.multipart.encoder import MultipartEncoder, MultipartEncoderMonitor
    except Exception:
        MultipartEncoder = MultipartEncoderMonitor = None

    # 1) garante MP4 streamável
    mp4_path, temps = ensure_streamable_mp4(path)
    w, h, dur = probe_video_params(mp4_path)

    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendVideo"
    size = mp4_path.stat().st_size
    filename = mp4_path.name

    fields = {
        "chat_id": str(chat_id),
        "caption": caption,
        "supports_streaming": "true",
    }
    if w > 0: fields["width"] = str(w)
    if h > 0: fields["height"] = str(h)
    if dur > 0: fields["duration"] = str(dur)

    headers = {"User-Agent": USER_AGENT}

    if MultipartEncoder and MultipartEncoderMonitor:
        fields["video"] = (filename, open(mp4_path, "rb"), "video/mp4")
        enc = MultipartEncoder(fields=fields)
        sent = {"n": 0}

        def cb(m: MultipartEncoderMonitor):
            sent["n"] = m.bytes_read
            pct = (sent["n"]/size*100) if size else 0.0
            logging.info("BOT upload %s: %.1f%% (%s/%s)", filename, pct, human_size(sent["n"]), human_size(size))

        mon = MultipartEncoderMonitor(enc, cb)
        headers.update({"Content-Type": mon.content_type})

        try:
            r = requests.post(url, data=mon, headers=headers, timeout=60*60)
            if r.ok:
                logging.info("BOT enviado: %s", filename)
                # Limpa temporários gerados nesta função
                for t in temps:
                    if t != path and t.exists():
                        with contextlib.suppress(Exception):
                            t.unlink()
                return True
            else:
                logging.error("BOT falhou [%s]: %s", r.status_code, r.text[:300])
                return False
        except Exception as e:
            logging.exception("BOT exceção ao enviar %s: %s", filename, e)
            return False
    else:
        # Fallback sem progresso
        with open(mp4_path, "rb") as f:
            files = {"video": (filename, f, "video/mp4")}
            try:
                r = requests.post(url, data=fields, files=files, headers=headers, timeout=60*60)
                if r.ok:
                    logging.info("BOT enviado: %s", filename)
                    for t in temps:
                        if t != path and t.exists():
                            with contextlib.suppress(Exception):
                                t.unlink()
                    return True
                else:
                    logging.error("BOT falhou [%s]: %s", r.status_code, r.text[:300])
                    return False
            except Exception as e:
                logging.exception("BOT exceção ao enviar %s: %s", filename, e)
                return False

# =========================
# Pipeline de envio
# =========================

def send_one(path: Path) -> bool:
    """
    Envia um arquivo (como vídeo) e apaga o original se configurado.
    """
    caption = build_caption(path)
    ok = upload_via_bot(TELEGRAM_CHAT_ID, path, caption)
    if ok and DELETE_AFTER_SEND:
        with contextlib.suppress(Exception):
            if path.exists():
                path.unlink()
        logging.info("Apagado: %s", path.name)
    return ok

def run_once():
    # 1) Recupera .part (opcional)
    if RECOVER_PARTS:
        recover_partials(DOWNLOAD_DIR)

    # 2) Envia arquivos prontos (varre recursivamente subpastas)
    files = list_ready_files(DOWNLOAD_DIR)
    if not files:
        logging.debug("Nenhum arquivo pronto para envio.")
        return

    for p in files:
        logging.info("Preparando envio: %s (%s)", p.name, human_size(p.stat().st_size))
        try:
            ok = send_one(p)
            if not ok:
                logging.error("Falha ao enviar: %s", p.name)
        except Exception:
            logging.exception("Erro inesperado ao enviar: %s", p.name)

# =========================
# Main
# =========================

STOP = False

def handle_sigterm(sig, frame):
    global STOP
    STOP = True
    logging.warning("Sinal recebido (%s). Encerrando após o ciclo atual...", sig)

def main():
    # Garante diretórios
    DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
    if MOVE_BAD_PARTS:
        QUARANTINE_DIR.mkdir(parents=True, exist_ok=True)

    logging.info(
        "Uploader iniciado. Pasta detectada: %s | WATCH=%s",
        str(DOWNLOAD_DIR), WATCH
    )

    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        logging.error("TELEGRAM_TOKEN e/ou TELEGRAM_CHAT_ID não configurados.")

    # Sinais para encerramento gracioso sob PM2
    signal.signal(signal.SIGINT, handle_sigterm)
    signal.signal(signal.SIGTERM, handle_sigterm)

    if not WATCH:
        run_once()
        return

    while not STOP:
        try:
            run_once()
            for _ in range(WATCH_INTERVAL):
                if STOP:
                    break
                time.sleep(1)
        except KeyboardInterrupt:
            break
        except Exception:
            logging.exception("Loop principal: erro inesperado")
            time.sleep(5)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass
