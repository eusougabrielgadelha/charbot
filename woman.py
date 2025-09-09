#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
woman.py — Coletor/baixador com retomada à prova de quedas

Principais recursos:
- Coleta URLs de salas a partir de uma listagem (Playwright headless).
- Baixa em paralelo com yt-dlp, usando .part (padrão) para permitir retomada.
- Se o processo cair/interromper:
  * Na próxima execução: varre .part estáveis e tenta finalizar para .mp4.
  * Em SIGINT/SIGTERM: antes de sair, tenta finalizar parciais.
- Pós-processamento: remux (copy) → fallback transcode 720p, opcionalmente normaliza FPS mínimo.

Dependências:
  pip install -U playwright yt-dlp
  python -m playwright install chromium
  python -m playwright install-deps chromium    # em Debian/Ubuntu
  sudo apt-get install -y ffmpeg
"""

import os
import re
import sys
import time
import glob
import shutil
import argparse
import logging
import threading
import subprocess
import signal
from dataclasses import dataclass
from typing import List, Optional, Dict, Tuple
from contextlib import contextmanager, suppress
from pathlib import Path
from collections import deque

# =========================
# Defaults
# =========================
DEFAULT_START_URL = "https://chaturbate.com/female-cams/"
DEFAULT_SELECTOR = 'li.roomCard a[data-testid="room-card-username"][href]'
DEFAULT_MAX_ACTIVE = 16
DEFAULT_LIMIT_ROOMS = 60
DEFAULT_DOWNLOAD_DIR = "download"
DEFAULT_LOG_DIR = "logs"
DEFAULT_CHECK_INTERVAL = 2.0
DEFAULT_NAV_TIMEOUT_MS = 60_000
DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
)

# Quanto tempo um .part precisa ficar sem ser modificado
# para considerarmos "estável" e tentar finalizar
DEFAULT_PART_STABLE_SEC = 60

# =========================
# Logging
# =========================
def setup_logging(log_dir: str):
    os.makedirs(log_dir, exist_ok=True)
    logfile = os.path.join(log_dir, "collector.log")
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(logfile, encoding="utf-8")
        ],
        datefmt="%H:%M:%S",
    )

class YTDLogger:
    def debug(self, msg): logging.debug(msg)
    def warning(self, msg): logging.warning(msg)
    def error(self, msg): logging.error(msg)

# =========================
# Utils
# =========================
def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)

def safe_name(s: str) -> str:
    s = s.strip()
    s = re.sub(r"[^\w\-.]+", "_", s, flags=re.UNICODE)
    return s[:180]

def run_cmd(cmd: List[str], check=False) -> Tuple[int, str]:
    try:
        out = subprocess.check_output(cmd, stderr=subprocess.STDOUT, text=True)
        return 0, out
    except subprocess.CalledProcessError as e:
        return e.returncode, e.output or ""

def which(bin_name: str) -> Optional[str]:
    for p in os.environ.get("PATH", "").split(os.pathsep):
        cand = os.path.join(p, bin_name)
        if os.path.isfile(cand) and os.access(cand, os.X_OK):
            return cand
    return None

def which_ffmpeg() -> Optional[str]:
    return which("ffmpeg")

def which_ffprobe() -> Optional[str]:
    return which("ffprobe")

def is_video_file(path: str) -> bool:
    ext = os.path.splitext(path)[1].lower()
    return ext in [".mp4", ".mkv", ".webm", ".mov", ".m4v"]

def stamp() -> str:
    return time.strftime("%Y%m%d-%H%M%S")

def is_stable_file(p: Path, min_age_sec: int) -> bool:
    try:
        return p.exists() and (time.time() - p.stat().st_mtime) >= min_age_sec
    except Exception:
        return False

# =========================
# Playwright helpers
# =========================
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError, Page

@contextmanager
def browser_context(user_agent: str, headless: bool = True, cookies_from_browser: Optional[str] = None,
                    cookie_file: Optional[str] = None, nav_timeout_ms: int = DEFAULT_NAV_TIMEOUT_MS):
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=headless, args=[
            "--no-sandbox",
            "--disable-setuid-sandbox",
            "--disable-dev-shm-usage",
            "--disable-gpu",
            "--disable-software-rasterizer",
        ])
        ctx = browser.new_context(user_agent=user_agent)
        if cookie_file and os.path.isfile(cookie_file):
            try:
                import json
                raw = Path(cookie_file).read_text(encoding="utf-8")
                cookies = json.loads(raw)
                ctx.add_cookies(cookies)
            except Exception as e:
                logging.warning("Falha ao carregar cookies de %s: %s", cookie_file, e)
        page = ctx.new_page()
        page.set_default_timeout(nav_timeout_ms)
        try:
            yield page
        finally:
            with suppress(Exception): page.close()
            with suppress(Exception): ctx.close()
            with suppress(Exception): browser.close()

def scroll_page(page: Page, steps: int = 0, pause_ms: int = 800):
    if steps <= 0:
        return
    for _ in range(steps):
        page.mouse.wheel(0, 4000)
        page.wait_for_timeout(pause_ms)

def collect_rooms(page: Page, start_url: str, selector: str, limit_rooms: int, scroll_steps: int = 0, scroll_pause_ms: int = 800) -> List[str]:
    logging.info("Navegando até %s", start_url)
    page.goto(start_url)
    scroll_page(page, steps=scroll_steps, pause_ms=scroll_pause_ms)
    page.wait_for_timeout(2500)
    anchors = page.query_selector_all(selector)
    urls = []
    for a in anchors:
        try:
            href = a.get_attribute("href") or ""
            if href and href.startswith("/"):
                href = "https://chaturbate.com" + href
            if href.startswith("https://"):
                urls.append(href)
        except Exception:
            pass
    urls = list(dict.fromkeys(urls))
    if limit_rooms and len(urls) > limit_rooms:
        urls = urls[:limit_rooms]
    logging.info("Coletadas %d salas.", len(urls))
    return urls

# =========================
# yt-dlp helpers
# =========================
@dataclass
class YTDLPEntry:
    url: str
    outtmpl: str
    tmpfile: str
    outfile: str

def build_outtmpl(username: str, download_dir: str) -> Tuple[str, str]:
    """
    Gera caminho final (.mp4) e esperado parcial (.mp4.part).
    O yt-dlp escreverá 'outfile.part' e, ao concluir, renomeará para 'outfile'.
    """
    subdir = os.path.join(download_dir, safe_name(username))
    ensure_dir(subdir)
    outfile = os.path.join(subdir, f"{stamp()}_{safe_name(username)}.mp4")
    tmpfile = outfile + ".part"
    return tmpfile, outfile

def parse_username_from_url(url: str) -> str:
    m = re.search(r"chaturbate\.com/([^/]+)/?", url)
    return safe_name(m.group(1)) if m else "unknown"

def ytdlp_download(url: str, outtmpl: str, extra_args: Optional[List[str]] = None) -> int:
    """
    Executa yt-dlp.
    Importante: NÃO usamos --no-part. Assim, o .part fica disponível para retomada.
    """
    args = [
        "yt-dlp",
        "--no-color",
        "--newline",
        "--retries", "5",
        "--fragment-retries", "5",
        "--concurrent-fragments", "5",
        "--downloader", "ffmpeg",
        "-o", outtmpl,
        url
    ]
    if extra_args:
        args += extra_args

    logging.info("Iniciando yt-dlp: %s", url)
    proc = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1)
    try:
        for line in proc.stdout:
            line = (line or "").rstrip()
            if line:
                # filtros simples para não poluir demais
                if ("[download]" in line) or ("[Chaturbate]" in line) or ("Merging formats" in line):
                    logging.info(line)
    except Exception:
        pass
    rc = proc.wait()
    logging.info("yt-dlp terminou rc=%s para %s", rc, url)
    return rc

def remux_copy_to_mp4(src: str, dst: str) -> bool:
    ffmpeg = which_ffmpeg()
    if not ffmpeg:
        logging.warning("ffmpeg não encontrado; remux pulado: %s", src)
        return False
    cmd = [ffmpeg, "-y", "-i", src, "-c", "copy", "-movflags", "+faststart", dst]
    rc, _ = run_cmd(cmd)
    ok = (rc == 0) and os.path.isfile(dst)
    if not ok:
        with suppress(Exception): os.path.isfile(dst) and os.remove(dst)
    return ok

def transcode_720_to_mp4(src: str, dst: str) -> bool:
    ffmpeg = which_ffmpeg()
    if not ffmpeg:
        logging.warning("ffmpeg não encontrado; transcode pulado: %s", src)
        return False
    cmd = [
        ffmpeg, "-y", "-i", src,
        "-c:v", "libx264", "-preset", "veryfast", "-vf", "scale=-2:720",
        "-c:a", "aac", "-b:a", "128k", "-movflags", "+faststart",
        dst
    ]
    rc, _ = run_cmd(cmd)
    ok = (rc == 0) and os.path.isfile(dst)
    if not ok:
        with suppress(Exception): os.path.isfile(dst) and os.remove(dst)
    return ok

def try_finalize_partial(tmp_path: str, min_fps: int = 0, do_720_when_fail: bool = True) -> bool:
    """
    Tenta transformar um arquivo parcial (.part) em .mp4 utilizável.
    1) remux (copy) → rápido
    2) fallback: transcode 720p
    Retorna True se gerou .mp4.
    """
    if not tmp_path or not os.path.isfile(tmp_path):
        return False

    if tmp_path.endswith(".part"):
        mp4_path = tmp_path[:-5]  # remove ".part"
    else:
        mp4_path = tmp_path + ".mp4"

    if os.path.isfile(mp4_path):
        # já existe final — nada a fazer
        return True

    # 1) tenta remux (copy)
    if remux_copy_to_mp4(tmp_path, mp4_path):
        logging.info("Remux OK: %s -> %s", os.path.basename(tmp_path), os.path.basename(mp4_path))
        return True

    # 2) fallback: transcode 720p
    tmp_out = mp4_path + ".__720p__.mp4"
    if transcode_720_to_mp4(tmp_path, tmp_out):
        try:
            os.replace(tmp_out, mp4_path)
            with suppress(Exception): os.remove(tmp_path)
            logging.info("Transcode 720p OK: %s", os.path.basename(mp4_path))
            return True
        except Exception as e:
            logging.error("Falha ao substituir após 720p: %s", e)
            with suppress(Exception): os.remove(tmp_out)

    return False

def salvage_outputs(tmp_path: Optional[str], out_path: Optional[str], min_fps: int = 0):
    """Tenta salvar algo útil caso download falhe."""
    if out_path and os.path.isfile(out_path):
        return
    if tmp_path and os.path.isfile(tmp_path):
        try_finalize_partial(tmp_path, min_fps=min_fps)

# =========================
# Job orchestration
# =========================
@dataclass
class JobInfo:
    url: str
    username: str
    tmpfile: str
    outfile: str
    status: str = "queued"  # queued, running, done, error

def worker(job: JobInfo, check_interval: float = DEFAULT_CHECK_INTERVAL, min_fps: int = 0):
    """Baixa uma sala com yt-dlp. Se cair, tenta finalizar parcial."""
    try:
        job.status = "running"
        # NÃO tocar no .part; deixe yt-dlp controlar

        rc = ytdlp_download(job.url, job.outfile)

        # Concluído normalmente?
        if rc == 0 and os.path.isfile(job.outfile):
            job.status = "done"
            return

        # Se não finalizou, tentar salvar parcial
        if os.path.isfile(job.tmpfile):
            ok = try_finalize_partial(job.tmpfile, min_fps=min_fps)
            if ok:
                job.status = "done"
                return

        job.status = "error"
    except Exception:
        logging.exception("Erro no worker %s", job.url)
        job.status = "error"

def build_jobs(urls: List[str], download_dir: str) -> List[JobInfo]:
    jobs: List[JobInfo] = []
    for url in urls:
        username = parse_username_from_url(url)
        tmp, outp = build_outtmpl(username, download_dir)
        jobs.append(JobInfo(url=url, username=username, tmpfile=tmp, outfile=outp))
    return jobs

# =========================
# Varreduras e pós-proc
# =========================
def sweep_finalize_partials(root_dir: str, min_fps: int = 0, min_age_sec: int = DEFAULT_PART_STABLE_SEC) -> int:
    """
    Varre diretório por *.part "estáveis" e tenta finalizar.
    Retorna quantos foram finalizados.
    """
    count = 0
    for part in Path(root_dir).rglob("*.part"):
        try:
            if is_stable_file(part, min_age_sec):
                if try_finalize_partial(str(part), min_fps=min_fps):
                    count += 1
        except Exception:
            logging.exception("Erro finalizando parcial: %s", part)
    if count:
        logging.info("Parciais finalizadas nesta varredura: %d", count)
    return count

# =========================
# Pipeline principal
# =========================
def main():
    parser = argparse.ArgumentParser(description="Coletor de salas + downloads paralelos (yt-dlp → MP4) com retomada.")
    parser.add_argument("--start-url", default=DEFAULT_START_URL, help="URL inicial da listagem.")
    parser.add_argument("--selector", default=DEFAULT_SELECTOR, help="Seletor CSS dos links de sala.")
    parser.add_argument("--max-active", type=int, default=DEFAULT_MAX_ACTIVE, help="Máximo de downloads simultâneos.")
    parser.add_argument("--limit-rooms", type=int, default=DEFAULT_LIMIT_ROOMS, help="Quantas salas coletar.")
    parser.add_argument("--download-dir", default=DEFAULT_DOWNLOAD_DIR, help="Diretório de saída.")
    parser.add_argument("--log-dir", default=DEFAULT_LOG_DIR, help="Diretório para logs.")
    parser.add_argument("--check-interval", type=float, default=DEFAULT_CHECK_INTERVAL, help="Intervalo (s) de checagem.")
    parser.add_argument("--nav-timeout-ms", type=int, default=DEFAULT_NAV_TIMEOUT_MS, help="Timeout de navegação (ms).")
    parser.add_argument("--user-agent", default=DEFAULT_USER_AGENT, help="User-Agent do navegador.")
    parser.add_argument("--headless", action="store_true", default=True, help="Headless (padrão).")
    parser.add_argument("--no-headless", dest="headless", action="store_false", help="Navegador com UI.")
    parser.add_argument("--scroll", type=int, default=0, help="Quantidade de scrolls para carregar mais cartões.")
    parser.add_argument("--scroll-pause-ms", type=int, default=800, help="Pausa entre scrolls (ms).")
    parser.add_argument("--cookie-file", default=None, help="Arquivo JSON de cookies (Playwright).")
    parser.add_argument("--min-fps", type=int, default=0, help="Se >0, normaliza FPS mínimo no remux/transcode (em recuperação).")
    parser.add_argument("--part-stable-sec", type=int, default=DEFAULT_PART_STABLE_SEC, help="Inatividade necessária do .part para finalizar.")
    args = parser.parse_args()

    ensure_dir(args.download_dir)
    ensure_dir(args.log_dir)
    setup_logging(args.log_dir)

    logging.info("=== Início ===")
    logging.info("Args: %s", args)

    # 0) Recuperação ANTES de iniciar: varre e tenta finalizar parciais
    sweep_finalize_partials(args.download_dir, min_fps=args.min_fps, min_age_sec=args.part_stable_sec)

    # 0.1) Instala tratamento de sinais para finalizar parciais antes de sair
    def _graceful_shutdown(signum, frame):
        logging.warning("Sinal %s recebido. Tentando finalizar parciais...", signum)
        try:
            sweep_finalize_partials(args.download_dir, min_fps=args.min_fps, min_age_sec=10)
        finally:
            os._exit(0)

    signal.signal(signal.SIGINT, _graceful_shutdown)
    signal.signal(signal.SIGTERM, _graceful_shutdown)

    # 1) Coleta de salas (Playwright)
    with browser_context(
        user_agent=args.user_agent,
        headless=args.headless,
        cookie_file=args.cookie_file,
        nav_timeout_ms=args.nav_timeout_ms
    ) as page:
        rooms = collect_rooms(
            page,
            start_url=args.start_url,
            selector=args.selector,
            limit_rooms=args.limit_rooms,
            scroll_steps=args.scroll,
            scroll_pause_ms=args.scroll_pause_ms
        )

    if not rooms:
        logging.warning("Nenhuma sala coletada. Encerrando.")
        # ainda assim, tenta finalizar parciais pendentes
        sweep_finalize_partials(args.download_dir, min_fps=args.min_fps, min_age_sec=args.part_stable_sec)
        return

    if len(rooms) < args.max_active:
        logging.info("Menos salas do que o desejado (%d < %d). Continuando.", len(rooms), args.max_active)

    initial = rooms[: args.max_active]
    remaining = deque(rooms[args.max_active :])
    seen = set(initial)

    progress_lock = threading.Lock()
    jobs_lock = threading.Lock()
    active_jobs: List[JobInfo] = []

    def spawn_job(url: str):
        username = parse_username_from_url(url)
        tmp, outp = build_outtmpl(username, args.download_dir)

        job = JobInfo(url=url, username=username, tmpfile=tmp, outfile=outp)
        with jobs_lock:
            active_jobs.append(job)

        def _runner():
            worker(job, check_interval=args.check_interval, min_fps=args.min_fps)
            # terminou esse job → dispara o próximo da fila (se houver)
            with jobs_lock:
                try:
                    active_jobs.remove(job)
                except ValueError:
                    pass
                if remaining:
                    next_url = remaining.popleft()
                    if next_url not in seen:
                        seen.add(next_url)
                    spawn_job(next_url)

        t = threading.Thread(target=_runner, daemon=True)
        t.start()

    # dispara os iniciais
    for u in initial:
        spawn_job(u)

    # aguarda terminar todos
    try:
        while True:
            with jobs_lock:
                total = len(seen)
                running = len(active_jobs)
                pend = len(remaining)
            done = total - (running + pend)
            logging.info("Fila: running=%d pend=%d done=%d total=%d", running, pend, done, total)
            if running == 0 and pend == 0:
                break
            time.sleep(5.0)
    except KeyboardInterrupt:
        logging.warning("Interrompido pelo usuário (KeyboardInterrupt).")

    # 2) Pós-proc final: mais uma varredura para “pegar” qualquer .part residual
    sweep_finalize_partials(args.download_dir, min_fps=args.min_fps, min_age_sec=10)

    logging.info("Pipeline encerrado.")

if __name__ == "__main__":
    main()
