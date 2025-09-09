#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import re
import sys
import time
import random
import glob
import shutil
import argparse
import logging
import threading
import subprocess
import inspect  # <<-- ADICIONADO
from dataclasses import dataclass
from typing import List, Optional, Dict, Tuple
from contextlib import contextmanager, suppress
from pathlib import Path
from collections import deque

# =========================
# Defaults
# =========================
DEFAULT_START_URL = "https://chaturbate.com/tag/squirt/"
DEFAULT_MAX_ACTIVE = 8  
DEFAULT_LIMIT_ROOMS = 100
DEFAULT_DOWNLOAD_DIR = "download"
DEFAULT_LOG_DIR = "logs"
DEFAULT_CHECK_INTERVAL = 2.0
DEFAULT_NAV_TIMEOUT_MS = 60_000
DEFAULT_SELECTOR = 'li.roomCard a[data-testid="room-card-username"][href]'
DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
)

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
        if cookies_from_browser:
            try:
                # tentativa básica: não garantido em VPS headless
                ctx = browser.new_context(storage_state=cookies_from_browser)
            except Exception:
                pass
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
    for i in range(steps):
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
    # grava em subpasta por username
    subdir = os.path.join(download_dir, safe_name(username))
    ensure_dir(subdir)
    # mp4 final:
    outfile = os.path.join(subdir, f"{stamp()}_{safe_name(username)}.mp4")
    # parcial:
    tmpfile = outfile + ".part"
    return tmpfile, outfile

def parse_username_from_url(url: str) -> str:
    # ex: https://chaturbate.com/USERNAME/
    m = re.search(r"chaturbate\.com/([^/]+)/?", url)
    return safe_name(m.group(1)) if m else "unknown"

def ytdlp_download(url: str, outtmpl: str, extra_args: Optional[List[str]] = None) -> int:
    args = [
        "yt-dlp",
        "--no-color",
        "--newline",
        "--no-part",             # vamos nós controlar .part
        "--retries", "3",
        "--fragment-retries", "3",
        "--concurrent-fragments", "5",
        "--downloader", "ffmpeg",
        "-o", outtmpl,
        url
    ]
    if extra_args:
        args += extra_args
    logging.info("Iniciando yt-dlp para: %s", url)
    proc = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1)
    last_line = ""
    try:
        for line in proc.stdout:
            last_line = line.rstrip()
            if "[download]" in last_line or "[Chaturbate]" in last_line:
                logging.info(last_line)
    except Exception:
        pass
    rc = proc.wait()
    logging.info("yt-dlp terminou rc=%s para %s", rc, url)
    return rc

def remux_inplace_safe(path: str, min_fps: int = 0) -> bool:
    """Remux e normaliza para mp4 h264/aac. Se min_fps>0, aumenta fps mínimo."""
    if not os.path.isfile(path):
        return False
    ffmpeg = which_ffmpeg()
    if not ffmpeg:
        logging.warning("ffmpeg não encontrado; remux pulado: %s", path)
        return True
    tmp_out = path + ".__tmp__.mp4"
    cmd = [
        ffmpeg, "-y",
        "-i", path,
        "-c:v", "libx264", "-preset", "veryfast", "-movflags", "+faststart",
        "-c:a", "aac", "-b:a", "128k"
    ]
    if min_fps > 0:
        cmd.extend(["-vf", f"fps=fps={min_fps}"])
    cmd.extend([tmp_out])
    logging.info("Remux: %s", os.path.basename(path))
    rc, out = run_cmd(cmd, check=False)
    if rc == 0 and os.path.isfile(tmp_out):
        try:
            os.replace(tmp_out, path)
            return True
        except Exception as e:
            logging.error("Falha ao substituir após remux: %s", e)
            with suppress(Exception): os.remove(tmp_out)
    else:
        with suppress(Exception): os.remove(tmp_out)
        logging.error("ffmpeg remux falhou em %s", path)
    return False

def run_ffmpeg_transcode(input_path: str, output_path: str, extra_args: Optional[List[str]] = None) -> bool:
    ffmpeg = which_ffmpeg()
    if not ffmpeg:
        logging.error("ffmpeg não encontrado para transcode.")
        return False
    cmd = [ffmpeg, "-y", "-i", input_path]
    if extra_args:
        cmd += extra_args
    cmd += [output_path]
    rc, out = run_cmd(cmd, check=False)
    return rc == 0 and os.path.isfile(output_path)

def try_finalize_partial(tmp_path: str, min_fps: int = 0, do_720_when_fail: bool = True) -> bool:
    """Se existir arquivo .part, tenta finalizar (remux/transcode) para mp4 definitivo."""
    if not tmp_path or not os.path.isfile(tmp_path):
        return False
    mp4_path = tmp_path[:-5] if tmp_path.endswith(".part") else tmp_path + ".mp4"
    if os.path.isfile(mp4_path):
        return True
    ok = remux_inplace_safe(tmp_path, min_fps=min_fps)
    if ok:
        try:
            os.rename(tmp_path, mp4_path)
            logging.info("Finalizado parcial → %s", mp4_path)
            return True
        except Exception as e:
            logging.error("Falha ao renomear parcial para mp4: %s", e)
            return False
    if do_720_when_fail:
        tmp_out = mp4_path + ".__from_part__.mp4"
        ok2 = run_ffmpeg_transcode(tmp_path, tmp_out, extra_args=["-vf", "scale=-2:720"])
        if ok2:
            try:
                os.replace(tmp_out, mp4_path)
                with suppress(Exception): os.remove(tmp_path)
                logging.info("Convertido parcial para 720p: %s", mp4_path)
                return True
            except Exception as e:
                logging.error("Falha ao substituir após 720p: %s", e)
                with suppress(Exception): os.remove(tmp_out)
        else:
            with suppress(Exception): os.remove(tmp_out)
    return False

def salvage_outputs(tmp_path: Optional[str], out_path: Optional[str], min_fps: int = 0):
    """Tenta salvar algo útil caso download falhe."""
    if out_path and os.path.isfile(out_path):
        remux_inplace_safe(out_path, min_fps=min_fps)
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
    last_info: Optional[dict] = None

def worker(job: JobInfo, check_interval: float = DEFAULT_CHECK_INTERVAL, min_fps: int = 0):
    """Baixa uma sala (yt-dlp) para arquivo. Controla .part/outfile."""
    try:
        job.status = "running"
        # Garantir tmpfile exista, mesmo com --no-part
        Path(job.tmpfile).touch(exist_ok=True)

        rc = ytdlp_download(job.url, job.outfile)
        if rc == 0 and os.path.isfile(job.outfile):
            # já veio mp4 final
            remux_inplace_safe(job.outfile, min_fps=min_fps)
            with suppress(Exception):
                if os.path.isfile(job.tmpfile):
                    os.remove(job.tmpfile)
            job.status = "done"
            return

        # Se não produziu outfile, tentar salvar parcial
        if os.path.isfile(job.tmpfile):
            ok = try_finalize_partial(job.tmpfile, min_fps=min_fps)
            if ok:
                job.status = "done"
                return

        job.status = "error"
    except Exception as e:
        logging.exception("Erro no worker %s: %s", job.url, e)
        job.status = "error"

def build_jobs(urls: List[str], download_dir: str) -> List[JobInfo]:
    jobs: List[JobInfo] = []
    for url in urls:
        username = parse_username_from_url(url)
        tmp, outp = build_outtmpl(username, download_dir)
        jobs.append(JobInfo(url=url, username=username, tmpfile=tmp, outfile=outp))
    return jobs

def monitor_progress(stop_evt: threading.Event, jobs: List[JobInfo]):
    while not stop_evt.is_set():
        total = len(jobs)
        done = sum(1 for j in jobs if j.status == "done")
        running = sum(1 for j in jobs if j.status == "running")
        err = sum(1 for j in jobs if j.status == "error")
        que = sum(1 for j in jobs if j.status == "queued")
        logging.info("Status: queued=%d running=%d done=%d error=%d / total=%d", que, running, done, err, total)
        stop_evt.wait(5.0)

# =========================
# Pós-processamento em lote
# =========================
def sweep_finalize_partials(root_dir: str, min_fps: int = 0):
    """Passa varrendo .part para tentar finalizar."""
    for part in Path(root_dir).rglob("*.part"):
        try_finalize_partial(str(part), min_fps=min_fps)

def sweep_transcode_everything_to_720(root_dir: str, already_done: Optional[set] = None):
    """Transcodifica tudo que for vídeo para 720p inplace (cuidado)."""
    if already_done is None:
        already_done = set()
    for p in Path(root_dir).rglob("*"):
        if not p.is_file():
            continue
        if str(p) in already_done:
            continue
        if is_video_file(str(p)):
            remux_inplace_safe(str(p), min_fps=30)

def finalize_to_720_from_sources(part_path: Optional[str], mp4_path: Optional[str]):
    """Fallback: se download falha, tenta gerar 720p a partir do que tiver."""
    if mp4_path and os.path.isfile(mp4_path):
        remux_inplace_safe(mp4_path, min_fps=30)
        return
    if part_path and os.path.isfile(part_path):
        try_finalize_partial(part_path, min_fps=30)

# =========================
# Pipeline principal
# =========================
def main():
    parser = argparse.ArgumentParser(description="Coletor de salas + downloads paralelos (yt-dlp → MP4).")
    parser.add_argument("--start-url", default=DEFAULT_START_URL, help="URL inicial.")
    parser.add_argument("--selector", default=DEFAULT_SELECTOR, help="Seletor de CSS dos links de sala.")
    parser.add_argument("--max-active", type=int, default=DEFAULT_MAX_ACTIVE, help="Máximo de downloads simultâneos.")
    parser.add_argument("--limit-rooms", type=int, default=DEFAULT_LIMIT_ROOMS, help="Quantas salas coletar.")
    parser.add_argument("--download-dir", default=DEFAULT_DOWNLOAD_DIR, help="Diretório de saída.")
    parser.add_argument("--log-dir", default=DEFAULT_LOG_DIR, help="Diretório para logs.")
    parser.add_argument("--check-interval", type=float, default=DEFAULT_CHECK_INTERVAL, help="Intervalo de checagem (s).")
    parser.add_argument("--nav-timeout-ms", type=int, default=DEFAULT_NAV_TIMEOUT_MS, help="Timeout de navegação (ms).")
    parser.add_argument("--user-agent", default=DEFAULT_USER_AGENT, help="User-Agent para o navegador.")
    parser.add_argument("--headless", action="store_true", default=True, help="Navegador headless (padrão).")
    parser.add_argument("--no-headless", dest="headless", action="store_false", help="Navegador com UI.")
    parser.add_argument("--scroll", type=int, default=0, help="Passos de scroll para carregar mais cartões (0 = nenhum).")
    parser.add_argument("--scroll-pause-ms", type=int, default=800, help="Pausa entre scrolls (ms).")
    parser.add_argument("--output-template", default="", help="(Ignorado) Nome é fixo por username.")
    parser.add_argument("--cookies-from-browser", choices=["chrome", "msedge", "firefox", "chromium"],
                        default=None, help="Tentar cookies do navegador local (pouco útil em VPS).")
    parser.add_argument("--cookie-file", default=None, help="Arquivo JSON de cookies (Playwright).")
    parser.add_argument("--min-fps", type=int, default=0, help="Se >0, força FPS mínimo no remux.")
    parser.add_argument("--postprocess-720", action="store_true", default=False,
                        help="Após rodar, tenta transcodificar tudo para 720p.")
    args = parser.parse_args()

    ensure_dir(args.download_dir)
    ensure_dir(args.log_dir)
    setup_logging(args.log_dir)

    logging.info("=== Início ===")
    logging.info("Args: %s", args)

    # Coleta de salas
    with browser_context(
        user_agent=args.user_agent,
        headless=args.headless,
        cookies_from_browser=args.cookies_from_browser,
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
        return

    if len(rooms) < args.max_active:
        logging.warning("Menos salas do que o desejado (%d < %d). Continuando.", len(rooms), args.max_active)

    initial = rooms[: args.max_active]
    remaining = deque(rooms[args.max_active :])
    seen = set(initial)

    progress_lock = threading.Lock()
    jobs_lock = threading.Lock()
    cancel_event = threading.Event()
    stop_monitor = threading.Event()
    progress_map: Dict[str, Dict[str, Optional[str]]] = {}
    active_jobs: List[JobInfo] = []

    def monitor_progress_thread():
        while not stop_monitor.is_set():
            with progress_lock:
                total = len(progress_map)
                done = sum(1 for v in progress_map.values() if v.get("status") == "done")
                running = sum(1 for v in progress_map.values() if v.get("status") == "running")
                err = sum(1 for v in progress_map.values() if v.get("status") == "error")
                que = sum(1 for v in progress_map.values() if v.get("status") == "queued")
            logging.info("Fila: queued=%d running=%d done=%d error=%d / total=%d",
                         que, running, done, err, total)
            time.sleep(5.0)

    monitor_thread = threading.Thread(target=monitor_progress_thread, daemon=True)
    monitor_thread.start()

    def spawn_job(url: str):
        username = parse_username_from_url(url)
        tmp, outp = build_outtmpl(username, args.download_dir)
        info = {"status": "queued", "tmpfile": tmp, "outfile": outp}
        with progress_lock:
            progress_map[url] = info
        job = JobInfo(url=url, username=username, tmpfile=tmp, outfile=outp)
        t = threading.Thread(target=_runner, args=(job,), daemon=True)
        with jobs_lock:
            active_jobs.append(job)
        t.start()

    def _runner(job: JobInfo):
        with progress_lock:
            progress_map[job.url]["status"] = "running"
        worker(job, check_interval=args.check_interval, min_fps=args.min_fps)
        with progress_lock:
            progress_map[job.url]["status"] = job.status
        with jobs_lock:
            # remove finished job
            for i, j in enumerate(active_jobs):
                if j.url == job.url:
                    active_jobs.pop(i)
                    break
        # quando terminar um, se houver mais, dispara próximo
        with jobs_lock:
            if remaining:
                next_url = remaining.popleft()
                if next_url not in seen:
                    seen.add(next_url)
                spawn_job(next_url)

    # dispara os iniciais
    for u in initial:
        spawn_job(u)

    # aguarda terminar todos
    try:
        while True:
            with jobs_lock:
                if not active_jobs and not remaining:
                    break
            time.sleep(1.0)
    except KeyboardInterrupt:
        logging.warning("Interrompido pelo usuário.")
        cancel_event.set()

    # pós-processamento opcional
    if args.postprocess_720:
        already = set()
        with progress_lock:
            for ent in progress_map.values():
                tmp = ent.get("tmpfile")
                outp = ent.get("outfile")
                if tmp and os.path.isfile(tmp):
                    already.add(os.path.abspath(tmp))
                if outp and os.path.isfile(outp):
                    already.add(os.path.abspath(outp))
                finalize_to_720_from_sources(tmp, outp)

            sweep_transcode_everything_to_720(args.download_dir, already_done=already)
    else:
        with progress_lock:
            tmpfiles = [ent.get("tmpfile") for ent in progress_map.values() if ent.get("tmpfile")]
            outfiles = [ent.get("outfile") for ent in progress_map.values() if ent.get("outfile")]
        for tmp in tmpfiles:
            try_finalize_partial(tmp, min_fps=args.min_fps)
        for outp in outfiles:
            if outp and os.path.isfile(outp):
                remux_inplace_safe(outp, min_fps=args.min_fps)
        sweep_finalize_partials(args.download_dir, min_fps=args.min_fps)

    stop_monitor.set()
    with suppress(Exception):
        monitor_thread.join(timeout=3)

    logging.info("Pipeline encerrado.")

if __name__ == "__main__":
    main()
