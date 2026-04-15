#!/usr/bin/env python3
"""
start.py — Inicializador Unificado
Dashboard Flask + Bot NIC.br
Modo Produção Ready 🚀
"""

import subprocess
import sys
import os
import time
import threading
import signal
import logging
import multiprocessing
from dotenv import load_dotenv

# =====================================================
# ENV
# =====================================================

load_dotenv()

DASHBOARD_HOST = os.getenv("DASHBOARD_HOST", "0.0.0.0")
DASHBOARD_PORT = os.getenv("DASHBOARD_PORT", "5000")

# =====================================================
# LOGGING
# =====================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

logger = logging.getLogger("START")

# =====================================================
# GLOBAL
# =====================================================

PROCESSES = []
RUNNING = True


# =====================================================
# BANNER
# =====================================================

def print_banner():
    print(f"""
\033[0;36m╔══════════════════════════════════════════════════════╗
║      SISTEMA DE MONITORAMENTO DE CONECTIVIDADE       ║
║                     SMEC · NIC.br                    ║
╠══════════════════════════════════════════════════════╣
║  Dashboard → http://{DASHBOARD_HOST}:{DASHBOARD_PORT}
║  Bot       → Coleta automática agendada
╚══════════════════════════════════════════════════════╝\033[0m
""")


# =====================================================
# PROCESS RUNNER
# =====================================================

def run_process(name, cmd, color):
    prefix = f"\033[{color}m[{name}]\033[0m"

    logger.info(f"Iniciando {name}")

    try:
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            cwd=os.path.dirname(os.path.abspath(__file__)),
        )

        PROCESSES.append(proc)

        for line in proc.stdout:
            if line:
                print(f"{prefix} {line.rstrip()}", flush=True)

        proc.wait()

        if proc.returncode not in (0, -15):
            logger.error(f"{name} encerrou com código {proc.returncode}")

    except Exception as e:
        logger.error(f"Erro ao executar {name}: {e}")


# =====================================================
# SHUTDOWN LIMPO
# =====================================================

def shutdown(signum=None, frame=None):
    global RUNNING

    logger.info("Encerrando sistema...")

    RUNNING = False

    for proc in PROCESSES:
        try:
            proc.terminate()
        except:
            pass

    time.sleep(2)

    for proc in PROCESSES:
        try:
            if proc.poll() is None:
                proc.kill()
        except:
            pass

    logger.info("Sistema encerrado.")
    sys.exit(0)


# =====================================================
# DEPENDÊNCIAS
# =====================================================

def check_requirements():

    required = [
        ("flask", "flask"),
        ("mysql.connector", "mysql-connector-python"),
        ("dotenv", "python-dotenv"),
        ("gevent", "gevent"),
        ("gunicorn", "gunicorn"),
    ]

    missing = []

    for module, package in required:
        try:
            __import__(module)
        except ImportError:
            missing.append(package)

    if missing:
        logger.error(
            "Dependências faltando:\n\npip install " + " ".join(missing)
        )
        return False

    return True


# =====================================================
# MAIN
# =====================================================

def main():

    print_banner()

    if not check_requirements():
        sys.exit(1)

    os.makedirs("logs", exist_ok=True)

    # -------------------------------------------------
    # INEPs
    # -------------------------------------------------

    if not os.path.exists("ineps.txt"):
        logger.warning("ineps.txt não encontrado!")
    else:
        with open("ineps.txt", encoding="utf-8") as f:
            total = sum(
                1 for l in f if l.strip() and not l.startswith("#")
            )
        logger.info(f"{total} INEPs carregados")

    # -------------------------------------------------
    # SIGNALS
    # -------------------------------------------------

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    python = sys.executable

    # -------------------------------------------------
    # GUNICORN CONFIG PROFISSIONAL
    # -------------------------------------------------

    cpu = multiprocessing.cpu_count()
    workers = min(5, cpu)

    logger.info(f"CPU detectada: {cpu}")
    logger.info(f"Workers Gunicorn: {workers}")

    gunicorn_cmd = [
        "gunicorn",
        "dashboard:app",
        "-k", "gevent",
        "-w", str(workers),
        "--worker-connections", "1000",
        "--timeout", "120",
        "--keep-alive", "5",
        "--max-requests", "10000",
        "--max-requests-jitter", "1000",
        "-b", f"{DASHBOARD_HOST}:{DASHBOARD_PORT}",
    ]

    # -------------------------------------------------
    # THREAD DASHBOARD
    # -------------------------------------------------

    t_dashboard = threading.Thread(
        target=run_process,
        args=("DASHBOARD", gunicorn_cmd, "34"),
        daemon=True,
    )

    # -------------------------------------------------
    # THREAD BOT
    # -------------------------------------------------

    t_bot = threading.Thread(
        target=run_process,
        args=("BOT", [python, "app.py"], "32"),
        daemon=True,
    )

    # -------------------------------------------------
    # START
    # -------------------------------------------------

    logger.info("Subindo Dashboard...")
    t_dashboard.start()

    time.sleep(4)

    logger.info("Subindo Bot de Coleta...")
    t_bot.start()

    logger.info(
        f"\033[0;32mSistema ONLINE → http://{DASHBOARD_HOST}:{DASHBOARD_PORT}\033[0m"
    )

    # -------------------------------------------------
    # LOOP PRINCIPAL
    # -------------------------------------------------

    try:
        while RUNNING:
            time.sleep(5)

            if not t_dashboard.is_alive() and not t_bot.is_alive():
                logger.warning("Todos processos encerraram.")
                break

    except KeyboardInterrupt:
        shutdown()


# =====================================================
# ENTRYPOINT
# =====================================================

if __name__ == "__main__":
    main()
