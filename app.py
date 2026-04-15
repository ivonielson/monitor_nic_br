import asyncio
from playwright.async_api import async_playwright
import pandas as pd
import re
import os
import sys
import time
import logging
from datetime import datetime
import sqlite3
import json
from dotenv import load_dotenv
import schedule
import threading
from datetime import datetime, timedelta
import re
from datetime import datetime

# Carregar variáveis de ambiente
load_dotenv()

# Configuração de logging
def setup_logging():
    if not os.path.exists('logs'):
        os.makedirs('logs')
    
    keep_latest_logs('logs', 5)
    log_filename = f"logs/execucao_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_filename, encoding='utf-8'),
            logging.StreamHandler(sys.stdout)
        ]
    )
    return logging.getLogger(__name__)

def keep_latest_logs(log_dir, keep_count):
    """
    Mantém apenas os 'keep_count' logs mais recentes no diretório
    
    Args:
        log_dir: Diretório onde os logs estão armazenados
        keep_count: Número de logs mais recentes para manter
    """
    try:
        # Listar todos os arquivos .log no diretório
        log_files = [f for f in os.listdir(log_dir) if f.startswith('execucao_') and f.endswith('.log')]
        
        if len(log_files) <= keep_count:
            return
        
        # Ordenar arquivos por data de modificação (mais antigos primeiro)
        log_files.sort(key=lambda x: os.path.getmtime(os.path.join(log_dir, x)))
        
        # Remover arquivos excedentes (os mais antigos)
        files_to_remove = log_files[:-keep_count]
        for filename in files_to_remove:
            file_path = os.path.join(log_dir, filename)
            try:
                os.remove(file_path)
                print(f"Log antigo removido: {filename}")
            except OSError as e:
                print(f"Erro ao remover {filename}: {e}")
                
    except Exception as e:
        print(f"Erro ao limpar logs antigos: {e}")

logger = setup_logging()

# Configuração do banco SQLite
DB_PATH = os.getenv('DB_PATH', 'dados.db')

URL = os.getenv('URL_COLETA', 'https://conectividadenaeducacao.nic.br/#sua-escola')
ARQUIVO_LOG_DETALHADO = "logs/detalhado_ineps.txt"
ARQUIVO_ERROS = "logs/erros_detalhados.txt"
ARQUIVO_CHECKPOINT = "logs/checkpoint.json"
ARQUIVO_COMPLETADO = "logs/completado.txt"
ARQUIVO_ULTIMA_EXECUCAO = "logs/ultima_execucao.json"

WORKERS = int(os.getenv('WORKERS', '1'))
MAX_RETRY = int(os.getenv('MAX_RETRY', '4'))
MAX_PAGINAS_POR_WORKER = int(os.getenv('MAX_PAGINAS_POR_WORKER', '1'))

# Colunas conforme solicitado
COLUNAS = [
    "INEP", "Nome_Escola", "Municipio_UF", "Gestao", "Total_Estudantes",
    "Estudantes_Maior_Turno", "Velocidade_Adequada", "Status",
    "Criterio_MEC", "Adequada", "Vel_Max_Mbps", "Download_Mbps",
    "Numero_Medicoes", "Ultima_Medicao_DataHora", "Vel_Max_Ultima_Medicao",
    "Status_Coleta"
]

# ===================== CONFIGURAÇÕES DE AGENDAMENTO =====================
def carregar_config_agendamento():
    """Carrega configurações de agendamento do .env"""
    config = {
        'ativo': os.getenv('AGENDAMENTO_ATIVO', 'false').lower() == 'true',
        'frequencia': os.getenv('AGENDAMENTO_FREQUENCIA', 'diario'),
        'horario': os.getenv('AGENDAMENTO_HORARIO', '02:00'),
        'dia_semana': int(os.getenv('AGENDAMENTO_DIA_SEMANA', '0')),
        'dia_mes': int(os.getenv('AGENDAMENTO_DIA_MES', '1')),
        'intervalo_minutos': int(os.getenv('AGENDAMENTO_INTERVALO_MINUTOS', '0'))
    }
    return config

def registrar_execucao():
    """Registra a data/hora da última execução"""
    dados = {
        'ultima_execucao': datetime.now().isoformat(),
        'status': 'completado'
    }
    with open(ARQUIVO_ULTIMA_EXECUCAO, 'w') as f:
        json.dump(dados, f)

def deve_executar_agora():
    """Verifica se deve executar baseado na configuração de agendamento"""
    config = carregar_config_agendamento()
    
    if not config['ativo']:
        logger.info("Agendamento desativado. Executando normalmente.")
        return True
    
    # Verificar última execução
    ultima_execucao = None
    if os.path.exists(ARQUIVO_ULTIMA_EXECUCAO):
        try:
            with open(ARQUIVO_ULTIMA_EXECUCAO, 'r') as f:
                dados = json.load(f)
                ultima_execucao = datetime.fromisoformat(dados['ultima_execucao'])
        except:
            pass
    
    agora = datetime.now()
    
    # Se nunca executou, executar
    if not ultima_execucao:
        logger.info("Primeira execução. Iniciando coleta...")
        return True
    
    # Verificar se passou o intervalo mínimo configurado
    if config['intervalo_minutos'] > 0:
        minutos_desde_ultima = (agora - ultima_execucao).total_seconds() / 60
        if minutos_desde_ultima < config['intervalo_minutos']:
            logger.info(f"Intervalo mínimo de {config['intervalo_minutos']} minutos não atingido. Próxima execução em {config['intervalo_minutos'] - minutos_desde_ultima:.0f} minutos.")
            return False
    
    # Verificar frequência
    if config['frequencia'] == 'diario':
        horario_exec = datetime.strptime(config['horario'], '%H:%M').time()
        if agora.time() >= horario_exec and ultima_execucao.date() < agora.date():
            logger.info(f"Agendamento diário no horário {config['horario']} - Executando...")
            return True
    
    elif config['frequencia'] == 'semanal':
        horario_exec = datetime.strptime(config['horario'], '%H:%M').time()
        if (agora.weekday() == config['dia_semana'] and 
            agora.time() >= horario_exec and 
            ultima_execucao.date() < agora.date()):
            logger.info(f"Agendamento semanal no {['domingo','segunda','terça','quarta','quinta','sexta','sábado'][config['dia_semana']]} às {config['horario']} - Executando...")
            return True
    
    elif config['frequencia'] == 'mensal':
        horario_exec = datetime.strptime(config['horario'], '%H:%M').time()
        if (agora.day == config['dia_mes'] and 
            agora.time() >= horario_exec and 
            (ultima_execucao.month != agora.month or ultima_execucao.year != agora.year)):
            logger.info(f"Agendamento mensal no dia {config['dia_mes']} às {config['horario']} - Executando...")
            return True
    
    return False

def resetar_checkpoint_automatico():
    """
    Verifica se todos os INEPs foram processados e reseta o checkpoint automaticamente
    Retorna True se resetou, False caso contrário
    """
    try:
        with open("ineps.txt", encoding='utf-8') as f:
            todos_ineps = set([l.strip() for l in f if l.strip()])
        
        processados = set()
        if os.path.exists(ARQUIVO_CHECKPOINT):
            with open(ARQUIVO_CHECKPOINT, 'r') as f:
                checkpoint = json.load(f)
                processados = set(checkpoint.get('processados', []))
        
        if processados and todos_ineps.issubset(processados):
            logger.info("\n" + "="*60)
            logger.info("🎉 TODOS OS INEPs FORAM PROCESSADOS! 🎉")
            logger.info("="*60)
            logger.info(f"Total processado: {len(processados)} INEPs")
            
            backup_path = None
            if os.path.exists(ARQUIVO_CHECKPOINT):
                backup_path = ARQUIVO_CHECKPOINT.replace('.json', f"_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
                os.rename(ARQUIVO_CHECKPOINT, backup_path)
                logger.info(f"✅ Backup do checkpoint salvo em: {backup_path}")
            
            with open(ARQUIVO_COMPLETADO, 'w', encoding='utf-8') as f:
                f.write(f"Processamento completado em: {datetime.now()}\n")
                f.write(f"Total INEPs: {len(todos_ineps)}\n")
                if backup_path:
                    f.write(f"Checkpoint anterior: {backup_path}\n")
                f.write(f"Status dos INEPs:\n")
                
                conn = get_db_connection()
                if conn:
                    try:
                        cursor = conn.cursor()
                        cursor.execute("""
                            SELECT Status_Coleta, COUNT(*) 
                            FROM resultados_inep 
                            GROUP BY Status_Coleta
                        """)
                        stats = cursor.fetchall()
                        for status, count in stats:
                            f.write(f"  {status}: {count}\n")
                    except:
                        pass
                    finally:
                        conn.close()
            
            logger.info("✅ Checkpoint resetado automaticamente!")
            logger.info("✅ Pronto para nova execução!")
            logger.info("="*60)
            
            registrar_execucao()
            
            return True
        else:
            if processados:
                logger.info(f"⏳ Processamento em andamento: {len(processados)}/{len(todos_ineps)} INEPs processados")
            return False
            
    except FileNotFoundError:
        logger.error("Arquivo ineps.txt não encontrado!")
        return False
    except Exception as e:
        logger.error(f"Erro ao resetar checkpoint: {e}")
        return False
# =====================================================================

# -----------------------------
# Funções de banco de dados (SQLite)
# -----------------------------
def get_db_connection():
    """Retorna uma conexão com o banco SQLite"""
    try:
        conn = sqlite3.connect(DB_PATH, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        return conn
    except Exception as e:
        logger.error(f"Erro ao conectar ao banco de dados: {e}")
        return None

def criar_tabela_se_nao_existe():
    """Cria a tabela se ela não existir"""
    conn = get_db_connection()
    if not conn:
        return False
    
    try:
        cursor = conn.cursor()
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS resultados_inep (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                INEP TEXT NOT NULL UNIQUE,
                Nome_Escola TEXT,
                Municipio_UF TEXT,
                Gestao TEXT,
                Total_Estudantes INTEGER,
                Estudantes_Maior_Turno INTEGER,
                Velocidade_Adequada TEXT,
                Status TEXT,
                Criterio_MEC TEXT,
                Adequada TEXT,
                Vel_Max_Mbps REAL,
                Download_Mbps REAL,
                Numero_Medicoes INTEGER,
                Ultima_Medicao_DataHora TEXT,
                Vel_Max_Ultima_Medicao REAL,
                Status_Coleta TEXT,
                Data_Coleta DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Índices
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_status_coleta ON resultados_inep(Status_Coleta)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_adequada ON resultados_inep(Adequada)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_data_coleta ON resultados_inep(Data_Coleta)")
        
        conn.commit()
        logger.info("Tabela verificada/criada com sucesso")
        return True
        
    except Exception as e:
        logger.error(f"Erro ao criar tabela: {e}")
        return False
    finally:
        conn.close()

def limpar_nome_escola(nome):
    """
    Remove duplicidade de 'Escola' no início do nome.
    Ex:
    Escola Escola Municipal -> Escola Municipal
    """
    if not nome:
        return nome

    nome = nome.strip()

    # remove repetição inicial "Escola Escola"
    nome = re.sub(r'^(Escola\s+)+', 'Escola ', nome, flags=re.IGNORECASE)

    return nome

def salvar_no_banco(dados):
    """Salva um registro no banco de dados com preservação de dados existentes"""
    max_tentativas = 3
    for tentativa in range(max_tentativas):
        conn = get_db_connection()
        if not conn:
            if tentativa < max_tentativas - 1:
                logger.warning(f"Tentativa {tentativa+1} de conexão com banco falhou. Aguardando...")
                time.sleep(2)
                continue
            else:
                logger.error("Não foi possível conectar ao banco após várias tentativas")
                return False
        
        try:
            cursor = conn.cursor()
            
            # Verificar se o registro já existe
            cursor.execute("SELECT * FROM resultados_inep WHERE INEP = ?", (dados['INEP'],))
            row = cursor.fetchone()
            dados_antigos = dict(row) if row else None
            
            if dados_antigos:
                logger.debug(f"Registro existente encontrado para INEP {dados['INEP']}")
            
            # Resolver valores com fallback para dados antigos
            nome_escola = dados['Nome_Escola'] if dados['Nome_Escola'] else (dados_antigos['Nome_Escola'] if dados_antigos else None)

            # 🔥 normalizar nome
            nome_escola = limpar_nome_escola(nome_escola)
            municipio_uf = dados['Municipio_UF'] if dados['Municipio_UF'] else (dados_antigos['Municipio_UF'] if dados_antigos else None)
            gestao = dados['Gestao'] if dados['Gestao'] else (dados_antigos['Gestao'] if dados_antigos else None)
            
            total_estudantes = None
            if dados['Total_Estudantes'] and str(dados['Total_Estudantes']).isdigit():
                total_estudantes = int(dados['Total_Estudantes'])
            elif dados_antigos and dados_antigos['Total_Estudantes']:
                total_estudantes = dados_antigos['Total_Estudantes']
            
            maior_turno = None
            if dados['Estudantes_Maior_Turno'] and str(dados['Estudantes_Maior_Turno']).isdigit():
                maior_turno = int(dados['Estudantes_Maior_Turno'])
            elif dados_antigos and dados_antigos['Estudantes_Maior_Turno']:
                maior_turno = dados_antigos['Estudantes_Maior_Turno']
            
            num_medicoes = None
            if dados['Numero_Medicoes'] and str(dados['Numero_Medicoes']).isdigit():
                num_medicoes = int(dados['Numero_Medicoes'])
            elif dados_antigos and dados_antigos['Numero_Medicoes']:
                num_medicoes = dados_antigos['Numero_Medicoes']
            
            vel_max = None
            if dados['Vel_Max_Mbps']:
                try:
                    vel_max = float(str(dados['Vel_Max_Mbps']).replace(',', '.'))
                except:
                    vel_max = dados_antigos['Vel_Max_Mbps'] if dados_antigos else None
            elif dados_antigos and dados_antigos['Vel_Max_Mbps']:
                vel_max = dados_antigos['Vel_Max_Mbps']
            
            download = None
            if dados['Download_Mbps']:
                try:
                    download = float(str(dados['Download_Mbps']).replace(',', '.'))
                except:
                    download = dados_antigos['Download_Mbps'] if dados_antigos else None
            elif dados_antigos and dados_antigos['Download_Mbps']:
                download = dados_antigos['Download_Mbps']
            
            vel_max_medicao = None
            if dados['Vel_Max_Ultima_Medicao']:
                try:
                    vel_max_medicao = float(str(dados['Vel_Max_Ultima_Medicao']).replace(',', '.'))
                except:
                    vel_max_medicao = dados_antigos['Vel_Max_Ultima_Medicao'] if dados_antigos else None
            elif dados_antigos and dados_antigos['Vel_Max_Ultima_Medicao']:
                vel_max_medicao = dados_antigos['Vel_Max_Ultima_Medicao']
            
            velocidade_adequada = dados['Velocidade_Adequada'] if dados['Velocidade_Adequada'] else (dados_antigos['Velocidade_Adequada'] if dados_antigos else None)
            status = dados['Status'] if dados['Status'] else (dados_antigos['Status'] if dados_antigos else None)
            criterio = dados['Criterio_MEC'] if dados['Criterio_MEC'] else (dados_antigos['Criterio_MEC'] if dados_antigos else None)
            
            if dados['Adequada'] and dados['Adequada'] in ["SIM", "NÃO"]:
                adequada = dados['Adequada']
            elif dados_antigos and dados_antigos['Adequada']:
                adequada = dados_antigos['Adequada']
            else:
                adequada = "SEM DADOS"
            
            ultima_medicao = dados['Ultima_Medicao_DataHora'] if dados['Ultima_Medicao_DataHora'] else (dados_antigos['Ultima_Medicao_DataHora'] if dados_antigos else None)
            status_coleta = dados['Status_Coleta']
            
            # Query para SQLite com INSERT OR REPLACE
            query = """
                INSERT OR REPLACE INTO resultados_inep (
                    INEP, Nome_Escola, Municipio_UF, Gestao, Total_Estudantes,
                    Estudantes_Maior_Turno, Velocidade_Adequada, Status,
                    Criterio_MEC, Adequada, Vel_Max_Mbps, Download_Mbps,
                    Numero_Medicoes, Ultima_Medicao_DataHora, Vel_Max_Ultima_Medicao,
                    Status_Coleta, Data_Coleta
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """
            
            values = (
                dados['INEP'], nome_escola, municipio_uf, gestao,
                total_estudantes, maior_turno, velocidade_adequada, status,
                criterio, adequada, vel_max, download,
                num_medicoes, ultima_medicao, vel_max_medicao,
                status_coleta
            )
            
            cursor.execute(query, values)
            conn.commit()
            
            if dados_antigos:
                if status_coleta == "SUCESSO":
                    logger.info(f"[DB] INEP {dados['INEP']} - ATUALIZADO com sucesso (novos dados)")
                else:
                    logger.info(f"[DB] INEP {dados['INEP']} - MANTIDO (coleta falhou, dados antigos preservados)")
            else:
                logger.info(f"[DB] INEP {dados['INEP']} - INSERIDO no banco")
            
            return True
            
        except Exception as e:
            logger.error(f"Erro ao salvar no banco (tentativa {tentativa+1}): {e}")
            if tentativa < max_tentativas - 1:
                time.sleep(2)
            continue
        finally:
            conn.close()
    
    return False

def carregar_checkpoint():
    """Carrega o checkpoint para saber quais INEPs já foram processados"""
    if os.path.exists(ARQUIVO_CHECKPOINT):
        try:
            with open(ARQUIVO_CHECKPOINT, 'r') as f:
                checkpoint = json.load(f)
                return set(checkpoint.get('processados', []))
        except:
            return set()
    return set()

def salvar_checkpoint(inep):
    """Salva checkpoint de um INEP processado"""
    processados = carregar_checkpoint()
    processados.add(inep)
    
    with open(ARQUIVO_CHECKPOINT, 'w') as f:
        json.dump({'processados': list(processados)}, f)

# -----------------------------
# Ler INEPs (ignorando já processados)
# -----------------------------
def carregar_ineps_nao_processados():
    """Carrega apenas INEPs que ainda não foram processados"""
    processados = carregar_checkpoint()
    
    try:
        with open("ineps.txt", encoding='utf-8') as f:
            todos_ineps = [l.strip() for l in f if l.strip()]
        
        nao_processados = [inep for inep in todos_ineps if inep not in processados]
        
        logger.info(f"Total INEPs no arquivo: {len(todos_ineps)}")
        logger.info(f"INEPs já processados: {len(processados)}")
        logger.info(f"INEPs a processar: {len(nao_processados)}")
        
        return nao_processados
        
    except FileNotFoundError:
        logger.error("Arquivo ineps.txt não encontrado!")
        sys.exit(1)

INEPS = carregar_ineps_nao_processados()

# -----------------------------
# Função para log de INEP individual
# -----------------------------
def log_inep(inep, status, detalhes="", erro=None):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    with open(ARQUIVO_LOG_DETALHADO, 'a', encoding='utf-8') as f:
        f.write(f"[{timestamp}] INEP: {inep} - Status: {status}")
        if detalhes:
            f.write(f" - {detalhes}")
        f.write("\n")
    
    if erro:
        with open(ARQUIVO_ERROS, 'a', encoding='utf-8') as f:
            f.write(f"[{timestamp}] INEP: {inep} - ERRO: {erro}\n")
            f.write("-" * 80 + "\n")

# -----------------------------
# Função para aguardar elemento com conteúdo
# -----------------------------
async def wait_for_element_with_content(page, selector, timeout=30000):
    """Aguarda até que o elemento exista e tenha conteúdo não vazio"""
    start_time = time.time()
    while time.time() - start_time < timeout / 1000:
        try:
            element = page.locator(selector).first
            if await element.count() > 0:
                text = (await element.inner_text()).strip()
                if text and text != "Número de Medições":
                    return True
        except:
            pass
        await asyncio.sleep(0.5)
    return False

# -----------------------------
# Função para determinar adequação (VALIDADA)
# -----------------------------
async def determinar_adequacao(criterio):
    """
    Determina se a velocidade é adequada baseado no texto do critério MEC
    Retorna: "SIM", "NÃO" ou "SEM DADOS"
    
    Texto para SIM:
    "Parabéns! De acordo com as medições de Internet realizadas nos últimos seis meses, sua escola está com velocidade adequada aos parâmetros do Ministério da Educação."
    
    Texto para NÃO:
    "Poxa! De acordo com as medições de Internet realizadas nos últimos 6 meses, sua escola não está com velocidade adequada aos parâmetros do Ministério da Educação."
    """
    if not criterio:
        logger.debug("⚠ Critério MEC vazio")
        return "SEM DADOS"
    
    criterio_limpo = criterio.strip().lower()
    
    # Log do critério para debug
    logger.debug(f"Analisando critério: {criterio_limpo[:100]}...")
    
    # Padrões específicos para SIM
    if "parabéns" in criterio_limpo and "está com velocidade adequada" in criterio_limpo:
        logger.debug("✓ Padrão SIM encontrado: 'Parabéns... está com velocidade adequada'")
        return "SIM"
    
    # Padrões específicos para NÃO
    if "poxa" in criterio_limpo and "não está com velocidade adequada" in criterio_limpo:
        logger.debug("✗ Padrão NÃO encontrado: 'Poxa... não está com velocidade adequada'")
        return "NÃO"
    
    # Fallback para variações
    if "parabéns" in criterio_limpo:
        logger.debug("✓ Palavra 'parabéns' encontrada - assumindo SIM")
        return "SIM"
    
    if "poxa" in criterio_limpo:
        logger.debug("✗ Palavra 'poxa' encontrada - assumindo NÃO")
        return "NÃO"
    
    if "está com velocidade adequada" in criterio_limpo or "esta com velocidade adequada" in criterio_limpo:
        logger.debug("✓ Frase de adequação positiva encontrada - assumindo SIM")
        return "SIM"
    
    if "não está com velocidade adequada" in criterio_limpo or "nao esta com velocidade adequada" in criterio_limpo:
        logger.debug("✗ Frase de inadequação encontrada - assumindo NÃO")
        return "NÃO"
    
    # Se não encontrou nenhum padrão
    logger.debug(f"⚠ Nenhum padrão encontrado. Texto: {criterio[:100]}")
    return "SEM DADOS"

# -----------------------------
# Função para validar se os dados correspondem ao INEP
# -----------------------------
async def validar_dados_inep(page, inep, worker_id):
    """
    Valida se os dados carregados correspondem ao INEP consultado
    Retorna True se válido, False caso contrário
    """
    try:
        # Verificar se o elemento de nome da escola tem conteúdo
        nome_elem = page.locator("#nome_escola.shiny-text-output").first
        if await nome_elem.count() == 0:
            logger.warning(f"[Worker {worker_id}] INEP {inep} - Elemento de nome não encontrado")
            return False
        
        nome_escola = (await nome_elem.inner_text()).strip()
        
        # Se o nome está vazio ou é "Número de Medições", é inválido
        if not nome_escola or nome_escola == "Número de Medições":
            logger.warning(f"[Worker {worker_id}] INEP {inep} - Nome da escola inválido: '{nome_escola}'")
            return False
        
        # Verificar se há algum indicador de erro na página
        page_text = await page.evaluate("document.body.innerText")
        if "não encontrado" in page_text.lower() or "inválido" in page_text.lower():
            logger.warning(f"[Worker {worker_id}] INEP {inep} - Página indica INEP inválido")
            return False
        
        # Verificar se o INEP no título ou URL corresponde (opcional)
        # Alguns sites mostram o INEP consultado em algum lugar
        
        logger.info(f"[Worker {worker_id}] INEP {inep} - Validação OK. Escola: {nome_escola[:50]}")
        return True
        
    except Exception as e:
        logger.error(f"[Worker {worker_id}] INEP {inep} - Erro na validação: {e}")
        return False

# -----------------------------
# Funções de extração
# -----------------------------
async def extrair_dados_escola(page, inep, worker_id):
    """Extrai dados usando seletores específicos"""
    resultados = {
        "nome_escola": "",
        "municipio_uf": "",
        "gestao": "",
        "total_estudantes": "",
        "maior_turno": "",
        "vel_adequada": ""
    }
    
    try:
        # Nome da escola
        if await wait_for_element_with_content(page, "#nome_escola.shiny-text-output", timeout=10000):
            nome_elem = page.locator("#nome_escola.shiny-text-output").first
            resultados["nome_escola"] = (await nome_elem.inner_text()).strip()
        
        # Município/UF
        uf_elem = page.locator("#uf_escola.shiny-text-output").first
        if await uf_elem.count() > 0:
            texto_uf = (await uf_elem.inner_text()).strip()
            if texto_uf:
                resultados["municipio_uf"] = texto_uf.replace("Município:", "").strip()
        
        # Gestão
        gestao_elem = page.locator("#dependencia_escola.shiny-text-output").first
        if await gestao_elem.count() > 0:
            texto_gestao = (await gestao_elem.inner_text()).strip()
            if texto_gestao:
                resultados["gestao"] = texto_gestao.replace("Gestão:", "").strip()
        
        # Total de estudantes
        estudantes_elem = page.locator("#estudantes_escola.shiny-text-output").first
        if await estudantes_elem.count() > 0:
            texto_estudantes = (await estudantes_elem.inner_text()).strip()
            match = re.search(r'(\d+)', texto_estudantes)
            if match:
                resultados["total_estudantes"] = match.group(1)
        
        # Estudantes no maior turno
        maior_turno_elem = page.locator("#estudantes_escola_maior_turno.shiny-text-output").first
        if await maior_turno_elem.count() > 0:
            texto_maior_turno = (await maior_turno_elem.inner_text()).strip()
            match = re.search(r'(\d+)', texto_maior_turno)
            if match:
                resultados["maior_turno"] = match.group(1)
        
        # Velocidade adequada
        vel_adequada_elem = page.locator("#vel_adequada.shiny-text-output").first
        if await vel_adequada_elem.count() > 0:
            texto_vel = (await vel_adequada_elem.inner_text()).strip()
            match = re.search(r'(\d+)', texto_vel)
            if match:
                resultados["vel_adequada"] = match.group(1)
        
    except Exception as e:
        logger.debug(f"[Worker {worker_id}] INEP {inep} - Erro na extração básica: {e}")
    
    return resultados

import re
from datetime import datetime

async def extrair_dados_conexao(page, inep, worker_id):
    """Extrai dados de velocidade e conexão"""

    dados = {
        "status": "",
        "vel_max": "",
        "download": "",
        "criterio": "",
        "medicoes": "",
        "instalacao": "",
        "ultima_medicao": "",
        "vel_max_medicao": ""
    }

    try:
        # ==============================
        # Status do medidor
        # ==============================
        status_elem = page.locator("#status_medidor").first
        if await status_elem.count() > 0:
            dados["status"] = (await status_elem.inner_text()).strip()

        # ==============================
        # Velocidade máxima
        # ==============================
        vel_max_elem = page.locator("#plano_estimado").first
        if await vel_max_elem.count() > 0:
            texto_vel_max = (await vel_max_elem.inner_text()).strip()
            match = re.search(r'([\d,\.]+)', texto_vel_max)
            if match:
                dados["vel_max"] = match.group(1).replace(",", ".")

        # ==============================
        # Download
        # ==============================
        download_elem = page.locator("#vel_download").first
        if await download_elem.count() > 0:
            texto_download = (await download_elem.inner_text()).strip()
            match = re.search(r'([\d,\.]+)', texto_download)
            if match:
                dados["download"] = match.group(1).replace(",", ".")

        # ==============================
        # Critério MEC
        # ==============================
        criterio_elem = page.locator("#atende_criterio_gice").first
        if await criterio_elem.count() > 0:
            dados["criterio"] = (await criterio_elem.inner_text()).strip()

            logger.debug(
                f"[Worker {worker_id}] INEP {inep} - Critério MEC: "
                f"{dados['criterio'][:100]}"
            )

        # ==============================
        # Número de medições
        # ==============================
        medicoes_elem = page.locator("#nro_medicoes").first
        if await medicoes_elem.count() > 0:
            dados["medicoes"] = (await medicoes_elem.inner_text()).strip()

        # ==============================
        # TABELA DE MEDIÇÕES
        # PEGAR A DATA MAIS RECENTE
        # ==============================
        try:
            linhas = page.locator("#DataTables_Table_0 tbody tr")
            total = await linhas.count()

            ultima_data = None
            linha_recente = None

            for i in range(total):
                linha = linhas.nth(i)
                cols = await linha.locator("td").all()

                if len(cols) >= 3:
                    instalacao = (await cols[0].inner_text()).strip()
                    data_medicao = (await cols[1].inner_text()).strip()
                    vel_max_medicao = (await cols[2].inner_text()).strip()

                    try:
                        # Exemplo: 30/03/26 - 08:12
                        data_convertida = datetime.strptime(
                            data_medicao,
                            "%d/%m/%y - %H:%M"
                        )

                        if not ultima_data or data_convertida > ultima_data:
                            ultima_data = data_convertida
                            linha_recente = {
                                "instalacao": instalacao,
                                "ultima_medicao": data_medicao,
                                "vel_max_medicao": vel_max_medicao
                            }

                    except Exception:
                        continue

            if linha_recente:
                dados.update(linha_recente)

        except Exception as e:
            logger.debug(
                f"[Worker {worker_id}] INEP {inep} - "
                f"Erro ao processar tabela de medições: {e}"
            )

    except Exception as e:
        logger.debug(
            f"[Worker {worker_id}] INEP {inep} - "
            f"Erro na extração de conexão: {e}"
        )

    return dados

# -----------------------------
# Função principal de consulta
# -----------------------------
async def consultar(page, inep, worker_id):
    inicio = time.time()
    logger.info(f"[Worker {worker_id}] ===== INICIANDO CONSULTA INEP {inep} =====")

    for tentativa in range(MAX_RETRY):
        try:
            logger.info(f"[Worker {worker_id}] INEP {inep} - Tentativa {tentativa+1}/{MAX_RETRY}")

            # Navegar para a URL
            await page.goto(URL, wait_until="domcontentloaded", timeout=60000)
            await page.wait_for_timeout(2000)

            # Aguardar campo de input
            await page.wait_for_selector("#inep_plano", timeout=30000)
            
            # Limpar campo - método mais agressivo
            await page.click("#inep_plano", timeout=5000)
            await page.keyboard.press("Control+A")
            await page.keyboard.press("Backspace")
            # Limpar também via JavaScript para garantir
            await page.evaluate("document.querySelector('#inep_plano').value = ''")
            await page.wait_for_timeout(1000)
            
            # Digitar INEP
            await page.fill("#inep_plano", inep, timeout=5000)
            await page.wait_for_timeout(1000)
            await page.keyboard.press("Enter")
            
            # Aguardar carregamento
            logger.info(f"[Worker {worker_id}] INEP {inep} - Aguardando carregamento dos dados...")
            
            # Primeiro, esperar que o elemento apareça
            try:
                await page.wait_for_selector("#nome_escola.shiny-text-output", timeout=10000)
            except:
                pass
            
            # Agora esperar ter conteúdo real
            try:
                await page.wait_for_function("""
                    () => {
                        const el = document.querySelector('#nome_escola.shiny-text-output');
                        if (!el) return false;
                        const text = el.innerText?.trim() || '';
                        return text.length > 0 && text !== 'Número de Medições';
                    }
                """, timeout=20000)
            except Exception as e:
                logger.warning(f"[Worker {worker_id}] INEP {inep} - Timeout aguardando conteúdo: {e}")
            
            # Validar se os dados correspondem ao INEP
            if not await validar_dados_inep(page, inep, worker_id):
                logger.warning(f"[Worker {worker_id}] INEP {inep} - Validação falhou. Possível dados incorretos.")
                
                # Verificar se é INEP inválido
                page_text = await page.evaluate("document.body.innerText")
                if "não encontrado" in page_text.lower() or "inválido" in page_text.lower():
                    status_coleta = "INEP_INVALIDO"
                    logger.warning(f"[Worker {worker_id}] INEP {inep} - INEP inválido")
                    log_inep(inep, "INEP_INVALIDO", "INEP não encontrado")
                    
                    resultado_erro = {
                        "INEP": inep,
                        "Nome_Escola": "",
                        "Municipio_UF": "",
                        "Gestao": "",
                        "Total_Estudantes": "",
                        "Estudantes_Maior_Turno": "",
                        "Velocidade_Adequada": "",
                        "Status": "",
                        "Criterio_MEC": "",
                        "Adequada": "",
                        "Vel_Max_Mbps": "",
                        "Download_Mbps": "",
                        "Numero_Medicoes": "",
                        "Ultima_Medicao_DataHora": "",
                        "Vel_Max_Ultima_Medicao": "",
                        "Status_Coleta": "INEP_INVALIDO"
                    }
                    
                    salvar_no_banco(resultado_erro)
                    salvar_checkpoint(inep)
                    return resultado_erro
                else:
                    # Se não é inválido mas a validação falhou, tentar novamente
                    if tentativa < MAX_RETRY - 1:
                        logger.info(f"[Worker {worker_id}] INEP {inep} - Tentando novamente...")
                        continue
                    else:
                        raise Exception("Validação de dados falhou após todas tentativas")
            
            logger.info(f"[Worker {worker_id}] INEP {inep} - Dados validados com sucesso")
            
            # Pequena pausa para garantir que todos os dados foram carregados
            await page.wait_for_timeout(2000)
            
            # Extrair dados da escola
            dados_escola = await extrair_dados_escola(page, inep, worker_id)
            
            nome_escola = dados_escola["nome_escola"]
            municipio_uf = dados_escola["municipio_uf"]
            gestao = dados_escola["gestao"]
            total_estudantes = dados_escola["total_estudantes"]
            maior_turno = dados_escola["maior_turno"]
            vel_adequada = dados_escola["vel_adequada"]
            
            # Extrair dados de conexão
            dados_conexao = await extrair_dados_conexao(page, inep, worker_id)
            
            # Log do critério MEC bruto para debug
            logger.info(f"[Worker {worker_id}] INEP {inep} - Critério MEC bruto: {dados_conexao['criterio']}")
            
            # Determinar adequação usando a função validada
            adequado = await determinar_adequacao(dados_conexao["criterio"])
            logger.info(f"[Worker {worker_id}] INEP {inep} - Adequação determinada: {adequado}")
            
            # Determinar status da coleta
            status_coleta = "SUCESSO"
            logger.info(f"[Worker {worker_id}] ✓ INEP {inep} - Escola: {nome_escola[:50]}")
            log_inep(inep, "SUCESSO", f"Escola: {nome_escola[:100]}")
            
            resultado = {
                "INEP": inep,
                "Nome_Escola": nome_escola,
                "Municipio_UF": municipio_uf,
                "Gestao": gestao,
                "Total_Estudantes": total_estudantes,
                "Estudantes_Maior_Turno": maior_turno,
                "Velocidade_Adequada": vel_adequada,
                "Status": dados_conexao["status"],
                "Criterio_MEC": dados_conexao["criterio"],
                "Adequada": adequado,
                "Vel_Max_Mbps": dados_conexao["vel_max"],
                "Download_Mbps": dados_conexao["download"],
                "Numero_Medicoes": dados_conexao["medicoes"],
                "Ultima_Medicao_DataHora": dados_conexao["ultima_medicao"],
                "Vel_Max_Ultima_Medicao": dados_conexao["vel_max_medicao"],
                "Status_Coleta": status_coleta
            }
            
            # Salvar no banco de dados
            if salvar_no_banco(resultado):
                logger.info(f"[Worker {worker_id}] INEP {inep} - Salvo no banco de dados")
            else:
                logger.error(f"[Worker {worker_id}] INEP {inep} - Falha ao salvar no banco")
            
            # Salvar checkpoint
            salvar_checkpoint(inep)
            
            tempo_total = time.time() - inicio
            logger.info(f"[Worker {worker_id}] INEP {inep} - FINALIZADO em {tempo_total:.2f}s")
            
            return resultado
            
        except Exception as e:
            logger.error(f"[Worker {worker_id}] INEP {inep} - Tentativa {tentativa+1} erro: {str(e)}")
            log_inep(inep, f"ERRO_TENTATIVA_{tentativa+1}", "", str(e))
            
            if tentativa < MAX_RETRY - 1:
                wait_time = 5 * (tentativa + 1)
                logger.info(f"[Worker {worker_id}] INEP {inep} - Aguardando {wait_time}s")
                await asyncio.sleep(wait_time)
            continue
    
    # Todas tentativas falharam
    tempo_total = time.time() - inicio
    logger.error(f"[Worker {worker_id}] INEP {inep} - TODAS TENTATIVAS FALHARAM")
    log_inep(inep, "ERRO_FATAL", f"Falhou após {MAX_RETRY} tentativas")
    
    resultado_erro = {
        "INEP": inep,
        "Nome_Escola": "",
        "Municipio_UF": "",
        "Gestao": "",
        "Total_Estudantes": "",
        "Estudantes_Maior_Turno": "",
        "Velocidade_Adequada": "",
        "Status": "",
        "Criterio_MEC": "",
        "Adequada": "",
        "Vel_Max_Mbps": "",
        "Download_Mbps": "",
        "Numero_Medicoes": "",
        "Ultima_Medicao_DataHora": "",
        "Vel_Max_Ultima_Medicao": "",
        "Status_Coleta": "ERRO_TODAS_TENTATIVAS"
    }
    
    # Salvar erro no banco também
    salvar_no_banco(resultado_erro)
    salvar_checkpoint(inep)
    
    return resultado_erro

# -----------------------------
# Worker com reinicialização periódica
# -----------------------------
async def worker(queue, p, resultados, worker_id):
    logger.info(f"[Worker {worker_id}] INICIANDO")
    
    contador = 0
    total = queue.qsize()
    logger.info(f"[Worker {worker_id}] Fila tem {total} INEPs para processar")
    
    browser = None
    context = None
    page = None
    
    try:
        while True:
            # Reiniciar browser periodicamente
            if contador % MAX_PAGINAS_POR_WORKER == 0:
                if browser:
                    logger.info(f"[Worker {worker_id}] Reiniciando navegador após {MAX_PAGINAS_POR_WORKER} consultas")
                    try:
                        if page:
                            await page.close()
                        if context:
                            await context.close()
                        if browser:
                            await browser.close()
                    except:
                        pass
                
                # Criar novo browser
                browser = await p.chromium.launch(
                    headless=True,
                    args=[
                        '--no-sandbox',
                        '--disable-setuid-sandbox',
                        '--disable-dev-shm-usage',
                        '--disable-gpu',
                        '--single-process',
                    ]
                )
                context = await browser.new_context(
                    viewport={'width': 1920, 'height': 1080},
                    user_agent='Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36'
                )
                page = await context.new_page()
                page.set_default_timeout(60000)
                logger.info(f"[Worker {worker_id}] Novo navegador criado")
            
            # Pegar próximo INEP da fila
            inep = await queue.get()
            
            if inep is None:
                logger.info(f"[Worker {worker_id}] Recebeu sinal de finalização")
                break
            
            contador += 1
            logger.info(f"[Worker {worker_id}] Progresso: {contador}/{total} - INEP {inep}")
            
            # Tentar processar com reconexão se necessário
            max_tentativas_worker = 2
            for tentativa_worker in range(max_tentativas_worker):
                try:
                    resultado = await consultar(page, inep, worker_id)
                    resultados.append(resultado)
                    break
                    
                except Exception as e:
                    if "Target page, context or browser has been closed" in str(e):
                        logger.warning(f"[Worker {worker_id}] Navegador fechado, recriando página...")
                        try:
                            # Recriar página
                            if page:
                                await page.close()
                            page = await context.new_page()
                            page.set_default_timeout(60000)
                        except:
                            # Se falhar, recriar contexto também
                            try:
                                if context:
                                    await context.close()
                                context = await browser.new_context(
                                    viewport={'width': 1920, 'height': 1080},
                                    user_agent='Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36'
                                )
                                page = await context.new_page()
                                page.set_default_timeout(60000)
                            except:
                                # Se tudo falhar, recriar browser
                                logger.error(f"[Worker {worker_id}] Erro crítico, reiniciando tudo...")
                                if browser:
                                    await browser.close()
                                browser = await p.chromium.launch(
                                    headless=True,
                                    args=[
                                        '--no-sandbox',
                                        '--disable-setuid-sandbox',
                                        '--disable-dev-shm-usage',
                                        '--disable-gpu',
                                        '--single-process',
                                    ]
                                )
                                context = await browser.new_context(
                                    viewport={'width': 1920, 'height': 1080},
                                    user_agent='Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36'
                                )
                                page = await context.new_page()
                                page.set_default_timeout(60000)
                        
                        if tentativa_worker < max_tentativas_worker - 1:
                            logger.info(f"[Worker {worker_id}] Tentativa {tentativa_worker+2} para INEP {inep}")
                            await asyncio.sleep(2)
                            continue
                    else:
                        logger.error(f"[Worker {worker_id}] Erro fatal: {e}", exc_info=True)
                        log_inep(inep, "ERRO_WORKER", "", str(e))
                        break

            queue.task_done()
            
            # Pequena pausa entre requisições
            await asyncio.sleep(1)
    
    except Exception as e:
        logger.error(f"[Worker {worker_id}] Erro no worker: {e}", exc_info=True)
    finally:
        try:
            if page:
                await page.close()
            if context:
                await context.close()
            if browser:
                await browser.close()
        except:
            pass
        logger.info(f"[Worker {worker_id}] FINALIZADO - Processou {contador} INEPs")

# -----------------------------
# Função principal de execução
# -----------------------------
async def executar_coleta():
    """Função principal que executa a coleta de dados"""
    logger.info("="*60)
    logger.info("INICIANDO COLETA DE DADOS")
    logger.info("="*60)
    
    # Verificar/criar tabela no banco
    if not criar_tabela_se_nao_existe():
        logger.error("Falha ao criar/verificar tabela no banco de dados")
        return
    
    # Limpar logs anteriores
    for arquivo in [ARQUIVO_LOG_DETALHADO, ARQUIVO_ERROS]:
        if os.path.exists(arquivo):
            os.remove(arquivo)
    
    # Criar cabeçalho dos logs
    with open(ARQUIVO_LOG_DETALHADO, 'w', encoding='utf-8') as f:
        f.write(f"LOG DETALHADO DE INEPs - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write("="*80 + "\n\n")
    
    # Carregar INEPs não processados
    global INEPS
    INEPS = carregar_ineps_nao_processados()
    
    # Se não houver INEPs para processar
    if not INEPS:
        logger.info("Todos os INEPs já foram processados!")
        # Reset automático para próxima execução agendada
        resetar_checkpoint_automatico()
        return
    
    # Criar fila
    queue = asyncio.Queue()
    for inep in INEPS:
        await queue.put(inep)
    
    logger.info(f"Fila criada com {queue.qsize()} INEPs")
    resultados = []
    
    async with async_playwright() as p:
        logger.info("Iniciando Playwright...")
        
        logger.info(f"Iniciando {WORKERS} workers com reinicialização a cada {MAX_PAGINAS_POR_WORKER} consultas...")
        workers = []
        for i in range(WORKERS):
            worker_task = asyncio.create_task(worker(queue, p, resultados, i+1))
            workers.append(worker_task)
            logger.info(f"Worker {i+1} criado")
        
        logger.info("Todos workers iniciados. Aguardando processamento...")
        logger.info("-" * 60)
        
        # Aguardar a fila ser processada
        await queue.join()
        logger.info("Fila processada completamente")
        
        # Enviar sinais de finalização
        for _ in workers:
            await queue.put(None)
        
        # Aguardar workers finalizarem
        await asyncio.gather(*workers)
        logger.info("Todos workers finalizados")
    
    # Estatísticas finais
    logger.info("\n" + "="*60)
    logger.info("RESUMO FINAL")
    logger.info("="*60)
    logger.info(f"Total INEPs no arquivo original: {len(INEPS) + len(carregar_checkpoint())}")
    logger.info(f"INEPs processados nesta execução: {len(resultados)}")
    logger.info(f"Total acumulado: {len(carregar_checkpoint())}")
    
    # Consultar banco para estatísticas
    conn = get_db_connection()
    if conn:
        try:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT Status_Coleta, COUNT(*) 
                FROM resultados_inep 
                GROUP BY Status_Coleta
            """)
            stats = cursor.fetchall()
            
            logger.info("\nEstatísticas do banco de dados:")
            for status, count in stats:
                logger.info(f"  {status}: {count}")
            
        except Exception as e:
            logger.error(f"Erro ao consultar banco: {e}")
        finally:
            conn.close()
    
    logger.info(f"\nLog detalhado: {ARQUIVO_LOG_DETALHADO}")
    logger.info(f"Log de erros: {ARQUIVO_ERROS}")
    
    # Registrar execução
    registrar_execucao()
    
    # Reset automático para próxima execução
    logger.info("\n" + "="*60)
    logger.info("🔍 VERIFICANDO PROCESSAMENTO PARA RESET AUTOMÁTICO")
    logger.info("="*60)
    
    if resetar_checkpoint_automatico():
        logger.info("\n✅ Sistema pronto para nova execução!")
        logger.info("   Para processar novamente, basta executar o script")
    else:
        logger.info("\n⏳ Ainda há INEPs pendentes ou nenhum checkpoint encontrado")
        logger.info("   Continue executando até completar todos")

# -----------------------------
# Função para executar em modo agendado
# -----------------------------
def executar_agendado():
    """Executa a coleta de forma agendada (loop contínuo)"""
    config = carregar_config_agendamento()
    
    logger.info("="*60)
    logger.info("SISTEMA DE COLETA AGENDADA")
    logger.info("="*60)
    logger.info(f"Agendamento: {'ATIVO' if config['ativo'] else 'INATIVO'}")
    
    if config['ativo']:
        logger.info(f"Frequência: {config['frequencia'].upper()}")
        if config['frequencia'] == 'diario':
            logger.info(f"Horário: {config['horario']}")
        elif config['frequencia'] == 'semanal':
            logger.info(f"Dia da semana: {['domingo','segunda','terça','quarta','quinta','sexta','sábado'][config['dia_semana']]} às {config['horario']}")
        elif config['frequencia'] == 'mensal':
            logger.info(f"Dia do mês: {config['dia_mes']} às {config['horario']}")
        
        if config['intervalo_minutos'] > 0:
            logger.info(f"Intervalo mínimo entre execuções: {config['intervalo_minutos']} minutos")
    
    logger.info("="*60)
    
    if not config['ativo']:
        logger.info("Modo: Execução única")
        # Executar uma vez
        asyncio.run(executar_coleta())
        return
    
    # Modo agendado - loop infinito
    logger.info("Modo: Execução contínua agendada")
    logger.info("Aguardando horário de execução...")
    
    while True:
        if deve_executar_agora():
            logger.info("\n" + "="*60)
            logger.info(f"Iniciando execução agendada em {datetime.now()}")
            logger.info("="*60)
            
            try:
                asyncio.run(executar_coleta())
                logger.info(f"Execução agendada finalizada em {datetime.now()}")
            except Exception as e:
                logger.error(f"Erro na execução agendada: {e}", exc_info=True)
            
            # Após execução, aguardar até próximo horário
            logger.info("Aguardando próxima execução...")
        
        # Aguardar 60 segundos antes de verificar novamente
        time.sleep(60)

# -----------------------------
# main
# -----------------------------
if __name__ == "__main__":
    # Executar o sistema
    executar_agendado()
