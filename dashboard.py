from flask import Flask, jsonify, render_template, send_from_directory
import sqlite3
import json
import os
import sys
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

app = Flask(__name__, 
           template_folder='templates',  # Pasta dos templates HTML
           static_folder='static',       # Pasta dos arquivos estáticos
           static_url_path='/static')    # URL para acessar os estáticos

# Configuração do banco SQLite
DB_PATH = os.getenv('DB_PATH', 'dados.db')

ARQUIVO_ULTIMA_EXECUCAO = "logs/ultima_execucao.json"
ARQUIVO_CHECKPOINT = "logs/checkpoint.json"

DASHBOARD_HOST = os.getenv('DASHBOARD_HOST', '0.0.0.0')
DASHBOARD_PORT = int(os.getenv('DASHBOARD_PORT', '5000'))
DASHBOARD_DEBUG = os.getenv('DASHBOARD_DEBUG', 'false').lower() == 'true'

def get_db_connection():
    try:
        conn = sqlite3.connect(DB_PATH, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        return conn
    except Exception as e:
        return None

def get_boot_info():
    info = {
        'ultima_coleta': 'Nunca executado',
        'proximo_agendamento': 'Não configurado',
        'status_agendamento': 'Desconhecido',
        'frequencia': os.getenv('AGENDAMENTO_FREQUENCIA', 'diario'),
        'horario': os.getenv('AGENDAMENTO_HORARIO', '02:00'),
        'agendamento_ativo': os.getenv('AGENDAMENTO_ATIVO', 'false').lower() == 'true',
        'total_checkpoint': 0
    }

    if os.path.exists(ARQUIVO_ULTIMA_EXECUCAO):
        try:
            with open(ARQUIVO_ULTIMA_EXECUCAO, 'r') as f:
                dados = json.load(f)
                ultima = datetime.fromisoformat(dados['ultima_execucao'])
                info['ultima_coleta'] = ultima.strftime('%d/%m/%Y %H:%M:%S')
                info['status_ultima'] = dados.get('status', 'desconhecido')

                if info['agendamento_ativo']:
                    agora = datetime.now()
                    horario_parts = info['horario'].split(':')
                    prox = agora.replace(hour=int(horario_parts[0]), minute=int(horario_parts[1]), second=0)
                    if prox <= agora:
                        prox += timedelta(days=1)
                    info['proximo_agendamento'] = prox.strftime('%d/%m/%Y %H:%M')
                else:
                    info['proximo_agendamento'] = 'Agendamento desativado'
        except:
            pass

    if os.path.exists(ARQUIVO_CHECKPOINT):
        try:
            with open(ARQUIVO_CHECKPOINT, 'r') as f:
                checkpoint = json.load(f)
                info['total_checkpoint'] = len(checkpoint.get('processados', []))
        except:
            pass

    return info

def converter_data_sqlite(data_str):
    """
    Converte string de data "dd/mm/yy - HH:MM" para formato YYYY-MM-DD
    Exemplo: "07/01/26 - 17:31" -> "2026-01-07"
    """
    if not data_str or data_str.strip() == '':
        return None
    
    try:
        # Extrair apenas a parte da data (antes do " - ")
        parte_data = data_str.split(' - ')[0].strip()
        # Formato: dd/mm/yy
        dia, mes, ano_curto = parte_data.split('/')
        ano = 2000 + int(ano_curto)  # Converte 26 -> 2026
        return f"{ano:04d}-{int(mes):02d}-{int(dia):02d}"
    except:
        return None

def get_dashboard_data():
    conn = get_db_connection()
    if not conn:
        return None

    try:
        cursor = conn.cursor()
        
        # Cards
        cursor.execute("SELECT COUNT(*) as total FROM resultados_inep")
        total_escolas = cursor.fetchone()['total']
        
        cursor.execute("SELECT COUNT(*) as total FROM resultados_inep WHERE Adequada = 'SIM'")
        dentro_padrao = cursor.fetchone()['total']
        
        cursor.execute("SELECT COUNT(*) as total FROM resultados_inep WHERE Adequada = 'NÃO'")
        fora_padrao = cursor.fetchone()['total']
        
        # Monitoramento atrasado: última medição > 30 dias ou sem medição
        # Buscar todas as escolas primeiro e filtrar no Python para garantir precisão
        cursor.execute("""
            SELECT INEP, Nome_Escola, Adequada, Ultima_Medicao_DataHora, 
                   Download_Mbps, Status_Coleta, Data_Coleta
            FROM resultados_inep
        """)
        todas_escolas_raw = cursor.fetchall()
        
        # Filtrar escolas atrasadas
        hoje = datetime.now()
        escolas_atrasadas_lista = []
        
        for escola in todas_escolas_raw:
            ultima_medicao = escola['Ultima_Medicao_DataHora']
            
            # Se não tem data de medição, considera atrasado
            if not ultima_medicao or ultima_medicao.strip() == '':
                escolas_atrasadas_lista.append(dict(escola))
                continue
            
            # Converter a data para objeto datetime
            try:
                # Extrair parte da data "dd/mm/yy"
                parte_data = ultima_medicao.split(' - ')[0].strip()
                dia, mes, ano_curto = parte_data.split('/')
                ano = 2000 + int(ano_curto)
                data_medicao = datetime(ano, int(mes), int(dia))
                
                # Calcular diferença em dias
                dias_diferenca = (hoje - data_medicao).days
                
                # Se mais de 30 dias, considera atrasado
                if dias_diferenca > 30:
                    escolas_atrasadas_lista.append(dict(escola))
                    
            except Exception as e:
                # Em caso de erro no parse, considera atrasado também
                escolas_atrasadas_lista.append(dict(escola))
        
        monitoramento_atrasado = len(escolas_atrasadas_lista)
        
        # Gráfico 1: distribuição por adequação
        cursor.execute("""
            SELECT Adequada, COUNT(*) as total
            FROM resultados_inep
            GROUP BY Adequada
        """)
        dist_adequacao = [dict(row) for row in cursor.fetchall()]
        
        # Gráfico 2: top velocidade download
        cursor.execute("""
            SELECT Nome_Escola, Download_Mbps
            FROM resultados_inep
            WHERE Download_Mbps IS NOT NULL AND Download_Mbps > 0
            ORDER BY Download_Mbps DESC
            LIMIT 10
        """)
        top_velocidade = [dict(row) for row in cursor.fetchall()]
        
        # Ordenar escolas atrasadas por data (mais antigas primeiro)
        escolas_atrasadas_lista.sort(key=lambda x: x['Ultima_Medicao_DataHora'] or '')
        
        # Tabela todas as escolas
        cursor.execute("""
            SELECT INEP, Nome_Escola, Total_Estudantes,
                   Adequada, Velocidade_Adequada, Download_Mbps, Vel_Max_Mbps,
                   Numero_Medicoes, Ultima_Medicao_DataHora, Status_Coleta, Data_Coleta
            FROM resultados_inep
            ORDER BY Nome_Escola
        """)
        todas_escolas = [dict(row) for row in cursor.fetchall()]
        
        return {
            'cards': {
                'total_escolas': total_escolas,
                'dentro_padrao': dentro_padrao,
                'fora_padrao': fora_padrao,
                'monitoramento_atrasado': monitoramento_atrasado
            },
            'graficos': {
                'dist_adequacao': dist_adequacao,
                'top_velocidade': top_velocidade
            },
            'escolas_atrasadas': escolas_atrasadas_lista,
            'todas_escolas': todas_escolas
        }
    except Exception as e:
        print(f"Erro ao buscar dados: {e}")
        return None
    finally:
        conn.close()

@app.route('/api/data')
def api_data():
    data = get_dashboard_data()
    boot = get_boot_info()
    if data is None:
        return jsonify({'error': 'Falha ao conectar ao banco de dados'}), 500
    
    return jsonify({
        'cards': data['cards'],
        'graficos': {
            'dist_adequacao': data['graficos']['dist_adequacao'],
            'top_velocidade': data['graficos']['top_velocidade']
        },
        'escolas_atrasadas': data['escolas_atrasadas'],
        'todas_escolas': data['todas_escolas'],
        'boot': boot,
        'atualizado_em': datetime.now().strftime('%d/%m/%Y %H:%M:%S')
    })

@app.route('/')
def index():
    return render_template('index.html')

if __name__ == '__main__':
    # Suprime o warning do servidor de desenvolvimento
    cli = sys.modules.get('flask.cli')
    if cli:
        cli.show_server_banner = lambda *args: None
    
    print("=" * 55)
    print(f"  Dashboard de Conectividade Escolar")
    print(f"  http://{DASHBOARD_HOST}:{DASHBOARD_PORT}")
    print("=" * 55)
    
    app.run(
        host=DASHBOARD_HOST,
        port=DASHBOARD_PORT,
        debug=DASHBOARD_DEBUG,
        use_reloader=False
    )
