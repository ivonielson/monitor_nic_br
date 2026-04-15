#!/bin/bash

# Cores para output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
PURPLE='\033[0;35m'
CYAN='\033[0;36m'
WHITE='\033[1;37m'
NC='\033[0m' # No Color

# Configurações
COMPOSE_FILE="docker-compose.yml"
PROJECT_NAME="smec-monitor"
APP_CONTAINER="smec-monitor"
NGINX_CONTAINER="smec-nginx"

# Função para verificar se docker está rodando
check_docker() {
    if ! docker info >/dev/null 2>&1; then
        echo -e "${RED}❌ Docker não está rodando ou não tem permissões${NC}"
        exit 1
    fi
}

# Função para mostrar cabeçalho
show_header() {
    clear
    echo -e "${CYAN}╔══════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${CYAN}║${WHITE}           DOCKER MANAGEMENT SYSTEM - SMEC MONITOR            ${CYAN}║${NC}"
    echo -e "${CYAN}╚══════════════════════════════════════════════════════════════╝${NC}"
    echo ""
}

# Função para mostrar status dos containers
show_status() {
    echo -e "${BLUE}📊 STATUS DOS CONTAINERS:${NC}"
    echo ""
    
    # App container
    if docker ps -a --format "table {{.Names}}\t{{.Status}}" | grep -q "$APP_CONTAINER"; then
        STATUS=$(docker ps -a --format "table {{.Names}}\t{{.Status}}" | grep "$APP_CONTAINER" | awk '{print $2}')
        if [[ $STATUS == *"Up"* ]]; then
            echo -e "  🟢 ${GREEN}$APP_CONTAINER: RUNNING${NC}"
        else
            echo -e "  🔴 ${RED}$APP_CONTAINER: STOPPED${NC}"
        fi
    else
        echo -e "  ⚪ ${YELLOW}$APP_CONTAINER: NOT CREATED${NC}"
    fi
    
    # Nginx container
    if docker ps -a --format "table {{.Names}}\t{{.Status}}" | grep -q "$NGINX_CONTAINER"; then
        STATUS=$(docker ps -a --format "table {{.Names}}\t{{.Status}}" | grep "$NGINX_CONTAINER" | awk '{print $2}')
        if [[ $STATUS == *"Up"* ]]; then
            echo -e "  🟢 ${GREEN}$NGINX_CONTAINER: RUNNING${NC}"
        else
            echo -e "  🔴 ${RED}$NGINX_CONTAINER: STOPPED${NC}"
        fi
    else
        echo -e "  ⚪ ${YELLOW}$NGINX_CONTAINER: NOT CREATED${NC}"
    fi
    
    echo ""
}

# Menu principal
show_menu() {
    echo -e "${WHITE}════════════════════════════════════════════════════════════${NC}"
    echo -e "${CYAN}🔧 MENU DE OPERAÇÕES:${NC}"
    echo ""
    echo -e "${GREEN}1)${NC} 🚀 Iniciar todos os containers"
    echo -e "${GREEN}2)${NC} 🛑 Parar todos os containers"
    echo -e "${GREEN}3)${NC} 🔄 Reiniciar todos os containers"
    echo -e "${GREEN}4)${NC} 🐚 Acessar shell do container APP"
    echo -e "${GREEN}5)${NC} 🌐 Acessar shell do container NGINX"
    echo -e "${GREEN}6)${NC} 📜 Ver logs do container APP"
    echo -e "${GREEN}7)${NC} 🌍 Ver logs do container NGINX"
    echo -e "${GREEN}8)${NC} 📁 Ver arquivos dentro do container APP"
    echo -e "${GREEN}9)${NC} 🔍 Ver estrutura de pastas do projeto"
    echo -e "${GREEN}10)${NC} 📦 Listar volumes Docker"
    echo -e "${GREEN}11)${NC} 🧹 Limpar containers parados e imagens não utilizadas"
    echo -e "${GREEN}12)${NC} 🔨 Reconstruir e reiniciar containers"
    echo -e "${GREEN}13)${NC} 📝 Verificar saúde do container APP"
    echo -e "${GREEN}14)${NC} 📊 Ver estatísticas de uso (CPU/Memória)"
    echo -e "${GREEN}15)${NC} 📋 Ver todos os containers em execução"
    echo -e "${GREEN}16)${NC} 🔄 Atualizar script Python sem reiniciar container"
    echo -e "${GREEN}17)${NC} 💾 Backup do banco de dados"
    echo -e "${GREEN}18)${NC} 🚪 Sair"
    echo ""
    echo -e "${YELLOW}════════════════════════════════════════════════════════════${NC}"
    echo -n -e "${WHITE}👉 Escolha uma opção [1-18]: ${NC}"
}

# Função para ver logs do app com follow
view_app_logs() {
    echo -e "${BLUE}📜 Logs do container APP (Ctrl+C para sair):${NC}"
    docker logs -f $APP_CONTAINER
}

# Função para ver logs do nginx
view_nginx_logs() {
    echo -e "${BLUE}🌍 Logs do container NGINX (Ctrl+C para sair):${NC}"
    docker logs -f $NGINX_CONTAINER
}

# Função para acessar shell do app
access_app_shell() {
    echo -e "${BLUE}🐚 Acessando shell do container $APP_CONTAINER...${NC}"
    echo -e "${YELLOW}Dica: Use 'exit' para sair${NC}"
    echo ""
    docker exec -it $APP_CONTAINER /bin/sh
}

# Função para acessar shell do nginx
access_nginx_shell() {
    echo -e "${BLUE}🌐 Acessando shell do container $NGINX_CONTAINER...${NC}"
    echo -e "${YELLOW}Dica: Use 'exit' para sair${NC}"
    echo ""
    docker exec -it $NGINX_CONTAINER /bin/sh
}

# Função para ver arquivos dentro do container
view_container_files() {
    echo -e "${BLUE}📁 Arquivos dentro do container $APP_CONTAINER:${NC}"
    echo -e "${YELLOW}Mostrando estrutura do /app:${NC}"
    echo ""
    docker exec $APP_CONTAINER ls -lah /app/
    echo ""
    echo -e "${YELLOW}Pressione ENTER para continuar...${NC}"
    read
}

# Função para ver estrutura de pastas do projeto
view_project_structure() {
    echo -e "${BLUE}📂 Estrutura de pastas do projeto local:${NC}"
    echo ""
    if command -v tree &> /dev/null; then
        tree -L 2 -I '__pycache__|*.pyc|.git'
    else
        ls -lah
    fi
    echo ""
    echo -e "${YELLOW}Pressione ENTER para continuar...${NC}"
    read
}

# Função para listar volumes
list_volumes() {
    echo -e "${BLUE}📦 Volumes Docker:${NC}"
    docker volume ls
    echo ""
    echo -e "${YELLOW}Pressione ENTER para continuar...${NC}"
    read
}

# Função para limpar sistema
clean_docker() {
    echo -e "${YELLOW}🧹 Limpando containers parados e imagens não utilizadas...${NC}"
    echo ""
    echo -e "${RED}ATENÇÃO: Isso removerá todos os containers parados e imagens não utilizadas!${NC}"
    echo -n -e "${YELLOW}Tem certeza? (s/N): ${NC}"
    read -r confirm
    if [[ $confirm == [sS] ]]; then
        docker system prune -f
        echo -e "${GREEN}✅ Limpeza concluída!${NC}"
    else
        echo -e "${YELLOW}❌ Operação cancelada${NC}"
    fi
    echo ""
    echo -e "${YELLOW}Pressione ENTER para continuar...${NC}"
    read
}

# Função para reconstruir containers
rebuild_containers() {
    echo -e "${BLUE}🔨 Reconstruindo e reiniciando containers...${NC}"
    echo -e "${YELLOW}Isso pode levar alguns minutos...${NC}"
    echo ""
    docker-compose -f $COMPOSE_FILE down
    docker-compose -f $COMPOSE_FILE build --no-cache
    docker-compose -f $COMPOSE_FILE up -d
    echo ""
    echo -e "${GREEN}✅ Containers reconstruídos e iniciados!${NC}"
    echo ""
    echo -e "${BLUE}📋 Status:${NC}"
    docker-compose -f $COMPOSE_FILE ps
    echo ""
    echo -e "${YELLOW}Pressione ENTER para continuar...${NC}"
    read
}

# Função para verificar saúde
check_health() {
    echo -e "${BLUE}🏥 Verificando saúde do container $APP_CONTAINER...${NC}"
    echo ""
    
    HEALTH_STATUS=$(docker inspect --format='{{.State.Health.Status}}' $APP_CONTAINER 2>/dev/null)
    
    if [ $? -eq 0 ]; then
        case $HEALTH_STATUS in
            healthy)
                echo -e "${GREEN}✅ Status: HEALTHY${NC}"
                ;;
            unhealthy)
                echo -e "${RED}❌ Status: UNHEALTHY${NC}"
                ;;
            starting)
                echo -e "${YELLOW}⏳ Status: STARTING${NC}"
                ;;
            *)
                echo -e "${YELLOW}⚠️ Status: $HEALTH_STATUS${NC}"
                ;;
        esac
        
        echo ""
        echo -e "${BLUE}Últimas verificações:${NC}"
        docker inspect --format='{{json .State.Health.Log}}' $APP_CONTAINER | jq '.[] | {Start: .Start, End: .End, ExitCode: .ExitCode}' 2>/dev/null || echo "Não foi possível obter histórico"
    else
        echo -e "${RED}❌ Container não encontrado ou healthcheck não configurado${NC}"
    fi
    
    echo ""
    echo -e "${YELLOW}Pressione ENTER para continuar...${NC}"
    read
}

# Função para mostrar estatísticas
show_stats() {
    echo -e "${BLUE}📊 Estatísticas de uso dos containers:${NC}"
    echo ""
    docker stats --no-stream --format "table {{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}\t{{.NetIO}}" $APP_CONTAINER $NGINX_CONTAINER
    echo ""
    echo -e "${YELLOW}Pressione ENTER para continuar...${NC}"
    read
}

# Função para listar containers em execução
list_containers() {
    echo -e "${BLUE}📋 Todos os containers em execução:${NC}"
    echo ""
    docker ps -a --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}\t{{.Image}}"
    echo ""
    echo -e "${YELLOW}Pressione ENTER para continuar...${NC}"
    read
}

# Função para atualizar script Python
update_python_script() {
    echo -e "${BLUE}🔄 Atualizar script Python em tempo real${NC}"
    echo ""
    
    # Listar arquivos Python disponíveis
    echo -e "${YELLOW}Arquivos Python disponíveis localmente:${NC}"
    ls -la *.py 2>/dev/null || echo "Nenhum arquivo .py encontrado"
    echo ""
    
    echo -n -e "${WHITE}Nome do arquivo Python para atualizar (ex: app.py): ${NC}"
    read -r python_file
    
    if [ -f "$python_file" ]; then
        echo -e "${BLUE}Copiando $python_file para o container...${NC}"
        docker cp "$python_file" $APP_CONTAINER:/app/
        
        if [ $? -eq 0 ]; then
            echo -e "${GREEN}✅ Arquivo copiado com sucesso!${NC}"
            echo ""
            echo -e "${YELLOW}Deseja reiniciar o container? (s/N): ${NC}"
            read -r restart_opt
            if [[ $restart_opt == [sS] ]]; then
                docker restart $APP_CONTAINER
                echo -e "${GREEN}✅ Container reiniciado${NC}"
            else
                echo -e "${YELLOW}⚠️ Lembre-se de reiniciar o container para aplicar as mudanças${NC}"
                echo -e "${YELLOW}   ou use: docker restart $APP_CONTAINER${NC}"
            fi
        else
            echo -e "${RED}❌ Erro ao copiar arquivo${NC}"
        fi
    else
        echo -e "${RED}❌ Arquivo $python_file não encontrado${NC}"
    fi
    
    echo ""
    echo -e "${YELLOW}Pressione ENTER para continuar...${NC}"
    read
}

# Função para backup do banco
backup_database() {
    echo -e "${BLUE}💾 Backup do banco de dados${NC}"
    echo ""
    
    BACKUP_DIR="./backups"
    mkdir -p $BACKUP_DIR
    
    BACKUP_FILE="$BACKUP_DIR/dados_$(date +%Y%m%d_%H%M%S).db"
    
    if [ -f "./dados.db" ]; then
        cp ./dados.db "$BACKUP_FILE"
        echo -e "${GREEN}✅ Backup criado: $BACKUP_FILE${NC}"
        
        # Mostrar tamanho
        SIZE=$(du -h "$BACKUP_FILE" | cut -f1)
        echo -e "${BLUE}📦 Tamanho: $SIZE${NC}"
        
        # Listar últimos 5 backups
        echo ""
        echo -e "${YELLOW}Últimos 5 backups:${NC}"
        ls -lh $BACKUP_DIR/dados_*.db 2>/dev/null | tail -5 || echo "Nenhum backup anterior"
    else
        echo -e "${RED}❌ Arquivo dados.db não encontrado${NC}"
    fi
    
    echo ""
    echo -e "${YELLOW}Pressione ENTER para continuar...${NC}"
    read
}

# Loop principal
main() {
    check_docker
    
    while true; do
        show_header
        show_status
        show_menu
        read -r choice
        
        case $choice in
            1)
                echo -e "${BLUE}🚀 Iniciando containers...${NC}"
                docker-compose -f $COMPOSE_FILE up -d
                echo -e "${GREEN}✅ Containers iniciados!${NC}"
                sleep 2
                ;;
            2)
                echo -e "${BLUE}🛑 Parando containers...${NC}"
                docker-compose -f $COMPOSE_FILE down
                echo -e "${GREEN}✅ Containers parados!${NC}"
                sleep 2
                ;;
            3)
                echo -e "${BLUE}🔄 Reiniciando containers...${NC}"
                docker-compose -f $COMPOSE_FILE restart
                echo -e "${GREEN}✅ Containers reiniciados!${NC}"
                sleep 2
                ;;
            4)
                access_app_shell
                ;;
            5)
                access_nginx_shell
                ;;
            6)
                view_app_logs
                ;;
            7)
                view_nginx_logs
                ;;
            8)
                view_container_files
                ;;
            9)
                view_project_structure
                ;;
            10)
                list_volumes
                ;;
            11)
                clean_docker
                ;;
            12)
                rebuild_containers
                ;;
            13)
                check_health
                ;;
            14)
                show_stats
                ;;
            15)
                list_containers
                ;;
            16)
                update_python_script
                ;;
            17)
                backup_database
                ;;
            18)
                echo -e "${GREEN}👋 Saindo... Até logo!${NC}"
                exit 0
                ;;
            *)
                echo -e "${RED}❌ Opção inválida! Pressione ENTER para continuar...${NC}"
                read
                ;;
        esac
    done
}

# Executar o script
main
