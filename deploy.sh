#!/bin/bash

echo "🚀 Implantando Monitor de Conectividade Escolar"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

# Carregar variáveis do .env
if [ -f ".env" ]; then
    export $(grep -v '^#' .env | grep -E '^DASHBOARD_PORT=' | xargs)
    export $(grep -v '^#' .env | grep -E '^DASHBOARD_HOST=' | xargs)
    export $(grep -v '^#' .env | grep -E '^NGINX_PORT=' | xargs)
fi

# Definir portas padrão se não estiverem no .env
DASHBOARD_PORT=${DASHBOARD_PORT:-5000}
DASHBOARD_HOST=${DASHBOARD_HOST:-localhost}
NGINX_PORT=${NGINX_PORT:-80}  # Porta padrão do Nginx

echo "📋 Configurações carregadas:"
echo "   Porta da App interna: $DASHBOARD_PORT"
echo "   Porta do Nginx (acesso externo): $NGINX_PORT"
echo "   Host: $DASHBOARD_HOST"
echo ""

# Verificar arquivos necessários
if [ ! -f "ineps.txt" ]; then
    echo "❌ Arquivo ineps.txt não encontrado!"
    echo "   Crie o arquivo com um INEP por linha"
    exit 1
fi
echo "✅ ineps.txt encontrado"

if [ ! -f ".env" ]; then
    echo "❌ Arquivo .env não encontrado!"
    exit 1
fi
echo "✅ .env encontrado"

# Verificar se nginx.conf existe
if [ ! -f "nginx.conf" ]; then
    echo "❌ Arquivo nginx.conf não encontrado!"
    echo "   Crie o arquivo de configuração do Nginx"
    exit 1
fi
echo "✅ nginx.conf encontrado"

# Criar diretórios
mkdir -p logs data
echo "✅ Diretórios criados"

# Parar containers antigos se existirem
echo ""
echo "🛑 Parando containers antigos..."
docker compose down 2>/dev/null

# Verificar se a porta do Nginx está disponível
echo ""
echo "🔍 Verificando disponibilidade da porta $NGINX_PORT..."
if sudo lsof -i :$NGINX_PORT > /dev/null 2>&1; then
    echo "⚠️  Atenção: A porta $NGINX_PORT já está em uso!"
    echo "   Processos usando a porta:"
    sudo lsof -i :$NGINX_PORT
    echo ""
    read -p "Deseja continuar mesmo assim? (s/N): " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Ss]$ ]]; then
        echo "❌ Implantação cancelada"
        exit 1
    fi
fi
echo "✅ Porta $NGINX_PORT disponível"

# Construir e iniciar
echo "📦 Construindo imagem..."
docker compose build --no-cache

echo ""
echo "🎯 Iniciando containers (App + Nginx)..."
docker compose up -d

# Aguardar e verificar status
echo "⏳ Aguardando inicialização..."
sleep 8

# Verificar se os containers estão rodando
if docker ps | grep -q smec-monitor && docker ps | grep -q smec-nginx; then
    echo ""
    echo "✅ Containers rodando!"
    echo "   - App: smec-monitor"
    echo "   - Nginx: smec-nginx"
else
    echo ""
    echo "❌ Containers não iniciaram corretamente"
    echo "   Verifique os logs: docker compose logs --tail=50"
    exit 1
fi

# Testar conexão via Nginx
echo ""
echo "🔍 Testando conexão via Nginx na porta $NGINX_PORT..."

# URL de teste (agora via Nginx)
TEST_URL="http://127.0.0.1:${NGINX_PORT}/api/data"

if curl -s --max-time 10 "$TEST_URL" > /dev/null 2>&1; then
    echo ""
    echo "✅✅✅ SISTEMA IMPLANTADO COM SUCESSO! ✅✅✅"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "📊 Dashboard: http://127.0.0.1:${NGINX_PORT}"
    if [ "$NGINX_PORT" != "80" ]; then
        echo "   (acessando via Nginx na porta $NGINX_PORT)"
    fi
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo ""
    echo "📝 Comandos úteis:"
    echo "   Ver logs:           docker compose logs -f"
    echo "   Ver logs do Nginx:  docker compose logs -f nginx"
    echo "   Ver logs da App:    docker compose logs -f app"
    echo "   Parar sistema:      docker compose down"
    echo "   Reiniciar:          docker compose restart"
    echo "   Status:             docker compose ps"
    echo "   Testar Nginx:       curl http://localhost:${NGINX_PORT}"
    echo ""
    
    # Mostrar estatísticas iniciais
    echo "📈 Estatísticas iniciais:"
    STATS=$(curl -s "$TEST_URL" 2>/dev/null)
    if [ $? -eq 0 ] && [ ! -z "$STATS" ]; then
        TOTAL=$(echo $STATS | python3 -c "import sys, json; print(json.load(sys.stdin).get('cards', {}).get('total_escolas', 'N/A'))" 2>/dev/null)
        DENTRO=$(echo $STATS | python3 -c "import sys, json; print(json.load(sys.stdin).get('cards', {}).get('dentro_padrao', 'N/A'))" 2>/dev/null)
        FORA=$(echo $STATS | python3 -c "import sys, json; print(json.load(sys.stdin).get('cards', {}).get('fora_padrao', 'N/A'))" 2>/dev/null)
        
        echo "   Total escolas: $TOTAL"
        echo "   Dentro padrão: $DENTRO"
        echo "   Fora padrão: $FORA"
    else
        echo "   Aguardando primeira coleta de dados..."
    fi
    
    # Mostrar informações dos containers
    echo ""
    echo "📊 Status dos containers:"
    docker compose ps
    
else
    echo ""
    echo "⚠️  Sistema iniciando, mas ainda não está respondendo via Nginx"
    echo ""
    echo "🔍 Diagnóstico:"
    echo "   1. Verifique os logs: docker compose logs --tail=30"
    echo "   2. Verifique se o Nginx está rodando: docker compose ps"
    echo "   3. Teste diretamente a app: docker exec smec-nginx wget -O- http://app:$NGINX_PORT/api/data"
    echo "   4. Verifique a porta $NGINX_PORT: sudo lsof -i :$NGINX_PORT"
    echo ""
    echo "📋 Últimos logs:"
    docker compose logs --tail=30
    
    # Mostrar logs específicos do Nginx se houver erro
    if docker compose logs nginx 2>&1 | grep -q "error"; then
        echo ""
        echo "🐛 Logs de erro do Nginx:"
        docker compose logs nginx | grep -i error | tail -5
    fi
fi

# Opção de acesso direto (debug)
echo ""
echo "💡 Dica: Para acesso direto à aplicação (debug), use:"
echo "   docker exec -it smec-monitor curl http://localhost:$NGINX_PORT/api/data"
