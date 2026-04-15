// Dados globais
let currentData = null;

// Renderizar informações do boot/agendamento
function renderBoot(boot) {
  document.getElementById('bootUltima').textContent = boot.ultima_coleta || '—';
  document.getElementById('bootProximo').textContent = boot.proximo_agendamento || '—';
  document.getElementById('bootFreq').textContent = (boot.frequencia || '—').toUpperCase();
  document.getElementById('bootHorario').textContent = boot.horario || '—';
  document.getElementById('bootCheckpoint').textContent = boot.total_checkpoint || '0';
  
  const statusEl = document.getElementById('bootStatus');
  if (boot.agendamento_ativo) {
    statusEl.innerHTML = '<span style="color:var(--green)">● AGENDADO ATIVO</span>';
  } else {
    statusEl.innerHTML = '<span style="color:var(--text-muted)">● MANUAL</span>';
  }
}

// Renderizar cards de estatísticas
function renderCards(cards) {
  const total = cards.total_escolas || 1;
  document.getElementById('cardTotal').textContent = cards.total_escolas || 0;
  document.getElementById('cardOk').textContent = cards.dentro_padrao || 0;
  document.getElementById('cardNok').textContent = cards.fora_padrao || 0;
  document.getElementById('cardLate').textContent = cards.monitoramento_atrasado || 0;
  
  const okPct = ((cards.dentro_padrao / total) * 100).toFixed(1);
  const nokPct = ((cards.fora_padrao / total) * 100).toFixed(1);
  const latePct = ((cards.monitoramento_atrasado / total) * 100).toFixed(1);
  
  document.getElementById('cardOkPct').textContent = okPct + '%';
  document.getElementById('cardNokPct').textContent = nokPct + '%';
  document.getElementById('cardLatePct').textContent = latePct + '%';
  
  document.getElementById('progressOk').style.width = okPct + '%';
  document.getElementById('progressNok').style.width = nokPct + '%';
  document.getElementById('progressLate').style.width = latePct + '%';
}

// Carregar dados da API
async function loadData(first = false) {
  try {
    const res = await fetch('/api/data');
    if (!res.ok) throw new Error('API error');
    const data = await res.json();
    currentData = data;

    renderCards(data.cards);
    renderBoot(data.boot);
    renderCharts(data.graficos);
    renderTables(data);

    const updateText = '📅 Atualizado: ' + data.atualizado_em;
    document.getElementById('lastUpdate').textContent = updateText;
    const printUpdateElem = document.getElementById('lastUpdatePrint');
    if (printUpdateElem) printUpdateElem.textContent = updateText;

    if (first) {
      document.getElementById('loadingOverlay').style.opacity = '0';
      setTimeout(() => document.getElementById('loadingOverlay').classList.add('hidden'), 400);
      const mc = document.getElementById('mainContent');
      mc.style.transition = 'opacity 0.4s';
      mc.style.opacity = '1';
    }
  } catch (e) {
    if (first) {
      document.getElementById('loadingOverlay').innerHTML = '<div style="text-align:center"><h2 style="color:var(--red);margin-bottom:8px">Falha ao Conectar</h2><p style="color:var(--text-muted)">Verifique a conexão com o banco de dados MySQL</p></div>';
    }
    console.error('Erro ao carregar dados:', e);
  }
}

// Inicialização
loadData(true);
setInterval(() => loadData(false), 5 * 60 * 1000); //5 minutos
