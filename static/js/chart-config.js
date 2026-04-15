// Configuração e gerenciamento dos gráficos
let charts = {};

function destroyChart(id) {
  if (charts[id]) {
    charts[id].destroy();
    delete charts[id];
  }
}

function renderCharts(graficos) {
  const palette = {
    green: 'rgba(16,185,129,0.85)',
    red: 'rgba(239,68,68,0.85)',
    muted: 'rgba(107,122,153,0.85)',
    blue: 'rgba(59,130,246,0.85)'
  };

  const defaults = {
    color: '#55575a',
    borderColor: 'rgba(255,255,255,0.07)',
    backgroundColor: 'rgba(255,255,255,0.03)'
  };

  Chart.defaults.color = defaults.color;
  Chart.defaults.borderColor = defaults.borderColor;

  // Gráfico de Rosca - Distribuição por Adequação
    // Gráfico de Rosca - Distribuição por Adequação (com percentuais)
  destroyChart('chartAdequacao');
  
  // Calcular total para percentuais
  const dadosAdequacao = graficos.dist_adequacao;
  const total = dadosAdequacao.reduce((sum, d) => sum + d.total, 0);
  
  // Mapear dados com percentuais
  const adqLabels = dadosAdequacao.map(d => {
    const label = d.Adequada || 'SEM DADOS';
    const valor = d.total;
    const percentual = total > 0 ? ((valor / total) * 100).toFixed(1) : 0;
    
    // Formatar label com percentual
    if (label === 'SIM') {
      return `✅ Dentro do Padrão (${percentual}%)`;
    } else if (label === 'NÃO') {
      return `⚠️ Fora do Padrão (${percentual}%)`;
    } else {
      return `❓ Sem Dados (${percentual}%)`;
    }
  });
  
  const adqColors = dadosAdequacao.map(d => {
    const label = d.Adequada || 'SEM DADOS';
    return label === 'SIM' ? palette.green : label === 'NÃO' ? palette.red : palette.muted;
  });
  
  charts['chartAdequacao'] = new Chart(document.getElementById('chartAdequacao'), {
    type: 'doughnut',
    data: {
      labels: adqLabels,
      datasets: [{ 
        data: dadosAdequacao.map(d => d.total), 
        backgroundColor: adqColors, 
        borderWidth: 2,
        borderColor: '#1e293b',
        hoverOffset: 10
      }]
    },
    options: {
      responsive: true, 
      maintainAspectRatio: false,
      plugins: {
        legend: { 
          position: 'bottom', 
          labels: { 
            padding: 16, 
            font: { family: 'Space Mono', size: 11, weight: 'bold' },
            usePointStyle: true,
            pointStyle: 'circle'
          } 
        },
        tooltip: {
          callbacks: {
            label: function(context) {
              const label = context.label || '';
              const value = context.raw || 0;
              const percentual = total > 0 ? ((value / total) * 100).toFixed(1) : 0;
              return `${label}: ${value} escola(s) (${percentual}%)`;
            }
          }
        }
      },
      cutout: '65%'
    }
  });

  // Gráfico de Barras - Top 10 Velocidade
  destroyChart('chartVelocidade');
  const velNomes = graficos.top_velocidade.map(d => {
    const n = d.Nome_Escola || 'Escola';
    return n.length > 30 ? n.slice(0, 30) + '…' : n;
  });
  
  charts['chartVelocidade'] = new Chart(document.getElementById('chartVelocidade'), {
    type: 'bar',
    data: {
      labels: velNomes,
      datasets: [{
        label: 'Download (Mbps)',
        data: graficos.top_velocidade.map(d => d.Download_Mbps),
        backgroundColor: graficos.top_velocidade.map((_, i) => `hsla(${200 + i * 6}, 75%, 55%, 0.9)`),
        borderColor: graficos.top_velocidade.map((_, i) => `hsl(${200 + i * 6}, 75%, 40%)`),
        borderWidth: 1,
        borderRadius: 6,
        borderSkipped: false
      }]
    },
    options: {
      responsive: true, 
      maintainAspectRatio: false,
      indexAxis: 'y',
      plugins: { legend: { display: false } },
      scales: {
        x: { 
          grid: { color: 'rgba(255,255,255,0.05)' }, 
          ticks: { font: { family: 'Space Mono', size: 10 } } 
        },
        y: { 
          grid: { display: false }, 
          ticks: { font: { size: 11 } } 
        }
      }
    }
  });
}
