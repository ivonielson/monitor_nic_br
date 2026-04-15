// Funções de manipulação das tabelas
function badgeAdequada(val) {
  if (!val) return '<span class="badge badge-sem">SEM DADOS</span>';
  if (val === 'SIM') return '<span class="badge badge-sim">✓ SIM</span>';
  if (val === 'NÃO' || val === 'NAO') return '<span class="badge badge-nao">✗ NÃO</span>';
  return '<span class="badge badge-sem">' + val + '</span>';
}

function filterTable(tableId, query) {
  const rows = document.querySelectorAll('#' + tableId + ' tbody tr');
  const q = query.toLowerCase();
  let visible = 0;
  rows.forEach(row => {
    const match = row.textContent.toLowerCase().includes(q);
    row.style.display = match ? '' : 'none';
    if (match) visible++;
  });
}

function scrollToTable(tableId) {
  const table = document.getElementById(tableId);
  if (table) {
    table.scrollIntoView({ behavior: 'smooth', block: 'start' });
    table.parentElement.parentElement.style.boxShadow = '0 0 0 2px var(--accent)';
    setTimeout(() => {
      table.parentElement.parentElement.style.boxShadow = '';
    }, 1500);
  }
}

function filterTableByAdequada(status) {
  const searchInput = document.getElementById('searchTodas');
  if (searchInput) {
    const filterValue = status === 'SIM' ? 'SIM' : 'NÃO';
    searchInput.value = filterValue;
    filterTable('tableTodas', filterValue);
    
    document.getElementById('tableTodas').scrollIntoView({ behavior: 'smooth', block: 'start' });
    
    const header = document.querySelector('#tableTodas');
    if (header) {
      header.parentElement.parentElement.style.boxShadow = '0 0 0 2px var(--green)';
      setTimeout(() => {
        header.parentElement.parentElement.style.boxShadow = '';
      }, 1500);
    }
  }
}



// Função para calcular dias em atraso
function calcularDiasAtraso(ultimaMedicao) {
  if (!ultimaMedicao || ultimaMedicao.trim() === '') {
    return null;
  }
  
  try {
    // Extrair a data no formato "dd/mm/yy"
    const parteData = ultimaMedicao.split(' - ')[0].trim();
    const partes = parteData.split('/');
    
    if (partes.length === 3) {
      const dia = parseInt(partes[0]);
      const mes = parseInt(partes[1]) - 1; // Mês em JS é 0-11
      let ano = parseInt(partes[2]);
      
      // Converter ano de 2 dígitos para 4 dígitos
      if (ano < 100) {
        ano = 2000 + ano;
      }
      
      const dataMedicao = new Date(ano, mes, dia);
      const dataAtual = new Date();
      
      // Calcular diferença em dias
      const diffTime = dataAtual - dataMedicao;
      const diffDays = Math.floor(diffTime / (1000 * 60 * 60 * 24));
      
      return diffDays;
    }
  } catch (e) {
    console.error('Erro ao calcular dias:', e);
  }
  
  return null;
}

// Função para formatar o badge de dias em atraso
function badgeDiasAtraso(dias) {
  if (dias === null || dias === undefined) {
    return '<span class="badge badge-sem">SEM MEDIÇÃO</span>';
  }
  
  if (dias <= 7) {
    return `<span class="badge badge-sim" style="background: rgba(16,185,129,0.15); color: #10b981;">${dias} dias</span>`;
  } else if (dias <= 15) {
    return `<span class="badge badge-warn" style="background: rgba(245,158,11,0.15); color: #f59e0b;">${dias} dias</span>`;
  } else if (dias <= 30) {
    return `<span class="badge badge-nao" style="background: rgba(239,68,68,0.15); color: #ef4444;">${dias} dias</span>`;
  } else {
    return `<span class="badge badge-erro" style="background: rgba(239,68,68,0.25); color: #ff6b6b; font-weight: bold;">${dias} dias</span>`;
  }
}


function renderTables(data) {
  // Tabela de escolas atrasadas
  const tbodyAt = document.getElementById('tbodyAtrasadas');
  tbodyAt.innerHTML = data.escolas_atrasadas.map(e => {
    const diasAtraso = calcularDiasAtraso(e.Ultima_Medicao_DataHora);
    
    return `
    <tr>
      <td style="font-family:var(--mono);color:var(--text-muted)">${e.INEP || '—'}</td>
      <td title="${e.Nome_Escola || ''}">${e.Nome_Escola || '—'}</td>
      <td>${badgeAdequada(e.Adequada)}</td>
      <td style="font-family:var(--mono);color:var(--yellow)">${e.Ultima_Medicao_DataHora || '<span style="color:var(--red)">Sem medição</span>'}</td>
      <td style="font-family:var(--mono);text-align:center">${badgeDiasAtraso(diasAtraso)}</td>
     `
  }).join('') || '<tr><td colspan="5" style="text-align:center;padding:32px;color:var(--text-muted)">Nenhuma escola com monitoramento atrasado</td></tr>';
  
  document.getElementById('countAtrasadas').textContent = data.escolas_atrasadas.length + ' escola(s)';

  // Tabela de todas as escolas
  const tbodyTd = document.getElementById('tbodyTodas');
  tbodyTd.innerHTML = data.todas_escolas.map(e => `
    <tr>
      <td style="font-family:var(--mono);color:var(--text-muted)">${e.INEP || '—'}</td>
      <td title="${e.Nome_Escola || ''}">${e.Nome_Escola || '—'}</td>
      <td style="font-family:var(--mono);text-align:right">${e.Total_Estudantes || '—'}</td>
      <td style="font-family:var(--mono);text-align:right">${e.Velocidade_Adequada ? parseFloat(e.Velocidade_Adequada).toFixed(1) : '—'}</td>
      <td style="font-family:var(--mono);text-align:right">${e.Vel_Max_Mbps ? parseFloat(e.Vel_Max_Mbps).toFixed(1) : '—'}</td>
      <td style="font-family:var(--mono);text-align:right">${e.Numero_Medicoes || '—'}</td>
      <td style="font-family:var(--mono);font-size:11px;color:var(--text-muted)">${e.Ultima_Medicao_DataHora || '—'}</td>
      <td>${badgeAdequada(e.Adequada)}</td>
      <td style="font-family:var(--mono);font-size:11px;color:var(--text-muted)">${e.Data_Coleta || '—'}</td>
     `
  ).join('') || '<tr><td colspan="9" style="text-align:center;padding:32px;color:var(--text-muted)">Nenhuma escola cadastrada</td>*';
  
  document.getElementById('countTodas').textContent = data.todas_escolas.length + ' escola(s)';
}
