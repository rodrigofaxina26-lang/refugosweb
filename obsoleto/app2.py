
from flask import Flask, request
import pandas as pd
import json
import shutil
import os
from datetime import datetime, date
import traceback

app = Flask(__name__)

# --- CONFIGURAÇÕES ---
ARQUIVO_REDE = r"P:\\QUALIDADE\\USUARIOS\\00. Dept. Qualidade\\07. Controle de Refugo\\2026\\REFUGO 2026 V5.xlsb"
ARQUIVO_LOCAL = "temp_processamento_refugo.xlsb"

_cache = {"df_sci": pd.DataFrame(), "df_spr": pd.DataFrame(), "last_modified": 0.0}

def file_has_changed(path):
    try:
        stat = os.stat(path)
        return stat.st_mtime > _cache["last_modified"]
    except:
        return True

def carregar_dados(force_reload=False):
    if force_reload:
        print("⚡ Forçando recarga dos dados (nova data solicitada)...")
        _cache["last_modified"] = 0.0
    
    if not file_has_changed(ARQUIVO_REDE) and not force_reload:
        print("🔁 Reutilizando cache (arquivo não alterado).")
        return _cache["df_sci"].copy(), _cache["df_spr"].copy()

    try:
        if not os.path.exists(ARQUIVO_REDE):
            print(f"❌ Arquivo não existe: {ARQUIVO_REDE}")
            return pd.DataFrame(), pd.DataFrame()

        print("✅ Carregando Excel...")
        shutil.copy2(ARQUIVO_REDE, ARQUIVO_LOCAL)

        with pd.ExcelFile(ARQUIVO_LOCAL, engine="pyxlsb") as xls:
            print(f"📄 Abas encontradas: {xls.sheet_names}")
            df_sci = pd.read_excel(xls, "SCI - QTD") if "SCI - QTD" in xls.sheet_names else pd.DataFrame()
            df_spr = pd.read_excel(xls, "SPR - QTD") if "SPR - QTD" in xls.sheet_names else pd.DataFrame()

        if os.path.exists(ARQUIVO_LOCAL):
            os.remove(ARQUIVO_LOCAL)

        dfs_processados = []
        for i, df in enumerate([df_sci, df_spr]):
            fonte = "SCI" if i == 0 else "SPR"
            if df.empty:
                print(f"⚠️ Planilha {fonte} vazia.")
                dfs_processados.append(pd.DataFrame())
                continue

            # Normalização rigorosa dos cabeçalhos (remove espaços, maiúsculas)
            df.columns = [str(c).strip().upper().replace(' ', '') for c in df.columns]
            print(f"🔍 Colunas lidas na {fonte}: {df.columns.tolist()}")

            # Tenta variações do nome da coluna DATA
            col_data_candidate = None
            for cand in ["DATA", "DAT", "DATAPROD", "DATAREFUGO", "DATA_REFUGO"]:
                if cand in df.columns:
                    col_data_candidate = cand
                    break
            
            if not col_data_candidate:
                print(f"❌ Coluna 'DATA' não encontrada na {fonte}. Colunas disponíveis: {df.columns.tolist()}")
                dfs_processados.append(pd.DataFrame())
                continue

            # Conversão numérica e de data
            df[col_data_candidate] = pd.to_numeric(df[col_data_candidate], errors="coerce")
            df = df.dropna(subset=[col_data_candidate])
            
            # Conversão para datetime (origin 1899-12-30 é padrão Excel)
            df[col_data_candidate] = pd.to_datetime(df[col_data_candidate], unit="D", origin="1899-12-30")
            
            # Remove datas inválidas (ex: números negativos do Excel)
            df = df[df[col_data_candidate].notna()].copy()
            df.rename(columns={col_data_candidate: "DATA"}, inplace=True)

            # Filtro de ano: considera >= 2025 para garantir dados de 2026
            ano_minimo = 2025 
            df = df[df["DATA"].dt.year >= ano_minimo].copy()
            print(f"✅ {fonte}: {len(df)} registros após filtro de ano (>= {ano_minimo}).")

            # Processamento de Quantidade
            col_qtde = next((c for c in df.columns if "QTDE" in c or "QTD" in c), None)
            if col_qtde:
                df["QTDE"] = pd.to_numeric(df[col_qtde], errors="coerce").fillna(0)
            else:
                df["QTDE"] = 0
                print(f"⚠️ Coluna QTDE não encontrada em {fonte}, usando 0.")

            # Processamento de Custo
            col_custo = next((c for c in df.columns if "CUSTO" in c or "VALOR" in c), None)
            if col_custo:
                custo_unit = pd.to_numeric(df[col_custo], errors="coerce").fillna(0)
                df["VALOR_CUSTO"] = (custo_unit * df["QTDE"]).fillna(0)
            else:
                df["VALOR_CUSTO"] = 0

            df["DATA_STR"] = df["DATA"].dt.strftime("%Y-%m-%d")
            dfs_processados.append(df)

        _cache["df_sci"] = dfs_processados[0].copy()
        _cache["df_spr"] = dfs_processados[1].copy()
        _cache["last_modified"] = os.stat(ARQUIVO_REDE).st_mtime
        print("💾 Cache atualizado com sucesso.")
        return _cache["df_sci"].copy(), _cache["df_spr"].copy()

    except Exception as e:
        print(f"💥 Erro crítico no carregamento: {e}")
        traceback.print_exc()
        return pd.DataFrame(), pd.DataFrame()

def get_modo_falha(row):
    if "MODO DE FALHA" in row and pd.notna(row["MODO DE FALHA"]) and str(row["MODO DE FALHA"]).strip():
        return str(row["MODO DE FALHA"]).strip()
    
    row_clean = {str(k).strip().upper().replace(' ', ''): v for k, v in row.items()}
    colunas_modo = ["MODODEFALHA", "MODO_FALHA", "MODO", "FALHA", "CAUSA", "MOTIVO", "DEFETO"]
    
    for col in colunas_modo:
        if col in row_clean and pd.notna(row_clean[col]) and str(row_clean[col]).strip():
            return str(row_clean[col]).strip()
    
    return "NÃO IDENTIFICADO"

def get_top3_diario(df_filtrado):
    if df_filtrado.empty or "QTDE" not in df_filtrado.columns:
        return {"top_qtde": [], "top_custo": []}
    
    if "PRODUTO" not in df_filtrado.columns:
        df_filtrado = df_filtrado.copy()
        df_filtrado["PRODUTO"] = "SEM PRODUTO"
    
    cols_base = ["PRODUTO", "QTDE", "VALOR_CUSTO"]
    top_qtde = df_filtrado.nlargest(3, "QTDE")[cols_base].copy()
    
    if "VALOR_CUSTO" in df_filtrado.columns:
        top_custo = df_filtrado.nlargest(3, "VALOR_CUSTO")[cols_base].copy()
    else:
        top_custo = pd.DataFrame()
    
    top_qtde_serial = []
    for idx in top_qtde.index:
        row_completa = df_filtrado.loc[idx]
        modo = get_modo_falha(row_completa)
        top_qtde_serial.append({
            "produto": str(row_completa.get("PRODUTO", "SEM PRODUTO")),
            "qtde": float(row_completa.get("QTDE", 0)),
            "custo": float(row_completa.get("VALOR_CUSTO", 0)),
            "modo_falha": modo
        })
    
    top_custo_serial = []
    for idx in top_custo.index:
        row_completa = df_filtrado.loc[idx]
        modo = get_modo_falha(row_completa)
        top_custo_serial.append({
            "produto": str(row_completa.get("PRODUTO", "SEM PRODUTO")),
            "qtde": float(row_completa.get("QTDE", 0)),
            "custo": float(row_completa.get("VALOR_CUSTO", 0)),
            "modo_falha": modo
        })
    
    return {"top_qtde": top_qtde_serial, "top_custo": top_custo_serial}

def filtrar_data_diaria(df, data_especifica):
    if df.empty or "DATA" not in df.columns:
        return pd.DataFrame()
    data_target = pd.to_datetime(data_especifica).date()
    return df[df["DATA"].dt.date == data_target].copy()

def criar_pareto_modo_falha(df, tipo="GERAL"):
    if df.empty:
        return {"modo": [], "qtde": [], "cumperc": [], "total": 0}
    
    df_temp = df.copy()
    df_temp["MODO_FALHA"] = df_temp.apply(get_modo_falha, axis=1)
    pareto = df_temp.groupby("MODO_FALHA")["QTDE"].sum().reset_index()
    pareto = pareto[pareto["QTDE"] > 0].sort_values("QTDE", ascending=False).reset_index(drop=True)
    pareto["CUMPERC"] = (pareto["QTDE"].cumsum() / pareto["QTDE"].sum() * 100).round(1)
    
    return {
        "modo": pareto["MODO_FALHA"].tolist()[:10],
        "qtde": pareto["QTDE"].tolist()[:10],
        "cumperc": pareto["CUMPERC"].tolist()[:10],
        "total": int(pareto["QTDE"].sum())
    }
def get_top3_problemas_por_produto(df, produto_nome):
    """
    Versão CORRIGIDA: Adicionado .str.upper() para evitar o AttributeError
    """
    if df.empty or not produto_nome: 
        return []
    
    # 1. Padronização: Agora com .str em todos os métodos de texto
    termo = str(produto_nome).strip().upper()
    df_temp = df.copy()
    
    # CORREÇÃO AQUI: adicionado .str antes de .upper()
    df_temp["PRODUTO_CHECK"] = df_temp["PRODUTO"].astype(str).str.strip().str.upper()

    # 2. FILTRO DE PRECISÃO:
    # Tenta busca exata primeiro
    df_p = df_temp[df_temp["PRODUTO_CHECK"] == termo].copy()
    
    # Se não achar nada exato, tenta a busca parcial
    if df_p.empty:
        df_p = df_temp[df_temp["PRODUTO_CHECK"].str.contains(termo, na=False)].copy()

    if df_p.empty: 
        return []

    # 3. Limpeza de dados
    df_p["QTDE"] = pd.to_numeric(df_p["QTDE"], errors='coerce').fillna(0)
    df_p = df_p[df_p["QTDE"] > 0]
    
    # 4. Identifica o Modo de Falha
    df_p["MODO_FALHA_FINAL"] = df_p.apply(get_modo_falha, axis=1)
    
    # 5. Agrupa e pega os 3 maiores
    res = df_p.groupby("MODO_FALHA_FINAL")["QTDE"].sum().nlargest(3).reset_index()
    res.columns = ["MODO_FALHA", "QTDE"]
    
    return res.to_dict("records")

@app.route("/produto_pareto")
def produto_pareto():
    p = request.args.get("produto", "").strip().upper()
    i, f = request.args.get("inicio"), request.args.get("fim")
    d1, d2 = carregar_dados()
    def filt(df):
        if df.empty: return df
        t = df.copy()
        if i: t = t[t["DATA"] >= pd.to_datetime(i)]
        if f: t = t[t["DATA"] <= pd.to_datetime(f)]
        return t
    return json.dumps({"sci": get_top3_problemas_por_produto(filt(d1), p), "spr": get_top3_problemas_por_produto(filt(d2), p)})


@app.route("/pareto")
def pareto():
    # Mesmos parâmetros que /filtrar
    data_inicio = request.args.get("inicio", (date.today() - pd.Timedelta(days=90)).strftime("%Y-%m-%d"))
    data_fim = request.args.get("fim", date.today().strftime("%Y-%m-%d"))
    produto = request.args.get("produto", "").strip().upper()
    
    force = pd.to_datetime(data_fim).date() >= date.today()
    df_sci_raw, df_spr_raw = carregar_dados(force_reload=force)
    
    # Aplica filtros IGUAIS ao /filtrar
    df_sci = df_sci_raw[(df_sci_raw["DATA"] >= pd.to_datetime(data_inicio)) &
                        (df_sci_raw["DATA"] <= pd.to_datetime(data_fim))].copy()
    
    df_spr = df_spr_raw[(df_spr_raw["DATA"] >= pd.to_datetime(data_inicio)) &
                        (df_spr_raw["DATA"] <= pd.to_datetime(data_fim))].copy()
    
    if produto:
        if "PRODUTO" in df_sci.columns:
            df_sci = df_sci[df_sci["PRODUTO"].astype(str).str.upper().str.contains(produto, na=False)]
        if "PRODUTO" in df_spr.columns:
            df_spr = df_spr[df_spr["PRODUTO"].astype(str).str.upper().str.contains(produto, na=False)]
    
    pareto_sci = criar_pareto_modo_falha(df_sci)
    pareto_spr = criar_pareto_modo_falha(df_spr)
    
    return json.dumps({
        "sci": pareto_sci, 
        "spr": pareto_spr,
        "filtros": f"{data_inicio} a {data_fim} | Produto: {produto or 'TODOS'}"
    })

@app.route("/detalhe")
def detalhe():
    data_especifica = request.args.get("data", date.today().strftime("%Y-%m-%d"))
    # CORRIGIDO: comparando date com date
    force = pd.to_datetime(data_especifica).date() >= date.today()
    df_sci_raw, df_spr_raw = carregar_dados(force_reload=force)
    df_completo = pd.concat([df_sci_raw, df_spr_raw], ignore_index=True)
    df_filtrado = filtrar_data_diaria(df_completo, data_especifica)
    top3 = get_top3_diario(df_filtrado)
    return json.dumps({
        "data": data_especifica,
        "total_registros": len(df_filtrado),
        "top3": top3
    })

@app.route("/filtrar")
def filtrar():
    data_inicio = request.args.get("inicio", (date.today() - pd.Timedelta(days=90)).strftime("%Y-%m-%d"))
    data_fim    = request.args.get("fim",    date.today().strftime("%Y-%m-%d"))
    produto     = request.args.get("produto", "").strip().upper()

    force = pd.to_datetime(data_fim).date() >= date.today()
    df_sci_raw, df_spr_raw = carregar_dados(force_reload=force)

    # 1. FILTRA DATA
    df_sci = df_sci_raw[(df_sci_raw["DATA"] >= pd.to_datetime(data_inicio)) &
                        (df_sci_raw["DATA"] <= pd.to_datetime(data_fim))].copy()
    df_spr = df_spr_raw[(df_spr_raw["DATA"] >= pd.to_datetime(data_inicio)) &
                        (df_spr_raw["DATA"] <= pd.to_datetime(data_fim))].copy()

    # 2. FILTRA PRODUTO (se informado)
    if produto:
        if "PRODUTO" in df_sci.columns:
            df_sci = df_sci[df_sci["PRODUTO"].astype(str).str.upper().str.contains(produto, na=False)]
        if "PRODUTO" in df_spr.columns:
            df_spr = df_spr[df_spr["PRODUTO"].astype(str).str.upper().str.contains(produto, na=False)]

    # 3. GROUPBY por DATA para RUN CHART
    if len(df_sci) == 0:
        res_sci = pd.DataFrame(columns=["DATA_STR", "QTDE", "VALOR_CUSTO"])
    else:
        res_sci = df_sci.groupby("DATA_STR")[["QTDE", "VALOR_CUSTO"]].sum().reset_index()

    if len(df_spr) == 0:
        res_spr = pd.DataFrame(columns=["DATA_STR", "QTDE", "VALOR_CUSTO"])
    else:
        res_spr = df_spr.groupby("DATA_STR")[["QTDE", "VALOR_CUSTO"]].sum().reset_index()

    # 4. MERGE RUN CHART
    run_df = pd.merge(res_sci, res_spr, on="DATA_STR", how="outer", suffixes=("_SCI", "_SPR")).fillna(0)
    run_df["TOTAL_Q"] = run_df["QTDE_SCI"] + run_df["QTDE_SPR"]
    run_df["TOTAL_C"] = run_df["VALOR_CUSTO_SCI"] + run_df["VALOR_CUSTO_SPR"]
    run_df = run_df.sort_values("DATA_STR")

    # 5. FUNÇÃO TOP10 CORRIGIDA (aqui dentro)
    def top10(df, col):
        if len(df) == 0 or "PRODUTO" not in df.columns:
            return [{"PRODUTO": "SEM DADOS", col: 0}]
        try:
            return df.groupby("PRODUTO")[col].sum().nlargest(10).reset_index().to_dict("records")
        except:
            return [{"PRODUTO": "ERRO", col: 0}]

    # 6. JSON FINAL
    dados_json = {
        "run": run_df.to_dict("records"),
        "sci_q": top10(df_sci, "QTDE"),
        "sci_c": top10(df_sci, "VALOR_CUSTO"),
        "spr_q": top10(df_spr, "QTDE"),
        "spr_c": top10(df_spr, "VALOR_CUSTO"),
        "periodo": f"{data_inicio} a {data_fim}",
        "total_dias": len(run_df)
    }
    return json.dumps(dados_json)

@app.route("/")
def index():
    return """
<!DOCTYPE html>
<html>
<head>
    <title>Dashboard Refugo 2026</title>
    <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
    <style>
        body { font-family: 'Segoe UI', sans-serif; margin: 30px; background: #f0f2f5; }
        .card { background: white; padding: 20px; border-radius: 12px; margin-bottom: 20px; box-shadow: 0 4px 6px rgba(0,0,0,0.05); }
        .filtros { background: #e8f4fd; padding: 15px; border-radius: 8px; margin-bottom: 20px; }
        .filtros input { padding: 8px; margin: 0 10px; border: 1px solid #ccc; border-radius: 4px; }
        .grid { display: grid; grid-template-columns: 1fr 1fr; gap: 20px; }
        button { background: #007bff; color: white; padding: 10px 20px; border: none; border-radius: 6px; cursor: pointer; }
        button:hover { background: #0056b3; }
        .top3-section, .pareto-section { background: #fff3cd; border: 2px solid #ffeaa7; margin: 20px 0; }
        .top3-grid, .pareto-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 15px; }
        .top3-card { background: #e8f5e8; padding: 15px; border-radius: 8px; border-left: 4px solid #28a745; }
        .top3-item { display: flex; justify-content: space-between; margin: 8px 0; padding: 12px; background: white; border-radius: 6px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
        .rank { font-size: 20px; font-weight: bold; color: #007bff; margin-right: 10px; }
        .modo-falha { color: #dc3545; font-weight: bold; font-size: 14px; }
    </style>
</head>
<body>
    <h1>📊 Gestão de Refugo 2026</h1>

<div class="card" style="border-left: 8px solid #6c5ce7; background: #f8f7ff;">
    <h3 style="color: #6c5ce7; margin-top:0;">🔍 Top 3 Defeitos por Produto</h3>
    <div style="display: flex; gap: 10px;">
        <input type="text" id="inpP" placeholder="Nome ou Código do Produto..." style="flex: 1; padding: 10px; border-radius: 5px; border: 1px solid #ccc;">
        <button onclick="verProd()" style="background: #6c5ce7;">ANALISAR</button>
    </div>
    <div id="resP" class="grid" style="margin-top: 15px; display:none;"></div>
</div>
    <div class="filtros top3-section">
        <strong>🏆 TOP 3 DO DIA:</strong><br>
        <input type="date" id="dataTop3" value="2026-03-26">
        <button onclick="mostrarTop3()">🔍 Ver TOP 3</button>
        <div id="statusTop3">Escolha data e clique</div>
    </div>
    <div class="filtros pareto-section">
        <strong>📈 PARETO MODOS FALHA:</strong>
        <button onclick="mostrarPareto()">📊 Pareto SCI/SPR</button>
        <div id="statusPareto">Clique para Pareto</div>
    </div>
    <div id="top3Result" style="display:none;">
        <div class="card top3-section">
            <h3 id="top3Title"></h3>
            <div class="top3-grid">
                <div class="top3-card">
                    <h4>📦 MAIORES QTDE</h4>
                    <div id="topQtde"></div>
                </div>
                <div class="top3-card">
                    <h4>💰 MAIORES CUSTOS</h4>
                    <div id="topCusto"></div>
                </div>
            </div>
        </div>
    </div>
    <div id="paretoResult" style="display:none;">
        <div class="card pareto-section">
            <h3>📊 PARETO - Modos de Falha 80/20</h3>
            <div class="pareto-grid">
                <div><div id="paretoSCI"></div></div>
                <div><div id="paretoSPR"></div></div>
            </div>
        </div>
    </div>
   <div class="filtros">
    <strong>📈 Evolução:</strong><br>
    De: <input type="date" id="dataInicio" value="2026-01-01">
    Até: <input type="date" id="dataFim" value="2026-03-26">
    Produto: <input type="text" id="produto" placeholder="Ex: Filtro ar, polia, etc." style="margin: 0 10px;">
    <button onclick="atualizarDashboard()">🔄 Atualizar</button>
    <div id="status">Carregando...</div>
</div>
    <div class="card"><div id="plotQ"></div></div>
    <div class="card"><div id="plotC"></div></div>
    <div class="grid">
        <div class="card"><div id="sq"></div></div>
        <div class="card"><div id="pq"></div></div>
        <div class="card"><div id="sc"></div></div>
        <div class="card"><div id="pc"></div></div>
    </div>
    <script>
        let dados = {}, dadosPareto = {};
        async function mostrarTop3() {
            const data = document.getElementById('dataTop3').value;
            document.getElementById('statusTop3').innerHTML = 'Carregando TOP 3...';
            document.getElementById('top3Result').style.display = 'none';
            try {
                const response = await fetch(`/detalhe?data=${data}`);
                const top3 = await response.json();
                document.getElementById('top3Title').innerHTML = `🏆 TOP 3 - ${top3.data} (${top3.total_registros} registros)`;
                let htmlQtde = '';
                top3.top3.top_qtde.forEach((item, i) => {
                    htmlQtde += `<div class="top3-item"><div class="rank">${i+1}º</div><div style="flex: 1;"><strong>${item.produto}</strong><br><span class="modo-falha">⚠️ ${item.modo_falha}</span><br><small>${item.qtde.toLocaleString()} peças | R$ ${item.custo.toLocaleString('pt-BR', {minimumFractionDigits: 2})}</small></div></div>`;
                });
                document.getElementById('topQtde').innerHTML = htmlQtde || 'Sem registros';
                let htmlCusto = '';
                top3.top3.top_custo.forEach((item, i) => {
                    htmlCusto += `<div class="top3-item"><div class="rank">${i+1}º</div><div style="flex: 1;"><strong>${item.produto}</strong><br><span class="modo-falha">⚠️ ${item.modo_falha}</span><br><small>${item.qtde.toLocaleString()} peças | R$ ${item.custo.toLocaleString('pt-BR', {minimumFractionDigits: 2})}</small></div></div>`;
                });
                document.getElementById('topCusto').innerHTML = htmlCusto || 'Sem registros';
                document.getElementById('top3Result').style.display = 'block';
                document.getElementById('statusTop3').innerHTML = '✅ TOP 3 carregado!';
            } catch(e) {
                document.getElementById('statusTop3').innerHTML = 'Erro: ' + e.message;
                console.error(e);
            }
        }
        async function mostrarPareto() {
    const inicio = document.getElementById('dataInicio').value;
    const fim = document.getElementById('dataFim').value;
    const produto = document.getElementById('produto').value.trim();
    
    const params = new URLSearchParams();
    if (inicio) params.append('inicio', inicio);
    if (fim) params.append('fim', fim);
    if (produto) params.append('produto', produto);
    
    document.getElementById('statusPareto').innerHTML = 'Carregando Pareto filtrado...';
    document.getElementById('paretoResult').style.display = 'none';
    
    try {
        const response = await fetch(`/pareto?${params}`);
        dadosPareto = await response.json();
        renderizarPareto();
        document.getElementById('statusPareto').innerHTML = `✅ Pareto: ${dadosPareto.filtros}`;
    } catch(e) {
        document.getElementById('statusPareto').innerHTML = 'Erro: ' + e.message;
    }
}
        function renderizarPareto() {
            const sci = dadosPareto.sci;
            Plotly.newPlot('paretoSCI', [
                {x: sci.modo, y: sci.qtde, type: 'bar', name: 'Qtde', marker: {color: '#d62728'}},
                {x: sci.modo, y: sci.cumperc, yaxis: 'y2', type: 'scatter', mode: 'lines+markers', name: '% Acum.', line: {color: '#ff7f0e'}}
            ], {
                title: `SCI (${sci.total.toLocaleString()} peças)`,
                yaxis: {title: 'Quantidade'},
                yaxis2: {title: '% Acumulado', overlaying: 'y', side: 'right', tickformat: '.0%'},
                height: 400
            });
            const spr = dadosPareto.spr;
            Plotly.newPlot('paretoSPR', [
                {x: spr.modo, y: spr.qtde, type: 'bar', name: 'Qtde', marker: {color: '#1f77b4'}},
                {x: spr.modo, y: spr.cumperc, yaxis: 'y2', type: 'scatter', mode: 'lines+markers', name: '% Acum.', line: {color: '#ff7f0e'}}
            ], {
                title: `SPR (${spr.total.toLocaleString()} peças)`,
                yaxis: {title: 'Quantidade'},
                yaxis2: {title: '% Acumulado', overlaying: 'y', side: 'right', tickformat: '.0%'},
                height: 400
            });
            document.getElementById('paretoResult').style.display = 'block';
            document.getElementById('statusPareto').innerHTML = '✅ Pareto carregado!';
        }
        async function carregarDados(inicio = null, fim = null, extraParams = null) {
    const params = extraParams || new URLSearchParams();
    if (inicio) params.append('inicio', inicio);
    if (fim)   params.append('fim', fim);
    const response = await fetch(`/filtrar?${params}`);
    dados = await response.json();
    document.getElementById('status').innerHTML = `Período: ${dados.periodo} | Registros: ${dados.run.length}`;
    renderizarDashboard();
}        function renderizarDashboard() {
            if (!dados.run || dados.run.length === 0) {
                document.getElementById('plotQ').innerHTML = '<p style="text-align:center;color:#666;">Nenhum dado encontrado para o período.</p>';
                document.getElementById('plotC').innerHTML = '';
                return;
            }
            Plotly.newPlot('plotQ', [{x: dados.run.map(i => i.DATA_STR), y: dados.run.map(i => i.TOTAL_Q), type: 'scatter', mode: 'lines+markers', line: {shape: 'spline', color: '#1f77b4'}}], {title: 'Evolução (Quantidade)'});
            Plotly.newPlot('plotC', [{x: dados.run.map(i => i.DATA_STR), y: dados.run.map(i => i.TOTAL_C), type: 'scatter', mode: 'lines+markers', line: {shape: 'spline', color: '#2ca02c'}}], {title: 'Evolução (Custo R$)'});
            function bar(id, data, x, y, title, color) {
                if (!data.length) {
                    document.getElementById(id).innerHTML = '<p style="text-align:center;color:#666;">Sem dados</p>';
                    return;
                }
                Plotly.newPlot(id, [{x: data.map(i => i[x]), y: data.map(i => i[y]), type: 'bar', marker: {color}}], {title, margin: {t: 50, b: 100}});
            }
            bar('sq', dados.sci_q, 'PRODUTO', 'QTDE', 'SCI Qtd', '#d62728');
            bar('pq', dados.spr_q, 'PRODUTO', 'QTDE', 'SPR Qtd', '#1f77b4');
            bar('sc', dados.sci_c, 'PRODUTO', 'VALOR_CUSTO', 'SCI Custo', '#d62728');
            bar('pc', dados.spr_c, 'PRODUTO', 'VALOR_CUSTO', 'SPR Custo', '#1f77b4');
        }
        function atualizarDashboard() {
    const inicio   = document.getElementById('dataInicio').value;
    const fim      = document.getElementById('dataFim').value;
    const produto  = document.getElementById('produto').value.trim();  // novo

    const params = new URLSearchParams();
    if (inicio)   params.append('inicio', inicio);
    if (fim)      params.append('fim', fim);
    if (produto)  params.append('produto', produto);  // só se não estiver vazio

    carregarDados(inicio, fim, params);  // 3º argumento opcional
}

async function verProd() {
    const p = document.getElementById('inpP').value;
    const i = document.getElementById('dataInicio').value;
    const f = document.getElementById('dataFim').value;
    const div = document.getElementById('resP');
    if(!p) return alert("Digite um produto");
    div.style.display = 'grid'; div.innerHTML = 'Buscando...';
    const res = await fetch(`/produto_pareto?produto=${p}&inicio=${i}&fim=${f}`);
    const data = await res.json();
    let h = `<div class="top3-card"><h4>SCI</h4>`;
    data.sci.forEach(d => h += `<div class="top3-item"><b>${d.MODO_FALHA}</b> <span>${d.QTDE} pçs</span></div>`);
    h += `</div><div class="top3-card"><h4>SPR</h4>`;
    data.spr.forEach(d => h += `<div class="top3-item"><b>${d.MODO_FALHA}</b> <span>${d.QTDE} pçs</span></div>`);
    div.innerHTML = h + `</div>`;
}

        carregarDados();
    </script>
</body>
</html>
    """

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5003)