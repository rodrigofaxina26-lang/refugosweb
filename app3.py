
from flask import Flask, request, render_template
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

            df.columns = [str(c).strip().upper().replace(' ', '') for c in df.columns]
            print(f"🔍 Colunas lidas na {fonte}: {df.columns.tolist()}")

            col_data_candidate = None
            for cand in ["DATA", "DAT", "DATAPROD", "DATAREFUGO", "DATA_REFUGO"]:
                if cand in df.columns:
                    col_data_candidate = cand
                    break

            if not col_data_candidate:
                print(f"❌ Coluna 'DATA' não encontrada na {fonte}. Colunas disponíveis: {df.columns.tolist()}")
                dfs_processados.append(pd.DataFrame())
                continue

            df[col_data_candidate] = pd.to_numeric(df[col_data_candidate], errors="coerce")
            df = df.dropna(subset=[col_data_candidate])
            df[col_data_candidate] = pd.to_datetime(df[col_data_candidate], unit="D", origin="1899-12-30")
            df = df[df[col_data_candidate].notna()].copy()
            df.rename(columns={col_data_candidate: "DATA"}, inplace=True)

            ano_minimo = 2025
            df = df[df["DATA"].dt.year >= ano_minimo].copy()
            print(f"✅ {fonte}: {len(df)} registros após filtro de ano (>= {ano_minimo}).")

            col_qtde = next((c for c in df.columns if "QTDE" in c or "QTD" in c), None)
            if col_qtde:
                df["QTDE"] = pd.to_numeric(df[col_qtde], errors="coerce").fillna(0)
            else:
                df["QTDE"] = 0
                print(f"⚠️ Coluna QTDE não encontrada em {fonte}, usando 0.")

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
    pareto["CUMPERC"] = (pareto["QTDE"].cumsum() / pareto["QTDE"].sum()).round(1)

    return {
        "modo": pareto["MODO_FALHA"].tolist()[:10],
        "qtde": pareto["QTDE"].tolist()[:10],
        "cumperc": pareto["CUMPERC"].tolist()[:10],
        "total": int(pareto["QTDE"].sum())
    }

def get_top3_problemas_por_produto(df, produto_nome):
    if df.empty or not produto_nome:
        return []

    termo = str(produto_nome).strip().upper()
    df_temp = df.copy()
    df_temp["PRODUTO_CHECK"] = df_temp["PRODUTO"].astype(str).str.strip().str.upper()

    df_p = df_temp[df_temp["PRODUTO_CHECK"] == termo].copy()
    if df_p.empty:
        df_p = df_temp[df_temp["PRODUTO_CHECK"].str.contains(termo, na=False)].copy()

    if df_p.empty:
        return []

    df_p["QTDE"] = pd.to_numeric(df_p["QTDE"], errors='coerce').fillna(0)
    df_p = df_p[df_p["QTDE"] > 0]
    df_p["MODO_FALHA_FINAL"] = df_p.apply(get_modo_falha, axis=1)
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
    data_inicio = request.args.get("inicio", (date.today() - pd.Timedelta(days=90)).strftime("%Y-%m-%d"))
    data_fim = request.args.get("fim", date.today().strftime("%Y-%m-%d"))
    produto = request.args.get("produto", "").strip().upper()

    force = pd.to_datetime(data_fim).date() >= date.today()
    df_sci_raw, df_spr_raw = carregar_dados(force_reload=force)

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

    df_sci = df_sci_raw[(df_sci_raw["DATA"] >= pd.to_datetime(data_inicio)) &
                        (df_sci_raw["DATA"] <= pd.to_datetime(data_fim))].copy()
    df_spr = df_spr_raw[(df_spr_raw["DATA"] >= pd.to_datetime(data_inicio)) &
                        (df_spr_raw["DATA"] <= pd.to_datetime(data_fim))].copy()

    if produto:
        if "PRODUTO" in df_sci.columns:
            df_sci = df_sci[df_sci["PRODUTO"].astype(str).str.upper().str.contains(produto, na=False)]
        if "PRODUTO" in df_spr.columns:
            df_spr = df_spr[df_spr["PRODUTO"].astype(str).str.upper().str.contains(produto, na=False)]

    if len(df_sci) == 0:
        res_sci = pd.DataFrame(columns=["DATA_STR", "QTDE", "VALOR_CUSTO"])
    else:
        res_sci = df_sci.groupby("DATA_STR")[["QTDE", "VALOR_CUSTO"]].sum().reset_index()

    if len(df_spr) == 0:
        res_spr = pd.DataFrame(columns=["DATA_STR", "QTDE", "VALOR_CUSTO"])
    else:
        res_spr = df_spr.groupby("DATA_STR")[["QTDE", "VALOR_CUSTO"]].sum().reset_index()

    run_df = pd.merge(res_sci, res_spr, on="DATA_STR", how="outer", suffixes=("_SCI", "_SPR")).fillna(0)
    run_df["TOTAL_Q"] = run_df["QTDE_SCI"] + run_df["QTDE_SPR"]
    run_df["TOTAL_C"] = run_df["VALOR_CUSTO_SCI"] + run_df["VALOR_CUSTO_SPR"]
    run_df = run_df.sort_values("DATA_STR")

    def top10(df, col):
        if len(df) == 0 or "PRODUTO" not in df.columns:
            return [{"PRODUTO": "SEM DADOS", col: 0}]
        try:
            return df.groupby("PRODUTO")[col].sum().nlargest(10).reset_index().to_dict("records")
        except:
            return [{"PRODUTO": "ERRO", col: 0}]

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
    return render_template("index.html")


if __name__ == "__main__":
    app.run(debug=False, host="0.0.0.0", port=5003)
