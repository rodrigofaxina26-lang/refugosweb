import pandas as pd
import matplotlib.pyplot as plt
import os

# -------------------------------------------------------
# CONFIGURAÇÃO INICIAL
# -------------------------------------------------------

print("Diretório atual:", os.getcwd())
print("Arquivos no diretório atual:", os.listdir())

# Nome do arquivo Excel na MESMA pasta deste .py
arquivo = "REFUGO_2025_V5.xlsx"

# Abas a serem lidas
abas_para_ler = [
    "SCI - QTD",
    "SPR - QTD",
    "Custo dos Produtos",
]

# -------------------------------------------------------
# LEITURA DAS PLANILHAS
# -------------------------------------------------------

planilhas = pd.read_excel(arquivo, sheet_name=abas_para_ler)

df_sci = planilhas["SCI - QTD"]
df_spr = planilhas["SPR - QTD"]
df_custo = planilhas["Custo dos Produtos"]

# -------------------------------------------------------
# PREPARAÇÃO DE CUSTO (USANDO CUSTO UN DA PLANILHA)
# -------------------------------------------------------

# 1) Renomear PROduto -> Produto na aba de custo
if "PRODUTO" in df_custo.columns:
    df_custo = df_custo.rename(columns={"PRODUTO": "Produto"})
elif "Produto" not in df_custo.columns:
    print("\n[AVISO] Não existe coluna 'PRODUTO' ou 'Produto' em 'Custo dos Produtos'.")

# 2) Garantir que CUSTO UN está numérico
if "CUSTO UN" in df_custo.columns:
    df_custo["CUSTO UN"] = pd.to_numeric(df_custo["CUSTO UN"], errors="coerce")
else:
    print("\n[AVISO] Coluna 'CUSTO UN' não encontrada em 'Custo dos Produtos'.")

# 3) Tabela de referência Produto + CUSTO UN
if "Produto" in df_custo.columns and "CUSTO UN" in df_custo.columns:
    df_custo_ref = (
        df_custo[["Produto", "CUSTO UN"]]
        .dropna(subset=["Produto"])
        .drop_duplicates()
    )
else:
    print("\n[AVISO] Problema ao criar df_custo_ref (Produto ou CUSTO UN ausentes).")
    df_custo_ref = pd.DataFrame(columns=["Produto", "CUSTO UN"])

# 4) Juntar custo na SCI e SPR via Produto
df_sci = df_sci.merge(df_custo_ref, on="Produto", how="left")
df_spr = df_spr.merge(df_custo_ref, on="Produto", how="left")

# 5) Calcular custo total por lançamento
df_sci["CUSTO_TOTAL"] = df_sci["Qtde"] * df_sci["CUSTO UN"]
df_spr["CUSTO_TOTAL"] = df_spr["Qtde"] * df_spr["CUSTO UN"]

print("\nCHECAGEM CUSTO SCI:")
print(df_sci[["Produto", "Qtde", "CUSTO UN", "CUSTO_TOTAL"]].head(20))

print("\n=== SCI - QTD - primeiras linhas ===")
print(df_sci.head())

print("\n=== SPR - QTD - primeiras linhas ===")
print(df_spr.head())

print("\n=== Custo dos Produtos - primeiras linhas ===")
print(df_custo.head())

print("\nCHECAGEM CUSTO SCI (repetição):")
print(df_sci[["Produto", "Qtde", "CUSTO UN", "CUSTO_TOTAL"]].head(20))

# -------------------------------------------------------
# FUNÇÕES AUXILIARES
# -------------------------------------------------------

def resumo_qtd_por_coluna(df, coluna_chave, coluna_qtd="Qtde"):
    """Soma quantidade por uma coluna categórica (ex.: Maquina, Produto)."""
    if coluna_qtd not in df.columns or coluna_chave not in df.columns:
        print(f"[AVISO] Coluna '{coluna_chave}' ou '{coluna_qtd}' não encontrada.")
        print("Colunas disponíveis:", list(df.columns))
        return None
    return (
        df.groupby(coluna_chave)[coluna_qtd]
        .sum()
        .reset_index()
        .sort_values(coluna_qtd, ascending=False)
    )

def grafico_barra_simples(df, eixo_x, eixo_y, titulo):
    plt.figure(figsize=(12, 5))
    plt.bar(df[eixo_x].astype(str), df[eixo_y])
    plt.xticks(rotation=45, ha="right")
    plt.title(titulo)
    plt.xlabel(eixo_x)
    plt.ylabel(eixo_y)
    plt.tight_layout()
    plt.show()

def resumo_soma_por_coluna(df, coluna_chave, coluna_valor):
    if coluna_valor not in df.columns or coluna_chave not in df.columns:
        print(f"[AVISO] Coluna '{coluna_chave}' ou '{coluna_valor}' não encontrada.")
        print("Colunas disponíveis:", list(df.columns))
        return None
    return (
        df.groupby(coluna_chave)[coluna_valor]
        .sum()
        .reset_index()
        .sort_values(coluna_valor, ascending=False)
    )

# -------------------------------------------------------
# CONVERSÃO DE DATAS
# -------------------------------------------------------

df_sci["Data"] = pd.to_datetime(df_sci["Data"], errors="coerce")
df_spr["Data"] = pd.to_datetime(df_spr["Data"], errors="coerce")

if "DT Emissao" in df_custo.columns:
    df_custo["DT Emissao"] = pd.to_datetime(df_custo["DT Emissao"], errors="coerce")

# -------------------------------------------------------
# 1) ANÁLISE GERAL - TOP MÁQUINAS E PRODUTOS
# -------------------------------------------------------

# TOP MÁQUINAS SCI (geral)
if "Maquina" in df_sci.columns and "Qtde" in df_sci.columns:
    top_maquinas_sci = resumo_qtd_por_coluna(df_sci, "Maquina", "Qtde")
    if top_maquinas_sci is not None:
        top_maquinas_sci_10 = top_maquinas_sci.head(10)
        print("\nTOP 10 MÁQUINAS - SCI (por Qtde, geral):")
        print(top_maquinas_sci_10)
        grafico_barra_simples(
            top_maquinas_sci_10,
            eixo_x="Maquina",
            eixo_y="Qtde",
            titulo="TOP 10 Máquinas - SCI (Qtde de Refugo - Geral)",
        )

# TOP MÁQUINAS SPR (geral)
if "Maquina" in df_spr.columns and "Qtde" in df_spr.columns:
    top_maquinas_spr = resumo_qtd_por_coluna(df_spr, "Maquina", "Qtde")
    if top_maquinas_spr is not None:
        top_maquinas_spr_10 = top_maquinas_spr.head(10)
        print("\nTOP 10 MÁQUINAS - SPR (por Qtde, geral):")
        print(top_maquinas_spr_10)
        grafico_barra_simples(
            top_maquinas_spr_10,
            eixo_x="Maquina",
            eixo_y="Qtde",
            titulo="TOP 10 Máquinas - SPR (Qtde de Refugo - Geral)",
        )
else:
    print("\n[INFO] SPR - QTD não possui coluna 'Maquina' (ou tem outro nome).")

# --- TOP MÁQUINAS POR CUSTO TOTAL ---

# SCI
if "Maquina" in df_sci.columns:
    top_maquinas_sci_custo = resumo_soma_por_coluna(df_sci, "Maquina", "CUSTO_TOTAL")
    if top_maquinas_sci_custo is not None:
        print("\nTOP 10 MÁQUINAS - SCI (Custo total, geral):")
        print(top_maquinas_sci_custo.head(10))
        grafico_barra_simples(
            top_maquinas_sci_custo.head(10),
            eixo_x="Maquina",
            eixo_y="CUSTO_TOTAL",
            titulo="TOP 10 Máquinas - SCI (Custo total de refugo - Geral)",
        )

# SPR
if "Maquina" in df_spr.columns:
    top_maquinas_spr_custo = resumo_soma_por_coluna(df_spr, "Maquina", "CUSTO_TOTAL")
    if top_maquinas_spr_custo is not None:
        print("\nTOP 10 MÁQUINAS - SPR (Custo total, geral):")
        print(top_maquinas_spr_custo.head(10))
        grafico_barra_simples(
            top_maquinas_spr_custo.head(10),
            eixo_x="Maquina",
            eixo_y="CUSTO_TOTAL",
            titulo="TOP 10 Máquinas - SPR (Custo total de refugo - Geral)",
        )

# TOP PRODUTOS SCI (geral)
col_prod_sci = "Produto"
if col_prod_sci in df_sci.columns:
    top_produtos_sci =Resumo_qtd = resumo_qtd_por_coluna(df_sci, col_prod_sci, "Qtde")
    top_produtos_sci = resumo_qtd_por_coluna(df_sci, col_prod_sci, "Qtde")
    if top_produtos_sci is not None:
        top_produtos_sci_10 = top_produtos_sci.head(10)
        print("\nTOP 10 PRODUTOS - SCI (por Qtde, geral):")
        print(top_produtos_sci_10)
        grafico_barra_simples(
            top_produtos_sci_10,
            eixo_x=col_prod_sci,
            eixo_y="Qtde",
            titulo="TOP 10 Produtos - SCI (Qtde de Refugo - Geral)",
        )
else:
    print("\n[INFO] SCI - QTD não possui coluna 'Produto' (confira o nome).")

# TOP PRODUTOS SPR (geral)
col_prod_spr = "Produto"
if col_prod_spr in df_spr.columns:
    top_produtos_spr = resumo_qtd_por_coluna(df_spr, col_prod_spr, "Qtde")
    if top_produtos_spr is not None:
        top_produtos_spr_10 = top_produtos_spr.head(10)
        print("\nTOP 10 PRODUTOS - SPR (por Qtde, geral):")
        print(top_produtos_spr_10)
        grafico_barra_simples(
            top_produtos_spr_10,
            eixo_x=col_prod_spr,
            eixo_y="Qtde",
            titulo="TOP 10 Produtos - SPR (Qtde de Refugo - Geral)",
        )
else:
    print("\n[INFO] SPR - QTD não possui coluna 'Produto' (confira o nome).")

# --- TOP PRODUTOS POR CUSTO TOTAL ---

# SCI
if "Produto" in df_sci.columns:
    top_prod_sci_custo = resumo_soma_por_coluna(df_sci, "Produto", "CUSTO_TOTAL")
    if top_prod_sci_custo is not None:
        print("\nTOP 10 PRODUTOS - SCI (Custo total, geral):")
        print(top_prod_sci_custo.head(10))
        grafico_barra_simples(
            top_prod_sci_custo.head(10),
            eixo_x="Produto",
            eixo_y="CUSTO_TOTAL",
            titulo="TOP 10 Produtos - SCI (Custo total de refugo - Geral)",
        )

# SPR
if "Produto" in df_spr.columns:
    top_prod_spr_custo = resumo_soma_por_coluna(df_spr, "Produto", "CUSTO_TOTAL")
    if top_prod_spr_custo is not None:
        print("\nTOP 10 PRODUTOS - SPR (Custo total, geral):")
        print(top_prod_spr_custo.head(10))
        grafico_barra_simples(
            top_prod_spr_custo.head(10),
            eixo_x="Produto",
            eixo_y="CUSTO_TOTAL",
            titulo="TOP 10 Produtos - SPR (Custo total de refugo - Geral)",
        )

# -------------------------------------------------------
# 2) RUN CHART - REFUGO TOTAL DIÁRIO (SCI + SPR)
# -------------------------------------------------------

daily_sci = (
    df_sci.dropna(subset=["Data"])
    .groupby("Data")["Qtde"]
    .sum()
    .reset_index()
    .rename(columns={"Qtde": "Qtde_SCI"})
)

daily_spr = (
    df_spr.dropna(subset=["Data"])
    .groupby("Data")["Qtde"]
    .sum()
    .reset_index()
    .rename(columns={"Qtde": "Qtde_SPR"})
)

daily_total = pd.merge(daily_sci, daily_spr, on="Data", how="outer").fillna(0)
daily_total["Qtde_Total"] = daily_total["Qtde_SCI"] + daily_total["Qtde_SPR"]

print("\nRefugo total diário (SCI + SPR) - primeiras linhas:")
print(daily_total.head())

plt.figure(figsize=(12, 5))
plt.plot(daily_total["Data"], daily_total["Qtde_Total"], marker="o", label="Total")
plt.plot(daily_total["Data"], daily_total["Qtde_SCI"], marker=".", linestyle="--", label="SCI")
plt.plot(daily_total["Data"], daily_total["Qtde_SPR"], marker=".", linestyle="--", label="SPR")
plt.title("Run chart - Refugo total diário (SCI + SPR)")
plt.xlabel("Data")
plt.ylabel("Qtde refugos")
plt.xticks(rotation=45, ha="right")
plt.legend()
plt.tight_layout()
plt.show()

# --- RUN CHART DIÁRIO - CUSTO TOTAL ---

daily_sci_custo = (
    df_sci.dropna(subset=["Data"])
    .groupby("Data")["CUSTO_TOTAL"]
    .sum()
    .reset_index()
    .rename(columns={"CUSTO_TOTAL": "CUSTO_SCI"})
)

daily_spr_custo = (
    df_spr.dropna(subset=["Data"])
    .groupby("Data")["CUSTO_TOTAL"]
    .sum()
    .reset_index()
    .rename(columns={"CUSTO_TOTAL": "CUSTO_SPR"})
)

daily_custo = pd.merge(daily_sci_custo, daily_spr_custo, on="Data", how="outer").fillna(0)
daily_custo["CUSTO_Total"] = daily_custo["CUSTO_SCI"] + daily_custo["CUSTO_SPR"]

print("\nRefugo total diário (Custo) - primeiras linhas:")
print(daily_custo.head())

plt.figure(figsize=(12, 5))
plt.plot(daily_custo["Data"], daily_custo["CUSTO_Total"], marker="o", label="Total")
plt.plot(daily_custo["Data"], daily_custo["CUSTO_SCI"], marker=".", linestyle="--", label="SCI")
plt.plot(daily_custo["Data"], daily_custo["CUSTO_SPR"], marker=".", linestyle="--", label="SPR")
plt.title("Run chart - Refugo total diário (Custo total) - SCI + SPR")
plt.xlabel("Data")
plt.ylabel("Custo total")
plt.xticks(rotation=45, ha="right")
plt.legend()
plt.tight_layout()
plt.show()

# -------------------------------------------------------
# RUN CHART MENSAL - REFUGO TOTAL (SCI + SPR) - BARRAS
# -------------------------------------------------------

daily_total["AnoMes"] = daily_total["Data"].dt.to_period("M").astype(str)

monthly_total = (
    daily_total.groupby("AnoMes")[["Qtde_SCI", "Qtde_SPR", "Qtde_Total"]]
    .sum()
    .reset_index()
)

print("\nRefugo total mensal (SCI + SPR) - primeiras linhas:")
print(monthly_total.head())

plt.figure(figsize=(12, 5))
largura = 0.3
x = range(len(monthly_total))

plt.bar([i - largura for i in x], monthly_total["Qtde_SCI"], width=largura, label="SCI")
plt.bar(x, monthly_total["Qtde_SPR"], width=largura, label="SPR")
plt.bar([i + largura for i in x], monthly_total["Qtde_Total"], width=largura, label="Total")

plt.xticks(x, monthly_total["AnoMes"], rotation=45, ha="right")
plt.title("Refugo total mensal (SCI, SPR e Total)")
plt.xlabel("Ano-Mês")
plt.ylabel("Qtde refugos")
plt.legend()
plt.tight_layout()
plt.show()

# --- RUN CHART MENSAL - CUSTO TOTAL (BARRAS) ---

daily_custo["AnoMes"] = daily_custo["Data"].dt.to_period("M").astype(str)

monthly_custo = (
    daily_custo.groupby("AnoMes")[["CUSTO_SCI", "CUSTO_SPR", "CUSTO_Total"]]
    .sum()
    .reset_index()
)

print("\nRefugo total mensal (Custo) - primeiras linhas:")
print(monthly_custo.head())

plt.figure(figsize=(12, 5))
largura = 0.3
x = range(len(monthly_custo))

plt.bar([i - largura for i in x], monthly_custo["CUSTO_SCI"], width=largura, label="SCI")
plt.bar(x, monthly_custo["CUSTO_SPR"], width=largura, label="SPR")
plt.bar([i + largura for i in x], monthly_custo["CUSTO_Total"], width=largura, label="Total")

plt.xticks(x, monthly_custo["AnoMes"], rotation=45, ha="right")
plt.title("Refugo total mensal (Custo total) - SCI, SPR e Total")
plt.xlabel("Ano-Mês")
plt.ylabel("Custo total")
plt.legend()
plt.tight_layout()
plt.show()

# -------------------------------------------------------
# 4) ANÁLISE POR DIA - TOP 3 (Qtde e Custo)
# -------------------------------------------------------

data_str = input("\nDigite a data (formato AAAA-MM-DD), ex: 2025-03-10: ")
try:
    data_alvo = pd.to_datetime(data_str)
except Exception:
    print("Data inválida. Encerrando.")
    raise SystemExit

sci_dia = df_sci[df_sci["Data"] == data_alvo]
spr_dia = df_spr[df_spr["Data"] == data_alvo]

print(f"\nResumo do dia {data_alvo.date()}:")
print("Registros SCI:", len(sci_dia), " | Registros SPR:", len(spr_dia))

# --- TOP 3 Máquinas por Qtde no dia (SCI e SPR) ---

if "Maquina" in sci_dia.columns and not sci_dia.empty:
    top3_maquina_sci_dia = (
        sci_dia.groupby("Maquina")["Qtde"]
        .sum()
        .reset_index()
        .sort_values("Qtde", ascending=False)
        .head(3)
    )
    print("\nTOP 3 MÁQUINAS - SCI (Qtde no dia):")
    print(top3_maquina_sci_dia)

    grafico_barra_simples(
        top3_maquina_sci_dia,
        eixo_x="Maquina",
        eixo_y="Qtde",
        titulo=f"TOP 3 Máquinas - SCI ({data_alvo.date()})",
    )
else:
    print("\n[INFO] SCI - sem dados ou sem 'Maquina' para essa data.")

if "Maquina" in spr_dia.columns and not spr_dia.empty:
    top3_maquina_spr_dia = (
        spr_dia.groupby("Maquina")["Qtde"]
        .sum()
        .reset_index()
        .sort_values("Qtde", ascending=False)
        .head(3)
    )
    print("\nTOP 3 MÁQUINAS - SPR (Qtde no dia):")
    print(top3_maquina_spr_dia)

    grafico_barra_simples(
        top3_maquina_spr_dia,
        eixo_x="Maquina",
        eixo_y="Qtde",
        titulo=f"TOP 3 Máquinas - SPR ({data_alvo.date()})",
    )
else:
    print("\n[INFO] SPR - sem dados ou sem 'Maquina' para essa data.")

# --- TOP 3 Produtos por Qtde no dia (SCI e SPR) ---

if "Produto" in sci_dia.columns and not sci_dia.empty:
    top3_prod_sci_dia = (
        sci_dia.groupby("Produto")["Qtde"]
        .sum()
        .reset_index()
        .sort_values("Qtde", ascending=False)
        .head(3)
    )
    print("\nTOP 3 PRODUTOS - SCI (Qtde no dia):")
    print(top3_prod_sci_dia)

    grafico_barra_simples(
        top3_prod_sci_dia,
        eixo_x="Produto",
        eixo_y="Qtde",
        titulo=f"TOP 3 Produtos - SCI ({data_alvo.date()})",
    )
else:
    print("\n[INFO] SCI - sem dados ou sem 'Produto' para essa data.")

if "Produto" in spr_dia.columns and not spr_dia.empty:
    top3_prod_spr_dia = (
        spr_dia.groupby("Produto")["Qtde"]
        .sum()
        .reset_index()
        .sort_values("Qtde", ascending=False)
        .head(3)
    )
    print("\nTOP 3 PRODUTOS - SPR (Qtde no dia):")
    print(top3_prod_spr_dia)

    grafico_barra_simples(
        top3_prod_spr_dia,
        eixo_x="Produto",
        eixo_y="Qtde",
        titulo=f"TOP 3 Produtos - SPR ({data_alvo.date()})",
    )
else:
    print("\n[INFO] SPR - sem dados ou sem 'Produto' para essa data.")

# --- TOP 3 por custo no dia (arquivo de custo) ---

if "DT Emissao" in df_custo.columns and "CUSTO" in df_custo.columns:
    custo_dia = df_custo[df_custo["DT Emissao"] == data_alvo]

    if not custo_dia.empty:
        col_prod_custo_dia = None
        if "Produto" in custo_dia.columns:
            col_prod_custo_dia = "Produto"

        if col_prod_custo_dia:
            top3_custo_prod_dia = (
                custo_dia.groupby(col_prod_custo_dia)["CUSTO"]
                .sum()
                .reset_index()
                .sort_values("CUSTO", ascending=False)
                .head(3)
            )
            print("\nTOP 3 PRODUTOS POR CUSTO NO DIA:")
            print(top3_custo_prod_dia)

            grafico_barra_simples(
                top3_custo_prod_dia,
                eixo_x=col_prod_custo_dia,
                eixo_y="CUSTO",
                titulo=f"TOP 3 Produtos por custo ({data_alvo.date()})",
            )
        else:
            print("\n[INFO] Não encontrei coluna de produto para custo no dia.")
    else:
        print("\n[INFO] Nenhum registro de custo para essa data em 'Custo dos Produtos'.")
else:
    print("\n[INFO] 'Custo dos Produtos' sem 'DT Emissao' ou 'CUSTO' para análise por dia.")

# --- TOP 3 PRODUTOS POR CUSTO_TOTAL NO DIA (SCI e SPR) ---

if not sci_dia.empty and "CUSTO_TOTAL" in sci_dia.columns:
    top3_prod_sci_dia_custo = resumo_soma_por_coluna(sci_dia, "Produto", "CUSTO_TOTAL")
    if top3_prod_sci_dia_custo is not None:
        top3 = top3_prod_sci_dia_custo.head(3)
        print("\nTOP 3 PRODUTOS - SCI (CUSTO_TOTAL no dia):")
        print(top3)
        grafico_barra_simples(
            top3,
            eixo_x="Produto",
            eixo_y="CUSTO_TOTAL",
            titulo=f"TOP 3 Produtos - SCI (Custo total) - {data_alvo.date()}",
        )

if not spr_dia.empty and "CUSTO_TOTAL" in spr_dia.columns:
    top3_prod_spr_dia_custo = resumo_soma_por_coluna(spr_dia, "Produto", "CUSTO_TOTAL")
    if top3_prod_spr_dia_custo is not None:
        top3 = top3_prod_spr_dia_custo.head(3)
        print("\nTOP 3 PRODUTOS - SPR (CUSTO_TOTAL no dia):")
        print(top3)
        grafico_barra_simples(
            top3,
            eixo_x="Produto",
            eixo_y="CUSTO_TOTAL",
            titulo=f"TOP 3 Produtos - SPR (Custo total) - {data_alvo.date()}",
        )
