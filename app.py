import os
import re
from datetime import datetime

import pandas as pd
import streamlit as st
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode, JsCode

from services.embalagens import carregar_ordens_fabric_abertas_com_embalagem
from services.ordem_fabric_service import (
    carregar_ofs_fechadas_producao,
    preparar_producao_diaria_semanal
)
from utils.formatters import formatar_numero_br


ARQUIVO_BANCO = "banco.txt"
ARQUIVO_LOGO_SIDEBAR = "Controller_virtual-Realfix.png"


st.set_page_config(
    page_title="Pprod | Painel de Produção",
    page_icon="🏭",
    layout="wide"
)

# =========================================================
# CABEÇALHO
# =========================================================
col_titulo, col_botao = st.columns([8, 1.5])

with col_titulo:
    st.title("🏭 Pprod | Painel de Produção")
    st.caption("Painel de monitoramento da produção")

with col_botao:
    st.write("")
    st.write("")
    if st.button("🔄 Atualizar", use_container_width=True):
        st.rerun()

st.markdown("""
<style>
.card-kpi {
    background: linear-gradient(180deg, #dceee3 0%, #cfe4d8 100%);
    border: 1px solid #bdd7c7;
    border-radius: 14px;
    padding: 14px 16px 12px 16px;
    min-height: 112px;
    box-shadow: 0 1px 2px rgba(0,0,0,0.04);
}

.card-kpi-titulo {
    font-size: 14px;
    font-weight: 700;
    color: #2d4b3b;
    margin-bottom: 8px;
}

.card-kpi-valor {
    font-size: 24px;
    font-weight: 800;
    color: #0f172a;
    line-height: 1.1;
    margin-bottom: 10px;
}

.card-kpi-rodape {
    display: flex;
    justify-content: space-between;
    align-items: center;
    gap: 8px;
    font-size: 12px;
    color: #111827;
}

.card-kpi-delta {
    font-weight: 700;
    white-space: nowrap;
}

.card-kpi-extra {
    text-align: right;
    white-space: nowrap;
}

.card-kpi-azul {
    background: linear-gradient(180deg, #dbeafe 0%, #cfe0fb 100%);
    border: 1px solid #bfd3f7;
}

.card-kpi-amarelo {
    background: linear-gradient(180deg, #fef3c7 0%, #f8e7ad 100%);
    border: 1px solid #edd48a;
}

.card-kpi-verde {
    background: linear-gradient(180deg, #dceee3 0%, #cfe4d8 100%);
    border: 1px solid #bdd7c7;
}

.card-faixas {
    background: linear-gradient(180deg, #ede9fe 0%, #ddd6fe 100%);
    border: 1px solid #c4b5fd;
    border-radius: 14px;
    padding: 14px 16px 12px 16px;
    min-height: 112px;
    box-shadow: 0 1px 2px rgba(0,0,0,0.04);
}

.card-faixas-titulo {
    font-size: 14px;
    font-weight: 700;
    color: #4c1d95;
    margin-bottom: 10px;
}

.card-faixas-linha {
    display: flex;
    justify-content: space-between;
    align-items: center;
    gap: 12px;
    font-size: 13px;
    color: #111827;
    padding: 4px 0;
    border-bottom: 1px dashed rgba(76, 29, 149, 0.15);
}

.card-faixas-linha:last-child {
    border-bottom: none;
}

.card-faixas-label {
    font-weight: 600;
}

.card-faixas-valor {
    font-weight: 800;
    color: #4c1d95;
}
</style>
""", unsafe_allow_html=True)

# =========================================================
# FUNÇÕES AUXILIARES
# =========================================================
def normalizar_texto(valor):
    if pd.isna(valor):
        return ""
    return str(valor).strip()


def validar_data_br(valor):
    if pd.isna(valor):
        return True

    valor = str(valor).strip()
    if valor == "":
        return True

    if not re.match(r"^\d{2}/\d{2}/\d{4}$", valor):
        return False

    try:
        datetime.strptime(valor, "%d/%m/%Y")
        return True
    except ValueError:
        return False


def converter_data_br_para_ordenacao(valor):
    if pd.isna(valor):
        return pd.NaT

    valor = str(valor).strip()
    if valor == "":
        return pd.NaT

    return pd.to_datetime(valor, format="%d/%m/%Y", dayfirst=True, errors="coerce")


def converter_serie_data(valor):
    if pd.isna(valor):
        return pd.NaT

    valor = str(valor).strip()
    if valor == "":
        return pd.NaT

    dt = pd.to_datetime(valor, format="%d/%m/%Y", dayfirst=True, errors="coerce")
    if pd.notna(dt):
        return dt

    return pd.to_datetime(valor, errors="coerce")


def prioridade_para_ordem(valor):
    if pd.isna(valor):
        return 999999

    valor = str(valor).strip().upper()
    if valor == "":
        return 999999

    match = re.match(r"^P(\d+)$", valor)
    if match:
        return int(match.group(1))

    return 999999


def garantir_colunas_novas(df):
    for col in ["Prioridade", "Nw_Data", "Dt.Subida", "PrzReal", "status_prod"]:
        if col not in df.columns:
            df[col] = ""

    if "Remover" not in df.columns:
        df["Remover"] = False

    return df


def lista_multiselect(df, coluna):
    if coluna not in df.columns:
        return []

    valores = (
        df[coluna]
        .fillna("")
        .astype(str)
        .str.strip()
    )

    valores = valores[valores != ""]
    return sorted(valores.unique().tolist())


def aplicar_filtro_multiselect(df, coluna, selecionados):
    if coluna not in df.columns or not selecionados:
        return df

    serie = df[coluna].fillna("").astype(str).str.strip()
    return df[serie.isin(selecionados)]


def aplicar_filtro_data(df, coluna, data_inicio=None, data_fim=None):
    if coluna not in df.columns:
        return df

    serie_data = df[coluna].apply(converter_serie_data)

    if data_inicio is not None:
        df = df[serie_data >= pd.to_datetime(data_inicio)]
        serie_data = df[coluna].apply(converter_serie_data)

    if data_fim is not None:
        df = df[serie_data <= pd.to_datetime(data_fim)]
        serie_data = df[coluna].apply(converter_serie_data)

    return df


def calcular_przreal(data_abertura, data_prev_entrega, nw_data):
    abertura = converter_serie_data(data_abertura)
    nw = converter_serie_data(nw_data)
    previsao = converter_serie_data(data_prev_entrega)

    data_base = nw if pd.notna(nw) else previsao

    if pd.isna(abertura) or pd.isna(data_base):
        return None

    return int((data_base.normalize() - abertura.normalize()).days)


def calcular_status_prod(status_of, data_prev_entrega, nw_data):
    status = normalizar_texto(status_of).upper()
    if status != "A":
        return ""

    hoje = pd.Timestamp.today().normalize()

    nw = converter_serie_data(nw_data)
    previsao = converter_serie_data(data_prev_entrega)
    data_base = nw if pd.notna(nw) else previsao

    if pd.isna(data_base):
        return ""

    data_base = pd.Timestamp(data_base).normalize()

    if data_base < hoje:
        return "🔴"
    elif data_base == hoje:
        return "🟡"
    return "🟢"


def render_card_kpi(titulo, valor, delta="", extra="", classe="card-kpi-verde"):
    st.markdown(
        f"""
        <div class="card-kpi {classe}">
            <div class="card-kpi-titulo">{titulo}</div>
            <div class="card-kpi-valor">{valor}</div>
            <div class="card-kpi-rodape">
                <div class="card-kpi-delta">{delta}</div>
                <div class="card-kpi-extra">{extra}</div>
            </div>
        </div>
        """,
        unsafe_allow_html=True
    )


def render_card_faixas_litros(of_ate_49, of_50_200, of_200_500, of_maior_500):
    html = f"""
<div class="card-faixas">
    <div class="card-faixas-titulo">Posição Litros por OF</div>
    <div class="card-faixas-linha">
        <span class="card-faixas-label">OFs até 49,99 litros</span>
        <span class="card-faixas-valor">{of_ate_49}</span>
    </div>
    <div class="card-faixas-linha">
        <span class="card-faixas-label">OFs de 50 a 200 litros</span>
        <span class="card-faixas-valor">{of_50_200}</span>
    </div>
    <div class="card-faixas-linha">
        <span class="card-faixas-label">OFs de 200,01 a 500 litros</span>
        <span class="card-faixas-valor">{of_200_500}</span>
    </div>
    <div class="card-faixas-linha">
        <span class="card-faixas-label">OFs acima de 500 litros</span>
        <span class="card-faixas-valor">{of_maior_500}</span>
    </div>
</div>
"""
    st.markdown(html, unsafe_allow_html=True)

# =========================================================
# BANCO TXT
# =========================================================
def carregar_banco_txt(caminho=ARQUIVO_BANCO):
    colunas = ["Nw_Data", "Nro_OF", "Produto OF", "Prioridade", "Dt.Subida"]

    if not os.path.exists(caminho):
        return pd.DataFrame(columns=colunas)

    try:
        df_banco = pd.read_csv(
            caminho,
            sep=";",
            dtype=str,
            encoding="utf-8"
        ).fillna("")

        mapa_colunas = {
            "Nro OF": "Nro_OF",
            "Nro_OF": "Nro_OF",
            "Produto OF": "Produto OF",
            "Produto_OF": "Produto OF",
            "Prioridade": "Prioridade",
            "Nw_Data": "Nw_Data",
            "Dt.Subida": "Dt.Subida",
        }

        df_banco = df_banco.rename(columns=mapa_colunas)

        for col in colunas:
            if col not in df_banco.columns:
                df_banco[col] = ""

        df_banco = df_banco[colunas].copy()

        df_banco["Nro_OF"] = df_banco["Nro_OF"].apply(normalizar_texto)
        df_banco["Produto OF"] = df_banco["Produto OF"].apply(normalizar_texto)
        df_banco["Prioridade"] = df_banco["Prioridade"].apply(normalizar_texto).str.upper()
        df_banco["Nw_Data"] = df_banco["Nw_Data"].apply(normalizar_texto)
        df_banco["Dt.Subida"] = df_banco["Dt.Subida"].apply(normalizar_texto)

        df_banco = df_banco.drop_duplicates(subset=["Nro_OF", "Produto OF"], keep="last")
        return df_banco

    except Exception as e:
        st.error(f"Erro ao ler o arquivo {caminho}: {e}")
        return pd.DataFrame(columns=colunas)


def salvar_banco_txt(df_tabela, caminho=ARQUIVO_BANCO):
    df_save = df_tabela.copy()

    if "Remover" in df_save.columns:
        df_save["Remover"] = df_save["Remover"].fillna(False).astype(bool)
        df_save = df_save[~df_save["Remover"]].copy()

    if "Nro OF" in df_save.columns:
        df_save = df_save.rename(columns={"Nro OF": "Nro_OF"})

    colunas_saida = ["Nw_Data", "Nro_OF", "Produto OF", "Prioridade", "Dt.Subida"]
    for col in colunas_saida:
        if col not in df_save.columns:
            df_save[col] = ""

    df_save = df_save[colunas_saida].copy()

    df_save = df_save[
        (df_save["Prioridade"].astype(str).str.strip() != "") |
        (df_save["Nw_Data"].astype(str).str.strip() != "") |
        (df_save["Dt.Subida"].astype(str).str.strip() != "")
    ].copy()

    df_save["Nro_OF"] = df_save["Nro_OF"].apply(normalizar_texto)
    df_save["Produto OF"] = df_save["Produto OF"].apply(normalizar_texto)
    df_save["Prioridade"] = df_save["Prioridade"].apply(normalizar_texto).str.upper()
    df_save["Nw_Data"] = df_save["Nw_Data"].apply(normalizar_texto)
    df_save["Dt.Subida"] = df_save["Dt.Subida"].apply(normalizar_texto)

    df_save = df_save.drop_duplicates(subset=["Nro_OF", "Produto OF"], keep="last")

    df_save["_ord_prioridade"] = df_save["Prioridade"].apply(prioridade_para_ordem)
    df_save["_ord_data"] = df_save["Nw_Data"].apply(converter_data_br_para_ordenacao)

    df_save = df_save.sort_values(
        by=["_ord_prioridade", "_ord_data", "Nro_OF", "Produto OF"],
        ascending=[True, True, True, True],
        na_position="last"
    ).drop(columns=["_ord_prioridade", "_ord_data"])

    df_save.to_csv(caminho, sep=";", index=False, encoding="utf-8")


def aplicar_banco_txt(df_principal, df_banco):
    df = df_principal.copy()

    df["nro_of"] = df["nro_of"].apply(normalizar_texto)
    df["produto"] = df["produto"].apply(normalizar_texto)

    if df_banco.empty:
        return garantir_colunas_novas(df)

    df_merge = df.merge(
        df_banco,
        how="left",
        left_on=["nro_of", "produto"],
        right_on=["Nro_OF", "Produto OF"]
    )

    if "Nro_OF" in df_merge.columns:
        df_merge = df_merge.drop(columns=["Nro_OF"])

    df_merge = garantir_colunas_novas(df_merge)

    df_merge["Prioridade"] = df_merge["Prioridade"].fillna("").astype(str).str.upper()
    df_merge["Nw_Data"] = df_merge["Nw_Data"].fillna("").astype(str)
    df_merge["Dt.Subida"] = df_merge["Dt.Subida"].fillna("").astype(str)
    df_merge["Remover"] = False

    return df_merge


def ordenar_dataframe(df):
    df = df.copy()

    df["_ord_prioridade"] = df["Prioridade"].apply(prioridade_para_ordem)
    df["_ord_data"] = df["Nw_Data"].apply(converter_data_br_para_ordenacao)

    df = df.sort_values(
        by=["_ord_prioridade", "_ord_data", "nro_of", "produto"],
        ascending=[True, True, True, True],
        na_position="last"
    ).drop(columns=["_ord_prioridade", "_ord_data"])

    return df

# =========================================================
# GRÁFICO
# =========================================================
@st.cache_data(ttl=300)
def obter_dados():
    return carregar_ordens_fabric_abertas_com_embalagem()


@st.cache_data(ttl=300)
def obter_dados_fechadas():
    return carregar_ofs_fechadas_producao()


def aplicar_mesmos_filtros_fechadas(df_fechadas, filtros):
    df = df_fechadas.copy()

    if "desc_cliente" in df.columns:
        df = aplicar_filtro_multiselect(df, "desc_cliente", filtros["cliente"])

    if "origem" in df.columns:
        df = aplicar_filtro_multiselect(df, "origem", filtros["origem"])

    if "produto" in df.columns:
        df = aplicar_filtro_multiselect(df, "produto", filtros["produto_of"])

    if "tipo_material" in df.columns:
        df = aplicar_filtro_multiselect(df, "tipo_material", filtros["tipo_material"])

    if filtros["prev_entrega_inicio"] is not None and "data_prev_entrega" in df.columns:
        df = aplicar_filtro_data(df, "data_prev_entrega", data_inicio=filtros["prev_entrega_inicio"])

    if filtros["prev_entrega_fim"] is not None and "data_prev_entrega" in df.columns:
        df = aplicar_filtro_data(df, "data_prev_entrega", data_fim=filtros["prev_entrega_fim"])

    return df


def render_grafico_producao_diaria_semanal(df_diario):
    st.markdown("### Análise Diária vs Média da Semana")

    if df_diario.empty:
        st.info("Não há OFs fechadas com data de fechamento para o período/filtros selecionados.")
        return

    df_plot = df_diario.copy()

    if "data" not in df_plot.columns:
        st.warning("A base do gráfico não possui a coluna 'data'.")
        return

    df_plot["data"] = pd.to_datetime(df_plot["data"], errors="coerce")
    df_plot = df_plot.dropna(subset=["data"]).copy()

    if df_plot.empty:
        st.info("Não há dados válidos para montar o gráfico.")
        return

    semanas_ordenadas = (
        df_plot[["ano", "semana"]]
        .drop_duplicates()
        .sort_values(["ano", "semana"])
    )

    if semanas_ordenadas.empty:
        st.info("Não há semanas disponíveis para exibir no gráfico.")
        return

    if len(semanas_ordenadas) > 5:
        semanas_ordenadas = semanas_ordenadas.tail(5)

    semanas_lista = list(semanas_ordenadas.itertuples(index=False, name=None))
    qtd_semanas = len(semanas_lista)

    fig = make_subplots(
        rows=1,
        cols=qtd_semanas,
        shared_yaxes=True,
        subplot_titles=[f"Semana {semana}" for _, semana in semanas_lista],
        horizontal_spacing=0.04
    )

    for i, (ano, semana) in enumerate(semanas_lista, start=1):
        df_semana = df_plot[
            (df_plot["ano"] == ano) &
            (df_plot["semana"] == semana)
        ].copy()

        df_semana = df_semana.sort_values("data")

        if df_semana.empty:
            continue

        df_semana["dia_label"] = df_semana["data"].dt.strftime("%d/%m")
        df_semana["litros_label"] = df_semana["litros"].apply(lambda x: formatar_numero_br(x, 1))
        df_semana["media_label"] = df_semana["media_semana"].apply(lambda x: formatar_numero_br(x, 1))

        cores = [
            "#2ca02c" if valor >= media else "#d62728"
            for valor, media in zip(df_semana["litros"], df_semana["media_semana"])
        ]

        fig.add_trace(
            go.Bar(
                x=df_semana["dia_label"],
                y=df_semana["litros"],
                marker_color=cores,
                text=df_semana["litros_label"],
                textposition="outside",
                name="OFs no Dia" if i == 1 else None,
                showlegend=(i == 1),
                hovertemplate="Dia: %{x}<br>Litros: %{y:.2f}<extra></extra>",
            ),
            row=1,
            col=i
        )

        fig.add_trace(
            go.Scatter(
                x=df_semana["dia_label"],
                y=df_semana["media_semana"],
                mode="lines+markers+text",
                line=dict(color="black", dash="dash"),
                marker=dict(color="black", size=7),
                text=df_semana["media_label"],
                textposition="bottom center",
                name="Média da Semana" if i == 1 else None,
                showlegend=(i == 1),
                hovertemplate="Dia: %{x}<br>Média semana: %{y:.2f}<extra></extra>",
            ),
            row=1,
            col=i
        )

        fig.update_xaxes(tickangle=-25, row=1, col=i)

    fig.update_layout(
        height=380,
        margin=dict(l=20, r=20, t=60, b=20),
        plot_bgcolor="white",
        paper_bgcolor="white",
        legend=dict(orientation="v", x=1.01, y=1)
    )

    fig.update_yaxes(
        title_text="Litros",
        showgrid=True,
        gridcolor="rgba(148,163,184,0.25)"
    )

    st.plotly_chart(fig, use_container_width=True)

# =========================================================
# SESSION STATE
# =========================================================
def inicializar_filtros():
    if "filtros_aplicados" not in st.session_state:
        st.session_state["filtros_aplicados"] = {
            "cliente": [],
            "origem": [],
            "produto_of": [],
            "embalagem": [],
            "codigo_produto_pedido": [],
            "tipo_material": [],
            "nw_data_inicio": None,
            "nw_data_fim": None,
            "prev_entrega_inicio": None,
            "prev_entrega_fim": None,
        }


def limpar_filtros():
    st.session_state["filtros_aplicados"] = {
        "cliente": [],
        "origem": [],
        "produto_of": [],
        "embalagem": [],
        "codigo_produto_pedido": [],
        "tipo_material": [],
        "nw_data_inicio": None,
        "nw_data_fim": None,
        "prev_entrega_inicio": None,
        "prev_entrega_fim": None,
    }


if "mensagem_salvo" not in st.session_state:
    st.session_state["mensagem_salvo"] = ""

inicializar_filtros()

if st.session_state["mensagem_salvo"]:
    st.success(st.session_state["mensagem_salvo"])
    st.session_state["mensagem_salvo"] = ""

# =========================================================
# APP
# =========================================================
try:
    df = obter_dados()

    colunas_esperadas = [
        "desc_cliente",
        "origem",
        "data_abertura",
        "data_prev_entrega",
        "status_of",
        "produto",
        "codigo_produto",
        "Produto_Ped",
        "embalagem",
        "tipo_material",
        "litros",
        "nro_of",
        "chave_of",
        "chave_pedido",
        "status_vinculo",
        "sequencia_atual",
        "desc_operacao_atual",
        "operacoes_percorridas",
        "qtde"
    ]

    for col in colunas_esperadas:
        if col not in df.columns:
            df[col] = 0 if col == "sequencia_atual" else ""

    df_banco = carregar_banco_txt()
    df = aplicar_banco_txt(df, df_banco)
    df = garantir_colunas_novas(df)
    df = ordenar_dataframe(df)

    st.subheader("Ordens de fabricação abertas")

    # =====================================================
    # SIDEBAR
    # =====================================================
    with st.sidebar:
        if os.path.exists(ARQUIVO_LOGO_SIDEBAR):
            st.image(ARQUIVO_LOGO_SIDEBAR, use_container_width=True)
        else:
            st.warning(f"Logo não encontrada: {ARQUIVO_LOGO_SIDEBAR}")

        st.markdown("## Filtros")

        opcoes_cliente = lista_multiselect(df, "desc_cliente")
        opcoes_origem = lista_multiselect(df, "origem")
        opcoes_produto_of = lista_multiselect(df, "produto")
        opcoes_embalagem = lista_multiselect(df, "embalagem")
        opcoes_codigo_produto = lista_multiselect(df, "codigo_produto")
        opcoes_tipo_material = lista_multiselect(df, "tipo_material")

        filtros_atuais = st.session_state["filtros_aplicados"]

        with st.form("form_filtros_sidebar", clear_on_submit=False):
            filtro_cliente = st.multiselect("Cliente", opcoes_cliente, default=filtros_atuais["cliente"])
            filtro_origem = st.multiselect("Origem", opcoes_origem, default=filtros_atuais["origem"])
            filtro_produto_of = st.multiselect("Produto OF", opcoes_produto_of, default=filtros_atuais["produto_of"])
            filtro_embalagem = st.multiselect("Embalagem", opcoes_embalagem, default=filtros_atuais["embalagem"])
            filtro_codigo_produto_pedido = st.multiselect(
                "Código produto pedido",
                opcoes_codigo_produto,
                default=filtros_atuais["codigo_produto_pedido"]
            )
            filtro_tipo_material = st.multiselect(
                "Tipo material",
                opcoes_tipo_material,
                default=filtros_atuais["tipo_material"]
            )

            st.markdown("### Nw_Datta")
            filtro_nw_data_inicio = st.date_input(
                "Nw_Datta início",
                value=filtros_atuais["nw_data_inicio"],
                format="DD/MM/YYYY"
            )
            filtro_nw_data_fim = st.date_input(
                "Nw_Datta final",
                value=filtros_atuais["nw_data_fim"],
                format="DD/MM/YYYY"
            )

            st.markdown("### Previsão Entrega")
            filtro_prev_entrega_inicio = st.date_input(
                "Previsão Entrega início",
                value=filtros_atuais["prev_entrega_inicio"],
                format="DD/MM/YYYY"
            )
            filtro_prev_entrega_fim = st.date_input(
                "Previsão Entrega final",
                value=filtros_atuais["prev_entrega_fim"],
                format="DD/MM/YYYY"
            )

            col_btn1, col_btn2 = st.columns(2)
            aplicar_filtros = col_btn1.form_submit_button("Aplicar filtros", use_container_width=True)
            limpar = col_btn2.form_submit_button("Limpar filtros", use_container_width=True)

        if aplicar_filtros:
            st.session_state["filtros_aplicados"] = {
                "cliente": filtro_cliente,
                "origem": filtro_origem,
                "produto_of": filtro_produto_of,
                "embalagem": filtro_embalagem,
                "codigo_produto_pedido": filtro_codigo_produto_pedido,
                "tipo_material": filtro_tipo_material,
                "nw_data_inicio": filtro_nw_data_inicio,
                "nw_data_fim": filtro_nw_data_fim,
                "prev_entrega_inicio": filtro_prev_entrega_inicio,
                "prev_entrega_fim": filtro_prev_entrega_fim,
            }

        if limpar:
            limpar_filtros()
            st.rerun()

    # =====================================================
    # FILTROS APLICADOS
    # =====================================================
    filtros = st.session_state["filtros_aplicados"]

    df_filtrado = df.copy()
    df_filtrado = aplicar_filtro_multiselect(df_filtrado, "desc_cliente", filtros["cliente"])
    df_filtrado = aplicar_filtro_multiselect(df_filtrado, "origem", filtros["origem"])
    df_filtrado = aplicar_filtro_multiselect(df_filtrado, "produto", filtros["produto_of"])
    df_filtrado = aplicar_filtro_multiselect(df_filtrado, "embalagem", filtros["embalagem"])
    df_filtrado = aplicar_filtro_multiselect(df_filtrado, "codigo_produto", filtros["codigo_produto_pedido"])
    df_filtrado = aplicar_filtro_multiselect(df_filtrado, "tipo_material", filtros["tipo_material"])

    if filtros["nw_data_inicio"] is not None:
        df_filtrado = aplicar_filtro_data(df_filtrado, "Nw_Data", data_inicio=filtros["nw_data_inicio"])

    if filtros["nw_data_fim"] is not None:
        df_filtrado = aplicar_filtro_data(df_filtrado, "Nw_Data", data_fim=filtros["nw_data_fim"])

    if filtros["prev_entrega_inicio"] is not None:
        df_filtrado = aplicar_filtro_data(df_filtrado, "data_prev_entrega", data_inicio=filtros["prev_entrega_inicio"])

    if filtros["prev_entrega_fim"] is not None:
        df_filtrado = aplicar_filtro_data(df_filtrado, "data_prev_entrega", data_fim=filtros["prev_entrega_fim"])

    df_filtrado = ordenar_dataframe(df_filtrado)

    # gráfico
    df_fechadas = obter_dados_fechadas()
    df_fechadas_filtrado = aplicar_mesmos_filtros_fechadas(df_fechadas, filtros)
    df_producao_diaria = preparar_producao_diaria_semanal(df_fechadas_filtrado)

    # novas colunas
    df_filtrado["PrzReal"] = df_filtrado.apply(
        lambda row: calcular_przreal(
            row.get("data_abertura"),
            row.get("data_prev_entrega"),
            row.get("Nw_Data")
        ),
        axis=1
    )

    df_filtrado["status_prod"] = df_filtrado.apply(
        lambda row: calcular_status_prod(
            row.get("status_of"),
            row.get("data_prev_entrega"),
            row.get("Nw_Data")
        ),
        axis=1
    )

    # =====================================================
    # MÉTRICAS
    # =====================================================
    total_ofs = len(df_filtrado)
    total_qtde = pd.to_numeric(df_filtrado["qtde"], errors="coerce").fillna(0).sum()
    total_litros = pd.to_numeric(df_filtrado["litros"], errors="coerce").fillna(0).sum()

    df_litros_dia = df_filtrado.copy()
    df_litros_dia["data_abertura_dt"] = pd.to_datetime(df_litros_dia["data_abertura"], errors="coerce")
    df_litros_dia["litros_num"] = pd.to_numeric(df_litros_dia["litros"], errors="coerce").fillna(0)

    df_litros_validos = df_litros_dia.dropna(subset=["data_abertura_dt"]).copy()

    if not df_litros_validos.empty:
        ultima_data = df_litros_validos["data_abertura_dt"].max()
        litros_ultimo_dia = df_litros_validos.loc[
            df_litros_validos["data_abertura_dt"] == ultima_data, "litros_num"
        ].sum()
        texto_litros_ultimo_dia = f"{formatar_numero_br(litros_ultimo_dia, 3)} no último dia | {ultima_data.strftime('%d/%m/%Y')}"
    else:
        texto_litros_ultimo_dia = "-"

    df_qtde_dia = df_filtrado.copy()
    df_qtde_dia["data_abertura_dt"] = pd.to_datetime(df_qtde_dia["data_abertura"], errors="coerce")
    df_qtde_dia["qtde_num"] = pd.to_numeric(df_qtde_dia["qtde"], errors="coerce").fillna(0)

    df_qtde_validos = df_qtde_dia.dropna(subset=["data_abertura_dt"]).copy()

    if not df_qtde_validos.empty:
        ultima_data_qtde = df_qtde_validos["data_abertura_dt"].max()
        qtde_ultimo_dia = df_qtde_validos.loc[
            df_qtde_validos["data_abertura_dt"] == ultima_data_qtde, "qtde_num"
        ].sum()
        texto_qtde_ultimo_dia = f"{formatar_numero_br(qtde_ultimo_dia, 3)} no último dia | {ultima_data_qtde.strftime('%d/%m/%Y')}"
    else:
        texto_qtde_ultimo_dia = "-"

    df_of_dia = df_filtrado.copy()
    df_of_dia["data_abertura_dt"] = pd.to_datetime(df_of_dia["data_abertura"], errors="coerce")
    df_of_validos = df_of_dia.dropna(subset=["data_abertura_dt"]).copy()

    if not df_of_validos.empty:
        ultima_data_of = df_of_validos["data_abertura_dt"].max()
        ofs_ultimo_dia = df_of_validos.loc[df_of_validos["data_abertura_dt"] == ultima_data_of].shape[0]
        texto_of_ultimo_dia = f"{ofs_ultimo_dia} no último dia | {ultima_data_of.strftime('%d/%m/%Y')}"
    else:
        texto_of_ultimo_dia = "-"

    # =====================================================
    # FAIXAS DE LITROS POR OF
    # =====================================================
    df_faixas = df_filtrado.copy()
    df_faixas["nro_of"] = df_faixas["nro_of"].fillna("").astype(str).str.strip()
    df_faixas["tipo_material"] = df_faixas["tipo_material"].fillna("").astype(str).str.strip().str.upper()
    df_faixas["litros_num"] = pd.to_numeric(df_faixas["litros"], errors="coerce").fillna(0)

    df_faixas = df_faixas[
        (df_faixas["nro_of"] != "") &
        (df_faixas["tipo_material"] != "PA")
    ].copy()

    litros_por_of = (
        df_faixas.groupby("nro_of", as_index=False)["litros_num"]
        .sum()
        .rename(columns={"litros_num": "litros_of"})
    )

    qtd_of_ate_49 = int((litros_por_of["litros_of"] <= 49.99).sum())
    qtd_of_50_200 = int(((litros_por_of["litros_of"] >= 50) & (litros_por_of["litros_of"] <= 200)).sum())
    qtd_of_200_500 = int(((litros_por_of["litros_of"] > 200) & (litros_por_of["litros_of"] <= 500)).sum())
    qtd_of_maior_500 = int((litros_por_of["litros_of"] > 500).sum())

    c1, c2, c3, c4 = st.columns(4)

    with c1:
        render_card_kpi(
            titulo="Total de OFs abertas",
            valor=f"{total_ofs}",
            delta="",
            extra=texto_of_ultimo_dia,
            classe="card-kpi-verde"
        )

    with c2:
        render_card_kpi(
            titulo="Quantidade total",
            valor=formatar_numero_br(total_qtde, 3),
            delta="",
            extra=texto_qtde_ultimo_dia,
            classe="card-kpi-azul"
        )

    with c3:
        render_card_kpi(
            titulo="Litros totais",
            valor=formatar_numero_br(total_litros, 3),
            delta="",
            extra=texto_litros_ultimo_dia,
            classe="card-kpi-amarelo"
        )

    with c4:
        render_card_faixas_litros(
            qtd_of_ate_49,
            qtd_of_50_200,
            qtd_of_200_500,
            qtd_of_maior_500
        )

    # =====================================================
    # LEGENDA
    # =====================================================
    st.markdown("### Legenda do status de produção")

    leg1, leg2, leg3 = st.columns(3)

    with leg1:
        st.markdown("""
        <div style="background-color: #f8d7da; border: 1px solid #f1aeb5; border-radius: 10px;
        padding: 10px 12px; font-weight: 600; color: #842029;">🔴 = PRODUÇÃO ATRASADA</div>
        """, unsafe_allow_html=True)

    with leg2:
        st.markdown("""
        <div style="background-color: #fff3cd; border: 1px solid #ffe69c; border-radius: 10px;
        padding: 10px 12px; font-weight: 600; color: #664d03;">🟡 = DATA DE ENTREGA</div>
        """, unsafe_allow_html=True)

    with leg3:
        st.markdown("""
        <div style="background-color: #d1e7dd; border: 1px solid #a3cfbb; border-radius: 10px;
        padding: 10px 12px; font-weight: 600; color: #0f5132;">🟢 = DENTRO DO PRAZO</div>
        """, unsafe_allow_html=True)

    with st.expander("Grafico Semanal"):
        render_grafico_producao_diaria_semanal(df_producao_diaria)

    # =====================================================
    # PREPARAÇÃO EXIBIÇÃO
    # =====================================================
    df_exibicao = df_filtrado.copy()

    df_exibicao["data_abertura"] = pd.to_datetime(df_exibicao["data_abertura"], errors="coerce").dt.strftime("%d/%m/%Y")
    df_exibicao["data_prev_entrega"] = pd.to_datetime(df_exibicao["data_prev_entrega"], errors="coerce").dt.strftime("%d/%m/%Y")

    for col in ["Prioridade", "Nw_Data", "Dt.Subida", "status_prod"]:
        df_exibicao[col] = df_exibicao[col].fillna("").astype(str)

    df_exibicao["PrzReal"] = pd.to_numeric(df_exibicao["PrzReal"], errors="coerce").apply(
        lambda x: int(x) if pd.notna(x) else ""
    )

    df_exibicao["Remover"] = df_exibicao["Remover"].fillna(False).astype(bool)

    df_exibicao["qtde"] = pd.to_numeric(df_exibicao["qtde"], errors="coerce").apply(
        lambda x: formatar_numero_br(x, 3) if pd.notna(x) else ""
    )

    df_exibicao["litros"] = pd.to_numeric(df_exibicao["litros"], errors="coerce").apply(
        lambda x: formatar_numero_br(x, 3) if pd.notna(x) else ""
    )

    df_exibicao["sequencia_atual"] = pd.to_numeric(df_exibicao["sequencia_atual"], errors="coerce").fillna(0).astype(int).astype(str)

    colunas_texto = [
        "codigo_produto",
        "Produto_Ped",
        "embalagem",
        "tipo_material",
        "nro_of",
        "chave_of",
        "chave_pedido",
        "status_vinculo",
        "desc_operacao_atual",
        "operacoes_percorridas",
        "Prioridade",
        "Nw_Data",
        "Dt.Subida",
    ]

    for col in colunas_texto:
        if col in df_exibicao.columns:
            df_exibicao[col] = df_exibicao[col].fillna("").astype(str)

    colunas_exibicao = [
        "Remover",
        "Prioridade",
        "Nw_Data",
        "Dt.Subida",
        "PrzReal",
        "status_prod",
        "desc_cliente",
        "origem",
        "data_abertura",
        "data_prev_entrega",
        "status_of",
        "produto",
        "codigo_produto",
        "Produto_Ped",
        "embalagem",
        "tipo_material",
        "litros",
        "nro_of",
        "sequencia_atual",
        "desc_operacao_atual",
        "operacoes_percorridas",
        "qtde"
    ]

    colunas_exibicao = [c for c in colunas_exibicao if c in df_exibicao.columns]
    df_exibicao = df_exibicao[colunas_exibicao]

    df_exibicao = df_exibicao.rename(columns={
        "Remover": "Remover",
        "Prioridade": "Prioridade",
        "Nw_Data": "Nw_Data",
        "Dt.Subida": "Dt.Subida",
        "PrzReal": "PrzReal",
        "status_prod": "status_prod",
        "desc_cliente": "Cliente",
        "origem": "Origem",
        "data_abertura": "Abertura",
        "data_prev_entrega": "Previsão Entrega",
        "status_of": "Status",
        "produto": "Produto OF",
        "codigo_produto": "Código Produto Pedido",
        "Produto_Ped": "Produto Pedido",
        "embalagem": "Embalagem",
        "tipo_material": "Tipo Material",
        "litros": "Litros",
        "nro_of": "Nro OF",
        "sequencia_atual": "Seq. Atual",
        "desc_operacao_atual": "Operação Atual",
        "operacoes_percorridas": "Operações Percorridas",
        "qtde": "Quantidade"
    })

    st.markdown("### Edição das informações")

    # =====================================================
    # AGGRID
    # =====================================================
    df_grid = df_exibicao.copy()

    gb = GridOptionsBuilder.from_dataframe(df_grid)
    gb.configure_default_column(editable=False, filter=True, sortable=True, resizable=True)
    gb.configure_grid_options(domLayout="normal", rowHeight=34)

    gb.configure_column("Remover", editable=True, cellEditor="agCheckboxCellEditor", width=90)
    gb.configure_column("Prioridade", editable=True, width=95)
    gb.configure_column("Nw_Data", editable=True, width=105)
    gb.configure_column("Dt.Subida", editable=True, width=105)

    gb.configure_column("status_prod", width=105)
    gb.configure_column("PrzReal", width=85)
    gb.configure_column("Cliente", width=240)
    gb.configure_column("Produto OF", width=120)
    gb.configure_column("Código Produto Pedido", width=150)
    gb.configure_column("Produto Pedido", width=130)
    gb.configure_column("Operações Percorridas", width=180)

    row_style = JsCode("""
    function(params) {
        const status = (params.data.status_prod || "").toString().trim();

        if (status === "🔴") {
            return {
                'backgroundColor': '#fdf0f2'
            };
        }
        if (status === "🟡") {
            return {
                'backgroundColor': '#fffbea'
            };
        }
        if (status === "🟢") {
            return {
                'backgroundColor': '#f1fbf5'
            };
        }
        return {};
    }
    """)

    gb.configure_grid_options(getRowStyle=row_style)

    grid_options = gb.build()

    grid_response = AgGrid(
        df_grid,
        gridOptions=grid_options,
        update_mode=GridUpdateMode.VALUE_CHANGED,
        data_return_mode="AS_INPUT",
        fit_columns_on_grid_load=False,
        allow_unsafe_jscode=True,
        enable_enterprise_modules=False,
        theme="streamlit",
        height=420,
        reload_data=False
    )

    df_editado = pd.DataFrame(grid_response["data"])

    if st.button("💾 Salvar banco.txt"):
        df_para_salvar = df_editado.copy()

        if "Prioridade" in df_para_salvar.columns:
            df_para_salvar["Prioridade"] = (
                df_para_salvar["Prioridade"]
                .fillna("")
                .astype(str)
                .str.strip()
                .str.upper()
            )

        if "Remover" in df_para_salvar.columns:
            df_para_salvar["Remover"] = df_para_salvar["Remover"].fillna(False).astype(bool)

        erros = []

        for idx, row in df_para_salvar.iterrows():
            remover = bool(row.get("Remover", False))
            prioridade = str(row.get("Prioridade", "")).strip().upper()
            nw_data = str(row.get("Nw_Data", "")).strip()
            dt_subida = str(row.get("Dt.Subida", "")).strip()
            nro_of = row.get("Nro OF", "")
            produto_of = row.get("Produto OF", "")

            if remover:
                continue

            if prioridade != "" and not re.match(r"^P\d+$", prioridade):
                erros.append(
                    f"Linha {idx + 1}: Prioridade inválida para OF {nro_of} / {produto_of}. Use P01, P02, P03..."
                )

            if not validar_data_br(nw_data):
                erros.append(
                    f"Linha {idx + 1}: Nw_Data inválida para OF {nro_of} / {produto_of}. Use dd/mm/yyyy."
                )

            if not validar_data_br(dt_subida):
                erros.append(
                    f"Linha {idx + 1}: Dt.Subida inválida para OF {nro_of} / {produto_of}. Use dd/mm/yyyy."
                )

        if erros:
            st.error("Foram encontrados erros de preenchimento.")
            for erro in erros:
                st.write(f"- {erro}")
        else:
            try:
                salvar_banco_txt(df_para_salvar, ARQUIVO_BANCO)
                st.session_state["mensagem_salvo"] = f"Dados gravados e reordenados com sucesso em {ARQUIVO_BANCO}."
                st.rerun()
            except Exception as e:
                st.error(f"Erro ao salvar o arquivo {ARQUIVO_BANCO}: {e}")

    with st.expander("Visualizar conteúdo atual do banco.txt"):
        df_banco_atual = carregar_banco_txt()
        if df_banco_atual.empty:
            st.write("O arquivo banco.txt ainda não possui registros.")
        else:
            st.dataframe(df_banco_atual, use_container_width=True, hide_index=True)

except Exception as e:
    st.error(f"Erro ao carregar os dados: {e}")