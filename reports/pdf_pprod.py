import os
from io import BytesIO
from datetime import datetime

import pandas as pd
import matplotlib.pyplot as plt

from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT
from reportlab.lib.pagesizes import A4, landscape
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import cm
from reportlab.platypus import (
    SimpleDocTemplate,
    Paragraph,
    Spacer,
    Table,
    TableStyle,
    Image,
    PageBreak,
)


# =========================================================
# CONFIGURAÇÕES
# =========================================================

TITULO_RELATORIO = "Ordens de Fabricação Abertas"
GERADO_POR = "Controladoria Remota"


# =========================================================
# FORMATADORES
# =========================================================

def formatar_numero_br(valor, casas=3):
    try:
        valor = float(valor)
        return f"{valor:,.{casas}f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except Exception:
        return "0,000"


def limpar_texto(valor):
    if pd.isna(valor):
        return ""
    return str(valor).strip()


def status_legenda(status):
    status = limpar_texto(status)

    if "🔴" in status:
        return "Atrasada"
    if "🟡" in status:
        return "Entrega hoje"
    if "🟢" in status:
        return "No prazo"

    return status


def preparar_data(valor):
    if pd.isna(valor) or valor == "":
        return ""

    try:
        return pd.to_datetime(valor, errors="coerce").strftime("%d/%m/%Y")
    except Exception:
        return str(valor)


# =========================================================
# HEADER / FOOTER
# =========================================================

def desenhar_cabecalho_rodape(canvas, doc):
    canvas.saveState()

    largura, altura = landscape(A4)

    canvas.setFillColor(colors.HexColor("#111827"))
    canvas.setFont("Helvetica-Bold", 10)
    canvas.drawString(1.2 * cm, altura - 0.9 * cm, TITULO_RELATORIO)

    canvas.setFont("Helvetica", 8)
    canvas.setFillColor(colors.HexColor("#6b7280"))
    canvas.drawRightString(
        largura - 1.2 * cm,
        altura - 0.9 * cm,
        f"Página {doc.page}"
    )

    canvas.setStrokeColor(colors.HexColor("#d1d5db"))
    canvas.line(1.2 * cm, altura - 1.15 * cm, largura - 1.2 * cm, altura - 1.15 * cm)

    canvas.setFont("Helvetica", 7)
    canvas.setFillColor(colors.HexColor("#6b7280"))
    canvas.drawString(
        1.2 * cm,
        0.7 * cm,
        f"Gerado em: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')} por: {GERADO_POR}"
    )

    canvas.drawRightString(
        largura - 1.2 * cm,
        0.7 * cm,
        "Controller AI"
    )

    canvas.restoreState()


# =========================================================
# CARDS
# =========================================================

def criar_card(titulo, valor, subtitulo, cor_fundo):
    dados = [
        [Paragraph(f"<b>{titulo}</b>", estilo_card_titulo)],
        [Paragraph(f"<b>{valor}</b>", estilo_card_valor)],
        [Paragraph(subtitulo or "-", estilo_card_subtitulo)],
    ]

    tabela = Table(dados, colWidths=[6.2 * cm], rowHeights=[0.55 * cm, 0.9 * cm, 0.55 * cm])
    tabela.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, -1), cor_fundo),
        ("BOX", (0, 0), (-1, -1), 0.6, colors.HexColor("#cbd5e1")),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("LEFTPADDING", (0, 0), (-1, -1), 8),
        ("RIGHTPADDING", (0, 0), (-1, -1), 8),
    ]))

    return tabela


def criar_cards_resumo(df):
    df = df.copy()

    total_ofs = len(df)

    total_qtde = pd.to_numeric(
        df.get("qtde", df.get("Quantidade", 0)),
        errors="coerce"
    ).fillna(0).sum()

    total_litros = pd.to_numeric(
        df.get("litros", df.get("Litros", 0)),
        errors="coerce"
    ).fillna(0).sum()

    df_faixas = df.copy()

    if "nro_of" not in df_faixas.columns and "Nro OF" in df_faixas.columns:
        df_faixas["nro_of"] = df_faixas["Nro OF"]

    if "tipo_material" not in df_faixas.columns and "Tipo Material" in df_faixas.columns:
        df_faixas["tipo_material"] = df_faixas["Tipo Material"]

    if "litros" not in df_faixas.columns and "Litros" in df_faixas.columns:
        df_faixas["litros"] = (
            df_faixas["Litros"]
            .astype(str)
            .str.replace(".", "", regex=False)
            .str.replace(",", ".", regex=False)
        )

    if "nro_of" in df_faixas.columns and "litros" in df_faixas.columns:
        df_faixas["nro_of"] = df_faixas["nro_of"].fillna("").astype(str).str.strip()
        df_faixas["litros_num"] = pd.to_numeric(df_faixas["litros"], errors="coerce").fillna(0)

        if "tipo_material" in df_faixas.columns:
            df_faixas["tipo_material"] = df_faixas["tipo_material"].fillna("").astype(str).str.upper()
            df_faixas = df_faixas[df_faixas["tipo_material"] != "PA"]

        litros_por_of = df_faixas.groupby("nro_of", as_index=False)["litros_num"].sum()

        qtd_ate_49 = int((litros_por_of["litros_num"] <= 49.99).sum())
        qtd_50_200 = int(((litros_por_of["litros_num"] >= 50) & (litros_por_of["litros_num"] <= 200)).sum())
        qtd_200_500 = int(((litros_por_of["litros_num"] > 200) & (litros_por_of["litros_num"] <= 500)).sum())
        qtd_maior_500 = int((litros_por_of["litros_num"] > 500).sum())
    else:
        qtd_ate_49 = qtd_50_200 = qtd_200_500 = qtd_maior_500 = 0

    card_1 = criar_card(
        "Total de OFs abertas",
        str(total_ofs),
        "",
        colors.HexColor("#d1e7dd")
    )

    card_2 = criar_card(
        "Quantidade total",
        formatar_numero_br(total_qtde, 3),
        "",
        colors.HexColor("#dbeafe")
    )

    card_3 = criar_card(
        "Litros totais",
        formatar_numero_br(total_litros, 3),
        "",
        colors.HexColor("#fef3c7")
    )

    faixas = Table([
        [Paragraph("<b>Posição Litros por OF</b>", estilo_card_titulo)],
        [f"Até 49,99 litros", str(qtd_ate_49)],
        [f"50 a 200 litros", str(qtd_50_200)],
        [f"200,01 a 500 litros", str(qtd_200_500)],
        [f"Acima de 500 litros", str(qtd_maior_500)],
    ], colWidths=[4.7 * cm, 1.5 * cm])

    faixas.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, -1), colors.HexColor("#ede9fe")),
        ("SPAN", (0, 0), (1, 0)),
        ("BOX", (0, 0), (-1, -1), 0.6, colors.HexColor("#c4b5fd")),
        ("GRID", (0, 1), (-1, -1), 0.25, colors.HexColor("#ddd6fe")),
        ("FONTNAME", (1, 1), (1, -1), "Helvetica-Bold"),
        ("TEXTCOLOR", (1, 1), (1, -1), colors.HexColor("#4c1d95")),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("LEFTPADDING", (0, 0), (-1, -1), 7),
        ("RIGHTPADDING", (0, 0), (-1, -1), 7),
    ]))

    tabela_cards = Table(
        [[card_1, card_2, card_3, faixas]],
        colWidths=[6.5 * cm, 6.5 * cm, 6.5 * cm, 6.7 * cm]
    )

    tabela_cards.setStyle(TableStyle([
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
    ]))

    return tabela_cards


# =========================================================
# FILTROS
# =========================================================

def criar_tabela_filtros(filtros):
    if not filtros:
        return Paragraph("Nenhum filtro aplicado.", estilo_normal)

    linhas = [["Filtro", "Valor"]]

    for chave, valor in filtros.items():
        if isinstance(valor, list):
            valor_formatado = ", ".join(map(str, valor)) if valor else "-"
        elif valor is None:
            valor_formatado = "-"
        else:
            valor_formatado = str(valor)

        linhas.append([chave, valor_formatado])

    tabela = Table(linhas, colWidths=[5 * cm, 20 * cm])

    tabela.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#1f2937")),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("GRID", (0, 0), (-1, -1), 0.25, colors.HexColor("#d1d5db")),
        ("BACKGROUND", (0, 1), (-1, -1), colors.HexColor("#f9fafb")),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("FONTSIZE", (0, 0), (-1, -1), 8),
        ("LEFTPADDING", (0, 0), (-1, -1), 6),
        ("RIGHTPADDING", (0, 0), (-1, -1), 6),
    ]))

    return tabela


# =========================================================
# LEGENDA
# =========================================================

def criar_legenda_status():
    tabela = Table([
        ["🔴 Produção atrasada", "🟡 Data de entrega", "🟢 Dentro do prazo"]
    ], colWidths=[8.6 * cm, 8.6 * cm, 8.6 * cm])

    tabela.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (0, 0), colors.HexColor("#f8d7da")),
        ("BACKGROUND", (1, 0), (1, 0), colors.HexColor("#fff3cd")),
        ("BACKGROUND", (2, 0), (2, 0), colors.HexColor("#d1e7dd")),
        ("BOX", (0, 0), (0, 0), 0.5, colors.HexColor("#f1aeb5")),
        ("BOX", (1, 0), (1, 0), 0.5, colors.HexColor("#ffe69c")),
        ("BOX", (2, 0), (2, 0), 0.5, colors.HexColor("#a3cfbb")),
        ("FONTNAME", (0, 0), (-1, -1), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, -1), 8),
        ("ALIGN", (0, 0), (-1, -1), "CENTER"),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("TOPPADDING", (0, 0), (-1, -1), 7),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 7),
    ]))

    return tabela


# =========================================================
# GRÁFICO
# =========================================================

def criar_grafico_producao(df_producao):
    if df_producao is None or df_producao.empty:
        return None

    import plotly.graph_objects as go
    from plotly.subplots import make_subplots

    df_plot = df_producao.copy()

    if "data" not in df_plot.columns:
        return None

    df_plot["data"] = pd.to_datetime(df_plot["data"], errors="coerce")
    df_plot = df_plot.dropna(subset=["data"]).copy()

    if df_plot.empty:
        return None

    semanas_ordenadas = (
        df_plot[["ano", "semana"]]
        .drop_duplicates()
        .sort_values(["ano", "semana"])
    )

    if semanas_ordenadas.empty:
        return None

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
        df_semana["litros"] = pd.to_numeric(df_semana["litros"], errors="coerce").fillna(0)
        df_semana["media_semana"] = pd.to_numeric(df_semana["media_semana"], errors="coerce").fillna(0)

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
            ),
            row=1,
            col=i
        )

        fig.update_xaxes(tickangle=-25, row=1, col=i)

    fig.update_layout(
        width=1500,
        height=430,
        margin=dict(l=20, r=20, t=60, b=40),
        plot_bgcolor="white",
        paper_bgcolor="white",
        legend=dict(orientation="v", x=1.01, y=1)
    )

    fig.update_yaxes(
        title_text="Litros",
        showgrid=True,
        gridcolor="rgba(148,163,184,0.25)"
    )

    buffer = BytesIO()
    fig.write_image(buffer, format="png", scale=2)
    buffer.seek(0)

    return buffer


# =========================================================
# TABELA PRINCIPAL
# =========================================================

def preparar_tabela_pdf(df):
    df = df.copy()

    colunas_pdf = [
        "Prioridade",
        "Nw_Data",
        "Previsão Entrega",
        "Status",
        "Dt.Subida",
        "status_prod",
        "Nro OF",
        "Cliente",
        "Origem",
        "Produto OF",
        "Embalagem",
        "Litros",
        "Quantidade",
        "Operação Atual",
    ]

    colunas_pdf = [c for c in colunas_pdf if c in df.columns]
    df = df[colunas_pdf].copy()

    if "status_prod" in df.columns:
        df["status_prod"] = df["status_prod"].apply(status_legenda)

    for col in df.columns:
        df[col] = df[col].apply(limpar_texto)

    df = df.rename(columns={
        "Previsão Entrega": "Previsão Entrega",
        "Dt.Subida": "Dt.Subida",
        "status_prod": "Status Prod.",
        "Produto OF": "Produto OF",
        "Operação Atual": "Operação Atual",
    })

    return df


def criar_tabela_dados(df, max_linhas=None):
    df_pdf = preparar_tabela_pdf(df)

    if max_linhas:
        df_pdf = df_pdf.head(max_linhas)

    dados = [list(df_pdf.columns)]

    for _, row in df_pdf.iterrows():
        dados.append([
            Paragraph(str(valor), estilo_tabela)
            for valor in row.tolist()
        ])

    larguras = []

    for col in df_pdf.columns:
        if col == "Cliente":
            larguras.append(5.0 * cm)
        elif col == "Operação":
            larguras.append(4.0 * cm)
        elif col == "Produto":
            larguras.append(2.2 * cm)
        elif col == "Nro OF":
            larguras.append(2.1 * cm)
        elif col in ["Litros", "Quantidade"]:
            larguras.append(1.8 * cm)
        elif col in ["Abertura", "Previsão", "Nw_Data", "Dt.Subida"]:
            larguras.append(1.8 * cm)
        elif col in ["Status Prod."]:
            larguras.append(1.8 * cm)
        else:
            larguras.append(1.3 * cm)

    tabela = Table(dados, colWidths=larguras, repeatRows=1)

    tabela.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#111827")),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, 0), 5.5),
        ("FONTSIZE", (0, 1), (-1, -1), 4.8),
        ("GRID", (0, 0), (-1, -1), 0.25, colors.HexColor("#d1d5db")),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("LEFTPADDING", (0, 0), (-1, -1), 2),
        ("RIGHTPADDING", (0, 0), (-1, -1), 2),
        ("TOPPADDING", (0, 0), (-1, -1), 2),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 2),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [
            colors.white,
            colors.HexColor("#f9fafb")
        ]),
    ]))

    return tabela


# =========================================================
# PDF PRINCIPAL
# =========================================================

def gerar_pdf_pprod(
    df_filtrado,
    df_exibicao=None,
    df_producao_diaria=None,
    df_producao=None,
    filtros=None,
    logo_path=None,
    max_linhas_tabela=None
):
    if df_producao_diaria is None and df_producao is not None:
        df_producao_diaria = df_producao

    if df_exibicao is None:
        df_exibicao = df_filtrado.copy()

    buffer = BytesIO()

    doc = SimpleDocTemplate(
        buffer,
        pagesize=landscape(A4),
        rightMargin=1.1 * cm,
        leftMargin=1.1 * cm,
        topMargin=1.5 * cm,
        bottomMargin=1.2 * cm,
    )

    elementos = []

    # Título
    elementos.append(Paragraph("Controller AI", estilo_titulo))
    elementos.append(Paragraph(TITULO_RELATORIO, estilo_subtitulo))
    elementos.append(Spacer(1, 0.25 * cm))

    # Logo opcional
    if logo_path and os.path.exists(logo_path):
        try:
            elementos.append(Image(logo_path, width=3.5 * cm, height=1.5 * cm))
            elementos.append(Spacer(1, 0.2 * cm))
        except Exception:
            pass

    # Resumo
    elementos.append(Paragraph("Resumo Executivo", estilo_secao))
    elementos.append(criar_cards_resumo(df_filtrado))
    elementos.append(Spacer(1, 0.35 * cm))

    # Legenda
    elementos.append(Paragraph("Legenda do status de produção", estilo_secao))
    elementos.append(criar_legenda_status())
    elementos.append(Spacer(1, 0.35 * cm))

    # Filtros
    elementos.append(Paragraph("Filtros aplicados", estilo_secao))
    elementos.append(criar_tabela_filtros(filtros or {}))
    elementos.append(Spacer(1, 0.35 * cm))

    # Gráfico
    grafico = criar_grafico_producao(df_producao_diaria)

    if grafico:
        elementos.append(Paragraph("Gráfico semanal", estilo_secao))
        elementos.append(Image(grafico, width=24 * cm, height=7 * cm))
        elementos.append(Spacer(1, 0.4 * cm))

    # Tabela
    elementos.append(PageBreak())
    elementos.append(Paragraph("Detalhamento das Ordens de Fabricação", estilo_secao))
    elementos.append(Spacer(1, 0.2 * cm))
    elementos.append(criar_tabela_dados(df_exibicao, max_linhas=max_linhas_tabela))

    doc.build(
        elementos,
        onFirstPage=desenhar_cabecalho_rodape,
        onLaterPages=desenhar_cabecalho_rodape
    )

    buffer.seek(0)
    return buffer.getvalue()


# =========================================================
# ESTILOS
# =========================================================

styles = getSampleStyleSheet()

estilo_titulo = ParagraphStyle(
    "estilo_titulo",
    parent=styles["Title"],
    fontName="Helvetica-Bold",
    fontSize=18,
    alignment=TA_CENTER,
    textColor=colors.HexColor("#111827"),
    spaceAfter=4,
)

estilo_subtitulo = ParagraphStyle(
    "estilo_subtitulo",
    parent=styles["Normal"],
    fontName="Helvetica",
    fontSize=11,
    alignment=TA_CENTER,
    textColor=colors.HexColor("#374151"),
    spaceAfter=12,
)

estilo_secao = ParagraphStyle(
    "estilo_secao",
    parent=styles["Heading2"],
    fontName="Helvetica-Bold",
    fontSize=11,
    textColor=colors.HexColor("#111827"),
    spaceBefore=6,
    spaceAfter=6,
)

estilo_normal = ParagraphStyle(
    "estilo_normal",
    parent=styles["Normal"],
    fontName="Helvetica",
    fontSize=8,
    textColor=colors.HexColor("#111827"),
)

estilo_card_titulo = ParagraphStyle(
    "estilo_card_titulo",
    parent=styles["Normal"],
    fontName="Helvetica-Bold",
    fontSize=8,
    textColor=colors.HexColor("#111827"),
)

estilo_card_valor = ParagraphStyle(
    "estilo_card_valor",
    parent=styles["Normal"],
    fontName="Helvetica-Bold",
    fontSize=15,
    textColor=colors.HexColor("#000000"),
)

estilo_card_subtitulo = ParagraphStyle(
    "estilo_card_subtitulo",
    parent=styles["Normal"],
    fontName="Helvetica",
    fontSize=6.5,
    alignment=TA_RIGHT,
    textColor=colors.HexColor("#374151"),
)

estilo_tabela = ParagraphStyle(
    "estilo_tabela",
    parent=styles["Normal"],
    fontName="Helvetica",
    fontSize=5.5,
    leading=6.5,
    textColor=colors.HexColor("#111827"),
)