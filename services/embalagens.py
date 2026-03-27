import pandas as pd
from database import get_connection
from services.litros import calcular_litros
from services.apontamento import enriquecer_com_apontamentos


def limpar_texto(valor) -> str:
    if pd.isna(valor):
        return ""
    return str(valor).strip()


def somente_base_produto(codigo_produto) -> str:
    """
    Extrai o código base do produto de forma robusta.

    Regras:
    - pega somente o primeiro bloco antes do espaço
    - remove o sufixo após o ponto

    Exemplos:
    600PR008.82 -> 600PR008
    600PR008.82 FUNDO XYZ -> 600PR008
    030CT107 -> 030CT107
    """
    texto = limpar_texto(codigo_produto)
    if texto == "":
        return ""

    texto = texto.split(" ")[0].strip()

    if "." in texto:
        texto = texto.split(".", 1)[0].strip()

    return texto


def extrair_embalagem(codigo_produto) -> str:
    """
    Extrai a embalagem a partir do sufixo após o ponto.

    Exemplos:
    579BR001.82 -> A-82
    030CT025.81 -> A-81
    """
    texto = limpar_texto(codigo_produto)
    if "." not in texto:
        return ""

    sufixo = texto.split(".", 1)[1].strip()
    if sufixo == "":
        return ""

    return f"A-{sufixo}"


def pad_numero_pedido(valor) -> str:
    texto = limpar_texto(valor)
    if texto == "":
        return ""
    try:
        return str(int(float(texto))).zfill(6)
    except Exception:
        return texto.zfill(6)


def pad_sequencia_pedido(valor) -> str:
    texto = limpar_texto(valor)
    if texto == "":
        return "00"

    try:
        numero = int(float(texto))
        if numero == 0:
            return "00"
        return str(numero).zfill(2)
    except Exception:
        return "00" if texto in ("0", "0.0") else texto.zfill(2)


def pad_sequencia_item(valor) -> str:
    texto = limpar_texto(valor)
    if texto == "":
        return ""
    try:
        return str(int(float(texto))).zfill(3)
    except Exception:
        return texto.zfill(3)


def normalizar_nro_of(nro_of) -> str:
    """
    Normaliza nro_of para:
    000040-00-001
    """
    texto = limpar_texto(nro_of).replace(" ", "")
    if texto == "":
        return ""

    partes = texto.split("-")

    if len(partes) == 3:
        p1, p2, p3 = partes

        try:
            p1 = str(int(float(p1))).zfill(6)
        except Exception:
            p1 = p1.zfill(6)

        try:
            p2 = str(int(float(p2))).zfill(2)
        except Exception:
            p2 = p2.zfill(2)

        try:
            p3 = str(int(float(p3))).zfill(3)
        except Exception:
            p3 = p3.zfill(3)

        return f"{p1}-{p2}-{p3}"

    return texto


def montar_chave_itens_pedido(numero_pedido, sequencia_pedido, sequencia_item_pedido, codigo_produto) -> str:
    pedido = pad_numero_pedido(numero_pedido)
    seq_pedido = pad_sequencia_pedido(sequencia_pedido)
    seq_item = pad_sequencia_item(sequencia_item_pedido)
    produto_base = somente_base_produto(codigo_produto)

    return f"{pedido}-{seq_pedido}-{seq_item}-{produto_base}"


def montar_chave_ordem_fabric(nro_of, produto) -> str:
    nro = normalizar_nro_of(nro_of)
    produto_base = somente_base_produto(produto)

    if nro == "" and produto_base == "":
        return ""

    return f"{nro}-{produto_base}"


def carregar_ordens_fabric_abertas_com_embalagem() -> pd.DataFrame:
    conn = get_connection()

    sql_of = """
        SELECT
            codigo_filial,
            numero_da_of,
            data_abertura,
            data_fechamento,
            produto,
            desc_produto,
            qtde,
            qtde_reprovada,
            custo_reprovado,
            qtde_produzida,
            custo_mps,
            custo_sff,
            total_horas,
            custos_mob,
            custo_despesa,
            vlr_requisicoes,
            custo_unitario,
            status_of,
            data_prev_entrega,
            cod_cliente,
            desc_cliente,
            origem,
            desc_origem,
            nro_of
        FROM ORDEM_FABRIC
        WHERE TRIM(COALESCE(status_of, '')) = 'A'
    """

    sql_itens_pedido = """
        SELECT
            codigo_filial,
            numero_pedido,
            sequencia_pedido,
            sequencia_item_pedido,
            situacao_item,
            quantidade_pedida,
            quantidade_pronta,
            quantidade_atendida,
            preco_unitario,
            situacao_tributaria_icms,
            percentual_icms,
            aliquota_reducao_icms,
            base_icms,
            valor_icms,
            percentual_icms_st,
            base_icms_st,
            valor_icms_st,
            situacao_tributaria_ipi,
            percentual_ipi,
            base_ipi,
            valor_ipi,
            percentual_comissao,
            valor_comissao,
            cfop_item,
            numero_oc_cliente,
            prioridade_item_pedido,
            codigo_produto,
            previsao_entrega,
            emitente,
            cliente,
            desc_cliente,
            vendedor,
            desc_vendedor,
            vendedor_ext,
            desc_vendedor_ext,
            nro_pedido,
            cme,
            origem,
            desc_origem,
            data_pedido,
            cod_unico_emp,
            emp_nome_fant,
            ped_prod_int,
            produto_compl,
            grupo_compl,
            subgrupo_compl,
            linha_compl,
            cliente_compl,
            vendedor_compl,
            origem_compl,
            valor_total
        FROM ITENS_PEDIDO
    """

    try:
        df_of = pd.read_sql(sql_of, conn)
        df_itens_pedido = pd.read_sql(sql_itens_pedido, conn)
    finally:
        conn.close()

    return preparar_dataframe_embalagens(df_of, df_itens_pedido)


def preparar_dataframe_embalagens(df_of: pd.DataFrame, df_itens_pedido: pd.DataFrame) -> pd.DataFrame:
    df_of = df_of.copy()
    df_itens_pedido = df_itens_pedido.copy()

    # -------------------------------
    # ORDEM_FABRIC
    # -------------------------------
    colunas_minimas_of = ["nro_of", "produto"]
    for col in colunas_minimas_of:
        if col not in df_of.columns:
            raise KeyError(f"A coluna '{col}' não existe na tabela ORDEM_FABRIC.")

    df_of["nro_of"] = df_of["nro_of"].fillna("").astype(str).str.strip()
    df_of["produto"] = df_of["produto"].fillna("").astype(str).str.strip()
    df_of["desc_cliente"] = df_of.get("desc_cliente", "").fillna("").astype(str).str.strip()
    df_of["origem"] = df_of.get("origem", "").fillna("").astype(str).str.strip()
    df_of["status_of"] = df_of.get("status_of", "").fillna("").astype(str).str.strip()

    df_of["chave_of"] = df_of.apply(
        lambda row: montar_chave_ordem_fabric(
            nro_of=row["nro_of"],
            produto=row["produto"]
        ),
        axis=1
    )

    # -------------------------------
    # ITENS_PEDIDO
    # -------------------------------
    colunas_minimas_itens = [
        "numero_pedido",
        "sequencia_pedido",
        "sequencia_item_pedido",
        "codigo_produto"
    ]

    for col in colunas_minimas_itens:
        if col not in df_itens_pedido.columns:
            raise KeyError(f"A coluna '{col}' não existe na tabela ITENS_PEDIDO.")

    df_itens_pedido["numero_pedido"] = df_itens_pedido["numero_pedido"].fillna("")
    df_itens_pedido["sequencia_pedido"] = df_itens_pedido["sequencia_pedido"].fillna(0)
    df_itens_pedido["sequencia_item_pedido"] = df_itens_pedido["sequencia_item_pedido"].fillna("")
    df_itens_pedido["codigo_produto"] = df_itens_pedido["codigo_produto"].fillna("").astype(str).str.strip()

    df_itens_pedido["produto_base"] = df_itens_pedido["codigo_produto"].apply(somente_base_produto)
    df_itens_pedido["embalagem"] = df_itens_pedido["codigo_produto"].apply(extrair_embalagem)

    df_itens_pedido["chave_pedido"] = df_itens_pedido.apply(
        lambda row: montar_chave_itens_pedido(
            numero_pedido=row["numero_pedido"],
            sequencia_pedido=row["sequencia_pedido"],
            sequencia_item_pedido=row["sequencia_item_pedido"],
            codigo_produto=row["codigo_produto"]
        ),
        axis=1
    )

    df_itens_pedido_vinculo = df_itens_pedido[
        ["chave_pedido", "codigo_produto", "produto_base", "embalagem"]
    ].copy()

    df_itens_pedido_vinculo = df_itens_pedido_vinculo.drop_duplicates(
        subset=["chave_pedido"],
        keep="first"
    )

    # -------------------------------
    # JOIN com pedido
    # -------------------------------
    df_joined = pd.merge(
        df_of,
        df_itens_pedido_vinculo,
        how="left",
        left_on="chave_of",
        right_on="chave_pedido"
    )

    df_joined["Produto_Ped"] = df_joined["codigo_produto"].fillna("").astype(str).str.strip()
    df_joined["embalagem"] = df_joined["embalagem"].fillna("").astype(str).str.strip()
    df_joined["status_vinculo"] = df_joined["codigo_produto"].fillna("").astype(str).str.strip().apply(
        lambda x: "OK" if x else "SEM VÍNCULO"
    )

    # -------------------------------
    # Litros / tipo_material
    # -------------------------------
    df_joined = calcular_litros(df_joined)

    if "tipo_material" not in df_joined.columns:
        df_joined["tipo_material"] = ""

    if "litros" not in df_joined.columns:
        df_joined["litros"] = 0

    df_joined["tipo_material"] = df_joined["tipo_material"].fillna("").astype(str)
    df_joined["litros"] = pd.to_numeric(df_joined["litros"], errors="coerce").fillna(0)

    # -------------------------------
    # Apontamentos
    # -------------------------------
    df_joined = enriquecer_com_apontamentos(df_joined)

    if "sequencia_atual" not in df_joined.columns:
        df_joined["sequencia_atual"] = 0

    if "desc_operacao_atual" not in df_joined.columns:
        df_joined["desc_operacao_atual"] = ""

    if "operacoes_percorridas" not in df_joined.columns:
        df_joined["operacoes_percorridas"] = ""

    # -------------------------------
    # Colunas finais
    # -------------------------------
    colunas_finais = [
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
        "sequencia_atual",
        "desc_operacao_atual",
        "operacoes_percorridas",
        "status_vinculo",
        "qtde"
    ]

    for col in colunas_finais:
        if col not in df_joined.columns:
            df_joined[col] = None

    df_joined = df_joined[colunas_finais].copy()

    df_joined["data_abertura"] = pd.to_datetime(df_joined["data_abertura"], errors="coerce")
    df_joined["data_prev_entrega"] = pd.to_datetime(df_joined["data_prev_entrega"], errors="coerce")
    df_joined["qtde"] = pd.to_numeric(df_joined["qtde"], errors="coerce")
    df_joined["litros"] = pd.to_numeric(df_joined["litros"], errors="coerce").fillna(0)
    df_joined["sequencia_atual"] = pd.to_numeric(df_joined["sequencia_atual"], errors="coerce").fillna(0).astype(int)

    df_joined = df_joined.sort_values(
        by=["data_prev_entrega", "data_abertura", "nro_of"],
        ascending=[True, True, True]
    ).reset_index(drop=True)

    return df_joined