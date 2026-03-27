import pandas as pd
from database import get_connection


def limpar_texto(valor) -> str:
    if pd.isna(valor):
        return ""
    return str(valor).strip()


def somente_base_produto(codigo_produto) -> str:
    """
    Se houver ponto, pega tudo antes do ponto.
    Ex.: 030CT025.81 -> 030CT025
    """
    texto = limpar_texto(codigo_produto)
    if "." in texto:
        return texto.split(".")[0].strip()
    return texto


def pad_numero_pedido(valor) -> str:
    """
    numero_pedido com 6 dígitos.
    Ex.: 40 -> 000040
    """
    texto = limpar_texto(valor)
    if texto == "":
        return ""
    try:
        return str(int(float(texto))).zfill(6)
    except Exception:
        return texto.zfill(6)


def pad_sequencia_pedido(valor) -> str:
    """
    sequencia_pedido:
    se 0 => 00
    senão mantém com 2 dígitos
    Ex.: 0 -> 00 | 5 -> 05 | 50 -> 50
    """
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
    """
    sequencia_item_pedido com 3 dígitos.
    Ex.: 1 -> 001
    """
    texto = limpar_texto(valor)
    if texto == "":
        return ""
    try:
        return str(int(float(texto))).zfill(3)
    except Exception:
        return texto.zfill(3)


def normalizar_nro_of(nro_of) -> str:
    """
    Normaliza nro_of para o padrão:
    000040-00-001

    Aceita formatos como:
    40-0-1
    40-00-001
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


def carregar_ordens_fabric_abertas() -> pd.DataFrame:
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
        FROM SOFTDIB_REALFIX.ORDEM_FABRIC
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
        FROM SOFTDIB_REALFIX.ITENS_PEDIDO
    """

    try:
        df_of = pd.read_sql(sql_of, conn)
        df_itens_pedido = pd.read_sql(sql_itens_pedido, conn)
    finally:
        conn.close()

    return preparar_dataframe_of(df_of, df_itens_pedido)


def preparar_dataframe_of(df_of: pd.DataFrame, df_itens_pedido: pd.DataFrame) -> pd.DataFrame:
    df_of = df_of.copy()
    df_itens_pedido = df_itens_pedido.copy()

    # --------------------------------------------------
    # ORDEM_FABRIC
    # --------------------------------------------------
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

    # --------------------------------------------------
    # ITENS_PEDIDO
    # --------------------------------------------------
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
        ["chave_pedido", "codigo_produto", "produto_base"]
    ].copy()

    df_itens_pedido_vinculo = df_itens_pedido_vinculo.drop_duplicates(
        subset=["chave_pedido"],
        keep="first"
    )

    # --------------------------------------------------
    # JOIN
    # --------------------------------------------------
    df_joined = pd.merge(
        df_of,
        df_itens_pedido_vinculo,
        how="left",
        left_on="chave_of",
        right_on="chave_pedido"
    )

    df_joined["Produto_Ped"] = df_joined["codigo_produto"].fillna("").astype(str).str.strip()
    df_joined["status_vinculo"] = df_joined["codigo_produto"].fillna("").astype(str).str.strip().apply(
        lambda x: "OK" if x else "SEM VÍNCULO"
    )

    # --------------------------------------------------
    # COLUNAS FINAIS
    # --------------------------------------------------
    colunas_finais = [
        "desc_cliente",
        "origem",
        "data_abertura",
        "data_prev_entrega",
        "status_of",
        "produto",
        "codigo_produto",
        "Produto_Ped",
        "nro_of",
        "chave_of",
        "chave_pedido",
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

    df_joined = df_joined.sort_values(
        by=["data_prev_entrega", "data_abertura", "nro_of"],
        ascending=[True, True, True]
    ).reset_index(drop=True)

    return df_joined

import pandas as pd
from database import get_connection


def limpar_texto(valor) -> str:
    if pd.isna(valor):
        return ""
    return str(valor).strip()


def somente_base_produto(codigo_produto) -> str:
    """
    Se houver ponto, pega tudo antes do ponto.
    Ex.: 030CT025.81 -> 030CT025
    """
    texto = limpar_texto(codigo_produto)
    if "." in texto:
        return texto.split(".")[0].strip()
    return texto


def carregar_produtos() -> pd.DataFrame:
    conn = get_connection()

    sql = """
        SELECT
            codigo_produto_material,
            tipo_material,
            peso_especifico
        FROM SOFTDIB_REALFIX.PRODUTO
    """

    try:
        df = pd.read_sql(sql, conn)
    finally:
        conn.close()

    df["codigo_produto_material"] = df["codigo_produto_material"].fillna("").astype(str).str.strip()
    df["tipo_material"] = df["tipo_material"].fillna("").astype(str).str.strip()
    df["peso_especifico"] = pd.to_numeric(df["peso_especifico"], errors="coerce")

    return df


def preparar_produto_info(df_base: pd.DataFrame) -> pd.DataFrame:
    df = df_base.copy()
    df_prod = carregar_produtos()

    df["produto"] = df["produto"].fillna("").astype(str).str.strip()
    df["produto_base"] = df["produto"].apply(somente_base_produto)

    # busca exata
    df_prod_exato = df_prod.rename(columns={
        "codigo_produto_material": "codigo_produto_material_exato",
        "tipo_material": "tipo_material_exato",
        "peso_especifico": "peso_especifico_exato"
    })

    df = pd.merge(
        df,
        df_prod_exato,
        how="left",
        left_on="produto",
        right_on="codigo_produto_material_exato"
    )

    # fallback pela base
    df_prod_base = df_prod.rename(columns={
        "codigo_produto_material": "codigo_produto_material_base",
        "tipo_material": "tipo_material_base",
        "peso_especifico": "peso_especifico_base"
    })

    df = pd.merge(
        df,
        df_prod_base,
        how="left",
        left_on="produto_base",
        right_on="codigo_produto_material_base"
    )

    df["tipo_material"] = df["tipo_material_exato"].where(
        df["tipo_material_exato"].notna() & (df["tipo_material_exato"].astype(str).str.strip() != ""),
        df["tipo_material_base"]
    )

    df["peso_especifico"] = df["peso_especifico_exato"].where(
        df["peso_especifico_exato"].notna(),
        df["peso_especifico_base"]
    )

    df["tipo_material"] = df["tipo_material"].fillna("").astype(str).str.strip()
    df["peso_especifico"] = pd.to_numeric(df["peso_especifico"], errors="coerce")

    return df


def carregar_ofs_fechadas_producao() -> pd.DataFrame:
    """
    Retorna OFs fechadas para análise de produção diária em litros.
    Usa qtde_produzida e data_fechamento.
    """
    conn = get_connection()

    sql = """
        SELECT
            codigo_filial,
            numero_da_of,
            data_abertura,
            data_fechamento,
            produto,
            desc_produto,
            qtde,
            qtde_produzida,
            status_of,
            data_prev_entrega,
            cod_cliente,
            desc_cliente,
            origem,
            desc_origem,
            nro_of
        FROM SOFTDIB_REALFIX.ORDEM_FABRIC
        WHERE TRIM(COALESCE(status_of, '')) = 'F'
          AND data_fechamento IS NOT NULL
    """

    try:
        df = pd.read_sql(sql, conn)
    finally:
        conn.close()

    if df.empty:
        return df

    df["nro_of"] = df["nro_of"].fillna("").astype(str).str.strip()
    df["produto"] = df["produto"].fillna("").astype(str).str.strip()
    df["desc_cliente"] = df.get("desc_cliente", "").fillna("").astype(str).str.strip()
    df["origem"] = df.get("origem", "").fillna("").astype(str).str.strip()
    df["status_of"] = df.get("status_of", "").fillna("").astype(str).str.strip()

    df["data_abertura"] = pd.to_datetime(df["data_abertura"], errors="coerce")
    df["data_fechamento"] = pd.to_datetime(df["data_fechamento"], errors="coerce")
    df["data_prev_entrega"] = pd.to_datetime(df["data_prev_entrega"], errors="coerce")

    df["qtde_produzida"] = pd.to_numeric(df["qtde_produzida"], errors="coerce").fillna(0)

    df = preparar_produto_info(df)

    # litros = qtde_produzida / peso_especifico
    df["litros"] = df.apply(
        lambda row: row["qtde_produzida"] / row["peso_especifico"]
        if pd.notna(row["qtde_produzida"]) and pd.notna(row["peso_especifico"]) and row["peso_especifico"] != 0
        else 0,
        axis=1
    )

    df["litros"] = pd.to_numeric(df["litros"], errors="coerce").fillna(0)

    return df


def preparar_producao_diaria_semanal(df_fechadas: pd.DataFrame) -> pd.DataFrame:
    """
    Prepara base diária por semana.
    Cada linha final = um dia.
    """
    if df_fechadas.empty:
        return pd.DataFrame()

    df = df_fechadas.copy()
    df = df.dropna(subset=["data_fechamento"]).copy()

    if df.empty:
        return pd.DataFrame()

    # cria a coluna explicitamente antes do groupby
    df["data"] = pd.to_datetime(df["data_fechamento"], errors="coerce").dt.normalize()
    df = df.dropna(subset=["data"]).copy()

    if df.empty:
        return pd.DataFrame()

    diario = (
        df.groupby("data", as_index=False)["litros"]
        .sum()
    )

    diario["semana"] = diario["data"].dt.isocalendar().week.astype(int)
    diario["ano"] = diario["data"].dt.isocalendar().year.astype(int)
    diario["dia_semana_num"] = diario["data"].dt.weekday
    diario["dia_label"] = diario["data"].dt.strftime("%d/%m")
    diario["media_semana"] = diario.groupby(["ano", "semana"])["litros"].transform("mean")

    diario["cor_barra"] = diario.apply(
        lambda row: "Acima da média" if row["litros"] >= row["media_semana"] else "Abaixo da média",
        axis=1
    )

    diario = diario.sort_values(["ano", "semana", "data"]).reset_index(drop=True)
    return diario