import pandas as pd
from database import get_connection


def limpar_texto(valor) -> str:
    if pd.isna(valor):
        return ""
    return str(valor).strip()


def somente_base_produto(codigo):
    """
    Remove sufixo após ponto.
    Ex.: 030CT025.81 -> 030CT025
    """
    texto = limpar_texto(codigo)
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

    return df


def calcular_litros(df_base: pd.DataFrame) -> pd.DataFrame:
    df = df_base.copy()

    # --------------------------------------------------
    # Carrega tabela PRODUTO
    # --------------------------------------------------
    df_prod = carregar_produtos()

    df_prod["codigo_produto_material"] = df_prod["codigo_produto_material"].astype(str).str.strip()
    df_prod["tipo_material"] = df_prod["tipo_material"].astype(str).str.strip()
    df_prod["peso_especifico"] = pd.to_numeric(df_prod["peso_especifico"], errors="coerce")

    # --------------------------------------------------
    # Preparar base da OF
    # --------------------------------------------------
    df["produto"] = df["produto"].astype(str).str.strip()
    df["produto_base"] = df["produto"].apply(somente_base_produto)

    # --------------------------------------------------
    # 1) JOIN PELO PRODUTO EXATO
    # --------------------------------------------------
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

    # --------------------------------------------------
    # 2) JOIN PELO PRODUTO BASE
    # --------------------------------------------------
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

    # --------------------------------------------------
    # Prioriza o produto exato; se não achar, usa a base
    # --------------------------------------------------
    df["tipo_material"] = df["tipo_material_exato"].where(
        df["tipo_material_exato"].notna() & (df["tipo_material_exato"].astype(str).str.strip() != ""),
        df["tipo_material_base"]
    )

    df["peso_especifico"] = df["peso_especifico_exato"].where(
        df["peso_especifico_exato"].notna(),
        df["peso_especifico_base"]
    )

    # --------------------------------------------------
    # Cálculo de litros
    # --------------------------------------------------
    df["qtde"] = pd.to_numeric(df["qtde"], errors="coerce")

    df["litros"] = df.apply(
        lambda row: row["qtde"] / row["peso_especifico"]
        if pd.notna(row["qtde"]) and pd.notna(row["peso_especifico"]) and row["peso_especifico"] != 0
        else 0,
        axis=1
    )

    # --------------------------------------------------
    # Limpeza final
    # --------------------------------------------------
    df["tipo_material"] = df["tipo_material"].fillna("").astype(str).str.strip()
    df["litros"] = pd.to_numeric(df["litros"], errors="coerce")

    return df