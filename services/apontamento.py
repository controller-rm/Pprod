import pandas as pd
from database import get_connection


def limpar_texto(valor) -> str:
    if pd.isna(valor):
        return ""
    return str(valor).strip()


def extrair_codigo_base(produto) -> str:
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
    texto = limpar_texto(produto)
    if texto == "":
        return ""

    texto = texto.split(" ")[0].strip()

    if "." in texto:
        texto = texto.split(".", 1)[0].strip()

    return texto


def normalizar_numero_of(numero_of) -> str:
    """
    Normaliza numero_of para o padrão:
    039015-00-002

    Exemplos:
    39015-00-002 -> 039015-00-002
    39015-0-2    -> 039015-00-002
    """
    texto = limpar_texto(numero_of).replace(" ", "")
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


def montar_chave_apontamento(numero_of, produto) -> str:
    """
    Gera a chave no mesmo padrão da chave_of do painel:
    numero_of_normalizado + "-" + produto_base

    Exemplo:
    2010-00-016 + 038CZ021 -> 002010-00-016-038CZ021
    """
    numero_normalizado = normalizar_numero_of(numero_of)
    produto_base = extrair_codigo_base(produto)

    if not numero_normalizado and not produto_base:
        return ""

    return f"{numero_normalizado}-{produto_base}"


def carregar_apontamentos() -> pd.DataFrame:
    conn = get_connection()

    sql = """
        SELECT
            numero_of,
            produto,
            sequencia_of,
            desc_operacao
        FROM APONTAMENTO
    """

    try:
        df = pd.read_sql(sql, conn)
    finally:
        conn.close()

    return df


def consolidar_apontamentos(df_apontamento: pd.DataFrame) -> pd.DataFrame:
    df = df_apontamento.copy()

    colunas_minimas = ["numero_of", "produto", "sequencia_of", "desc_operacao"]
    for col in colunas_minimas:
        if col not in df.columns:
            raise KeyError(f"A coluna '{col}' não existe na tabela APONTAMENTO.")

    df["numero_of"] = df["numero_of"].fillna("").astype(str).str.strip()
    df["produto"] = df["produto"].fillna("").astype(str).str.strip()
    df["desc_operacao"] = df["desc_operacao"].fillna("").astype(str).str.strip()
    df["sequencia_of"] = pd.to_numeric(df["sequencia_of"], errors="coerce").fillna(0).astype(int)

    df["chave_of"] = df.apply(
        lambda row: montar_chave_apontamento(
            numero_of=row["numero_of"],
            produto=row["produto"]
        ),
        axis=1
    )

    df = df[df["chave_of"] != ""].copy()

    df = df.sort_values(
        by=["chave_of", "sequencia_of"],
        ascending=[True, True]
    ).reset_index(drop=True)

    def montar_fluxo_operacoes(subdf: pd.DataFrame) -> str:
        pares = []
        vistos = set()

        for _, row in subdf.iterrows():
            seq = int(row["sequencia_of"]) if pd.notna(row["sequencia_of"]) else 0
            oper = limpar_texto(row["desc_operacao"])
            if not oper:
                continue

            chave = (seq, oper)
            if chave not in vistos:
                vistos.add(chave)
                pares.append(f"{seq} - {oper}")

        return " → ".join(pares)

    df_fluxo = (
        df.groupby("chave_of", as_index=False)
        .apply(lambda g: pd.Series({
            "operacoes_percorridas": montar_fluxo_operacoes(g)
        }))
        .reset_index(drop=True)
    )

    idx_ultimos = df.groupby("chave_of")["sequencia_of"].idxmax()
    df_ultimos = df.loc[idx_ultimos, ["chave_of", "sequencia_of", "desc_operacao"]].copy()
    df_ultimos = df_ultimos.rename(columns={
        "sequencia_of": "sequencia_atual",
        "desc_operacao": "desc_operacao_atual"
    })

    df_final = pd.merge(
        df_fluxo,
        df_ultimos,
        how="left",
        on="chave_of"
    )

    df_final["sequencia_atual"] = pd.to_numeric(df_final["sequencia_atual"], errors="coerce").fillna(0).astype(int)
    df_final["desc_operacao_atual"] = df_final["desc_operacao_atual"].fillna("").astype(str)
    df_final["operacoes_percorridas"] = df_final["operacoes_percorridas"].fillna("").astype(str)

    return df_final


def enriquecer_com_apontamentos(df_base: pd.DataFrame) -> pd.DataFrame:
    """
    Recebe o dataframe do painel, que já possui a coluna chave_of,
    e adiciona:
    - sequencia_atual
    - desc_operacao_atual
    - operacoes_percorridas
    """
    df = df_base.copy()

    if "chave_of" not in df.columns:
        df["sequencia_atual"] = 0
        df["desc_operacao_atual"] = ""
        df["operacoes_percorridas"] = ""
        return df

    df_apontamento = carregar_apontamentos()
    df_apontamento = consolidar_apontamentos(df_apontamento)

    df = pd.merge(
        df,
        df_apontamento[["chave_of", "sequencia_atual", "desc_operacao_atual", "operacoes_percorridas"]],
        how="left",
        on="chave_of"
    )

    if "sequencia_atual" not in df.columns:
        df["sequencia_atual"] = 0

    if "desc_operacao_atual" not in df.columns:
        df["desc_operacao_atual"] = ""

    if "operacoes_percorridas" not in df.columns:
        df["operacoes_percorridas"] = ""

    df["sequencia_atual"] = pd.to_numeric(df["sequencia_atual"], errors="coerce").fillna(0).astype(int)
    df["desc_operacao_atual"] = df["desc_operacao_atual"].fillna("").astype(str)
    df["operacoes_percorridas"] = df["operacoes_percorridas"].fillna("").astype(str)

    return df