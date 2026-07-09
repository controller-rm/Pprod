import pandas as pd


def formatar_data_br(valor):
    if pd.isna(valor):
        return ""
    try:
        return pd.to_datetime(valor).strftime("%d/%m/%Y")
    except Exception:
        return str(valor)


def formatar_numero_br(valor, casas=2):
    if pd.isna(valor):
        return ""
    try:
        return f"{float(valor):,.{casas}f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except Exception:
        return str(valor)