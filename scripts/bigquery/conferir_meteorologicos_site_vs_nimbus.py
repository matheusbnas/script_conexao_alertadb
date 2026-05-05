#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Conferência: arquivos *_Met.txt (download Alerta Rio) vs. NIMBUS e/ou BigQuery (GCP).

Uso:
  python scripts/bigquery/conferir_meteorologicos_site_vs_nimbus.py --fonte gcp \\
    --estacao-id 11 iraja_202602_Met.txt iraja_202603_Met.txt

Requer .env:
  - GCP: BIGQUERY_PROJECT_ID e credenciais (credentials.json ou BIGQUERY_CREDENTIALS_PATH)
  - NIMBUS: DB_ORIGEM_* (somente se --fonte nimbus ou ambos)
"""

from __future__ import annotations

import argparse
import os
import re
import sys
import unicodedata
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from google.cloud import bigquery
from google.oauth2 import service_account
from sqlalchemy import create_engine
from urllib.parse import quote_plus

project_root = Path(__file__).resolve().parent.parent.parent
load_dotenv(dotenv_path=project_root / ".env")


def obter_variavel(nome: str, obrigatoria: bool = True, padrao: str | None = None) -> str | None:
    valor = os.getenv(nome)
    if not valor or (isinstance(valor, str) and valor.strip() == ""):
        if obrigatoria:
            raise ValueError(f"Variável obrigatória não encontrada: {nome}")
        return padrao
    return valor.strip() if isinstance(valor, str) else valor


def carregar_origem() -> dict:
    return {
        "host": obter_variavel("DB_ORIGEM_HOST"),
        "port": obter_variavel("DB_ORIGEM_PORT", obrigatoria=False, padrao="5432"),
        "dbname": obter_variavel("DB_ORIGEM_NAME"),
        "user": obter_variavel("DB_ORIGEM_USER"),
        "password": obter_variavel("DB_ORIGEM_PASSWORD"),
        "sslmode": obter_variavel("DB_ORIGEM_SSLMODE", obrigatoria=False, padrao="disable"),
        "connect_timeout": 15,
    }


def resolver_caminho_credenciais_gcp() -> Path | None:
    credentials_padrao = project_root / "credentials" / "credentials.json"
    credentials_path_env = obter_variavel("BIGQUERY_CREDENTIALS_PATH", obrigatoria=False)
    if credentials_path_env:
        p = Path(credentials_path_env)
        if p.exists():
            return p
        if credentials_padrao.exists():
            return credentials_padrao
        return None
    if credentials_padrao.exists():
        return credentials_padrao
    return None


def criar_cliente_bigquery():
    project_id = obter_variavel("BIGQUERY_PROJECT_ID")
    cred_path = resolver_caminho_credenciais_gcp()
    if not cred_path:
        raise FileNotFoundError(
            "Credenciais GCP não encontradas. Use credentials/credentials.json "
            "ou BIGQUERY_CREDENTIALS_PATH no .env"
        )
    cred = service_account.Credentials.from_service_account_file(str(cred_path))
    return bigquery.Client(project=project_id, credentials=cred), project_id


def carregar_met_bigquery(
    client: bigquery.Client,
    project_id: str,
    dataset_id: str,
    table_id: str,
    estacao_id: int,
    inicio_utc: datetime,
    fim_utc: datetime,
) -> pd.DataFrame:
    """Lê a tabela meteorológicos no BQ no intervalo [inicio, fim)."""
    t0 = pd.Timestamp(inicio_utc).strftime("%Y-%m-%d %H:%M:%S")
    t1 = pd.Timestamp(fim_utc).strftime("%Y-%m-%d %H:%M:%S")
    fqtn = f"`{project_id}.{dataset_id}.{table_id}`"
    # Schema do pipeline usa dirVento / velVento; em algumas cargas o BQ pode expor tudo minúsculo.
    sql_camel = f"""
    SELECT dia_utc, estacao_id, chuva, dirVento, velVento, temperatura, pressao, umidade
    FROM {fqtn}
    WHERE estacao_id = {int(estacao_id)}
      AND dia_utc >= TIMESTAMP('{t0}')
      AND dia_utc < TIMESTAMP('{t1}')
    ORDER BY dia_utc
    """
    sql_lower = f"""
    SELECT dia_utc, estacao_id, chuva, dirvento, velvento, temperatura, pressao, umidade
    FROM {fqtn}
    WHERE estacao_id = {int(estacao_id)}
      AND dia_utc >= TIMESTAMP('{t0}')
      AND dia_utc < TIMESTAMP('{t1}')
    ORDER BY dia_utc
    """
    job = client.query(sql_camel)
    try:
        df = job.to_dataframe()
    except Exception:
        df = client.query(sql_lower).to_dataframe()
    df.columns = [str(c).lower() for c in df.columns]
    if df.empty:
        return df
    df = df.copy()
    df["dia_utc"] = pd.to_datetime(df["dia_utc"], utc=True, errors="coerce")
    df["ts_utc"] = df["dia_utc"].dt.tz_convert("UTC").dt.tz_localize(None)
    for c in ["chuva", "dirvento", "velvento", "temperatura", "pressao", "umidade", "ponto_orvalho"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


def normalizar_header(h: str) -> str:
    h = unicodedata.normalize("NFD", str(h))
    h = "".join(ch for ch in h if unicodedata.category(ch) != "Mn")
    return h.lower().strip()


def _tentar_ler_csv(path: Path, sep: str | None, header: int | None) -> pd.DataFrame | None:
    for enc in ("utf-8-sig", "utf-8", "latin-1", "cp1252"):
        try:
            return pd.read_csv(path, sep=sep, header=header, engine="python", encoding=enc)
        except Exception:
            continue
    return None


_RE_LINHA_MET_ALERTARIO = re.compile(
    r"^(\d{2}/\d{2}/\d{4})\s+(\d{2}:\d{2}:\d{2})\s+(.+)$"
)


def _token_para_float(tok: str) -> float | None:
    t = str(tok).strip().upper()
    if t == "ND" or t == "":
        return None
    try:
        return float(t.replace(",", "."))
    except ValueError:
        return None


def carregar_txt_meteorologico_alerta_rio(path: Path) -> pd.DataFrame:
    """Layout dos TXT *_Met.txt do download (ex.: iraja_202602_Met.txt).

    - Linhas 1–4: título / texto (HBV).
    - Linha com ``Dia ... Hora ... Chuva ...``: cabeçalho; linha seguinte com unidades.
    - Dados: ``DD/MM/AAAA  HH:MM:SS`` + colunas (HBV costuma vir vazio → 6 tokens após data/hora).
    - ``ND`` = sem dado.
    """
    texto: str | None = None
    for enc in ("utf-8-sig", "utf-8", "latin-1", "cp1252"):
        try:
            texto = path.read_text(encoding=enc)
            break
        except Exception:
            continue
    if texto is None:
        raise ValueError(f"Não foi possível decodificar: {path}")

    linhas = texto.splitlines()
    idx_cab = None
    for i, ln in enumerate(linhas):
        s = ln.strip()
        if s.startswith("Dia ") and "Hora" in s and "Chuva" in s:
            idx_cab = i
            break
    if idx_cab is None:
        raise ValueError(f"Cabeçalho 'Dia / Hora / Chuva' não encontrado em: {path}")

    # Pular linha de unidades, ex.: ``(graus)     (Km/h)...``
    i0 = idx_cab + 2
    rows: list[dict] = []
    for ln in linhas[i0:]:
        s = ln.strip()
        if not s:
            continue
        m = _RE_LINHA_MET_ALERTARIO.match(s)
        if not m:
            continue
        dia_s, hora_s, resto = m.group(1), m.group(2), m.group(3)
        toks = resto.split()
        # HBV vazio: chuva, dirvento, velvento, temperatura, pressao, umidade (6 tokens)
        # Com token HBV: 7 tokens (raro neste export)
        if len(toks) == 6:
            chuva, dve, vve, temp, press, umid = toks
        elif len(toks) == 7:
            _hbv, chuva, dve, vve, temp, press, umid = toks
        else:
            continue

        rows.append(
            {
                "dia_txt": dia_s,
                "hora_txt": hora_s,
                "chuva": _token_para_float(chuva),
                "dirvento": _token_para_float(dve),
                "velvento": _token_para_float(vve),
                "temperatura": _token_para_float(temp),
                "pressao": _token_para_float(press),
                "umidade": _token_para_float(umid),
            }
        )

    if not rows:
        raise ValueError(f"Nenhuma linha de dados válida em: {path}")

    out = pd.DataFrame(rows)
    comb = out["dia_txt"].astype(str) + " " + out["hora_txt"].astype(str)
    # Horário de referência do arquivo: America/Sao_Paulo (texto menciona HBV).
    out["ts_site"] = pd.to_datetime(comb, dayfirst=True, errors="coerce")
    out = out.dropna(subset=["ts_site"]).copy()
    if out["ts_site"].dt.tz is None:
        out["ts_utc"] = out["ts_site"].dt.tz_localize(
            "America/Sao_Paulo", ambiguous="NaT", nonexistent="shift_forward"
        )
        out["ts_utc"] = out["ts_utc"].dt.tz_convert("UTC").dt.tz_localize(None)
    else:
        out["ts_utc"] = out["ts_site"].dt.tz_convert("UTC").dt.tz_localize(None)

    out["arquivo"] = path.name
    out = out.drop(columns=["dia_txt", "hora_txt"], errors="ignore")
    return out


def carregar_txt_site(path: Path) -> pd.DataFrame:
    """Lê TXT do site; tenta delimitadores comuns e detecta colunas por nome."""
    if not path.is_file():
        raise FileNotFoundError(path)

    # Export texto fixo do Sistema Alerta Rio (*_Met.txt)
    try:
        amostra = path.read_text(encoding="utf-8", errors="ignore")[:800]
    except Exception:
        amostra = ""
    if "Relatório Meteorológico" in amostra and "Dados normalizados" in amostra and "HBV" in amostra:
        return carregar_txt_meteorologico_alerta_rio(path)

    tentativas: list[tuple[str | None, int | None]] = [
        ("\t", 0),
        (";", 0),
        (",", 0),
        (r"\s+", None),  # header na primeira linha não vazia
    ]

    df_raw: pd.DataFrame | None = None
    for sep, hdr in tentativas:
        df_try = _tentar_ler_csv(path, sep, hdr)
        if df_try is not None and df_try.shape[1] >= 2:
            df_raw = df_try
            break

    if df_raw is None:
        raise ValueError(f"Não foi possível ler o arquivo: {path}")

    df_raw.columns = [normalizar_header(c) for c in df_raw.columns]

    # Mapear colunas de data/hora
    col_dia = None
    col_hora = None
    col_ts = None
    for c in df_raw.columns:
        if c in ("data_hora", "timestamp", "datahora"):
            col_ts = c
        elif c == "dia" or c.startswith("data"):
            col_dia = c
        elif "hora" in c and "direc" not in c:
            col_hora = c

    ts_list: list[pd.Timestamp] = []
    if col_ts:
        ts_list = pd.to_datetime(df_raw[col_ts], dayfirst=True, errors="coerce")
    elif col_dia and col_hora:
        comb = df_raw[col_dia].astype(str).str.strip() + " " + df_raw[col_hora].astype(str).str.strip()
        ts_list = pd.to_datetime(comb, dayfirst=True, errors="coerce")
    else:
        raise ValueError(
            f"Não achei colunas de data/hora. Cabeçalhos: {list(df_raw.columns)} — "
            "ajuste o parser ou envie as 5 primeiras linhas do arquivo."
        )

    out = pd.DataFrame({"ts_site": ts_list})

    def pick_metric(candidatos: list[str]) -> pd.Series | None:
        for nome in df_raw.columns:
            for alvo in candidatos:
                if alvo in nome:
                    return pd.to_numeric(df_raw[nome], errors="coerce")
        return None

    chuva = pick_metric(["chuva", "precip", "precipit"])
    # Não usar só "vento": colide com "direcao do vento".
    dir_v = pick_metric(["direc"])
    vel_v = pick_metric(["veloc"])
    temp = pick_metric(["temperat"])
    press = pick_metric(["press"])
    umid = pick_metric(["umid"])

    if chuva is not None:
        out["chuva"] = chuva
    if dir_v is not None:
        out["dirvento"] = dir_v
    if vel_v is not None:
        out["velvento"] = vel_v
    if temp is not None:
        out["temperatura"] = temp
    if press is not None:
        out["pressao"] = press
    if umid is not None:
        out["umidade"] = umid

    out = out.dropna(subset=["ts_site"]).copy()
    # Horário local SP (arquivo do site); converter para UTC como o pipeline faz com horaLeitura
    if out["ts_site"].dt.tz is None:
        out["ts_utc"] = out["ts_site"].dt.tz_localize("America/Sao_Paulo", ambiguous="NaT", nonexistent="shift_forward")
        out["ts_utc"] = out["ts_utc"].dt.tz_convert("UTC").dt.tz_localize(None)
    else:
        out["ts_utc"] = out["ts_site"].dt.tz_convert("UTC").dt.tz_localize(None)

    out["arquivo"] = path.name
    return out


def query_nimbus_mesma_logica(estacao_id: int, inicio_utc: datetime, fim_utc: datetime) -> str:
    """Mesma agregação de sincronizar_meteorologicos_nimbus_bigquery (sem filtro incremental)."""
    t0 = inicio_utc.strftime("%Y-%m-%d %H:%M:%S+00:00")
    t1 = fim_utc.strftime("%Y-%m-%d %H:%M:%S+00:00")
    return f"""
SELECT
    l."horaLeitura" AS data_hora,
    l.estacao_id AS id_estacao,
    e.nome AS nome_estacao,
    MAX(
        CASE
            -- Até jan/2020 todas as estações usam coleta de 15 min (m15).
            WHEN l."horaLeitura" < TIMESTAMPTZ '2020-02-01 00:00:00+00' THEN elc.m15
            -- Guaratiba/São Cristóvão iniciam coleta de 5 min em mar/2020.
            WHEN (
                e.nome ILIKE 'Guaratiba%%'
                OR e.nome ILIKE 'Sao Cristovao%%'
                OR e.nome ILIKE 'São Cristóvão%%'
            ) AND l."horaLeitura" < TIMESTAMPTZ '2020-03-01 00:00:00+00' THEN elc.m15
            -- Demais casos já em 5 min.
            ELSE elc.m05
        END
    ) AS chuva,
    MAX(CASE WHEN s.nome = 'Direcao do Vento'      THEN ls.valor END) AS dirvento,
    MAX(CASE WHEN s.nome = 'Velocidade do Vento'   THEN ls.valor END) AS velvento,
    MAX(CASE WHEN s.nome = 'Temperatuda do Ar'     THEN ls.valor END) AS temperatura,
    MAX(CASE WHEN s.nome = 'Pressao ATM'           THEN ls.valor END) AS pressao,
    MAX(CASE WHEN s.nome = 'Umidade do Ar'         THEN ls.valor END) AS umidade,
    CASE
        WHEN MAX(CASE WHEN s.nome = 'Temperatuda do Ar' THEN ls.valor END) IS NULL
          OR MAX(CASE WHEN s.nome = 'Umidade do Ar' THEN ls.valor END) IS NULL
          OR MAX(CASE WHEN s.nome = 'Umidade do Ar' THEN ls.valor END) <= 0
        THEN NULL
        ELSE (
            243.04 * (
                LN(MAX(CASE WHEN s.nome = 'Umidade do Ar' THEN ls.valor END) / 100.0) +
                (
                    17.625 * MAX(CASE WHEN s.nome = 'Temperatuda do Ar' THEN ls.valor END)
                ) / (
                    243.04 + MAX(CASE WHEN s.nome = 'Temperatuda do Ar' THEN ls.valor END)
                )
            )
        ) / (
            17.625 - (
                LN(MAX(CASE WHEN s.nome = 'Umidade do Ar' THEN ls.valor END) / 100.0) +
                (
                    17.625 * MAX(CASE WHEN s.nome = 'Temperatuda do Ar' THEN ls.valor END)
                ) / (
                    243.04 + MAX(CASE WHEN s.nome = 'Temperatuda do Ar' THEN ls.valor END)
                )
            )
        )
    END AS ponto_orvalho
FROM public.estacoes_leiturasensor ls
JOIN public.estacoes_leitura l ON ls.leitura_id = l.id
LEFT JOIN public.estacoes_leiturachuva elc ON elc.leitura_id = l.id
JOIN public.estacoes_sensor s ON ls.sensor_id = s.id
JOIN public.estacoes_estacao e ON e.id = l.estacao_id
WHERE l.estacao_id = {estacao_id}
  AND l."horaLeitura" >= '{t0}'::timestamptz
  AND l."horaLeitura" < '{t1}'::timestamptz
GROUP BY l."horaLeitura", l.estacao_id, e.nome
ORDER BY l."horaLeitura";
"""


def preparar_nimbus(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["data_hora"] = pd.to_datetime(df["data_hora"], utc=True, errors="coerce")
    df["ts_utc"] = df["data_hora"].dt.tz_convert("UTC").dt.tz_localize(None)
    for c in ["chuva", "dirvento", "velvento", "temperatura", "pressao", "umidade", "ponto_orvalho"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


def comparar(site: pd.DataFrame, ref: pd.DataFrame, tol: float, rotulo_ref: str = "ref") -> None:
    """Junta por ``ts_utc`` (UTC naive) e compara métricas numéricas."""
    tag = rotulo_ref.replace(" ", "_").lower()
    metricas = [
        c
        for c in ["chuva", "dirvento", "velvento", "temperatura", "pressao", "umidade", "ponto_orvalho"]
        if c in site.columns and c in ref.columns
    ]
    if not metricas:
        print(f"Nenhuma métrica comum entre site e {rotulo_ref} (verifique cabeçalhos / schema).")
        return

    s = site.rename(columns={m: f"{m}_site" for m in metricas})
    r = ref.rename(columns={m: f"{m}_{tag}" for m in metricas})

    merged = pd.merge(s, r, on="ts_utc", how="outer", indicator=True)
    print(f"\n========== TXT vs {rotulo_ref.upper()} ==========")
    print("--- Resumo de junção (ts_utc) ---")
    print(merged["_merge"].value_counts().to_string())

    inner = merged[merged["_merge"] == "both"].copy()
    print(f"\nLinhas em comum (mesmo instante UTC): {len(inner)}")

    for m in metricas:
        a = inner[f"{m}_site"]
        b = inner[f"{m}_{tag}"]
        mask = a.notna() & b.notna()
        if not mask.any():
            continue
        diff = (a[mask] - b[mask]).abs()
        print(f"\n{m}: max|site-{tag}| = {diff.max():.6g}  (tol {tol})  linhas comparáveis: {int(mask.sum())}")
        ruins = diff > tol
        if ruins.any():
            amostra = inner.loc[mask & ruins].nsmallest(10, f"{m}_site")
            print(f"  Exemplos com |diff| > tol (até 10):")
            cols_show = ["ts_utc", f"{m}_site", f"{m}_{tag}"]
            print(amostra[cols_show].to_string(index=False))

    only_site = merged[merged["_merge"] == "left_only"].head(5)
    only_ref = merged[merged["_merge"] == "right_only"].head(5)
    if len(only_site):
        print(f"\nAmostra só no TXT (sem match {rotulo_ref}):")
        print(only_site[["ts_utc"] + [f"{m}_site" for m in metricas if f"{m}_site" in only_site.columns]].to_string(index=False))
    if len(only_ref):
        print(f"\nAmostra só no {rotulo_ref} (sem match TXT):")
        print(only_ref[["ts_utc"] + [f"{m}_{tag}" for m in metricas if f"{m}_{tag}" in only_ref.columns]].to_string(index=False))


def main() -> int:
    ap = argparse.ArgumentParser(description="Conferir TXT do site vs BigQuery (GCP) e/ou NIMBUS")
    ap.add_argument("arquivos", nargs="+", type=str, help="Caminhos dos iraja_*_Met.txt")
    ap.add_argument("--estacao-id", type=int, default=11, help="ID estação (Irajá = 11)")
    ap.add_argument("--tol", type=float, default=0.05, help="Tolerância numérica (mm, °C, etc.)")
    ap.add_argument(
        "--fonte",
        choices=("gcp", "nimbus", "ambos"),
        default="gcp",
        help="Onde buscar a referência: BigQuery (gcp), PostgreSQL (nimbus) ou ambos",
    )
    ap.add_argument(
        "--dataset",
        type=str,
        default=None,
        help="Dataset BigQuery (padrão: env BIGQUERY_DATASET_ID_NIMBUS ou alertadb_cor_raw)",
    )
    ap.add_argument(
        "--tabela",
        type=str,
        default=None,
        help="Tabela meteorológicos no BQ (padrão: env BIGQUERY_TABLE_ID_METEOROLOGICOS ou meteorologicos)",
    )
    args = ap.parse_args()

    paths = [Path(p).expanduser() for p in args.arquivos]
    site_parts: list[pd.DataFrame] = []
    for p in paths:
        print(f"Lendo TXT: {p}")
        site_parts.append(carregar_txt_site(p))

    site = pd.concat(site_parts, ignore_index=True)
    site = site.sort_values("ts_utc").drop_duplicates(subset=["ts_utc"], keep="last")
    print(f"Total linhas TXT (únicas por ts_utc): {len(site)}")
    print("Colunas TXT:", [c for c in site.columns if c not in ("arquivo", "ts_site")])

    tmin = pd.Timestamp(site["ts_utc"].min()).to_pydatetime().replace(tzinfo=timezone.utc)
    tmax = pd.Timestamp(site["ts_utc"].max()).to_pydatetime().replace(tzinfo=timezone.utc) + timedelta(seconds=1)

    if args.fonte in ("gcp", "ambos"):
        dataset_id = args.dataset or obter_variavel(
            "BIGQUERY_DATASET_ID_NIMBUS", obrigatoria=False, padrao="alertadb_cor_raw"
        )
        table_id = args.tabela or obter_variavel(
            "BIGQUERY_TABLE_ID_METEOROLOGICOS", obrigatoria=False, padrao="meteorologicos"
        )
        client, project_id = criar_cliente_bigquery()
        print(
            f"\nConsultando BigQuery `{project_id}.{dataset_id}.{table_id}` "
            f"estacao_id={args.estacao_id} entre {tmin} e {tmax} UTC ..."
        )
        gcp_df = carregar_met_bigquery(
            client, project_id, str(dataset_id), str(table_id), args.estacao_id, tmin, tmax
        )
        print(f"Linhas BigQuery: {len(gcp_df)}")
        comparar(site, gcp_df, args.tol, rotulo_ref="GCP")

    if args.fonte in ("nimbus", "ambos"):
        origem = carregar_origem()
        user_encoded = quote_plus(origem["user"])
        password_encoded = quote_plus(origem["password"])
        connection_string = (
            f"postgresql+psycopg2://{user_encoded}:{password_encoded}"
            f"@{origem['host']}:{origem['port']}/{origem['dbname']}"
        )
        engine = create_engine(
            connection_string,
            connect_args={
                "sslmode": origem.get("sslmode", "disable"),
                "connect_timeout": int(origem.get("connect_timeout", 15)),
                "client_encoding": "UTF8",
            },
            pool_pre_ping=True,
        )
        sql = query_nimbus_mesma_logica(args.estacao_id, tmin, tmax)
        print(f"\nConsultando NIMBUS estacao_id={args.estacao_id} entre {tmin} e {tmax} UTC ...")
        nimbus = pd.read_sql(sql, engine)
        nimbus = preparar_nimbus(nimbus)
        print(f"Linhas NIMBUS: {len(nimbus)}")
        comparar(site, nimbus, args.tol, rotulo_ref="NIMBUS")

    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as e:
        print(f"Erro: {e}", file=sys.stderr)
        raise SystemExit(1)
