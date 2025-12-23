import os
import re
import time
from datetime import datetime
from io import BytesIO

import numpy as np
import pandas as pd
import streamlit as st

# Google Sheets
import gspread
from google.oauth2.service_account import Credentials
from gspread.exceptions import APIError

# PDF
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib.units import mm
from reportlab.platypus import Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle

# Charts
import matplotlib.pyplot as plt


# =========================
# CONFIG
# =========================
APP_TITLE = "Financeiro do Casal"
MESES_RE = re.compile(r"^\d{2}/\d{2}$")
SPREADSHEET_ID = "1hF93_DhTauLwfspzIKfj30uxadNmUbBIE94GLmK5rtw"

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

MONTHS_PT = [
    "Janeiro", "Fevereiro", "Março", "Abril", "Maio", "Junho",
    "Julho", "Agosto", "Setembro", "Outubro", "Novembro", "Dezembro"
]


# =========================
# Utils
# =========================
def _brl(v: float) -> str:
    try:
        return f"R$ {float(v):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except Exception:
        return "R$ 0,00"


def _clean_status(s: str) -> str:
    if pd.isna(s):
        return ""
    s = str(s).strip().upper()
    s = s.replace("A PAGAR", "EM ABERTO")
    s = s.replace("A RECEBER", "EM ABERTO")
    return s


def _is_done_status(s: str) -> bool:
    s = _clean_status(s)
    return s in ("PAGO", "RECEBIDO")


def _to_float_br(x) -> float:
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return 0.0
    if isinstance(x, (int, float)):
        return float(x)
    s = str(x).strip()
    if not s:
        return 0.0
    s = s.replace("R$", "").replace(" ", "")
    if "," in s and "." in s:
        s = s.replace(".", "").replace(",", ".")
    elif "," in s and "." not in s:
        s = s.replace(",", ".")
    try:
        return float(s)
    except Exception:
        return 0.0


def _norm_headers(raw_headers):
    headers = []
    used = {}
    for i, h in enumerate(raw_headers):
        h2 = (h or "").strip()
        if h2 == "":
            h2 = f"COL_{i}"
        base = h2
        if base in used:
            used[base] += 1
            h2 = f"{base}_{used[base]}"
        else:
            used[base] = 1
        headers.append(h2)
    return headers


def _pick_col(headers: list[str], keywords: list[str]) -> str | None:
    def norm(s):
        return (s or "").strip().lower()

    for h in headers:
        hn = norm(h)
        if any(k in hn for k in keywords):
            return h
    return None


# =========================
# Charts helpers
# =========================
def _donut(ax, labels, values, title: str):
    vals = [float(v) for v in values]
    if sum(vals) <= 0:
        ax.text(0.5, 0.5, "Sem dados", ha="center", va="center")
        ax.set_title(title)
        ax.axis("off")
        return

    ax.pie(
        vals,
        labels=labels,
        autopct=lambda p: f"{p:.0f}%" if p >= 8 else "",
        startangle=90,
        wedgeprops=dict(width=0.45),
    )
    ax.set_title(title)
    ax.axis("equal")


def _bar_top(ax, series: pd.Series, title: str, top_n: int = 8):
    s = series.copy()
    s = s[s > 0].sort_values(ascending=False).head(top_n)
    if s.empty:
        ax.text(0.5, 0.5, "Sem dados", ha="center", va="center")
        ax.set_title(title)
        ax.axis("off")
        return

    ax.barh(list(reversed(s.index.tolist())), list(reversed(s.values.tolist())))
    ax.set_title(title)
    ax.grid(True, axis="x", alpha=0.25)


# =========================
# Mês (robusto)
# =========================
def parse_month_title(title: str):
    a, b = title.split("/")
    x = int(a)
    y = int(b)

    if x > 12 and y <= 12:
        ano = x
        mes = y
    elif y > 12 and x <= 12:
        ano = y
        mes = x
    else:
        mes = x
        ano = y

    label = f"{mes:02d}/{ano:02d}"
    return mes, ano, label


def label_month_long(mes: int, ano_2d: int) -> str:
    ano_full = 2000 + int(ano_2d)
    nome = MONTHS_PT[int(mes) - 1] if 1 <= int(mes) <= 12 else f"Mês {mes}"
    return f"{nome}/{ano_full}"


def build_month_options(titles: list[str]):
    opts = []
    for t in titles:
        try:
            mes, ano, label = parse_month_title(t)
            opts.append(
                {"title": t, "mes": mes, "ano": ano, "label": label, "label_long": label_month_long(mes, ano)}
            )
        except Exception:
            continue
    opts.sort(key=lambda d: (d["ano"], d["mes"]))
    return opts


# =========================
# Backoff (quota)
# =========================
def _call_with_backoff(func, *args, **kwargs):
    delays = [0.25, 0.7, 1.4, 2.4]
    last_err = None
    for d in delays:
        try:
            return func(*args, **kwargs)
        except APIError as e:
            last_err = e
            msg = str(e)
            if "429" in msg or "Quota exceeded" in msg:
                time.sleep(d)
                continue
            raise
    if last_err:
        raise last_err


# =========================
# Google auth
# =========================
@st.cache_resource
def _gs_client():
    if "gcp_service_account" in st.secrets:
        creds = Credentials.from_service_account_info(st.secrets["gcp_service_account"], scopes=SCOPES)
        return gspread.authorize(creds)

    if "local_json_path" in st.secrets:
        p = st.secrets["local_json_path"]
        if not os.path.exists(p):
            raise FileNotFoundError(f"JSON não encontrado em: {p}")
        creds = Credentials.from_service_account_file(p, scopes=SCOPES)
        return gspread.authorize(creds)

    raise RuntimeError("Credenciais não encontradas em st.secrets")


@st.cache_resource
def _gs_sheet():
    return _gs_client().open_by_key(SPREADSHEET_ID)


@st.cache_resource
def get_spreadsheet_info():
    sh = _gs_sheet()
    url = getattr(sh, "url", None) or f"https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}"
    return {"title": sh.title, "url": url, "id": SPREADSHEET_ID}


# =========================
# Header auto-detect
# =========================
def detect_header_row(values: list[list[str]], max_scan_rows: int = 60) -> int:
    if not values:
        return 0

    targets = [
        ["descrição", "descricao", "descr", "histórico", "historico"],
        ["valor", "r$"],
        ["status", "situação", "situacao"],
        ["tipo", "natureza"],
        ["data", "dt"],
        ["categoria", "conta", "cat"],
        ["recorr", "reco"],
    ]

    scan = min(len(values), max_scan_rows)
    best_i = 0
    best_score = -1

    for i in range(scan):
        row_l = [str(c or "").strip().lower() for c in values[i]]
        score = 0
        for group in targets:
            if any(any(g in cell for g in group) for cell in row_l):
                score += 1
        if score > best_score:
            best_score = score
            best_i = i

    return best_i if best_score >= 2 else 0


def trim_trailing_empty_cols(matrix: list[list[str]]) -> list[list[str]]:
    if not matrix:
        return matrix
    max_len = max(len(r) for r in matrix)
    norm = [r + [""] * (max_len - len(r)) for r in matrix]

    last_nonempty = -1
    for j in range(max_len):
        if any(str(norm[i][j]).strip() != "" for i in range(len(norm))):
            last_nonempty = j

    if last_nonempty == -1:
        return matrix
    return [r[: last_nonempty + 1] for r in norm]


# =========================
# Sheets listing + reading
# =========================
@st.cache_data(ttl=120)
def list_sheet_titles():
    sh = _gs_sheet()
    return [ws.title for ws in sh.worksheets()]


@st.cache_data(ttl=60)
def read_month_sheet_cached(sheet_title: str) -> tuple[pd.DataFrame, dict]:
    ws = _gs_sheet().worksheet(sheet_title)
    values = _call_with_backoff(ws.get_all_values)

    if not values:
        return pd.DataFrame(), {}

    values = trim_trailing_empty_cols(values)

    header_idx0 = detect_header_row(values, max_scan_rows=120)
    header_row_num = header_idx0 + 1  # 1-based
    raw_headers = values[header_idx0]
    rows = values[header_idx0 + 1 :]

    headers = _norm_headers(raw_headers)
    df_raw = pd.DataFrame(rows, columns=headers)

    c_desc = _pick_col(headers, ["descr", "hist", "lan", "descricao", "descrição", "historico", "histórico"])
    c_tipo = _pick_col(headers, ["tipo", "natureza"])
    c_rec = _pick_col(headers, ["reco", "recorr"])
    c_conta = _pick_col(headers, ["conta", "categoria", "cat"])
    c_val = _pick_col(headers, ["valor", "r$"])
    c_sta = _pick_col(headers, ["status", "situ", "pago", "pagamento"])
    c_data = _pick_col(headers, ["data", "dt", "dia"])

    mapping = {
        "Data": c_data or "",
        "Descrição": c_desc or "",
        "Tipo": c_tipo or "",
        "Recorrência": c_rec or "",
        "Conta": c_conta or "",
        "Valor": c_val or "",
        "Status": c_sta or "",
        "_headers": headers,
        "_header_row_num": header_row_num,
        "_data_start_row_num": header_row_num + 1,
    }

    def getcol(colname, default=""):
        src = mapping[colname]
        if src and src in df_raw.columns:
            return df_raw[src]
        return default

    df = pd.DataFrame(
        {
            "Data": getcol("Data", ""),
            "Descrição": getcol("Descrição", ""),
            "Tipo": getcol("Tipo", ""),
            "Recorrência": getcol("Recorrência", ""),
            "Conta": getcol("Conta", ""),
            "Valor": getcol("Valor", ""),
            "Status": getcol("Status", ""),
        }
    )

    df["Mes"] = sheet_title
    df["Fonte"] = "Planilha"

    start = mapping["_data_start_row_num"]
    df["__row"] = [start + i for i in range(len(df))]

    df["Descrição"] = df["Descrição"].fillna("").astype(str).str.strip()
    df["Tipo"] = df["Tipo"].fillna("").astype(str).str.strip().str.title()
    df["Recorrência"] = df["Recorrência"].fillna("").astype(str).str.strip().str.title()
    df["Conta"] = df["Conta"].fillna("").astype(str).str.strip()
    df["Status"] = df["Status"].fillna("").apply(_clean_status)
    df["Valor"] = df["Valor"].apply(_to_float_br)
    df["Data"] = df["Data"].fillna("").astype(str).str.strip()

    df = df[~((df["Descrição"] == "") & (df["Valor"].fillna(0) == 0))].copy()
    df = df.reset_index(drop=True)

    df["ID"] = df.apply(lambda r: f"{sheet_title}-{int(r['__row'])}", axis=1)
    return df, mapping


def _validate_mapping_for_insert(mapping: dict) -> tuple[bool, str]:
    required = ["Descrição", "Valor", "Status"]
    missing = [k for k in required if not mapping.get(k)]
    if missing:
        return False, f"Mapeamento incompleto no Sheets: faltando coluna para {', '.join(missing)}."
    return True, ""


def update_row_in_sheet(sheet_title: str, row_number: int, mapping: dict, payload: dict) -> None:
    ws = _gs_sheet().worksheet(sheet_title)
    headers = mapping.get("_headers") or _call_with_backoff(ws.row_values, mapping.get("_header_row_num", 1))
    headers = _norm_headers(headers)

    def col_idx(header_name: str) -> int | None:
        if not header_name:
            return None
        try:
            return headers.index(header_name) + 1
        except ValueError:
            return None

    field_map = {
        "Data": mapping.get("Data", ""),
        "Descrição": mapping.get("Descrição", ""),
        "Tipo": mapping.get("Tipo", ""),
        "Recorrência": mapping.get("Recorrência", ""),
        "Conta": mapping.get("Conta", ""),
        "Valor": mapping.get("Valor", ""),
        "Status": mapping.get("Status", ""),
    }

    updates = []
    for field, header in field_map.items():
        if field not in payload:
            continue

        idx = col_idx(header)
        if idx is None:
            continue

        val = payload.get(field, "")
        if field == "Valor":
            try:
                val = float(val)
                val = f"{val:.2f}".replace(".", ",")
            except Exception:
                val = ""
        updates.append((row_number, idx, str(val)))

    if not updates:
        return

    min_c = min(c for r, c, v in updates)
    max_c = max(c for r, c, v in updates)

    cells = _call_with_backoff(ws.range, row_number, min_c, row_number, max_c)
    lookup = {(r, c): v for r, c, v in updates}

    for cell in cells:
        key = (cell.row, cell.col)
        if key in lookup:
            cell.value = lookup[key]

    _call_with_backoff(ws.update_cells, cells)


def append_row_in_table(sheet_title: str, mapping: dict, payload: dict) -> None:
    """
    ✅ Salva dentro do bloco da tabela
    ✅ ws.range + ws.update_cells (estável)
    """
    ws = _gs_sheet().worksheet(sheet_title)
    header_row = mapping.get("_header_row_num", 1)

    headers = mapping.get("_headers") or _call_with_backoff(ws.row_values, header_row)
    headers = _norm_headers(headers)

    def col_idx(header_name: str) -> int:
        return headers.index(header_name) + 1

    desc_header = mapping.get("Descrição", "")
    if not desc_header:
        raise RuntimeError("Não consegui mapear a coluna de Descrição no cabeçalho.")
    desc_col = col_idx(desc_header)

    col_vals = _call_with_backoff(ws.col_values, desc_col)

    last = header_row
    for i in range(len(col_vals), 0, -1):
        if str(col_vals[i - 1]).strip() != "" and i > header_row:
            last = i
            break

    next_row = last + 1

    def v(field: str):
        return payload.get(field, "")

    val_num = _to_float_br(v("Valor"))
    val_str = f"{val_num:.2f}".replace(".", ",")

    fields_order = ["Data", "Descrição", "Tipo", "Recorrência", "Conta", "Valor", "Status"]
    updates = []
    for field in fields_order:
        h = mapping.get(field, "")
        if not h:
            continue
        c = col_idx(h)
        value = val_str if field == "Valor" else str(v(field))
        updates.append((next_row, c, value))

    min_c = min(c for r, c, v_ in updates)
    max_c = max(c for r, c, v_ in updates)

    cells = _call_with_backoff(ws.range, next_row, min_c, next_row, max_c)
    lookup = {(r, c): v_ for r, c, v_ in updates}

    for cell in cells:
        key = (cell.row, cell.col)
        if key in lookup:
            cell.value = lookup[key]

    _call_with_backoff(ws.update_cells, cells)


# =========================
# KPIs
# =========================
def kpis(df: pd.DataFrame) -> dict:
    if df is None or df.empty:
        return {
            "Receita (recebida)": 0.0,
            "Receita (a receber)": 0.0,
            "Despesa (paga)": 0.0,
            "Despesa (em aberto)": 0.0,
            "Saldo (realizado)": 0.0,
            "Saldo (esperado)": 0.0,
        }

    d = df.copy()
    d["Valor"] = pd.to_numeric(d["Valor"], errors="coerce").fillna(0.0)
    d["Tipo"] = d["Tipo"].fillna("").astype(str).str.title()
    d["Status"] = d["Status"].fillna("").apply(_clean_status)

    rec = d[d["Tipo"].str.contains("Receita", na=False)]
    des = d[d["Tipo"].str.contains("Despesa", na=False)]

    receita_recebida = rec[rec["Status"].isin(["RECEBIDO", "PAGO"])]["Valor"].sum()
    receita_aberto = rec[~rec["Status"].isin(["RECEBIDO", "PAGO"])]["Valor"].sum()

    despesa_paga = des[des["Status"].isin(["PAGO", "RECEBIDO"])]["Valor"].sum()
    despesa_aberto = des[~des["Status"].isin(["PAGO", "RECEBIDO"])]["Valor"].sum()

    saldo_realizado = receita_recebida - despesa_paga
    saldo_esperado = (receita_recebida + receita_aberto) - (despesa_paga + despesa_aberto)

    return {
        "Receita (recebida)": float(receita_recebida),
        "Receita (a receber)": float(receita_aberto),
        "Despesa (paga)": float(despesa_paga),
        "Despesa (em aberto)": float(despesa_aberto),
        "Saldo (realizado)": float(saldo_realizado),
        "Saldo (esperado)": float(saldo_esperado),
    }


# =========================
# PDF report
# =========================
def _pdf_table_open_items(story, title: str, df_open: pd.DataFrame, styles):
    story.append(Paragraph(f"<b>{title}</b>", styles["Heading2"]))
    story.append(Spacer(1, 2 * mm))

    total_itens = len(df_open)
    total_valor = float(df_open["Valor"].sum()) if total_itens else 0.0
    story.append(Paragraph(f"Itens: <b>{total_itens}</b> — Soma: <b>{_brl(total_valor)}</b>", styles["Normal"]))
    story.append(Spacer(1, 3 * mm))

    if df_open.empty:
        story.append(Paragraph("Nenhum item em aberto neste recorte.", styles["Normal"]))
        story.append(Spacer(1, 4 * mm))
        return

    MAX_ROWS = 250
    d = df_open.head(MAX_ROWS).copy()

    rows = [["Data", "Descrição", "Tipo", "Valor", "Status"]]
    for _, r in d.iterrows():
        rows.append(
            [
                str(r.get("Data", "")),
                str(r.get("Descrição", "")),
                str(r.get("Tipo", "")),
                _brl(float(r.get("Valor", 0))),
                str(r.get("Status", "")),
            ]
        )

    t = Table(rows, colWidths=[22 * mm, 92 * mm, 25 * mm, 25 * mm, 26 * mm])
    t.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#111827")),
                ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
                ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                ("GRID", (0, 0), (-1, -1), 0.4, colors.HexColor("#E5E7EB")),
                ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#FAFAFA")]),
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ("ALIGN", (3, 1), (3, -1), "RIGHT"),
            ]
        )
    )
    story.append(t)
    story.append(Spacer(1, 6 * mm))


def build_pdf_report(df_periodo: pd.DataFrame, meses_sel: list[str], titulo: str = "Relatório Financeiro do Casal") -> bytes:
    styles = getSampleStyleSheet()
    buffer = BytesIO()
    doc = SimpleDocTemplate(
        buffer,
        pagesize=A4,
        leftMargin=15 * mm,
        rightMargin=15 * mm,
        topMargin=15 * mm,
        bottomMargin=15 * mm,
    )

    story = []
    story.append(Paragraph(f"<b>{titulo}</b>", styles["Title"]))
    story.append(Spacer(1, 4 * mm))
    story.append(Paragraph(f"Período: <b>{', '.join(meses_sel)}</b>", styles["Normal"]))
    story.append(Paragraph(f"Gerado em: <b>{datetime.now().strftime('%d/%m/%Y %H:%M')}</b>", styles["Normal"]))
    story.append(Spacer(1, 6 * mm))

    d = df_periodo.copy() if df_periodo is not None else pd.DataFrame()
    if not d.empty:
        d["Status"] = d.get("Status", "").fillna("").apply(_clean_status)
        d["Tipo"] = d.get("Tipo", "").fillna("").astype(str).str.title()
        d["Valor"] = pd.to_numeric(d.get("Valor", 0), errors="coerce").fillna(0.0)
    else:
        d = pd.DataFrame(columns=["Data", "Descrição", "Tipo", "Valor", "Status"])

    kp = kpis(d)
    resumo_data = [
        ["Receita (recebida)", _brl(kp["Receita (recebida)"])],
        ["Receita (a receber)", _brl(kp["Receita (a receber)"])],
        ["Despesa (paga)", _brl(kp["Despesa (paga)"])],
        ["Despesa (em aberto)", _brl(kp["Despesa (em aberto)"])],
        ["Saldo (realizado)", _brl(kp["Saldo (realizado)"])],
        ["Saldo (esperado)", _brl(kp["Saldo (esperado)"])],
    ]
    t = Table([["Indicador", "Valor"]] + resumo_data, colWidths=[90 * mm, 70 * mm])
    t.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#111827")),
                ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
                ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                ("GRID", (0, 0), (-1, -1), 0.5, colors.HexColor("#E5E7EB")),
                ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#FAFAFA")]),
                ("ALIGN", (1, 1), (1, -1), "RIGHT"),
            ]
        )
    )
    story.append(t)
    story.append(Spacer(1, 8 * mm))

    done = d["Status"].isin(["PAGO", "RECEBIDO"])
    abertos = d[~done].copy()

    rec_open = abertos[abertos["Tipo"].str.contains("Receita", na=False)].copy().sort_values(by=["Valor"], ascending=False)
    des_open = abertos[abertos["Tipo"].str.contains("Despesa", na=False)].copy().sort_values(by=["Valor"], ascending=False)

    story.append(Paragraph("<b>Lançamentos em Aberto</b>", styles["Heading2"]))
    story.append(Spacer(1, 2 * mm))
    story.append(
        Paragraph(
            f"Total em aberto: <b>{len(abertos)}</b> — Soma: <b>{_brl(float(abertos['Valor'].sum()))}</b>",
            styles["Normal"],
        )
    )
    story.append(Spacer(1, 5 * mm))

    _pdf_table_open_items(story, "Receitas em Aberto", rec_open, styles)
    _pdf_table_open_items(story, "Despesas em Aberto", des_open, styles)

    doc.build(story)
    pdf = buffer.getvalue()
    buffer.close()
    return pdf


# =========================
# UX: filtros por clique no card
# =========================
def _set_lancamentos_filter(tipo: str | None, status_mode: str | None, title: str):
    st.session_state["page"] = "Lançamentos"
    st.session_state["lanc_filter_tipo"] = tipo
    st.session_state["lanc_filter_status_mode"] = status_mode
    st.session_state["lanc_filter_title"] = title
    st.session_state["lanc_search"] = ""
    st.rerun()


# =========================
# THEME
# =========================
st.set_page_config(page_title=APP_TITLE, page_icon="🌙", layout="wide")

st.markdown(
    """
    <style>
    .stApp { background: #0b0f19; color: #e5e7eb; }
    .block-container { padding-top: 1.0rem; padding-bottom: 2rem; }
    h1, h2, h3, h4, h5, h6 { color: #f9fafb !important; letter-spacing: -0.02em; }

    section[data-testid="stSidebar"] {
        background: #0e1424 !important;
        border-right: 1px solid rgba(255,255,255,0.06);
    }
    section[data-testid="stSidebar"] * { color: #e5e7eb !important; }

    /* botões gerais */
    .stButton > button, .stDownloadButton > button {
        background: rgba(255,255,255,0.06) !important;
        color: #f9fafb !important;
        border: 1px solid rgba(255,255,255,0.10) !important;
        border-radius: 12px !important;
        padding: 0.55rem 0.85rem !important;
        font-weight: 700 !important;
        width: 100%;
    }
    .stButton > button[kind="primary"] {
        background: #22c55e !important;
        color: #0b0f19 !important;
        border: none !important;
        font-weight: 900 !important;
    }

    /* ===== Cards premium: HTML + botão invisível por cima ===== */
    .pcard {
        position: relative;
        border-radius: 16px;
    }

    .pcard .pcard-ui {
        background: rgba(255,255,255,0.04);
        border: 1px solid rgba(255,255,255,0.08);
        border-radius: 16px;
        padding: 14px 14px;
        box-shadow: 0 10px 24px rgba(0,0,0,0.35);
        transition: transform 120ms ease, border 120ms ease;
        min-height: 92px;
        display: flex;
        flex-direction: column;
        justify-content: center;
    }

    .pcard:hover .pcard-ui {
        border: 1px solid rgba(255,255,255,0.16);
        transform: translateY(-1px);
    }

    .pcard .pcard-title {
        color: #9ca3af;
        font-size: 0.90rem;
        margin-bottom: 6px;
        line-height: 1.1;
    }

    .pcard .pcard-value {
        color: #f9fafb;
        font-size: 1.65rem;
        font-weight: 900;
        letter-spacing: -0.02em;
        line-height: 1.05;
    }

    /* Botão invisível cobrindo todo o card */
    .pcard .stButton {
        position: absolute;
        inset: 0;
        margin: 0;
        z-index: 10;
    }

    .pcard .stButton > button {
        width: 100% !important;
        height: 100% !important;
        opacity: 0 !important;
        padding: 0 !important;
        border: none !important;
        background: transparent !important;
    }

    .pcard .stButton > button:focus,
    .pcard .stButton > button:active {
        outline: none !important;
        box-shadow: none !important;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

st.title(APP_TITLE)

if "page" not in st.session_state:
    st.session_state["page"] = "Resumo"


def card_clickable(key: str, title: str, value: str, on_click=None):
    """
    Card premium:
    - Renderiza o card em HTML (título pequeno, valor grande)
    - Coloca um st.button invisível por cima, cobrindo 100% do card
    """
    st.markdown(f'<div class="pcard" id="pcard-{key}">', unsafe_allow_html=True)
    st.markdown(
        f"""
        <div class="pcard-ui">
            <div class="pcard-title">{title}</div>
            <div class="pcard-value">{value}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    clicked = st.button(" ", key=f"{key}__overlay", use_container_width=True)
    st.markdown("</div>", unsafe_allow_html=True)

    if clicked and on_click:
        on_click()


# =========================
# Sidebar
# =========================
info = get_spreadsheet_info()

try:
    titles = list_sheet_titles()
except Exception as e:
    st.error(f"Erro ao conectar no Google Sheets: {e}")
    st.stop()

month_titles = [t for t in titles if MESES_RE.match(str(t).strip())]
options = build_month_options(month_titles)

if not options:
    st.warning("Não encontrei abas mensais no formato 01/26 (ou 26/01).")
    st.stop()

with st.sidebar:
    st.markdown("### Navegação")
    PAGES = ["Resumo", "Lançar", "Lançamentos", "Relatórios"]
    page = st.radio("", PAGES, index=PAGES.index(st.session_state["page"]), label_visibility="collapsed")
    st.session_state["page"] = page

    st.markdown("### Mês")
    default_title = options[-1]["title"]
    mes_title = st.selectbox(
        "Selecione o mês",
        [o["title"] for o in options],
        index=[o["title"] for o in options].index(default_title),
        format_func=lambda t: next((o["label_long"] for o in options if o["title"] == t), t),
    )

    if st.button("🔄 Atualizar dados", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

sel_opt = next(o for o in options if o["title"] == mes_title)
mes_label_long = sel_opt["label_long"]

df_mes, mapping_mes = read_month_sheet_cached(mes_title)


# =========================
# RESUMO (tabs + cards premium clicáveis)
# =========================
if page == "Resumo":
    st.subheader(f"Resumo — {mes_label_long}")
    ks = kpis(df_mes)

    tab1, tab2, tab3 = st.tabs(["Visão Geral", "Categorias", "Em aberto"])

    with tab1:
        c1, c2, c3 = st.columns(3, gap="large")
        with c1:
            card_clickable(
                "card_saldo_real",
                "Saldo (realizado)",
                _brl(ks["Saldo (realizado)"]),
                on_click=lambda: _set_lancamentos_filter(None, "done", "Itens pagos/recebidos"),
            )
        with c2:
            card_clickable(
                "card_saldo_esp",
                "Saldo (esperado)",
                _brl(ks["Saldo (esperado)"]),
                on_click=lambda: _set_lancamentos_filter(None, None, "Todos os lançamentos"),
            )
        with c3:
            card_clickable(
                "card_des_open",
                "Despesa (em aberto)",
                _brl(ks["Despesa (em aberto)"]),
                on_click=lambda: _set_lancamentos_filter("Despesa", "open", "Despesas em aberto"),
            )

        c4, c5, c6 = st.columns(3, gap="large")
        with c4:
            card_clickable(
                "card_rec_done",
                "Receita (recebida)",
                _brl(ks["Receita (recebida)"]),
                on_click=lambda: _set_lancamentos_filter("Receita", "done", "Receitas recebidas"),
            )
        with c5:
            card_clickable(
                "card_rec_open",
                "Receita (a receber)",
                _brl(ks["Receita (a receber)"]),
                on_click=lambda: _set_lancamentos_filter("Receita", "open", "Receitas a receber"),
            )
        with c6:
            card_clickable(
                "card_des_done",
                "Despesa (paga)",
                _brl(ks["Despesa (paga)"]),
                on_click=lambda: _set_lancamentos_filter("Despesa", "done", "Despesas pagas"),
            )

        st.markdown("### Gráficos (visão geral)")
        if df_mes is None or df_mes.empty:
            st.info("Sem dados para gerar gráficos.")
        else:
            d = df_mes.copy()
            d["Valor"] = pd.to_numeric(d["Valor"], errors="coerce").fillna(0.0)
            d["Tipo"] = d["Tipo"].fillna("").astype(str).str.title()
            d["Status"] = d["Status"].fillna("").apply(_clean_status)

            done_mask = d["Status"].apply(_is_done_status)
            rec = d[d["Tipo"].str.contains("Receita", na=False)].copy()
            des = d[d["Tipo"].str.contains("Despesa", na=False)].copy()

            rec_recebida = rec[done_mask.loc[rec.index]]["Valor"].sum()
            rec_aberto = rec[~done_mask.loc[rec.index]]["Valor"].sum()

            des_paga = des[done_mask.loc[des.index]]["Valor"].sum()
            des_aberto = des[~done_mask.loc[des.index]]["Valor"].sum()

            colA, colB = st.columns(2, gap="large")
            with colA:
                fig, ax = plt.subplots()
                _donut(ax, ["Recebida", "A receber"], [rec_recebida, rec_aberto], "Receitas")
                st.pyplot(fig, use_container_width=True)

            with colB:
                fig, ax = plt.subplots()
                _donut(ax, ["Paga", "Em aberto"], [des_paga, des_aberto], "Despesas")
                st.pyplot(fig, use_container_width=True)

    with tab2:
        st.markdown("### Categorias (Top 8)")
        if df_mes is None or df_mes.empty:
            st.info("Sem dados.")
        else:
            d = df_mes.copy()
            d["Valor"] = pd.to_numeric(d["Valor"], errors="coerce").fillna(0.0)
            d["Tipo"] = d["Tipo"].fillna("").astype(str).str.title()
            d["Status"] = d["Status"].fillna("").apply(_clean_status)
            d["Conta"] = d["Conta"].fillna("").astype(str).str.strip()

            done_mask = d["Status"].apply(_is_done_status)
            des = d[d["Tipo"].str.contains("Despesa", na=False)].copy()

            colC, colD = st.columns(2, gap="large")
            with colC:
                fig, ax = plt.subplots()
                s_open = des[~done_mask.loc[des.index]].groupby("Conta")["Valor"].sum()
                _bar_top(ax, s_open, "Despesas por Categoria (em aberto)")
                st.pyplot(fig, use_container_width=True)

            with colD:
                fig, ax = plt.subplots()
                s_paid = des[done_mask.loc[des.index]].groupby("Conta")["Valor"].sum()
                _bar_top(ax, s_paid, "Despesas por Categoria (pagas)")
                st.pyplot(fig, use_container_width=True)

    with tab3:
        st.markdown("### Itens em aberto (rápido)")
        if df_mes is None or df_mes.empty:
            st.info("Sem dados.")
        else:
            d = df_mes.copy()
            d["Valor"] = pd.to_numeric(d["Valor"], errors="coerce").fillna(0.0)
            d["Status"] = d["Status"].fillna("").apply(_clean_status)
            d["Tipo"] = d["Tipo"].fillna("").astype(str).str.title()
            done_mask = d["Status"].apply(_is_done_status)
            open_df = d[~done_mask].copy().sort_values(by="Valor", ascending=False)

            if open_df.empty:
                st.success("Tudo certo ✅ Nenhum item em aberto.")
            else:
                st.dataframe(
                    open_df[["Data", "Descrição", "Tipo", "Conta", "Valor", "Status"]],
                    use_container_width=True,
                    hide_index=True,
                )
                st.button(
                    "Abrir esses itens em Lançamentos",
                    type="primary",
                    use_container_width=True,
                    on_click=lambda: _set_lancamentos_filter(None, "open", "Itens em aberto"),
                )


# =========================
# LANÇAR
# =========================
elif page == "Lançar":
    st.subheader(f"Lançar — {mes_label_long}")

    ok_map, msg_map = _validate_mapping_for_insert(mapping_mes)
    if not ok_map:
        st.error(msg_map)
        st.stop()

    contas = sorted([c for c in df_mes["Conta"].dropna().astype(str).unique().tolist() if c.strip()]) if not df_mes.empty else []
    recs = sorted([c for c in df_mes["Recorrência"].dropna().astype(str).unique().tolist() if c.strip()]) if not df_mes.empty else ["Fixa", "Variável"]

    tipos = ["Receita", "Despesa"]
    status_opts = ["EM ABERTO", "PAGO", "RECEBIDO"]

    c1, c2, c3 = st.columns([1.2, 1, 1])
    with c1:
        data = st.text_input("Data", value=datetime.now().strftime("%d/%m/%Y"))
    with c2:
        tipo = st.selectbox("Tipo", tipos, index=1)
    with c3:
        status = st.selectbox("Status", status_opts, index=0)

    desc = st.text_input("Descrição")

    c4, c5, c6 = st.columns([2, 1, 1])
    with c4:
        conta = st.selectbox("Categoria (Conta)", options=contas if contas else ["(Digite abaixo)"], index=0)
        if conta == "(Digite abaixo)":
            conta = st.text_input("Categoria (Conta) - manual")
    with c5:
        recorr = st.selectbox("Recorrência", options=recs if recs else ["Fixa", "Variável"], index=0)
    with c6:
        valor = st.text_input("Valor")

    if st.button("Salvar lançamento", type="primary", use_container_width=True):
        payload = {
            "Data": data.strip(),
            "Descrição": desc.strip(),
            "Tipo": tipo.strip(),
            "Recorrência": recorr.strip(),
            "Conta": conta.strip(),
            "Valor": _to_float_br(valor),
            "Status": status.strip(),
        }

        if not payload["Descrição"]:
            st.error("Informe a descrição.")
        elif payload["Valor"] <= 0:
            st.error("Informe um valor válido.")
        else:
            try:
                append_row_in_table(mes_title, mapping_mes, payload)
                st.success("Lançamento salvo ✅ (dentro da tabela)")
                st.cache_data.clear()
                st.rerun()
            except Exception as e:
                st.error(f"Erro ao salvar: {e}")


# =========================
# LANÇAMENTOS
# =========================
elif page == "Lançamentos":
    title_hint = st.session_state.get("lanc_filter_title", "")
    if title_hint:
        st.subheader(f"Lançamentos — {mes_label_long}  •  {title_hint}")
    else:
        st.subheader(f"Lançamentos — {mes_label_long}")

    f_tipo = st.session_state.get("lanc_filter_tipo", None)
    f_status_mode = st.session_state.get("lanc_filter_status_mode", None)

    if "lanc_search" not in st.session_state:
        st.session_state["lanc_search"] = ""
    q = st.text_input("Buscar (Descrição / Categoria)", value=st.session_state["lanc_search"])
    st.session_state["lanc_search"] = q

    cA, cB, cC = st.columns([2, 2, 1])
    with cA:
        st.caption(f"Filtro Tipo: **{f_tipo if f_tipo else 'Todos'}**")
    with cB:
        st.caption(f"Filtro Status: **{'Em aberto' if f_status_mode=='open' else 'Pago/Recebido' if f_status_mode=='done' else 'Todos'}**")
    with cC:
        if st.button("Limpar filtros", use_container_width=True):
            st.session_state["lanc_filter_tipo"] = None
            st.session_state["lanc_filter_status_mode"] = None
            st.session_state["lanc_filter_title"] = ""
            st.session_state["lanc_search"] = ""
            st.rerun()

    dfx = df_mes.copy()
    if dfx.empty:
        st.info("Sem lançamentos neste mês.")
        st.stop()

    if f_tipo:
        dfx = dfx[dfx["Tipo"].fillna("").astype(str).str.title() == f_tipo].copy()

    dfx["Status"] = dfx["Status"].fillna("").apply(_clean_status)
    done_mask = dfx["Status"].apply(_is_done_status)
    if f_status_mode == "open":
        dfx = dfx[~done_mask].copy()
    elif f_status_mode == "done":
        dfx = dfx[done_mask].copy()

    if q.strip():
        qq = q.strip().lower()
        dfx = dfx[
            dfx["Descrição"].fillna("").astype(str).str.lower().str.contains(qq)
            | dfx["Conta"].fillna("").astype(str).str.lower().str.contains(qq)
        ].copy()

    if dfx.empty:
        st.info("Sem lançamentos para este filtro.")
        st.stop()

    st.markdown("### Edição rápida (Status e Valor)")
    st.caption("Edite na tabela e clique em **Salvar alterações**. Não apaga outras colunas.")

    view_cols = ["Data", "Descrição", "Tipo", "Conta", "Recorrência", "Valor", "Status", "__row"]
    dview = dfx[view_cols].copy()

    edited = st.data_editor(
        dview,
        use_container_width=True,
        hide_index=True,
        column_config={
            "__row": st.column_config.NumberColumn("Linha", disabled=True),
            "Valor": st.column_config.NumberColumn("Valor", format="%.2f"),
            "Status": st.column_config.SelectboxColumn("Status", options=["EM ABERTO", "PAGO", "RECEBIDO"]),
        },
        disabled=["Data", "Descrição", "Tipo", "Conta", "Recorrência"],
        key="editor_lanc",
    )

    base = dview.copy()
    base["Status"] = base["Status"].apply(_clean_status)
    edited2 = edited.copy()
    edited2["Status"] = edited2["Status"].apply(_clean_status)
    edited2["Valor"] = pd.to_numeric(edited2["Valor"], errors="coerce").fillna(0.0)

    changed_rows = []
    for i in range(len(base)):
        if float(base.loc[i, "Valor"]) != float(edited2.loc[i, "Valor"]) or str(base.loc[i, "Status"]) != str(edited2.loc[i, "Status"]):
            changed_rows.append(i)

    if st.button("Salvar alterações", type="primary", use_container_width=True):
        if not changed_rows:
            st.info("Nenhuma alteração detectada.")
        else:
            ok = 0
            fail = 0
            for i in changed_rows:
                row_number = int(edited2.loc[i, "__row"])
                payload = {"Valor": float(edited2.loc[i, "Valor"]), "Status": str(edited2.loc[i, "Status"])}
                try:
                    update_row_in_sheet(mes_title, row_number, mapping_mes, payload)
                    ok += 1
                except Exception:
                    fail += 1

            st.cache_data.clear()
            if fail == 0:
                st.success(f"Salvo! ✅ ({ok} itens atualizados)")
            else:
                st.warning(f"Concluído com avisos: {ok} ok / {fail} falharam. Tente novamente.")
            st.rerun()


# =========================
# RELATÓRIOS
# =========================
elif page == "Relatórios":
    st.subheader("Relatórios")

    labels_map = {o["label_long"]: o["title"] for o in options}
    labels_list = list(labels_map.keys())
    default_sel = [options[-1]["label_long"]]

    meses_sel_labels = st.multiselect("Meses", labels_list, default=default_sel)

    if st.button("Gerar PDF", type="primary", use_container_width=True):
        dfs = []
        for lab in meses_sel_labels:
            t = labels_map[lab]
            dfx, _ = read_month_sheet_cached(t)
            if dfx is not None and not dfx.empty:
                dfs.append(dfx)

        if not dfs:
            st.warning("Sem dados nos meses selecionados.")
            st.stop()

        df_periodo = pd.concat(dfs, ignore_index=True)
        pdf_bytes = build_pdf_report(df_periodo, meses_sel_labels, "Relatório Financeiro do Casal")

        st.download_button(
            "Baixar PDF",
            data=pdf_bytes,
            file_name=f"relatorio_financeiro_{datetime.now().strftime('%Y%m%d_%H%M')}.pdf",
            mime="application/pdf",
            use_container_width=True,
        )
