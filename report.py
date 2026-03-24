import re
import streamlit as st
import pandas as pd

st.set_page_config(page_title="Delivery Sorter", page_icon="🚚", layout="wide")

# =========================
# CONFIG
# =========================
REQUIRED_COLUMNS = [
    "BILL_NUMBER",
    "DESTNAME",
    "DESTINATION",
    "DELIVER_BY",
    "DELIVER_BY_END",
    "PALLETS",
    "PIECES",
    "DESTPROV",
    "DESTCITY",
    "REQUESTED_EQUIPMEN",
]

OPTIONAL_COLUMNS = [
    "END_ZONE",
    "DESTZONE",
]

FINAL_COLUMNS = [
    "BILL_NUMBER",
    "DESTNAME",
    "DESTINATION",
    "DELIVER_BY",
    "DELIVER_BY_END",
    "PALLETS",
    "PIECES",
    "DESTPROV",
    "POSTAL_CODE",
    "DESTCITY",
    "REQUESTED_EQUIPMEN",
]

SEARCHABLE_COLUMNS = FINAL_COLUMNS

# TJX STORES
TJX_PREFIXES = (
    "WINNERS",
    "HOMESENSE",
    "MARSHALLS",
)

EXCLUDED_DESTNAME_PREFIXES_MAIN = (
    "WINNERS",
    "HOMESENSE",
    "MARSHALLS",
    "REMCO CANADIAN CONSOL",
    "JERRY COHEN FORWARDERS LTD",
    "REMCO CANADAIAN CONSOL",
    "MTL FREIGHT",
)

EXCLUDED_DESTNAME_PREFIXES_STOKES = (
    "THINK KITCHEN",
    "STOKES",
)

# =========================
# HELPERS
# =========================
def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [str(col).strip().upper() for col in df.columns]
    return df


def load_csv(uploaded_file) -> pd.DataFrame:
    try:
        df = pd.read_csv(uploaded_file)
    except Exception:
        uploaded_file.seek(0)
        df = pd.read_csv(uploaded_file, encoding="latin1")
    return df


def parse_datetime_column(series: pd.Series) -> pd.Series:
    s = series.fillna("").astype(str).str.strip()
    parsed = pd.to_datetime(s, errors="coerce")

    mask = parsed.isna() & s.ne("")
    if mask.any():
        parsed.loc[mask] = pd.to_datetime(
            s.loc[mask],
            format="%m/%d/%Y %I:%M %p",
            errors="coerce"
        )
    return parsed


def clean_numeric_column(series: pd.Series) -> pd.Series:
    return pd.to_numeric(
        series.fillna("")
        .astype(str)
        .str.replace(",", "", regex=False)
        .str.strip(),
        errors="coerce"
    ).fillna(0)


def extract_canadian_postal(text: str) -> str:
    if pd.isna(text):
        return ""
    value = str(text).upper().strip()
    match = re.search(r"\b([A-Z]\d[A-Z])\s?(\d[A-Z]\d)\b", value)
    if match:
        return f"{match.group(1)} {match.group(2)}"
    return ""


def extract_us_zip(text: str) -> str:
    if pd.isna(text):
        return ""
    match = re.search(r"\b\d{5}(?:-\d{4})?\b", str(text))
    return match.group(0) if match else ""


def extract_postal_code(text: str) -> str:
    return extract_canadian_postal(text) or extract_us_zip(text)


def get_postal_code(row) -> str:
    for col in ["END_ZONE", "DESTZONE", "DESTINATION", "DESTNAME"]:
        if col in row.index:
            postal = extract_postal_code(row.get(col, ""))
            if postal:
                return postal
    return ""


def prepare_data(df: pd.DataFrame) -> pd.DataFrame:
    df = normalize_columns(df)

    missing = [col for col in REQUIRED_COLUMNS if col not in df.columns]
    if missing:
        raise ValueError(f"Missing columns: {missing}")

    df = df[REQUIRED_COLUMNS + [c for c in OPTIONAL_COLUMNS if c in df.columns]]

    df = df[~df["BILL_NUMBER"].str.upper().str.startswith("LB", na=False)]
    df = df[df["BILL_NUMBER"].ne("")]

    df["PIECES"] = clean_numeric_column(df["PIECES"])
    df["PALLETS"] = clean_numeric_column(df["PALLETS"])

    df["DELIVER_BY_DT"] = parse_datetime_column(df["DELIVER_BY"])
    df["DELIVER_BY_END_DT"] = parse_datetime_column(df["DELIVER_BY_END"])

    df["POSTAL_CODE"] = df.apply(get_postal_code, axis=1)

    df["SORT"] = df["DELIVER_BY_END_DT"].combine_first(df["DELIVER_BY_DT"])

    df = df.sort_values(by=["SORT", "POSTAL_CODE", "BILL_NUMBER"])

    df["DELIVER_BY"] = df["DELIVER_BY_DT"].dt.strftime("%Y-%m-%d %I:%M %p").fillna("")
    df["DELIVER_BY_END"] = df["DELIVER_BY_END_DT"].dt.strftime("%Y-%m-%d %I:%M %p").fillna("")

    return df[FINAL_COLUMNS]


def is_tjx(name: str) -> bool:
    if not name:
        return False
    return str(name).upper().startswith(TJX_PREFIXES)


def remove_tjx(df):
    return df[~df["DESTNAME"].apply(is_tjx)].copy()


def keep_only_tjx(df):
    return df[df["DESTNAME"].apply(is_tjx)].copy()


def remove_stokes(df):
    return df[~df["DESTNAME"].str.upper().str.startswith(EXCLUDED_DESTNAME_PREFIXES_STOKES)].copy()


def search_filter(df, field, value):
    if not value:
        return df
    return df[df[field].astype(str).str.contains(value, case=False, na=False)]


# =========================
# UI
# =========================
st.title("🚚 Delivery Sorter")

uploaded_file = st.file_uploader("Upload CSV", type=["csv"])

if uploaded_file:
    df = prepare_data(load_csv(uploaded_file))

    st.subheader("Filters")

    col1, col2, col3, col4, col5 = st.columns([1.2, 1.2, 1.2, 1.2, 2])

    remove_tjx_check = col1.checkbox("Remove TJX")
    only_tjx_check = col2.checkbox("Show ONLY TJX")
    remove_stokes_check = col3.checkbox("Remove Stokes")

    field = col4.selectbox("Field", SEARCHABLE_COLUMNS)
    search = col5.text_input("Search")

    working_df = df.copy()

    # PRIORITY LOGIC
    if only_tjx_check:
        working_df = keep_only_tjx(working_df)
    elif remove_tjx_check:
        working_df = remove_tjx(working_df)

    if remove_stokes_check:
        working_df = remove_stokes(working_df)

    working_df = search_filter(working_df, field, search)

    st.metric("Total Deliveries", len(working_df))
    st.metric("Total Pallets", working_df["PALLETS"].sum())

    st.dataframe(working_df, use_container_width=True)

    st.download_button(
        "Download CSV",
        working_df.to_csv(index=False).encode(),
        "sorted.csv"
    )
