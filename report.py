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

SEARCHABLE_COLUMNS = [
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

    mask = parsed.isna() & s.ne("") & s.ne("nan") & s.ne("NaT")
    if mask.any():
        parsed.loc[mask] = pd.to_datetime(
            s.loc[mask],
            format="%m/%d/%Y %I:%M %p",
            errors="coerce"
        )

    mask = parsed.isna() & s.ne("") & s.ne("nan") & s.ne("NaT")
    if mask.any():
        parsed.loc[mask] = pd.to_datetime(
            s.loc[mask],
            format="%m/%d/%Y %I:%M:%S %p",
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

    value = str(text).strip()
    match = re.search(r"\b\d{5}(?:-\d{4})?\b", value)

    if match:
        return match.group(0)

    return ""


def extract_postal_code(text: str) -> str:
    canadian = extract_canadian_postal(text)
    if canadian:
        return canadian

    us_zip = extract_us_zip(text)
    if us_zip:
        return us_zip

    return ""


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
        raise ValueError(f"Missing required columns: {', '.join(missing)}")

    cols_to_keep = REQUIRED_COLUMNS.copy()
    for col in OPTIONAL_COLUMNS:
        if col in df.columns:
            cols_to_keep.append(col)

    df = df[cols_to_keep].copy()

    text_cols = [
        "BILL_NUMBER",
        "DESTNAME",
        "DESTINATION",
        "DESTPROV",
        "DESTCITY",
        "REQUESTED_EQUIPMEN",
        "DELIVER_BY",
        "DELIVER_BY_END",
    ]

    for col in ["END_ZONE", "DESTZONE"]:
        if col in df.columns:
            text_cols.append(col)

    for col in text_cols:
        if col in df.columns:
            df[col] = df[col].fillna("").astype(str).str.strip()

    df = df[~df["BILL_NUMBER"].str.upper().str.startswith("LB", na=False)].copy()
    df = df[df["BILL_NUMBER"].ne("")].copy()

    df["PIECES"] = clean_numeric_column(df["PIECES"])
    df["PALLETS"] = clean_numeric_column(df["PALLETS"])

    df["DELIVER_BY_DT"] = parse_datetime_column(df["DELIVER_BY"])
    df["DELIVER_BY_END_DT"] = parse_datetime_column(df["DELIVER_BY_END"])

    df["POSTAL_CODE"] = df.apply(get_postal_code, axis=1)

    df = df.drop_duplicates(subset=["BILL_NUMBER"], keep="first").copy()

    df["SORT_DATETIME"] = df["DELIVER_BY_END_DT"].combine_first(df["DELIVER_BY_DT"])
    df["SORT_POSTAL"] = df["POSTAL_CODE"].fillna("").astype(str).str.upper().str.strip()

    df = df.sort_values(
        by=["SORT_DATETIME", "DELIVER_BY_DT", "SORT_POSTAL", "BILL_NUMBER"],
        ascending=[True, True, True, True],
        na_position="last"
    ).reset_index(drop=True)

    df["DELIVER_BY"] = df["DELIVER_BY_DT"].dt.strftime("%Y-%m-%d %I:%M %p").fillna("")
    df["DELIVER_BY_END"] = df["DELIVER_BY_END_DT"].dt.strftime("%Y-%m-%d %I:%M %p").fillna("")

    final_df = df[FINAL_COLUMNS].copy()
    return final_df


def remove_main_excluded_destinations(df: pd.DataFrame) -> pd.DataFrame:
    dest_upper = df["DESTNAME"].fillna("").astype(str).str.strip().str.upper()
    mask_excluded = dest_upper.str.startswith(EXCLUDED_DESTNAME_PREFIXES_MAIN, na=False)
    return df[~mask_excluded].copy()


def remove_stokes_destinations(df: pd.DataFrame) -> pd.DataFrame:
    dest_upper = df["DESTNAME"].fillna("").astype(str).str.strip().str.upper()
    mask_excluded = dest_upper.str.startswith(EXCLUDED_DESTNAME_PREFIXES_STOKES, na=False)
    return df[~mask_excluded].copy()


def apply_search_filter(df: pd.DataFrame, selected_field: str, search_value: str) -> pd.DataFrame:
    if not search_value or not selected_field:
        return df

    return df[
        df[selected_field]
        .fillna("")
        .astype(str)
        .str.contains(search_value, case=False, na=False)
    ].copy()


def dataframe_to_csv_bytes(df: pd.DataFrame) -> bytes:
    return df.to_csv(index=False).encode("utf-8")


# =========================
# UI
# =========================
st.title("🚚 Delivery Sorter")
st.write(
    "Upload your CSV file to extract deliveries, exclude bill numbers starting with LB, "
    "detect postal codes, sort the deliveries, and search by any selected field."
)

uploaded_file = st.file_uploader("Upload CSV file", type=["csv"])

if uploaded_file is not None:
    try:
        raw_df = load_csv(uploaded_file)
        final_df = prepare_data(raw_df)

        st.subheader("Filters")

        filter_col1, filter_col2, filter_col3, filter_col4 = st.columns([1.4, 1.1, 1.1, 2])

        remove_main_group = filter_col1.checkbox(
            "Remove Winners / Homesense / Marshalls / Remco / Jerry Cohen",
            value=False
        )

        remove_stokes = filter_col2.checkbox(
            "Remove Stokes / Think Kitchen",
            value=False
        )

        selected_field = filter_col3.selectbox(
            "Select a field",
            SEARCHABLE_COLUMNS
        )

        search_value = filter_col4.text_input(
            "Search value",
            value="",
            placeholder="Type to search..."
        )

        working_df = final_df.copy()

        if remove_main_group:
            working_df = remove_main_excluded_destinations(working_df)

        if remove_stokes:
            working_df = remove_stokes_destinations(working_df)

        filtered_df = apply_search_filter(working_df, selected_field, search_value)

        st.success(f"File loaded successfully. {len(filtered_df)} delivery record(s) displayed.")

        col1, col2, col3 = st.columns(3)
        col1.metric("Total Deliveries", len(filtered_df))
        col2.metric("Total Pieces", int(filtered_df["PIECES"].sum()) if len(filtered_df) > 0 else 0)
        col3.metric("Total Pallets", float(filtered_df["PALLETS"].sum()) if len(filtered_df) > 0 else 0.0)

        st.subheader("Sorted Deliveries")
        st.dataframe(filtered_df, use_container_width=True, hide_index=True)

        st.download_button(
            label="📥 Download filtered CSV",
            data=dataframe_to_csv_bytes(filtered_df),
            file_name="sorted_deliveries.csv",
            mime="text/csv",
        )

    except Exception as e:
        st.error(f"Error: {e}")

else:
    st.info("Please upload a CSV file to begin.")