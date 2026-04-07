import re
import streamlit as st
import pandas as pd
from st_aggrid import AgGrid, GridOptionsBuilder, DataReturnMode, JsCode

st.set_page_config(page_title="Delivery Day Planner", page_icon="🚚", layout="wide")

# =========================
# CONFIG
# =========================
REQUIRED_COLUMNS = [
    "BILL_NUMBER",
    "DELIVERY_APPT_REQ",
    "DELIVER_BY",
    "DELIVER_BY_END",
]

OPTIONAL_COLUMNS = [
    "CURRENT_STATUS",
    "DELIVERY_APPT_MADE",
    "CARE_OF_NAME",
    "CARE_OF_ADDR1",
    "CARE_OF_CITY",
    "CARE_OF_ADDR2",
    "CARE_OF_PROV",
    "CARE_OF_PC",
    "ROLLUP_PIECES",
    "ROLLUP_PALLETS",
    "COMMODITY",
    "ROLLUP_CUBE",
    "ROLLUP_WEIGHT",
    "ROUTE_DESIGNATION",
    "LATEST_PICK_UP_BY",
    "CONSIGNEE_AVG_DWELL_TIME",
]

TJX_PREFIXES = (
    "WINNERS",
    "HOMESENSE",
    "MARSHALLS",
)

EXCLUDED_DESTNAME_PREFIXES_STOKES = (
    "THINK KITCHEN",
    "STOKES",
)

IGNORED_CUSTOMER_NAMES = {
    "REMCO CANADIAN CONSOL",
    "HOMESENSE #025 REMCO DOCK",
    "WINNERS (20) REMCO DOCK",
}

DISPLAY_COLUMNS_PRIORITY = [
    "PLANNED",
    "BILL_NUMBER",
    "CURRENT_STATUS",
    "CUSTOMER_NAME",
    "DELIVERY_APPT_REQ",
    "DELIVERY_APPT_MADE",
    "DELIVER_BY",
    "DELIVER_BY_END",
    "ROLLUP_PALLETS",
    "ROLLUP_PIECES",
    "CARE_OF_CITY",
    "CARE_OF_PROV",
    "CARE_OF_PC",
]

# =========================
# HELPERS
# =========================
def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [str(col).strip().upper() for col in df.columns]
    return df


def load_csv(uploaded_file) -> pd.DataFrame:
    read_options = [
        {"dtype": str},
        {"encoding": "latin1", "dtype": str},
        {"encoding": "cp1252", "dtype": str},
    ]

    last_error = None
    for opts in read_options:
        try:
            uploaded_file.seek(0)
            return pd.read_csv(uploaded_file, **opts)
        except Exception as e:
            last_error = e

    raise last_error


def parse_datetime_value(value):
    if pd.isna(value):
        return pd.NaT

    s = str(value).strip()
    if not s or s.upper() == "NAN":
        return pd.NaT

    s = re.sub(r"\s+", " ", s)

    parsed = pd.to_datetime(s, errors="coerce")
    if pd.notna(parsed):
        return parsed

    formats = [
        "%Y-%m-%d %I:%M:%S %p",
        "%Y-%m-%d %I:%M %p",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%d",
        "%m/%d/%Y %I:%M:%S %p",
        "%m/%d/%Y %I:%M %p",
        "%m/%d/%Y %H:%M:%S",
        "%m/%d/%Y %H:%M",
        "%m/%d/%Y",
        "%d/%m/%Y %I:%M:%S %p",
        "%d/%m/%Y %I:%M %p",
        "%d/%m/%Y %H:%M:%S",
        "%d/%m/%Y %H:%M",
        "%d/%m/%Y",
    ]

    for fmt in formats:
        try:
            return pd.to_datetime(s, format=fmt, errors="raise")
        except Exception:
            pass

    return pd.NaT


def parse_datetime_column(series: pd.Series) -> pd.Series:
    return series.apply(parse_datetime_value)


def clean_numeric_column(series: pd.Series) -> pd.Series:
    return pd.to_numeric(
        series.fillna("")
        .astype(str)
        .str.replace(",", "", regex=False)
        .str.strip(),
        errors="coerce"
    ).fillna(0)


def normalize_bool_text(value) -> str:
    if pd.isna(value):
        return ""
    v = str(value).strip().lower()
    if v in ["true", "yes", "y", "1"]:
        return "TRUE"
    if v in ["false", "no", "n", "0"]:
        return "FALSE"
    return str(value).strip().upper()


def normalize_name(value) -> str:
    if pd.isna(value):
        return ""
    return str(value).strip().upper()


def get_customer_name(row) -> str:
    for col in ["CARE_OF_NAME", "DESTNAME", "CUSTOMER_NAME", "CONSIGNEE", "SHIP_TO_NAME"]:
        if col in row.index:
            val = str(row.get(col, "")).strip()
            if val and val.upper() != "NAN":
                return val
    return ""


def is_tjx(name: str) -> bool:
    if not name:
        return False
    return str(name).upper().startswith(TJX_PREFIXES)


def is_stokes(name: str) -> bool:
    if not name:
        return False
    return str(name).upper().startswith(EXCLUDED_DESTNAME_PREFIXES_STOKES)


def is_ignored_customer(name: str) -> bool:
    if not name:
        return False
    ignored = {normalize_name(x) for x in IGNORED_CUSTOMER_NAMES}
    return normalize_name(name) in ignored


def compute_window_hours(start_dt, end_dt) -> float:
    if pd.notna(start_dt) and pd.notna(end_dt):
        hours = (end_dt - start_dt).total_seconds() / 3600
        return round(max(hours, 0), 2)
    return 9999.0


def compute_priority_score(row) -> float:
    appt_req = str(row.get("DELIVERY_APPT_REQ_NORM", "")).upper() == "TRUE"
    deliver_by_dt = row.get("DELIVER_BY_DT")
    deliver_by_end_dt = row.get("DELIVER_BY_END_DT")
    window_hours = row.get("WINDOW_HOURS", 9999.0)

    base = 0 if appt_req else 100000

    if pd.notna(deliver_by_end_dt):
        end_value = deliver_by_end_dt.timestamp()
    elif pd.notna(deliver_by_dt):
        end_value = deliver_by_dt.timestamp()
    else:
        end_value = 9999999999

    return base + (window_hours * 1000) + end_value


def prepare_data(df: pd.DataFrame) -> pd.DataFrame:
    df = normalize_columns(df)

    missing = [col for col in REQUIRED_COLUMNS if col not in df.columns]
    if missing:
        raise ValueError(f"Missing columns: {missing}")

    keep_cols = list(dict.fromkeys(REQUIRED_COLUMNS + OPTIONAL_COLUMNS + list(df.columns)))
    df = df[[c for c in keep_cols if c in df.columns]].copy()

    df["BILL_NUMBER"] = df["BILL_NUMBER"].fillna("").astype(str).str.strip()
    df = df[df["BILL_NUMBER"].ne("")]
    df = df[~df["BILL_NUMBER"].str.upper().str.startswith("LB", na=False)]

    df["DELIVER_BY_DT"] = parse_datetime_column(df["DELIVER_BY"])
    df["DELIVER_BY_END_DT"] = parse_datetime_column(df["DELIVER_BY_END"])

    df["PLANNING_DT"] = df["DELIVER_BY_DT"].combine_first(df["DELIVER_BY_END_DT"])
    df = df[df["PLANNING_DT"].notna()].copy()
    df["PLANNING_DATE"] = df["PLANNING_DT"].dt.date

    if "ROLLUP_PIECES" in df.columns:
        df["ROLLUP_PIECES"] = clean_numeric_column(df["ROLLUP_PIECES"])
    else:
        df["ROLLUP_PIECES"] = 0

    if "ROLLUP_PALLETS" in df.columns:
        df["ROLLUP_PALLETS"] = clean_numeric_column(df["ROLLUP_PALLETS"])
    else:
        df["ROLLUP_PALLETS"] = 0

    df["DELIVERY_APPT_REQ_NORM"] = df["DELIVERY_APPT_REQ"].apply(normalize_bool_text)

    if "DELIVERY_APPT_MADE" in df.columns:
        df["DELIVERY_APPT_MADE"] = df["DELIVERY_APPT_MADE"].apply(normalize_bool_text)
    else:
        df["DELIVERY_APPT_MADE"] = ""

    if "CURRENT_STATUS" not in df.columns:
        df["CURRENT_STATUS"] = ""

    df["CUSTOMER_NAME"] = df.apply(get_customer_name, axis=1)

    df["WINDOW_HOURS"] = df.apply(
        lambda row: compute_window_hours(row["DELIVER_BY_DT"], row["DELIVER_BY_END_DT"]),
        axis=1
    )
    df["PRIORITY_SCORE"] = df.apply(compute_priority_score, axis=1)

    df["DELIVER_BY"] = df["DELIVER_BY_DT"].dt.strftime("%Y-%m-%d %I:%M %p").fillna("")
    df["DELIVER_BY_END"] = df["DELIVER_BY_END_DT"].dt.strftime("%Y-%m-%d %I:%M %p").fillna("")

    return df


def remove_tjx(df: pd.DataFrame) -> pd.DataFrame:
    return df[~df["CUSTOMER_NAME"].apply(is_tjx)].copy()


def keep_only_tjx(df: pd.DataFrame) -> pd.DataFrame:
    return df[df["CUSTOMER_NAME"].apply(is_tjx)].copy()


def remove_stokes(df: pd.DataFrame) -> pd.DataFrame:
    return df[~df["CUSTOMER_NAME"].apply(is_stokes)].copy()


def remove_ignored_customers(df: pd.DataFrame) -> pd.DataFrame:
    return df[~df["CUSTOMER_NAME"].apply(is_ignored_customer)].copy()


# =========================
# AGGRID STYLE
# =========================
cell_style_js = JsCode("""
function(params) {
    if (params.colDef.field === 'PLANNED') {
        return {
            'color': '#111111',
            'fontWeight': 'normal',
            'textAlign': 'center'
        };
    }

    const req = String(params.data.DELIVERY_APPT_REQ || '').toUpperCase();
    const made = String(params.data.DELIVERY_APPT_MADE || '').toUpperCase();

    if (req === 'TRUE' && made === 'FALSE') {
        return {
            'color': '#d62828',
            'fontWeight': '700'
        };
    }

    if (req === 'TRUE' && made === 'TRUE') {
        return {
            'color': '#2e8b57',
            'fontWeight': '700'
        };
    }

    return {
        'color': '#111111'
    };
}
""")

planned_cell_style_js = JsCode("""
function(params) {
    return {
        'textAlign': 'center'
    };
}
""")


# =========================
# UI
# =========================
st.title("🚚 Delivery Day Planner")

uploaded_file = st.file_uploader("Upload CSV", type=["csv"])

if uploaded_file:
    try:
        raw_df = load_csv(uploaded_file)
        df = prepare_data(raw_df)

        available_dates = sorted(df["PLANNING_DATE"].dropna().unique())

        if not available_dates:
            st.warning("Aucune date valide trouvée dans le fichier.")
            st.stop()

        st.subheader("1) Select a delivery date")
        selected_date = st.selectbox(
            "Dates found in the file",
            available_dates,
            format_func=lambda x: x.strftime("%Y-%m-%d")
        )

        working_df = df[df["PLANNING_DATE"] == selected_date].copy()

        st.subheader("2) Filters")
        col1, col2, col3 = st.columns(3)

        remove_tjx_check = col1.checkbox("Remove TJX")
        only_tjx_check = col2.checkbox("Show ONLY TJX")
        remove_stokes_check = col3.checkbox("Remove Stokes")

        working_df = remove_ignored_customers(working_df)

        if only_tjx_check:
            working_df = keep_only_tjx(working_df)
        elif remove_tjx_check:
            working_df = remove_tjx(working_df)

        if remove_stokes_check:
            working_df = remove_stokes(working_df)

        working_df = working_df.sort_values(
            by=["PRIORITY_SCORE", "WINDOW_HOURS", "DELIVER_BY_END_DT", "DELIVER_BY_DT", "BILL_NUMBER"],
            ascending=[True, True, True, True, True]
        ).reset_index(drop=True)

        state_key = f"planned_map_{selected_date}"
        if state_key not in st.session_state:
            st.session_state[state_key] = {}

        display_columns = [col for col in DISPLAY_COLUMNS_PRIORITY if col in working_df.columns]
        display_df = working_df[display_columns].copy()

        if "DELIVERY_APPT_REQ_NORM" in working_df.columns:
            display_df["DELIVERY_APPT_REQ"] = working_df["DELIVERY_APPT_REQ_NORM"].values

        bill_numbers = working_df["BILL_NUMBER"].astype(str).tolist()
        saved_map = st.session_state[state_key]

        planned_values = [bool(saved_map.get(bill, False)) for bill in bill_numbers]

        if "PLANNED" not in display_df.columns:
            display_df.insert(0, "PLANNED", planned_values)
        else:
            display_df["PLANNED"] = planned_values

        total_deliveries = len(working_df)
        total_pallets = working_df["ROLLUP_PALLETS"].sum() if "ROLLUP_PALLETS" in working_df.columns else 0
        total_pieces = working_df["ROLLUP_PIECES"].sum() if "ROLLUP_PIECES" in working_df.columns else 0

        total_red = (
            (
                (working_df["DELIVERY_APPT_REQ_NORM"].astype(str).str.upper() == "TRUE") &
                (working_df["DELIVERY_APPT_MADE"].astype(str).str.upper() == "FALSE")
            ).sum()
            if "DELIVERY_APPT_REQ_NORM" in working_df.columns and "DELIVERY_APPT_MADE" in working_df.columns
            else 0
        )

        total_green = (
            (
                (working_df["DELIVERY_APPT_REQ_NORM"].astype(str).str.upper() == "TRUE") &
                (working_df["DELIVERY_APPT_MADE"].astype(str).str.upper() == "TRUE")
            ).sum()
            if "DELIVERY_APPT_REQ_NORM" in working_df.columns and "DELIVERY_APPT_MADE" in working_df.columns
            else 0
        )

        planned_count_before = int(display_df["PLANNED"].astype(bool).sum()) if "PLANNED" in display_df.columns else 0

        st.subheader("3) Daily planning summary")
        m1, m2, m3, m4, m5, m6 = st.columns(6)
        m1.metric("Total Deliveries", total_deliveries)
        m2.metric("Total Pallets", f"{total_pallets:,.0f}")
        m3.metric("Total Pieces", f"{total_pieces:,.0f}")
        m4.metric("Appt Missing", int(total_red))
        m5.metric("Appt Done", int(total_green))
        m6.metric("Planned", int(planned_count_before))

        st.markdown(f"**Bills déjà planifiés : {planned_count_before} / {len(display_df)}**")

        st.subheader("4) Daily planning view")

        gb = GridOptionsBuilder.from_dataframe(display_df)
        gb.configure_default_column(
            editable=False,
            filter=True,
            sortable=True,
            resizable=True,
            cellStyle=cell_style_js,
        )

        gb.configure_column(
            "PLANNED",
            headerName="PLANNED",
            editable=True,
            cellEditor="agCheckboxCellEditor",
            cellRenderer="agCheckboxCellRenderer",
            width=110,
            pinned="left",
            cellStyle=planned_cell_style_js,
        )

        for col in display_df.columns:
            if col != "PLANNED":
                gb.configure_column(col, cellStyle=cell_style_js)

        gb.configure_grid_options(
            rowSelection="single",
            suppressRowClickSelection=True,
            domLayout="normal",
        )

        grid_options = gb.build()

        grid_response = AgGrid(
            display_df,
            gridOptions=grid_options,
            data_return_mode=DataReturnMode.AS_INPUT,
            update_on=["cellValueChanged"],
            fit_columns_on_grid_load=False,
            allow_unsafe_jscode=True,
            enable_enterprise_modules=False,
            height=650,
            theme="streamlit",
            key=f"aggrid_{selected_date}",
        )

        grid_data = grid_response.get("data", display_df)
        edited_df = pd.DataFrame(grid_data)

        if edited_df.empty:
            edited_df = display_df.copy()

        updated_map = {}
        for _, row in edited_df.iterrows():
            bill = str(row["BILL_NUMBER"])
            updated_map[bill] = bool(row["PLANNED"])

        st.session_state[state_key] = updated_map

        export_df = working_df.copy()
        export_df.insert(0, "PLANNED", edited_df["PLANNED"].astype(bool).values)

        csv_export = export_df.to_csv(index=False).encode("utf-8")
        st.download_button(
            "Download filtered day CSV",
            csv_export,
            file_name=f"delivery_plan_{selected_date}.csv",
            mime="text/csv"
        )

    except Exception as e:
        st.error(f"Error processing file: {e}")
