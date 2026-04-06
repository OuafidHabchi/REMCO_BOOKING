import re
import streamlit as st
import pandas as pd

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

# ✅ AJOUTE ICI LES CUSTOMER NAMES À IGNORER
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
    """
    Plus le score est petit, plus la livraison est prioritaire.
    Logique :
    - appointment requis = priorité plus haute
    - fenêtre plus courte = priorité plus haute
    - heure de fin plus tôt = priorité plus haute
    """
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

    # Date utilisée pour sélection de journée
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


def get_row_style(row):
    """
    Rouge si DELIVERY_APPT_REQ = TRUE et DELIVERY_APPT_MADE = FALSE
    """
    appt_req = str(row.get("DELIVERY_APPT_REQ", "")).upper() == "TRUE"
    appt_made_false = str(row.get("DELIVERY_APPT_MADE", "")).upper() == "FALSE"

    if appt_req and appt_made_false:
        return "background-color: #ff4d4d; color: white; font-weight: 700;"
    return ""


def build_styler(df: pd.DataFrame):
    styles = pd.DataFrame("", index=df.index, columns=df.columns)

    for idx, row in df.iterrows():
        row_style = get_row_style(row)
        if row_style:
            for col in df.columns:
                if col != "PLANNED":
                    styles.at[idx, col] = row_style

    return df.style.apply(lambda _: styles, axis=None)


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

        # Ignore custom customer names
        working_df = remove_ignored_customers(working_df)

        if only_tjx_check:
            working_df = keep_only_tjx(working_df)
        elif remove_tjx_check:
            working_df = remove_tjx(working_df)

        if remove_stokes_check:
            working_df = remove_stokes(working_df)

        # TRI FINAL
        working_df = working_df.sort_values(
            by=["PRIORITY_SCORE", "WINDOW_HOURS", "DELIVER_BY_END_DT", "DELIVER_BY_DT", "BILL_NUMBER"],
            ascending=[True, True, True, True, True]
        ).reset_index(drop=True)

        st.subheader("3) Daily planning view")

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
        total_planned = st.session_state.get(f"planned_count_{selected_date}", 0)

        m1, m2, m3, m4, m5 = st.columns(5)
        m1.metric("Total Deliveries", total_deliveries)
        m2.metric("Total Pallets", f"{total_pallets:,.0f}")
        m3.metric("Total Pieces", f"{total_pieces:,.0f}")
        m4.metric("Appt Req TRUE / Made FALSE", int(total_red))
        m5.metric("Planned", int(total_planned))

        display_columns = [col for col in DISPLAY_COLUMNS_PRIORITY if col in working_df.columns]
        display_df = working_df[display_columns].copy()

        if "DELIVERY_APPT_REQ_NORM" in working_df.columns:
            display_df["DELIVERY_APPT_REQ"] = working_df["DELIVERY_APPT_REQ_NORM"].values

        # ✅ Colonne checkbox au début
        if "PLANNED" not in display_df.columns:
            display_df.insert(0, "PLANNED", False)
        else:
            display_df["PLANNED"] = display_df["PLANNED"].fillna(False).astype(bool)

        editor_key = f"delivery_editor_{selected_date}"

        edited_df = st.data_editor(
            display_df,
            use_container_width=True,
            height=650,
            hide_index=True,
            key=editor_key,
            column_config={
                "PLANNED": st.column_config.CheckboxColumn(
                    "PLANNED",
                    help="Coche si cette livraison est déjà planifiée",
                    default=False,
                )
            },
            disabled=[col for col in display_df.columns if col != "PLANNED"],
        )

        planned_count = int(edited_df["PLANNED"].sum()) if "PLANNED" in edited_df.columns else 0
        st.session_state[f"planned_count_{selected_date}"] = planned_count

        st.markdown(f"**Bills déjà planifiés : {planned_count} / {len(edited_df)}**")

        # ✅ Petit tableau rouge/normal avec checkbox gardée
        styled_preview = build_styler(edited_df)

        with st.expander("Preview with alert colors"):
            st.dataframe(
                styled_preview,
                use_container_width=True,
                height=500
            )

        export_df = working_df.copy()
        export_df.insert(0, "PLANNED", edited_df["PLANNED"].values)

        csv_export = export_df.to_csv(index=False).encode("utf-8")
        st.download_button(
            "Download filtered day CSV",
            csv_export,
            file_name=f"delivery_plan_{selected_date}.csv",
            mime="text/csv"
        )

    except Exception as e:
        st.error(f"Erreur: {e}")
