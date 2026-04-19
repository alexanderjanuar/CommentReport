from __future__ import annotations

from datetime import date
import hmac
import json
import os
from pathlib import Path
import re

import gspread
import pandas as pd
import streamlit as st
from oauth2client.service_account import ServiceAccountCredentials
from dotenv import load_dotenv
from openai import OpenAI
try:
    from apify_client import ApifyClient
    APIFY_AVAILABLE = True
except ImportError:
    APIFY_AVAILABLE = False


APP_DIR = Path(__file__).resolve().parent
DEFAULT_CREDENTIALS = APP_DIR / "credentials.json"
DEFAULT_COMMENT_SHEET_KEY = "12r5OAZXCea_gATd9EEZu9ZCUVHktnpenoRqMefr-GI0"
POST_DETAIL_ACTOR_ID = "nH2AHrwxeTRJoN5hX"
COMMENT_ACTOR_ID = "shUXaQyLGCVuUYG36"
PARENT_ENV = APP_DIR / ".env"
COMMENT_COLUMNS = [
    "Nomor",
    "Username",
    "Komentar",
    "Username Target",
    "Link Post",
    "Timestamp",
]
SCOPE = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive",
]

load_dotenv(dotenv_path=PARENT_ENV if PARENT_ENV.exists() else None)
DEEPSEEK_API_KEY = os.getenv("DEEPSEEK_API_KEY")
APIFY_API_TOKEN = os.getenv("APIFY_API_TOKEN", "")
APP_PASSWORD = os.getenv("APP_PASSWORD", "")
deepseek_client = (
    OpenAI(api_key=DEEPSEEK_API_KEY, base_url="https://api.deepseek.com")
    if DEEPSEEK_API_KEY
    else None
)


st.set_page_config(
    page_title="Laporan Komentar",
    page_icon="ðŸ“„",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown(
    """
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Manrope:wght@400;500;600;700;800&display=swap');

    html, body, [class*="css"] {
        font-family: 'Manrope', sans-serif;
    }

    .stApp {
        background: #ffffff;
        color: #1f2937;
    }

    .block-container {
        padding-top: 1.5rem;
        padding-bottom: 2rem;
    }

    .hero {
        background: white;
        border: 1px solid #e5e7eb;
        border-radius: 18px;
        padding: 1.2rem 1.3rem;
        margin-bottom: 1rem;
    }

    .hero-title {
        font-size: 1.9rem;
        font-weight: 800;
        margin: 0;
        color: #111827;
    }

    .hero-subtitle {
        margin-top: 0.45rem;
        color: #6b7280;
        font-size: 0.98rem;
    }

    .metric-box {
        background: white;
        border: 1px solid #e5e7eb;
        border-radius: 16px;
        padding: 1rem;
    }

    .metric-label {
        font-size: 0.82rem;
        color: #6b7280;
        font-weight: 700;
        text-transform: uppercase;
        letter-spacing: 0.04em;
    }

    .metric-value {
        font-size: 1.9rem;
        font-weight: 800;
        color: #111827;
        margin-top: 0.4rem;
        line-height: 1.05;
    }

    div[data-testid="stDataFrame"] {
        width: 100%;
    }

    @media (max-width: 900px) {
        .block-container {
            padding-top: 1rem;
            padding-bottom: 1.5rem;
            padding-left: 0.9rem;
            padding-right: 0.9rem;
        }

        .hero {
            padding: 1rem;
            border-radius: 16px;
        }

        .hero-title {
            font-size: 1.45rem;
            line-height: 1.2;
        }

        .hero-subtitle {
            font-size: 0.92rem;
            line-height: 1.5;
        }

        .metric-box {
            padding: 0.9rem;
            border-radius: 14px;
        }

        .metric-label {
            font-size: 0.76rem;
        }

        .metric-value {
            font-size: 1.45rem;
        }

        button[kind="secondary"],
        button[kind="primary"] {
            min-height: 2.75rem;
        }
    }

    @media (max-width: 640px) {
        .block-container {
            padding-left: 0.75rem;
            padding-right: 0.75rem;
        }

        .hero-title {
            font-size: 1.25rem;
        }

        .hero-subtitle {
            font-size: 0.88rem;
        }

        div[data-baseweb="tab-list"] {
            gap: 0.35rem;
            flex-wrap: wrap;
        }

        button[data-baseweb="tab"] {
            white-space: normal;
            height: auto;
            min-height: 2.5rem;
            padding-top: 0.45rem;
            padding-bottom: 0.45rem;
        }
    }

    </style>
    """,
    unsafe_allow_html=True,
)


def format_number(value: int | float) -> str:
    return f"{int(value):,}".replace(",", ".")


def parse_datetime(series: pd.Series) -> pd.Series:
    parsed = pd.to_datetime(series, errors="coerce", utc=True)
    if parsed.notna().any():
        return parsed.dt.tz_localize(None)

    parsed = pd.to_datetime(series, format="%Y-%m-%d %H:%M:%S", errors="coerce", utc=True)
    return parsed.dt.tz_localize(None)


def get_service_account_info() -> dict | None:
    if "gcp_service_account" not in st.secrets:
        return None

    secret_section = st.secrets["gcp_service_account"]
    if hasattr(secret_section, "to_dict"):
        return secret_section.to_dict()
    return dict(secret_section)


def authorize_client(credentials_path: Path):
    service_account_info = get_service_account_info()
    if service_account_info:
        creds = ServiceAccountCredentials.from_json_keyfile_dict(service_account_info, SCOPE)
        return gspread.authorize(creds)

    creds = ServiceAccountCredentials.from_json_keyfile_name(str(credentials_path), SCOPE)
    return gspread.authorize(creds)


def normalize_post_url(url: str) -> str:
    return str(url or "").strip().rstrip("/")


@st.cache_data(ttl=600, show_spinner=False)
def fetch_post_detail_from_apify(token: str, post_url: str, target_username: str) -> dict:
    if not APIFY_AVAILABLE:
        raise RuntimeError("apify-client belum terpasang. Jalankan: pip install apify-client")
    if not token:
        raise ValueError("Apify API token wajib diisi.")
    if not post_url:
        raise ValueError("Link post tidak valid.")
    if not target_username:
        raise ValueError("Username target tidak tersedia untuk mengambil detail postingan.")

    client = ApifyClient(token)
    run_input = {
        "username": [target_username],
        "resultsLimit": 20,
        "skipPinnedPosts": False,
        "dataDetailLevel": "basicData",
    }
    run = client.actor(POST_DETAIL_ACTOR_ID).call(run_input=run_input)
    items = list(client.dataset(run["defaultDatasetId"]).iterate_items())
    if not items:
        raise RuntimeError("Apify tidak mengembalikan detail postingan.")

    target_url = normalize_post_url(post_url)
    for item in items:
        item_url = normalize_post_url(item.get("url"))
        if item_url == target_url:
            return item

    raise RuntimeError(
        "Detail postingan tidak ditemukan dari hasil Apify untuk username target ini. "
        "Coba pastikan Username Target di sheet sesuai dengan pemilik postingan."
    )


@st.cache_data(ttl=600, show_spinner=False)
def scrape_post_comments_from_apify(token: str, post_url: str, max_comments: int = 1000) -> list[dict]:
    if not APIFY_AVAILABLE:
        raise RuntimeError("apify-client belum terpasang. Jalankan: pip install apify-client")
    if not token:
        raise ValueError("Apify API token wajib diisi.")
    if not post_url:
        raise ValueError("Link post tidak valid.")

    client = ApifyClient(token)
    run_input = {
        "code_or_id_or_url": [post_url],
        "sort_by": "popular",
        "max_comments": max_comments,
        "scrape_replies": False,
        "max_replies": 15,
    }
    run = client.actor(COMMENT_ACTOR_ID).call(run_input=run_input)
    items = list(client.dataset(run["defaultDatasetId"]).iterate_items())
    return [item for item in items if item.get("content_type") != "caption"]


@st.cache_data(ttl=600, show_spinner=False)
def analyze_comment_sentiments_deepseek(caption: str, comments_json: str) -> list[str]:
    if not deepseek_client:
        raise RuntimeError("DEEPSEEK_API_KEY belum dikonfigurasi.")

    comments = json.loads(comments_json)
    if not comments:
        return []
    expected_count = len(comments)

    numbered = "\n".join(
        f"{i+1}. {str(comment.get('text', '')).strip()[:300]}"
        for i, comment in enumerate(comments)
    )

    prompt = f"""Klasifikasikan sentimen setiap komentar Instagram berdasarkan CAPTION dan konteks percakapan.

Gunakan hanya 3 label:
- POSITIF: mendukung, memuji, setuju, berharap baik.
- NEGATIF: kritik, marah, kecewa, protes, mengejek, menyindir, sarkastik, satir, ironi, pujian palsu, serangan halus.
- NETRAL: pertanyaan murni atau komentar informasional tanpa nada emosi yang jelas.

Aturan penting:
- Fokus pada maksud komentar, bukan arti literal saja.
- Sindiran, satir, sarkasme, ironi, nyinyiran, atau pertanyaan retoris yang menyerang harus dihitung NEGATIF.
- Jika komentar tampak memuji tetapi sebenarnya mengejek atau merendahkan, hitung NEGATIF.
- Jangan terlalu mudah memberi label NETRAL. Jika ragu antara NETRAL dan NEGATIF untuk komentar sinis/menyindir, pilih NEGATIF.

Contoh:
- "wah hebat sekali, realisasinya mana?" -> NEGATIF
- "mantap, makin kacau" -> NEGATIF
- "semoga lancar" -> POSITIF
- "jam berapa acaranya?" -> NETRAL

Jawab HANYA dengan JSON array:
["POSITIF", "NEGATIF", "NETRAL", ...]

Jumlah item harus sama persis dengan jumlah komentar.

CAPTION:
{caption[:7000]}

KOMENTAR ({len(comments)} buah):
{numbered}

Jawab hanya dengan JSON array:"""

    response = deepseek_client.chat.completions.create(
        model="deepseek-chat",
        temperature=0.0,
        max_tokens=1200,
        messages=[
            {
                "role": "system",
                "content": "Kamu adalah analis sentimen komentar media sosial. Baca konteks caption. Perlakukan sindiran, satir, sarkasme, ironi, dan pujian palsu sebagai NEGATIF. Jawab hanya dengan JSON array sentimen.",
            },
            {"role": "user", "content": prompt},
        ],
    )
    raw = response.choices[0].message.content.strip()

    def _normalize_label(value: object) -> str | None:
        text = str(value).strip().lower()
        if "positif" in text or "positive" in text:
            return "positive"
        if "negatif" in text or "negative" in text:
            return "negative"
        if "netral" in text or "neutral" in text:
            return "neutral"
        return None

    normalized: list[str] = []

    match = re.search(r"\[.*?\]", raw, re.DOTALL)
    if match:
        try:
            labels = json.loads(match.group())
            for label in labels:
                normalized_label = _normalize_label(label)
                if normalized_label:
                    normalized.append(normalized_label)
        except Exception:
            pass

    if not normalized:
        quoted_labels = re.findall(
            r'"(POSITIF|NEGATIF|NETRAL|POSITIVE|NEGATIVE|NEUTRAL)"',
            raw,
            flags=re.IGNORECASE,
        )
        for label in quoted_labels:
            normalized_label = _normalize_label(label)
            if normalized_label:
                normalized.append(normalized_label)

    if not normalized:
        raise ValueError(f"Format respons DeepSeek tidak valid: {raw[:200]}")

    if len(normalized) > expected_count:
        normalized = normalized[:expected_count]

    return normalized


@st.cache_data(ttl=180, show_spinner=False)
def load_comment_sheet(credentials_path: str, spreadsheet_key: str, worksheet_index: int) -> pd.DataFrame:
    client = authorize_client(Path(credentials_path))
    spreadsheet = client.open_by_key(spreadsheet_key)
    worksheet = spreadsheet.get_worksheet(worksheet_index)
    records = worksheet.get_all_records()

    if not records:
        return pd.DataFrame(columns=COMMENT_COLUMNS)

    df = pd.DataFrame(records)
    for column in COMMENT_COLUMNS:
        if column not in df.columns:
            df[column] = ""
    return df[COMMENT_COLUMNS].copy()


def prepare_comment_df(df: pd.DataFrame) -> pd.DataFrame:
    prepared = df.copy()
    if prepared.empty:
        return prepared

    prepared["Username"] = prepared["Username"].fillna("").astype(str).str.strip()
    prepared["Komentar"] = prepared["Komentar"].fillna("").astype(str).str.strip()
    prepared["Username Target"] = prepared["Username Target"].fillna("").astype(str).str.strip()
    prepared["Link Post"] = prepared["Link Post"].fillna("").astype(str).str.strip()
    prepared["Timestamp Parsed"] = parse_datetime(prepared["Timestamp"])
    prepared["Tanggal"] = prepared["Timestamp Parsed"].dt.date
    prepared = prepared.sort_values("Timestamp Parsed", ascending=False, na_position="last")
    return prepared


def render_metric(label: str, value: str) -> None:
    st.markdown(
        f"""
        <div class="metric-box">
            <div class="metric-label">{label}</div>
            <div class="metric-value">{value}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def is_authenticated() -> bool:
    return bool(st.session_state.get("is_authenticated", False))


def handle_password_login() -> None:
    attempted_password = st.session_state.get("sidebar_password", "")
    if hmac.compare_digest(attempted_password, APP_PASSWORD):
        st.session_state["is_authenticated"] = True
        if not st.session_state.get("welcome_dialog_seen", False):
            st.session_state["show_welcome_dialog"] = True
            st.session_state["welcome_dialog_seen"] = True
        st.session_state.pop("password_error", None)
        st.session_state["sidebar_password"] = ""
        st.rerun()

    st.session_state["password_error"] = "Password salah."


@st.dialog("Panduan Singkat")
def show_welcome_dialog() -> None:
    st.write(
        "Selamat datang di dashboard laporan komentar. "
        "Halaman ini membantu Anda melihat ringkasan komentar, memantau akun target, dan membaca analisis per postingan."
    )
    st.markdown(
        """
        **Cara pakai cepat**

        1. Pilih `Rentang tanggal` di sidebar untuk menentukan periode data yang ingin dilihat.
        2. Lihat kartu ringkasan di bagian atas untuk mengetahui total komentar, jumlah akun target, dan jumlah link post.
        3. Buka tab `Dashboard` untuk melihat distribusi komentar per akun target.
        4. Buka tab `Analisis per Postingan` lalu pilih akun target untuk melihat detail postingan, perbandingan komentar, dan sentimen.
        5. Buka tab `Raw Data` jika ingin melihat data mentah atau mengunduh CSV.

        **Tips**

        - Gunakan tombol `Muat Ulang` di sidebar jika data terbaru belum tampil.
        - Jika detail Apify atau sentimen belum muncul, tunggu sebentar lalu coba muat ulang lagi.
        """
    )
    if st.button("Saya mengerti", use_container_width=True):
        st.session_state["show_welcome_dialog"] = False
        st.rerun()


st.markdown(
    """
    <div class="hero">
        <h1 class="hero-title">Laporan Komentar</h1>
        <div class="hero-subtitle">
            Dashboard sederhana yang hanya membaca sheet laporan komentar dari otomasi.
        </div>
    </div>
    """,
    unsafe_allow_html=True,
)

credentials_path = str(DEFAULT_CREDENTIALS)
spreadsheet_key = DEFAULT_COMMENT_SHEET_KEY
worksheet_index = 0
apify_token = APIFY_API_TOKEN

with st.sidebar:
    st.subheader("Akses Dashboard")
    if not APP_PASSWORD:
        st.error("APP_PASSWORD belum dikonfigurasi di environment atau Streamlit secrets.")
    elif not is_authenticated():
        st.text_input(
            "Password",
            type="password",
            key="sidebar_password",
            on_change=handle_password_login,
        )
        if st.session_state.get("password_error"):
            st.error(st.session_state["password_error"])
    else:
        st.success("Akses diberikan.")
        if st.button("Lihat panduan", use_container_width=True):
            st.session_state["show_welcome_dialog"] = True
            st.rerun()
        if st.button("Logout", use_container_width=True):
            st.session_state["is_authenticated"] = False
            st.session_state.pop("password_error", None)
            st.session_state["show_welcome_dialog"] = False
            st.session_state["welcome_dialog_seen"] = False
            st.rerun()

if not APP_PASSWORD:
    st.warning("Dashboard dikunci, tetapi APP_PASSWORD belum diset.")
    st.stop()

if not is_authenticated():
    st.warning("Masukkan password yang benar di sidebar untuk membuka dashboard.")
    st.stop()

if st.session_state.get("show_welcome_dialog", False):
    st.session_state["show_welcome_dialog"] = False
    show_welcome_dialog()

with st.sidebar:
    refresh = st.button("Muat Ulang", use_container_width=True)

if refresh:
    load_comment_sheet.clear()

load_error = None
try:
    comments_raw = load_comment_sheet(credentials_path, spreadsheet_key, int(worksheet_index))
except Exception as exc:
    comments_raw = pd.DataFrame(columns=COMMENT_COLUMNS)
    load_error = str(exc)

comments_df = prepare_comment_df(comments_raw)

if comments_df.empty:
    st.warning("Belum ada data komentar yang bisa ditampilkan dari sheet laporan komentar.")
    if load_error:
        st.error(load_error)
    st.stop()

available_dates = comments_df["Tanggal"].dropna()
min_date = available_dates.min() if not available_dates.empty else date.today()
max_date = available_dates.max() if not available_dates.empty else date.today()

with st.sidebar:
    st.subheader("Filter")
    date_range = st.date_input(
        "Rentang tanggal",
        value=(min_date, max_date),
        min_value=min_date,
        max_value=max_date,
    )
    if isinstance(date_range, (tuple, list)) and len(date_range) == 2:
        start_date, end_date = date_range
    else:
        start_date, end_date = min_date, max_date

filtered_df = comments_df.copy()
filtered_df = filtered_df[filtered_df["Tanggal"].notna()]
filtered_df = filtered_df[
    (filtered_df["Tanggal"] >= start_date) & (filtered_df["Tanggal"] <= end_date)
]

filtered_df = filtered_df.sort_values("Timestamp Parsed", ascending=False, na_position="last")

total_comments = len(filtered_df)
unique_targets = filtered_df["Username Target"].replace("", pd.NA).dropna().nunique()
unique_posts = filtered_df["Link Post"].replace("", pd.NA).dropna().nunique()
metric_cols = st.columns(3)
with metric_cols[0]:
    render_metric("Total Komentar", format_number(total_comments))
with metric_cols[1]:
    render_metric("Akun Target", format_number(unique_targets))
with metric_cols[2]:
    render_metric("Link Post", format_number(unique_posts))

top_targets = (
    filtered_df[filtered_df["Username Target"] != ""]
    .groupby("Username Target")
    .size()
    .reset_index(name="Jumlah Komentar")
    .sort_values("Jumlah Komentar", ascending=False)
)

display_df = filtered_df[
    ["Timestamp Parsed", "Username", "Username Target", "Komentar", "Link Post"]
].rename(
    columns={
        "Timestamp Parsed": "Waktu",
        "Username": "Username Pengirim",
        "Username Target": "Username Target",
        "Komentar": "Komentar",
        "Link Post": "Link Post",
    }
)

tab_dashboard, tab_analysis, tab_raw = st.tabs(["Dashboard", "Analisis per Postingan", "Raw Data"])

with tab_dashboard:
    st.subheader("Akun Target Teratas")
    st.caption("Target yang paling sering menerima komentar.")
    if top_targets.empty:
        st.info("Belum ada data akun target.")
    else:
        chart_left, chart_right = st.columns(2)
        total_target_comments = int(top_targets["Jumlah Komentar"].sum())

        with chart_left:
            st.markdown("**Distribusi Komentar per Akun Target**")
            bar_df = top_targets.rename(
                columns={
                    "Username Target": "target",
                    "Jumlah Komentar": "jumlah",
                }
            )
            if total_target_comments > 0:
                bar_df["persentase"] = (bar_df["jumlah"] / total_target_comments * 100).round(1)
                bar_df["label"] = bar_df["persentase"].astype(str) + "%"
            else:
                bar_df["persentase"] = 0.0
                bar_df["label"] = "0%"
            st.vega_lite_chart(
                bar_df,
                {
                    "layer": [
                        {
                            "mark": {"type": "bar", "cornerRadiusEnd": 4},
                            "encoding": {
                                "y": {
                                    "field": "target",
                                    "type": "nominal",
                                    "sort": "-x",
                                    "title": "Username Target",
                                },
                                "x": {
                                    "field": "jumlah",
                                    "type": "quantitative",
                                    "title": "Jumlah Komentar",
                                },
                                "tooltip": [
                                    {"field": "target", "type": "nominal", "title": "Username Target"},
                                    {"field": "jumlah", "type": "quantitative", "title": "Jumlah Komentar"},
                                    {"field": "persentase", "type": "quantitative", "title": "Persentase (%)"},
                                ],
                                "color": {"value": "#2563eb"},
                            },
                        },
                        {
                            "mark": {
                                "type": "text",
                                "align": "left",
                                "baseline": "middle",
                                "dx": 6,
                                "color": "#111827",
                                "fontSize": 12,
                                "fontWeight": "bold",
                            },
                            "encoding": {
                                "y": {
                                    "field": "target",
                                    "type": "nominal",
                                    "sort": "-x",
                                },
                                "x": {
                                    "field": "jumlah",
                                    "type": "quantitative",
                                },
                                "text": {"field": "label", "type": "nominal"},
                            },
                        },
                    ],
                },
                use_container_width=True,
            )

        with chart_right:
            st.markdown("**Proporsi Komentar per Akun Target**")
            pie_df = top_targets.rename(
                columns={
                    "Username Target": "target",
                    "Jumlah Komentar": "jumlah",
                }
            )
            st.vega_lite_chart(
                pie_df,
                {
                    "mark": {"type": "arc", "innerRadius": 0},
                    "encoding": {
                        "theta": {"field": "jumlah", "type": "quantitative"},
                        "color": {
                            "field": "target",
                            "type": "nominal",
                            "legend": {"title": "Username Target"},
                        },
                        "tooltip": [
                            {"field": "target", "type": "nominal", "title": "Username Target"},
                            {"field": "jumlah", "type": "quantitative", "title": "Jumlah Komentar"},
                        ],
                    },
                },
                use_container_width=True,
            )

with tab_analysis:
    st.subheader("Analisis per Postingan")
    st.caption("Pilih username target Instagram, lalu tekan tombol untuk memuat analisis postingan.")

    target_post_options = (
        filtered_df[filtered_df["Username Target"] != ""]
        .groupby("Username Target")["Link Post"]
        .nunique()
        .reset_index(name="Jumlah Post")
        .sort_values(["Jumlah Post", "Username Target"], ascending=[False, True])
    )

    if target_post_options.empty:
        st.info("Belum ada akun target yang bisa dianalisis.")
    else:
        target_labels = [
            f"@{row['Username Target']} ({row['Jumlah Post']} postingan)"
            for _, row in target_post_options.iterrows()
        ]
        label_to_target = {
            f"@{row['Username Target']} ({row['Jumlah Post']} postingan)": row["Username Target"]
            for _, row in target_post_options.iterrows()
        }

        selected_target_label = st.selectbox(
            "Pilih username target",
            options=target_labels,
            index=0,
            key="selected_target_label",
        )
        selected_target_username = label_to_target[selected_target_label]
        loaded_target_username = st.session_state.get("analysis_loaded_target")

        action_cols = st.columns([1, 1, 2])
        with action_cols[0]:
            load_analysis = st.button(
                "Tampilkan analisis",
                key="load_selected_target_analysis",
                use_container_width=True,
            )
        with action_cols[1]:
            refresh_analysis = st.button(
                "Muat ulang detail",
                key="refresh_selected_target_analysis",
                use_container_width=True,
            )

        if load_analysis:
            st.session_state["analysis_loaded_target"] = selected_target_username
            loaded_target_username = selected_target_username

        if refresh_analysis:
            fetch_post_detail_from_apify.clear()
            scrape_post_comments_from_apify.clear()
            analyze_comment_sentiments_deepseek.clear()
            st.session_state["analysis_loaded_target"] = selected_target_username
            loaded_target_username = selected_target_username

        if loaded_target_username != selected_target_username:
            st.info("Tekan `Tampilkan analisis` untuk memuat data postingan dari akun target yang dipilih.")
            st.stop()

        target_posts = (
            filtered_df[
                (filtered_df["Username Target"] == selected_target_username)
                & (filtered_df["Link Post"] != "")
            ]
            .copy()
            .sort_values("Timestamp Parsed", ascending=False, na_position="last")
        )
        unique_posts = (
            target_posts[["Link Post"]]
            .drop_duplicates()
            .reset_index(drop=True)
        )

        for idx, row in unique_posts.iterrows():
            selected_post_url = row["Link Post"]
            system_post_df = target_posts[target_posts["Link Post"] == selected_post_url].copy()
            system_comment_count = len(system_post_df)
            unique_sender_count = system_post_df["Username"].replace("", pd.NA).dropna().nunique()
            latest_sent_at = system_post_df["Timestamp Parsed"].max()

            apify_error = None
            apify_post = None
            with st.spinner("Memuat detail postingan terpilih..."):
                try:
                    apify_post = fetch_post_detail_from_apify(
                        apify_token,
                        selected_post_url,
                        selected_target_username,
                    )
                except Exception as exc:
                    apify_error = str(exc)

            st.markdown(f"**Postingan {idx + 1}**")

            if apify_post:
                total_instagram_comments = apify_post.get("commentsCount", 0) or 0
                likes_count = apify_post.get("likesCount", 0) or 0
                owner_username = str(apify_post.get("ownerUsername") or "").strip()
                owner_full_name = str(apify_post.get("ownerFullName") or "").strip()
                caption = str(apify_post.get("caption") or "").strip()
                posted_at = str(apify_post.get("timestamp") or "").strip()
                remaining_comments = max(int(total_instagram_comments) - int(system_comment_count), 0)

                compare_cols = st.columns(3)
                with compare_cols[0]:
                    render_metric("Komentar di Instagram", format_number(total_instagram_comments))
                with compare_cols[1]:
                    render_metric("Komentar dari Sistem", format_number(system_comment_count))
                with compare_cols[2]:
                    render_metric("Selisih", format_number(remaining_comments))

                st.markdown("**Detail Postingan**")
                detail_rows = [
                    ["Username Target", f"@{owner_username}" if owner_username else f"@{selected_target_username}"],
                    ["Nama Akun", owner_full_name or "-"],
                    ["Jumlah Like", format_number(likes_count)],
                    ["Waktu Posting", posted_at or "-"],
                    ["Pengirim Komentar Unik", format_number(unique_sender_count)],
                    ["Komentar Terakhir dari Sistem", str(latest_sent_at) if pd.notna(latest_sent_at) else "-"],
                    ["Link Post", selected_post_url],
                ]
                st.dataframe(
                    pd.DataFrame(detail_rows, columns=["Item", "Nilai"]),
                    use_container_width=True,
                    hide_index=True,
                )

                if caption:
                    st.markdown("**Caption Postingan**")
                    st.write(caption)
            elif apify_error:
                st.warning(f"Detail Apify belum bisa dimuat: {apify_error}")

            chart_col_left, chart_col_right = st.columns(2)

            with chart_col_left:
                st.markdown("**Perbandingan Komentar**")
                if apify_post:
                    total_instagram_comments = int(apify_post.get("commentsCount", 0) or 0)
                    system_comments_for_chart = int(system_comment_count)
                    other_comments = max(total_instagram_comments - system_comments_for_chart, 0)

                    comparison_df = pd.DataFrame(
                        [
                            {"kategori": "Komentar Sistem", "jumlah": system_comments_for_chart},
                            {"kategori": "Komentar Lainnya", "jumlah": other_comments},
                        ]
                    )
                    total_comparison = int(comparison_df["jumlah"].sum())
                    if total_comparison > 0:
                        comparison_df["persentase"] = (
                            comparison_df["jumlah"] / total_comparison * 100
                        ).round(1)
                    else:
                        comparison_df["persentase"] = 0.0

                    system_percentage = float(
                        comparison_df.loc[
                            comparison_df["kategori"] == "Komentar Sistem", "persentase"
                        ].iloc[0]
                    )
                    other_percentage = float(
                        comparison_df.loc[
                            comparison_df["kategori"] == "Komentar Lainnya", "persentase"
                        ].iloc[0]
                    )

                    percentage_cols = st.columns(2)
                    with percentage_cols[0]:
                        render_metric("Persentase Sistem", f"{system_percentage:.1f}%")
                    with percentage_cols[1]:
                        render_metric("Persentase Lainnya", f"{other_percentage:.1f}%")

                    st.markdown("<div style='height: 18px;'></div>", unsafe_allow_html=True)

                    st.vega_lite_chart(
                        comparison_df,
                        {
                            "layer": [
                                {
                                    "transform": [{"filter": "datum.kategori === 'Komentar Lainnya'"}],
                                    "mark": {
                                        "type": "arc",
                                        "outerRadius": 150,
                                        "stroke": "#ffffff",
                                        "strokeWidth": 2,
                                    },
                                    "encoding": {
                                        "theta": {"field": "jumlah", "type": "quantitative"},
                                        "color": {
                                            "field": "kategori",
                                            "type": "nominal",
                                            "scale": {
                                                "domain": ["Komentar Sistem", "Komentar Lainnya"],
                                                "range": ["#2563eb", "#d1d5db"],
                                            },
                                            "legend": {"title": None},
                                        },
                                        "tooltip": [
                                            {"field": "kategori", "type": "nominal", "title": "Kategori"},
                                            {"field": "jumlah", "type": "quantitative", "title": "Jumlah"},
                                            {"field": "persentase", "type": "quantitative", "title": "Persentase (%)"},
                                        ],
                                    },
                                },
                                {
                                    "transform": [{"filter": "datum.kategori === 'Komentar Sistem'"}],
                                    "mark": {
                                        "type": "arc",
                                        "outerRadius": 164,
                                        "stroke": "#ffffff",
                                        "strokeWidth": 3,
                                        "cornerRadius": 2,
                                    },
                                    "encoding": {
                                        "theta": {"field": "jumlah", "type": "quantitative"},
                                        "color": {
                                            "field": "kategori",
                                            "type": "nominal",
                                            "scale": {
                                                "domain": ["Komentar Sistem", "Komentar Lainnya"],
                                                "range": ["#2563eb", "#d1d5db"],
                                            },
                                            "legend": None,
                                        },
                                        "tooltip": [
                                            {"field": "kategori", "type": "nominal", "title": "Kategori"},
                                            {"field": "jumlah", "type": "quantitative", "title": "Jumlah"},
                                            {"field": "persentase", "type": "quantitative", "title": "Persentase (%)"},
                                        ],
                                    },
                                },
                            ],
                        },
                        use_container_width=True,
                    )

            combined_comment_rows = []
            public_comments: list[dict] = []
            sentiment_error = None
            sentiment_labels: list[str] = []
            system_sentiment_error = None
            system_sentiment_labels: list[str] = []

            if apify_post:
                caption_for_sentiment = str(apify_post.get("caption") or "").strip()

                with st.spinner("Memuat komentar publik dan analisis sentimen..."):
                    try:
                        public_comments = scrape_post_comments_from_apify(
                            apify_token,
                            selected_post_url,
                            max_comments=1000,
                        )
                        comment_payload = json.dumps(public_comments, ensure_ascii=False)
                        sentiment_labels = analyze_comment_sentiments_deepseek(
                            caption_for_sentiment,
                            comment_payload,
                        )
                    except Exception as exc:
                        sentiment_error = str(exc)

                if not system_post_df.empty:
                    with st.spinner("Menganalisis sentimen komentar sistem..."):
                        try:
                            system_comments_for_ai = [
                                {"text": str(text).strip()}
                                for text in system_post_df["Komentar"].fillna("").astype(str).tolist()
                                if str(text).strip()
                            ]
                            system_comment_payload = json.dumps(system_comments_for_ai, ensure_ascii=False)
                            system_sentiment_labels = analyze_comment_sentiments_deepseek(
                                caption_for_sentiment,
                                system_comment_payload,
                            )
                        except Exception as exc:
                            system_sentiment_error = str(exc)

            system_comments_reset = system_post_df.reset_index(drop=True)
            for i, row_system in system_comments_reset.iterrows():
                sentiment_value = (
                    system_sentiment_labels[i]
                    if i < len(system_sentiment_labels)
                    else ""
                )
                combined_comment_rows.append(
                    {
                        "Waktu": row_system.get("Timestamp Parsed"),
                        "Komentar": f"(Sistem) {str(row_system.get('Komentar', '')).strip()}",
                        "Sentiment": sentiment_value.capitalize() if sentiment_value else "",
                    }
                )

            for i, public_comment in enumerate(public_comments):
                raw_time = public_comment.get("created_at_utc") or public_comment.get("timestamp") or ""
                parsed_time = pd.to_datetime(raw_time, errors="coerce", utc=True)
                if pd.notna(parsed_time):
                    parsed_time = parsed_time.tz_localize(None)
                sentiment_value = sentiment_labels[i] if i < len(sentiment_labels) else ""
                combined_comment_rows.append(
                    {
                        "Waktu": parsed_time,
                        "Komentar": f"(Publik) {str(public_comment.get('text', '')).strip()}",
                        "Sentiment": sentiment_value.capitalize() if sentiment_value else "",
                    }
                )

            combined_comment_table = pd.DataFrame(combined_comment_rows)
            if not combined_comment_table.empty:
                combined_comment_table = combined_comment_table.sort_values(
                    "Waktu",
                    ascending=False,
                    na_position="last",
                )

            with chart_col_right:
                st.markdown("**Sentimen Komentar**")
                if not combined_comment_table.empty:
                    sentiment_counts = (
                        combined_comment_table["Sentiment"]
                        .fillna("")
                        .astype(str)
                        .str.lower()
                        .value_counts()
                    )
                    if int(sentiment_counts.sum()) > 0:
                        positive_count = int(sentiment_counts.get("positive", 0))
                        neutral_count = int(sentiment_counts.get("neutral", 0))
                        negative_count = int(sentiment_counts.get("negative", 0))

                        sentiment_metric_cols = st.columns(3)
                        with sentiment_metric_cols[0]:
                            render_metric("Positif", format_number(positive_count))
                        with sentiment_metric_cols[1]:
                            render_metric("Netral", format_number(neutral_count))
                        with sentiment_metric_cols[2]:
                            render_metric("Negatif", format_number(negative_count))

                        st.markdown("<div style='height: 16px;'></div>", unsafe_allow_html=True)
                        sentiment_chart_df = pd.DataFrame(
                            [
                                {"sentimen": "Positif", "jumlah": positive_count},
                                {"sentimen": "Netral", "jumlah": neutral_count},
                                {"sentimen": "Negatif", "jumlah": negative_count},
                            ]
                        )
                        total_sentiment = int(sentiment_chart_df["jumlah"].sum())
                        if total_sentiment > 0:
                            sentiment_chart_df["persentase"] = (
                                sentiment_chart_df["jumlah"] / total_sentiment * 100
                            ).round(1)
                        else:
                            sentiment_chart_df["persentase"] = 0.0
                        st.vega_lite_chart(
                            sentiment_chart_df,
                            {
                                "mark": {
                                    "type": "arc",
                                    "outerRadius": 150,
                                    "stroke": "#ffffff",
                                    "strokeWidth": 2,
                                },
                                "encoding": {
                                    "theta": {"field": "jumlah", "type": "quantitative"},
                                    "color": {
                                        "field": "sentimen",
                                        "type": "nominal",
                                        "scale": {
                                            "domain": ["Positif", "Netral", "Negatif"],
                                            "range": ["#16a34a", "#9ca3af", "#dc2626"],
                                        },
                                        "legend": {"title": None},
                                    },
                                    "tooltip": [
                                        {"field": "sentimen", "type": "nominal", "title": "Sentimen"},
                                        {"field": "jumlah", "type": "quantitative", "title": "Jumlah"},
                                        {"field": "persentase", "type": "quantitative", "title": "Persentase (%)"},
                                    ],
                                },
                            },
                            use_container_width=True,
                        )
                    else:
                        st.info("Belum ada data sentimen yang bisa ditampilkan.")
                elif sentiment_error:
                    st.warning(f"Analisis sentimen DeepSeek belum bisa dimuat: {sentiment_error}")

            st.markdown("<div style='height: 20px;'></div>", unsafe_allow_html=True)
            st.markdown("**Komentar untuk Postingan Ini**")
            if not combined_comment_table.empty:
                st.dataframe(combined_comment_table, use_container_width=True, hide_index=True)
            else:
                st.info("Belum ada komentar yang bisa ditampilkan untuk postingan ini.")

            if system_sentiment_error:
                st.caption(f"Sentimen komentar sistem belum bisa dimuat sepenuhnya: {system_sentiment_error}")

            if idx < len(unique_posts) - 1:
                st.divider()

with tab_raw:
    st.subheader("Data Mentah Akun Target")
    st.caption("Daftar lengkap akun target berdasarkan jumlah komentar.")
    if top_targets.empty:
        st.info("Belum ada data akun target.")
    else:
        st.dataframe(top_targets, use_container_width=True, hide_index=True)

    st.subheader("Log Komentar")
    st.caption("Daftar komentar yang tercatat di sheet laporan komentar.")
    st.dataframe(display_df, use_container_width=True, hide_index=True)

    st.download_button(
        "Unduh CSV",
        data=display_df.to_csv(index=False).encode("utf-8-sig"),
        file_name="laporan_komentar.csv",
        mime="text/csv",
    )

if load_error:
    with st.expander("Detail error sumber data"):
        st.error(load_error)


