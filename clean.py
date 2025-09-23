import streamlit as st
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
import io

# ========= PARAMÈTRES =========
PROJECT_ID = "datalake-380714"
DATASET_ID = "pole_agri"
TABLE_WITH_SPACE = "client web_agrizone_client"  # adapte si besoin
ROW_LIMIT = 0  # 0 ou None = pas de limite
# ==============================

# Connexion BigQuery
def bq_to_dataframe(credentials, project_id: str, dataset_id: str, table_name: str, row_limit=None) -> pd.DataFrame:
    client = bigquery.Client(credentials=credentials, project=project_id)

    table_fqn = f"`{project_id}.{dataset_id}.{table_name}`"
    query = f"SELECT * FROM {table_fqn}"
    if row_limit and row_limit > 0:
        query += f" LIMIT {int(row_limit)}"

    job = client.query(query)

    try:
        df = job.result().to_dataframe(create_bqstorage_client=True)
    except Exception:
        df = job.result().to_dataframe()

    return df


def clean_clients(df: pd.DataFrame) -> pd.DataFrame:
    # Filtrage email
    df = df.dropna(subset=["email_client"])
    df = df.drop_duplicates(subset=["email_client"])

    # Colonnes nettoyées
    df["Email"] = df["email_client"].astype(str).str.strip()
    df["First Name"] = df["prenom_client"].astype(str).str.strip().str.title()
    df["Last Name"] = df["nom_client"].astype(str).str.strip().str.title()
    df["Country"] = df["libelle_lg_pays"].astype(str).str.strip().str[:2].str.upper()

    # Nettoyage CP
    df["Zip"] = (
        df["code_postal_adr_client"]
        .astype(str)
        .str.replace(r"[\s.]", "", regex=True)
        .str.strip()
        .str[:5]
    )
    df["Zip"] = df["Zip"].where(df["Zip"].str.fullmatch(r"\d{5}") == True, pd.NA)

    # Téléphone
    digits = df["portable_client"].astype(str).str.replace(r"\D", "", regex=True)
    df["N° de mobile"] = "+33" + digits.str[-9:]
    df = df[df["N° de mobile"].str.len() == 12]

    # Final
    cols = ["Email", "First Name", "Last Name", "Country", "Zip", "N° de mobile"]
    df_final = df[cols].copy()

    # Normaliser vides
    for c in cols:
        df_final.loc[:, c] = df_final[c].astype("string").str.strip()
    df_final = df_final.replace({r"^\s*$": pd.NA}, regex=True)
    df_final = df_final.replace({"nan": pd.NA, "None": pd.NA})
    df_final = df_final.dropna(how="any")

    return df_final


# ====================== STREAMLIT APP ======================
st.title("📊 Export & Nettoyage Clients BigQuery")

# Récupération des credentials depuis secrets.toml
if "gcp_service_account" not in st.secrets:
    st.error("⚠️ Merci de configurer vos credentials GCP dans `.streamlit/secrets.toml`")
    st.stop()

creds_dict = st.secrets["gcp_service_account"]
credentials = service_account.Credentials.from_service_account_info(creds_dict)

if st.button("Lancer l'extraction et nettoyage"):
    with st.spinner("Extraction des données BigQuery..."):
        df_raw = bq_to_dataframe(
            credentials=credentials,
            project_id=PROJECT_ID,
            dataset_id=DATASET_ID,
            table_name=TABLE_WITH_SPACE,
            row_limit=ROW_LIMIT or None,
        )

    st.success(f"Données brutes récupérées : {len(df_raw)} lignes")
    st.dataframe(df_raw.head(20))

    with st.spinner("Nettoyage des données..."):
        df_final = clean_clients(df_raw)

    st.success(f"Données nettoyées : {len(df_final)} lignes")
    st.dataframe(df_final.head(20))

    # Export Excel en mémoire
    buffer = io.BytesIO()
    df_final.to_excel(buffer, index=False, engine="openpyxl")
    buffer.seek(0)

    st.download_button(
        label="📥 Télécharger le fichier Excel",
        data=buffer,
        file_name="export_clients_clean.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
