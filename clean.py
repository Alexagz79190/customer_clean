import streamlit as st
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
import io
import bcrypt

st.title("🔐 Portail sécurisé - Export BigQuery")

# ==================== LOGIN MAISON ====================
if "logged_in" not in st.session_state:
    st.session_state["logged_in"] = False

if not st.session_state["logged_in"]:
    st.subheader("Connexion requise")

    username = st.text_input("Nom d'utilisateur")
    password = st.text_input("Mot de passe", type="password")

    if st.button("Se connecter"):
        # Récupérer les comptes depuis secrets.toml
        valid_users = st.secrets["users"]["usernames"]
        valid_names = st.secrets["users"]["names"]
        valid_hashes = st.secrets["users"]["passwords"]

        if username in valid_users:
            idx = valid_users.index(username)
            hashed_pwd = valid_hashes[idx]

            if bcrypt.checkpw(password.encode(), hashed_pwd.encode()):
                st.session_state["logged_in"] = True
                st.session_state["name"] = valid_names[idx]
                st.success(f"Bienvenue {st.session_state['name']} 🎉")
                st.rerun()  # ✅ rerun propre
            else:
                st.error("Mot de passe incorrect ❌")
        else:
            st.error("Utilisateur inconnu ❌")

    st.stop()

# ==================== SI LOGIN OK ====================
st.success(f"Connecté en tant que {st.session_state['name']} ✅")

# Bouton de déconnexion
if st.button("Se déconnecter"):
    st.session_state["logged_in"] = False
    st.rerun()

# ==================== BIGQUERY ====================
PROJECT_ID = "datalake-380714"
DATASET_ID = "pole_agri"
TABLE_WITH_SPACE = "client web_agrizone_client"
ROW_LIMIT = 0  # 0 = pas de limite

creds_dict = st.secrets["gcp_service_account"]
credentials_gcp = service_account.Credentials.from_service_account_info(creds_dict)
client = bigquery.Client(credentials=credentials_gcp, project=creds_dict["project_id"])

def bq_to_dataframe(row_limit=None) -> pd.DataFrame:
    table_fqn = f"`{PROJECT_ID}.{DATASET_ID}.{TABLE_WITH_SPACE}`"
    query = f"SELECT * FROM {table_fqn}"
    if row_limit and row_limit > 0:
        query += f" LIMIT {int(row_limit)}"
    job = client.query(query)
    return job.result().to_dataframe()

def clean_clients(df: pd.DataFrame) -> pd.DataFrame:
    df = df.dropna(subset=["email_client"])
    df = df.drop_duplicates(subset=["email_client"])

    df["Email"] = df["email_client"].astype(str).str.strip()
    df["First Name"] = df["prenom_client"].astype(str).str.strip().str.title()
    df["Last Name"] = df["nom_client"].astype(str).str.strip().str.title()
    df["Country"] = df["libelle_lg_pays"].astype(str).str.strip().str[:2].str.upper()

    df["Zip"] = (
        df["code_postal_adr_client"]
        .astype(str)
        .str.replace(r"[\s.]", "", regex=True)
        .str.strip()
        .str[:5]
    )
    df["Zip"] = df["Zip"].where(df["Zip"].str.fullmatch(r"\d{5}") == True, pd.NA)

    digits = df["portable_client"].astype(str).str.replace(r"\D", "", regex=True)
    df["N° de mobile"] = "+33" + digits.str[-9:]
    df = df[df["N° de mobile"].str.len() == 12]

    cols = ["Email", "First Name", "Last Name", "Country", "Zip", "N° de mobile"]
    df_final = df[cols].copy()

    for c in cols:
        df_final.loc[:, c] = df_final[c].astype("string").str.strip()
    df_final = df_final.replace({r"^\s*$": pd.NA}, regex=True)
    df_final = df_final.replace({"nan": pd.NA, "None": pd.NA})
    df_final = df_final.dropna(how="any")

    return df_final

# ==================== INTERFACE ====================
if st.button("📥 Extraire et nettoyer les données BigQuery"):
    with st.spinner("Connexion à BigQuery..."):
        df_raw = bq_to_dataframe(ROW_LIMIT or None)
    st.write(f"✅ Données brutes : {len(df_raw)} lignes")
    st.dataframe(df_raw.head(20))

    with st.spinner("Nettoyage des données..."):
        df_clean = clean_clients(df_raw)
    st.write(f"✅ Données nettoyées : {len(df_clean)} lignes")
    st.dataframe(df_clean.head(20))

    # Export Excel
    buffer = io.BytesIO()
    df_clean.to_excel(buffer, index=False, engine="openpyxl")
    buffer.seek(0)
    st.download_button(
        label="⬇️ Télécharger le fichier Excel",
        data=buffer,
        file_name="export_clients_clean.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
