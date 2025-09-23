import streamlit as st
import streamlit_authenticator as stauth
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
import io

# ==================== CONFIG UTILISATEURS ====================
# ⚠️ Mets les mots de passe hashés pour plus de sécurité !
usernames = ["alexis", "admin"]
names = ["Alexis", "Administrateur"]
passwords = ["test123", "admin123"]  # à hasher en vrai !

hashed_passwords = stauth.Hasher(passwords).generate()

authenticator = stauth.Authenticate(
    dict(zip(usernames, names)),
    dict(zip(usernames, hashed_passwords)),
    "cookie_name",
    "signature_key",
    cookie_expiry_days=1
)

# ==================== LOGIN ====================
st.title("🔐 Portail sécurisé - Export BigQuery")

name, authentication_status, username = authenticator.login("Login", "main")

if authentication_status == False:
    st.error("Utilisateur ou mot de passe incorrect ❌")
elif authentication_status == None:
    st.warning("Veuillez entrer vos identifiants 🔑")
elif authentication_status:

    # ==================== PARAMÈTRES BIGQUERY ====================
    PROJECT_ID = "datalake-380714"
    DATASET_ID = "pole_agri"
    TABLE_WITH_SPACE = "client web_agrizone_client"
    ROW_LIMIT = 0

    # Charger credentials depuis secrets.toml
    creds_dict = st.secrets["gcp_service_account"]
    credentials = service_account.Credentials.from_service_account_info(creds_dict)
    client = bigquery.Client(credentials=credentials, project=creds_dict["project_id"])

    # ==================== FONCTIONS ====================
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

    # ==================== INTERFACE STREAMLIT ====================
    st.success(f"Bienvenue {name} 🎉")

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
