import streamlit as st
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
import io
import bcrypt

# ==================== CONFIG GCP ====================
PROJECT_ID = "datalake-380714"
DATASET_ID = "pole_agri"

TABLES = {
    "client": "client web_agrizone_client",
    "produit": "produit web_agrizone_produit_description",
    "commande": "commande web_agrizone_commande",
}

ROW_LIMIT = 0  # 0 = toutes les lignes

# ==================== AUTH ====================
st.set_page_config(page_title="Portail sécurisé", layout="wide")

if "logged_in" not in st.session_state:
    st.session_state["logged_in"] = False

if not st.session_state["logged_in"]:
    st.title("🔐 Connexion requise")
    username = st.text_input("Nom d'utilisateur")
    password = st.text_input("Mot de passe", type="password")

    if st.button("Se connecter"):
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
                st.rerun()
            else:
                st.error("Mot de passe incorrect ❌")
        else:
            st.error("Utilisateur inconnu ❌")
    st.stop()

# ==================== CONNECT GCP ====================
creds_dict = st.secrets["gcp_service_account"]
credentials_gcp = service_account.Credentials.from_service_account_info(creds_dict)
client = bigquery.Client(credentials=credentials_gcp, project=PROJECT_ID)

# ==================== FUNCTIONS ====================
def bq_to_dataframe(table_name: str, row_limit=None) -> pd.DataFrame:
    table_fqn = f"`{PROJECT_ID}.{DATASET_ID}.{table_name}`"
    query = f"SELECT * FROM {table_fqn}"
    if row_limit and row_limit > 0:
        query += f" LIMIT {int(row_limit)}"
    job = client.query(query)
    return job.result().to_dataframe()

def clean_clients(df: pd.DataFrame) -> pd.DataFrame:
    df = df.dropna(subset=["email_client"]).drop_duplicates(subset=["email_client"])
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
    df_final = df_final.replace({"nan": pd.NA, "None": pd.NA}).dropna(how="any")
    return df_final

def query_commandes(date_min="2020-01-01"):
    QUERY = f"""
    WITH commandes AS (
      SELECT
        c.numero_commande,
        c.code_produit,
        c.quantite,
        c.prix_total_ht,
        c.prix_achat,
        p.libelle AS libelle_produit,
        p.prix_vente_ht,
        COALESCE(NULLIF(p.famille4, ''), NULLIF(p.famille3, ''), NULLIF(p.famille2, ''), p.famille1) AS famille_finale
      FROM `{PROJECT_ID}.{DATASET_ID}.{TABLES['commande']}` c
      LEFT JOIN `{PROJECT_ID}.{DATASET_ID}.{TABLES['produit']}` p
        ON c.code_produit = p.code
      WHERE c.date_validation IS NOT NULL
        AND SAFE.PARSE_DATE('%Y-%m-%d', c.date_validation) >= DATE '{date_min}'
    )
    SELECT
      code_produit,
      libelle_produit,
      famille_finale,
      COUNT(DISTINCT numero_commande) AS nb_commandes,
      SUM(quantite) AS quantite_totale,
      SUM(prix_total_ht) AS ca,
      ROUND(SUM(prix_total_ht) / COUNT(DISTINCT numero_commande), 2) AS panier_moyen,
      ANY_VALUE(prix_vente_ht) AS prix_vente
    FROM commandes
    GROUP BY code_produit, libelle_produit, famille_finale
    ORDER BY ca DESC
    """
    return client.query(QUERY).result().to_dataframe()

def query_stats_famille():
    QUERY = f"""
    WITH commandes AS (
      SELECT
        c.code_produit,
        c.quantite,
        c.prix_total_ht,
        c.prix_achat,
        p.libelle,
        COALESCE(NULLIF(p.famille4, ''), NULLIF(p.famille3, ''), NULLIF(p.famille2, ''), p.famille1) AS famille_finale,
        COALESCE(NULLIF(p.famille4_url, ''), NULLIF(p.famille3_url, ''), NULLIF(p.famille2_url, ''), p.famille1_url) AS famille_url
      FROM `{PROJECT_ID}.{DATASET_ID}.{TABLES['commande']}` c
      LEFT JOIN `{PROJECT_ID}.{DATASET_ID}.{TABLES['produit']}` p
        ON c.code_produit = p.code
      WHERE c.date_validation IS NOT NULL
    )
    SELECT
      famille_finale,
      famille_url,
      SUM(prix_total_ht) AS ca,
      SUM(prix_total_ht - prix_achat * quantite) AS marge,
      SAFE_DIVIDE(SUM(prix_total_ht - prix_achat * quantite), SUM(prix_total_ht)) * 100 AS pct_marge
    FROM commandes
    GROUP BY famille_finale, famille_url
    ORDER BY ca DESC
    """
    return client.query(QUERY).result().to_dataframe()

def export_excel(df: pd.DataFrame, filename: str, decimal_comma=True):
    buffer = io.BytesIO()
    df_export = df.copy()
    if decimal_comma:
        for col in df_export.select_dtypes(include=["float", "int"]).columns:
            df_export[col] = df_export[col].map(lambda x: str(round(x, 2)).replace(".", ",") if pd.notnull(x) else "")
    df_export.to_excel(buffer, index=False, engine="openpyxl")
    buffer.seek(0)
    st.download_button(
        "⬇️ Télécharger " + filename,
        data=buffer,
        file_name=filename,
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )

# ==================== NAVIGATION ====================
st.sidebar.title("📂 Menu")
page = st.sidebar.radio("Navigation", ["Clients", "Commandes", "Statistiques par famille"])

# ==================== PAGE CLIENTS ====================
if page == "Clients":
    st.header("👥 Extraction des clients")
    if st.button("📥 Extraire et nettoyer les données clients"):
        with st.spinner("Connexion à BigQuery..."):
            df_raw = bq_to_dataframe(TABLES["client"], ROW_LIMIT or None)
        st.write(f"✅ Données brutes : {len(df_raw)} lignes")
        st.dataframe(df_raw.head(20))

        with st.spinner("Nettoyage des données..."):
            df_clean = clean_clients(df_raw)
        st.write(f"✅ Données nettoyées : {len(df_clean)} lignes")
        st.dataframe(df_clean.head(20))

        export_excel(df_clean, "export_clients_clean.xlsx")

# ==================== PAGE COMMANDES ====================
elif page == "Commandes":
    st.header("🛒 Analyse des commandes")

    seuil_ventes = 2
    seuil_panier_moyen = 250
    seuil_chiffre_affaire = 180

    with st.spinner("Récupération des commandes..."):
        df_cmd = query_commandes()

    st.write(f"✅ {len(df_cmd)} lignes récupérées")
    st.dataframe(df_cmd.head(20))

    # Filtres
    filtered = df_cmd[
        (df_cmd["nb_commandes"] >= seuil_ventes) &
        (df_cmd["panier_moyen"] >= seuil_panier_moyen) &
        (df_cmd["ca"] >= seuil_chiffre_affaire)
    ]

    st.write(f"✅ {len(filtered)} lignes après filtrage")
    st.dataframe(filtered.head(20))

    # Export complet
    export_excel(filtered, "commandes_filtrees.xlsx")

    # Split par prix de vente
    sup_800 = filtered[filtered["prix_vente"] > 800]
    inf_800 = filtered[filtered["prix_vente"] <= 800]

    export_excel(sup_800, "commandes_prix_sup_800.xlsx")
    export_excel(inf_800, "commandes_prix_inf_800.xlsx")

# ==================== PAGE STATS FAMILLE ====================
elif page == "Statistiques par famille":
    st.header("📊 Statistiques par famille de produits")

    with st.spinner("Récupération des statistiques..."):
        df_stats = query_stats_famille()

    familles = st.multiselect(
        "Sélectionnez les familles à analyser",
        options=df_stats["famille_finale"].dropna().unique(),
        default=[]
    )

    if familles:
        df_stats = df_stats[df_stats["famille_finale"].isin(familles)]

    st.dataframe(df_stats)

    export_excel(df_stats, "stats_famille.xlsx")
