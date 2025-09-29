import streamlit as st
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
import io
import bcrypt
import datetime

# ==================== CONFIG ====================
PROJECT_ID = "datalake-380714"
DATASET_ID = "pole_agri"

TABLES = {
    "client": "client web_agrizone_client",
    "produit": "produit web_agrizone_produit_description",
    "commande": "commande web_agrizone_commande",
}

# Seuils Panier Moyen
SEUIL_VENTES = 2
SEUIL_PANIER_MOYEN = 250
SEUIL_CA = 180

# Authentification GCP
creds_dict = st.secrets["gcp_service_account"]
credentials_gcp = service_account.Credentials.from_service_account_info(creds_dict)
client = bigquery.Client(credentials=credentials_gcp, project=creds_dict["project_id"])

# ==================== FONCTIONS ====================
def export_excel(df, filename):
    """Exporte un DataFrame en Excel et propose un bouton de téléchargement"""
    buffer = io.BytesIO()
    df.to_excel(buffer, index=False, engine="openpyxl")
    buffer.seek(0)
    st.download_button(
        label=f"⬇️ Télécharger {filename}",
        data=buffer,
        file_name=filename,
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

def bq_query(query: str) -> pd.DataFrame:
    """Exécute une requête BigQuery et retourne un DataFrame pandas"""
    job = client.query(query)
    df = job.result().to_dataframe()
    return df

def clean_clients(df: pd.DataFrame) -> pd.DataFrame:
    """Nettoyage spécifique pour la page Clients"""
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
    df_final = df_final.replace({"nan": pd.NA, "None": pd.NA})
    df_final = df_final.dropna(how="any")
    return df_final

# ==================== LOGIN ====================
if "logged_in" not in st.session_state:
    st.session_state["logged_in"] = False

if not st.session_state["logged_in"]:
    st.title("🔐 Export datas Agrizone")
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
                st.rerun()
            else:
                st.error("Mot de passe incorrect ❌")
        else:
            st.error("Utilisateur inconnu ❌")
    st.stop()

# ==================== MENU ====================
st.sidebar.title(f"Bienvenue {st.session_state['name']} 🎉")
page = st.sidebar.radio("Navigation", ["Clients", "Panier moyen", "Statistiques Famille"])

# Bouton de déconnexion
if st.sidebar.button("🚪 Se déconnecter"):
    st.session_state["logged_in"] = False
    st.session_state.clear()  # pour vider tout l'état (filtres, sessions…)
    st.rerun()

# ==================== PAGE CLIENTS ====================
if page == "Clients":
    st.header("👥 Nettoyage Clients")
    if st.button("📥 Extraire et nettoyer les clients"):
        query = f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.{TABLES['client']}`"
        df_raw = bq_query(query)
        st.write(f"✅ Données brutes : {len(df_raw)} lignes")
        df_clean = clean_clients(df_raw)
        st.write(f"✅ Données nettoyées : {len(df_clean)} lignes")
        st.dataframe(df_clean.head(20))
        export_excel(df_clean, "clients_clean.xlsx")

# ==================== PAGE PANIER MOYEN ====================
elif page == "Panier moyen":
    st.header("🛒 Analyse Panier Moyen")
    date_min = st.date_input("Date de début", value=datetime.date(2020, 1, 1), format="DD/MM/YYYY")
    date_max = st.date_input("Date de fin", value=datetime.date.today(), format="DD/MM/YYYY")

    if st.button("📥 Calculer panier moyen"):
        query = f"""
    SELECT
        c.numero_commande,
        c.date_validation,
        c.code_produit,
        c.quantite,
        c.prix_total_ht,
        c.prix_achat,
        p.libelle AS libelle_produit,
        p.famille1, p.famille2, p.famille3, p.famille4
    FROM `{PROJECT_ID}.{DATASET_ID}.{TABLES['commande']}` c
    LEFT JOIN `{PROJECT_ID}.{DATASET_ID}.{TABLES['produit']}` p
        ON c.code_produit = p.code
    WHERE c.date_validation IS NOT NULL
      AND SAFE.CAST(c.date_validation AS DATE) BETWEEN "{date_min}" AND "{date_max}"
"""
        df = bq_query(query)

        # Calculs
        ventes = df.groupby("code_produit")["numero_commande"].nunique().reset_index(name="nb_commandes")
        qte = df.groupby("code_produit")["quantite"].sum().reset_index(name="quantite_totale")
        ca = df.groupby("code_produit")["prix_total_ht"].sum().reset_index(name="ca_total")

        df_merge = df.merge(ventes, on="code_produit").merge(qte, on="code_produit").merge(ca, on="code_produit")
        df_merge["panier_moyen"] = (df_merge["ca_total"] / df_merge["nb_commandes"]).round(2)

        # Famille escalade
        df_merge["famille"] = (
            df_merge["famille4"].fillna(df_merge["famille3"])
            .fillna(df_merge["famille2"])
            .fillna(df_merge["famille1"])
        )

        df_export = df_merge[
            ["code_produit", "libelle_produit", "famille", "nb_commandes", "quantite_totale", "ca_total", "panier_moyen"]
        ].drop_duplicates()

        # Application des seuils
        df_export = df_export[
            (df_export["nb_commandes"] >= SEUIL_VENTES) &
            (df_export["panier_moyen"] >= SEUIL_PANIER_MOYEN) &
            (df_export["ca_total"] >= SEUIL_CA)
        ]

        # Exports
        st.dataframe(df_export.head(20))
        export_excel(df_export, "panier_moyen.xlsx")
        export_excel(df_export[df_export["ca_total"] > 800], "panier_moyen_ca_sup_800.xlsx")
        export_excel(df_export[df_export["ca_total"] <= 800], "panier_moyen_ca_inf_800.xlsx")

# ==================== PAGE STATISTIQUES FAMILLE ====================
elif page == "Statistiques Famille":
    st.header("📊 Statistiques par Famille")
    date_debut = st.date_input("Date de début", value=datetime.date(2025, 1, 1), format="DD/MM/YYYY")
    date_fin = st.date_input("Date de fin", value=datetime.date.today(), format="DD/MM/YYYY")

    if st.button("📥 Générer statistiques"):
        query = f"""
        SELECT
            c.numero_commande,
            c.date_validation,
            c.code_produit,
            c.quantite,
            c.prix_total_ht,
            c.prix_achat,
            p.code,
            p.famille1, p.famille1_url,
            p.famille2, p.famille2_url,
            p.famille3, p.famille3_url,
            p.famille4, p.famille4_url
        FROM `{PROJECT_ID}.{DATASET_ID}.{TABLES['commande']}` c
        LEFT JOIN `{PROJECT_ID}.{DATASET_ID}.{TABLES['produit']}` p
            ON c.code_produit = p.code
        WHERE c.date_validation IS NOT NULL
          AND DATE(c.date_validation) BETWEEN "{date_debut}" AND "{date_fin}"
        """
        df = bq_query(query)

        # 🔹 Normaliser les clés
        df["code_produit"] = df["code_produit"].astype(str).str.strip()
        df["code"] = df["code"].astype(str).str.strip()

        # 🔹 Nettoyer familles vides
        for col in ["famille1", "famille2", "famille3", "famille4"]:
            df[col] = df[col].replace("", pd.NA).replace(" ", pd.NA)
        for col in ["famille1_url", "famille2_url", "famille3_url", "famille4_url"]:
            df[col] = df[col].replace("", pd.NA).replace(" ", pd.NA)

        # 🔹 Escalade famille (famille4 > famille3 > famille2 > famille1)
        df["famille"] = (
            df["famille4"].fillna(df["famille3"])
            .fillna(df["famille2"])
            .fillna(df["famille1"])
        )
        df["url"] = (
            df["famille4_url"].fillna(df["famille3_url"])
            .fillna(df["famille2_url"])
            .fillna(df["famille1_url"])
        )

        # 🔹 Calcul marge
        df["marge_calc"] = df["prix_total_ht"] - (df["prix_achat"] * df["quantite"])

        # 🔹 Agrégation
        df_grouped = df.groupby(["famille", "url"]).agg(
            ca_total=("prix_total_ht", "sum"),
            marge=("marge_calc", "sum")
        ).reset_index()
        df_grouped["%marge"] = (df_grouped["marge"] / df_grouped["ca_total"] * 100).round(2)

        # Résultats
        st.write(f"✅ {len(df_grouped)} familles analysées")
        st.dataframe(df_grouped.head(20))
        export_excel(df_grouped, "stats_famille.xlsx")
