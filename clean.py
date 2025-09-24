import streamlit as st
import pandas as pd
import datetime
import io
import bcrypt
from google.cloud import bigquery
from google.oauth2 import service_account

# ==================== CONFIG ====================
st.set_page_config(page_title="Customer Clean", layout="wide")

# Authentification utilisateur
if "logged_in" not in st.session_state:
    st.session_state["logged_in"] = False

if not st.session_state["logged_in"]:
    st.title("🔐 Portail sécurisé")

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

st.sidebar.success(f"Connecté en tant que {st.session_state['name']} ✅")
if st.sidebar.button("Se déconnecter"):
    st.session_state["logged_in"] = False
    st.rerun()

# ==================== BIGQUERY ====================
PROJECT_ID = "datalake-380714"
DATASET_ID = "pole_agri"
TABLES = {
    "client": "client web_agrizone_client",
    "produit": "produit web_agrizone_produit_description",
    "commande": "commande web_agrizone_commande",
}

creds_dict = st.secrets["gcp_service_account"]
credentials_gcp = service_account.Credentials.from_service_account_info(creds_dict)
client = bigquery.Client(credentials=credentials_gcp, project=creds_dict["project_id"])

def bq_query(query: str) -> pd.DataFrame:
    return client.query(query).result().to_dataframe()

def export_excel(df, filename):
    buffer = io.BytesIO()
    df.to_excel(buffer, index=False, engine="openpyxl")
    buffer.seek(0)
    st.download_button(
        label=f"⬇️ Télécharger {filename}",
        data=buffer,
        file_name=filename,
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

# ==================== PAGES ====================
page = st.sidebar.radio("Navigation", ["Clients", "Panier Moyen", "Statistiques Famille"])

# ---------- PAGE CLIENTS ----------
if page == "Clients":
    st.header("📋 Export Clients BigQuery")
    if st.button("📥 Extraire et nettoyer les données Clients"):
        query = f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.{TABLES['client']}`"
        df = bq_query(query)

        # Nettoyage simple
        df = df.dropna(subset=["email_client"]).drop_duplicates(subset=["email_client"])
        df["Email"] = df["email_client"].astype(str).str.strip()
        df["First Name"] = df["prenom_client"].astype(str).str.strip().str.title()
        df["Last Name"] = df["nom_client"].astype(str).str.strip().str.title()
        df["Country"] = df["libelle_lg_pays"].astype(str).str.strip().str[:2].str.upper()
        df["Zip"] = (
            df["code_postal_adr_client"].astype(str).str.replace(r"[\s.]", "", regex=True).str.strip().str[:5]
        )
        df["Zip"] = df["Zip"].where(df["Zip"].str.fullmatch(r"\d{5}") == True, pd.NA)
        digits = df["portable_client"].astype(str).str.replace(r"\D", "", regex=True)
        df["N° de mobile"] = "+33" + digits.str[-9:]
        df = df[df["N° de mobile"].str.len() == 12]

        final = df[["Email", "First Name", "Last Name", "Country", "Zip", "N° de mobile"]].copy()
        st.dataframe(final.head(20))
        export_excel(final, "clients_clean.xlsx")

# ---------- PAGE PANIER MOYEN ----------
elif page == "Panier Moyen":
    st.header("🛒 Analyse Panier Moyen")

    # Dates persistantes
    if "date_debut_panier" not in st.session_state:
        st.session_state["date_debut_panier"] = datetime.date(2020, 1, 1)
    if "date_fin_panier" not in st.session_state:
        st.session_state["date_fin_panier"] = datetime.date.today()

    col1, col2 = st.columns(2)
    with col1:
        st.date_input("Date de début", key="date_debut_panier")
    with col2:
        st.date_input("Date de fin", key="date_fin_panier")

    # Seuils
    seuil_ventes = 2
    seuil_panier_moyen = 250
    seuil_ca = 180

    if st.button("📥 Générer analyse"):
        query = f"""
        WITH commandes AS (
          SELECT
            c.numero_commande,
            c.code_produit,
            c.quantite,
            c.prix_total_ht,
            c.prix_achat,
            p.libelle,
            p.prix_vente_ht AS prix_vente
          FROM `{PROJECT_ID}.{DATASET_ID}.{TABLES['commande']}` c
          LEFT JOIN `{PROJECT_ID}.{DATASET_ID}.{TABLES['produit']}` p
            ON c.code_produit = p.code
          WHERE c.date_validation IS NOT NULL
            AND DATE(c.date_validation) BETWEEN "{st.session_state['date_debut_panier']}" AND "{st.session_state['date_fin_panier']}"
        )
        SELECT
          code_produit,
          libelle,
          prix_vente,
          COUNT(DISTINCT numero_commande) AS nb_commandes,
          SUM(quantite) AS quantite_vendue,
          SUM(prix_total_ht) AS ca,
          ROUND(SAFE_DIVIDE(SUM(prix_total_ht), COUNT(DISTINCT numero_commande)), 2) AS panier_moyen
        FROM commandes
        GROUP BY code_produit, libelle, prix_vente
        ORDER BY ca DESC
        """
        df = bq_query(query)

        # Application des seuils
        df = df[
            (df["nb_commandes"] >= seuil_ventes)
            & (df["panier_moyen"].astype(float) >= seuil_panier_moyen)
            & (df["ca"].astype(float) >= seuil_ca)
        ]

        st.dataframe(df.head(20))
        export_excel(df, "panier_moyen_complet.xlsx")

        # Export sup >800
        sup800 = df[df["prix_vente"] > 800]
        if not sup800.empty:
            export_excel(sup800, "panier_moyen_prix_sup800.xlsx")

        # Export <=800
        inf800 = df[df["prix_vente"] <= 800]
        if not inf800.empty:
            export_excel(inf800, "panier_moyen_prix_inf_ou_egal800.xlsx")

# ---------- PAGE STATISTIQUES FAMILLE ----------
elif page == "Statistiques Famille":
    st.header("📊 Statistiques par Famille")

    # Dates persistantes
    if "date_debut_fam" not in st.session_state:
        st.session_state["date_debut_fam"] = datetime.date(2025, 1, 1)
    if "date_fin_fam" not in st.session_state:
        st.session_state["date_fin_fam"] = datetime.date.today()

    col1, col2 = st.columns(2)
    with col1:
        st.date_input("Date de début", key="date_debut_fam")
    with col2:
        st.date_input("Date de fin", key="date_fin_fam")

    if st.button("📥 Générer statistiques"):
        query = f"""
        SELECT
            c.numero_commande,
            c.date_validation,
            c.quantite,
            c.prix_total_ht,
            c.prix_achat,
            p.famille1, p.famille1_url,
            p.famille2, p.famille2_url,
            p.famille3, p.famille3_url,
            p.famille4, p.famille4_url
        FROM `{PROJECT_ID}.{DATASET_ID}.{TABLES['commande']}` c
        LEFT JOIN `{PROJECT_ID}.{DATASET_ID}.{TABLES['produit']}` p
            ON c.code_produit = p.code
        WHERE c.date_validation IS NOT NULL
          AND DATE(c.date_validation) BETWEEN "{st.session_state['date_debut_fam']}" AND "{st.session_state['date_fin_fam']}"
        """
        df = bq_query(query)

        # Escalade familles
        df["famille"] = df["famille4"].fillna(df["famille3"]).fillna(df["famille2"]).fillna(df["famille1"])
        df["url"] = df["famille4_url"].fillna(df["famille3_url"]).fillna(df["famille2_url"]).fillna(df["famille1_url"])

        # Filtres persistants familles
        for k in ["f1", "f2", "f3", "f4"]:
            if k not in st.session_state:
                st.session_state[k] = []

        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.multiselect("Famille 1", sorted(df["famille1"].dropna().unique().tolist()), default=st.session_state["f1"], key="f1")
        with col2:
            st.multiselect("Famille 2", sorted(df["famille2"].dropna().unique().tolist()), default=st.session_state["f2"], key="f2")
        with col3:
            st.multiselect("Famille 3", sorted(df["famille3"].dropna().unique().tolist()), default=st.session_state["f3"], key="f3")
        with col4:
            st.multiselect("Famille 4", sorted(df["famille4"].dropna().unique().tolist()), default=st.session_state["f4"], key="f4")

        if st.session_state["f1"]:
            df = df[df["famille1"].isin(st.session_state["f1"])]
        if st.session_state["f2"]:
            df = df[df["famille2"].isin(st.session_state["f2"])]
        if st.session_state["f3"]:
            df = df[df["famille3"].isin(st.session_state["f3"])]
        if st.session_state["f4"]:
            df = df[df["famille4"].isin(st.session_state["f4"])]

        # Calculs
        df["marge_calc"] = df["prix_total_ht"] - (df["prix_achat"] * df["quantite"])
        df_grouped = df.groupby(["famille", "url"]).agg(
            ca_total=("prix_total_ht", "sum"),
            marge=("marge_calc", "sum")
        ).reset_index()
        df_grouped["%marge"] = (df_grouped["marge"] / df_grouped["ca_total"] * 100).round(2)

        st.dataframe(df_grouped.head(20))
        export_excel(df_grouped, "stats_famille.xlsx")
