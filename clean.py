import streamlit as st
import pandas as pd
import io
import datetime
from google.cloud import bigquery
from google.oauth2 import service_account

# ==================== CONFIG ====================
PROJECT_ID = "datalake-380714"
DATASET_ID = "pole_agri"
TABLES = {
    "client": "client web_agrizone_client",
    "produit": "produit web_agrizone_produit_description",
    "commande": "commande web_agrizone_commande",
}

# Auth GCP depuis secrets.toml
creds_dict = st.secrets["gcp_service_account"]
credentials_gcp = service_account.Credentials.from_service_account_info(creds_dict)
client = bigquery.Client(credentials=credentials_gcp, project=PROJECT_ID)

# ==================== UTILS ====================
def bq_query(sql: str) -> pd.DataFrame:
    job = client.query(sql)
    return job.result().to_dataframe()

def export_excel(df: pd.DataFrame, filename: str):
    buffer = io.BytesIO()
    df.to_excel(buffer, index=False, engine="openpyxl")
    buffer.seek(0)
    st.download_button(
        label=f"⬇️ Télécharger {filename}",
        data=buffer,
        file_name=filename,
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

# ==================== AUTH SIMPLE ====================
if "logged_in" not in st.session_state:
    st.session_state["logged_in"] = False

if not st.session_state["logged_in"]:
    st.subheader("🔐 Connexion requise")
    username = st.text_input("Nom d'utilisateur")
    password = st.text_input("Mot de passe", type="password")

    if st.button("Se connecter"):
        valid_users = st.secrets["users"]["usernames"]
        valid_names = st.secrets["users"]["names"]
        valid_pass = st.secrets["users"]["passwords"]

        if username in valid_users:
            idx = valid_users.index(username)
            if password == valid_pass[idx]:  # ⚠️ ici mot de passe en clair, à remplacer par bcrypt si besoin
                st.session_state["logged_in"] = True
                st.session_state["name"] = valid_names[idx]
                st.success(f"Bienvenue {st.session_state['name']} 🎉")
                st.rerun()
            else:
                st.error("Mot de passe incorrect ❌")
        else:
            st.error("Utilisateur inconnu ❌")

    st.stop()

st.sidebar.success(f"✅ Connecté en tant que {st.session_state['name']}")

if st.sidebar.button("Se déconnecter"):
    st.session_state["logged_in"] = False
    st.rerun()

# ==================== MENU ====================
page = st.sidebar.radio("📑 Navigation", ["Clients", "Panier Moyen", "Statistiques Famille"])

# ==================== PAGE CLIENTS ====================
if page == "Clients":
    st.header("📧 Export Clients nettoyés")
    query = f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.{TABLES['client']}` LIMIT 1000"
    df = bq_query(query)
    st.dataframe(df.head(20))
    export_excel(df, "clients.xlsx")

# ==================== PAGE PANIER MOYEN ====================
elif page == "Panier Moyen":
    st.header("🛒 Analyse Panier Moyen")

    # Date début / fin persistantes
    if "date_debut_cmd" not in st.session_state:
        st.session_state["date_debut_cmd"] = datetime.date(2020, 1, 1)
    if "date_fin_cmd" not in st.session_state:
        st.session_state["date_fin_cmd"] = datetime.date.today()

    col1, col2 = st.columns(2)
    with col1:
        date_debut = st.date_input(
            "Date de début (jj/mm/aaaa)",
            value=st.session_state["date_debut_cmd"],
            format="DD/MM/YYYY",
            key="date_debut_cmd"
        )
    with col2:
        date_fin = st.date_input(
            "Date de fin (jj/mm/aaaa)",
            value=st.session_state["date_fin_cmd"],
            format="DD/MM/YYYY",
            key="date_fin_cmd"
        )

    seuil_ventes = 2
    seuil_panier_moyen = 250
    seuil_ca = 180

    if st.button("📥 Lancer analyse"):
        query = f"""
        SELECT
            c.numero_commande,
            c.date_validation,
            c.code_produit,
            c.quantite,
            c.prix_total_ht,
            c.prix_achat,
            p.libelle,
            p.famille4, p.famille3, p.famille2, p.famille1
        FROM `{PROJECT_ID}.{DATASET_ID}.{TABLES['commande']}` c
        LEFT JOIN `{PROJECT_ID}.{DATASET_ID}.{TABLES['produit']}` p
            ON c.code_produit = p.code
        WHERE c.date_validation IS NOT NULL
          AND DATE(c.date_validation) BETWEEN "{date_debut}" AND "{date_fin}"
        """
        df = bq_query(query)

        # Escalade famille
        df["famille"] = df["famille4"].fillna(df["famille3"]).fillna(df["famille2"]).fillna(df["famille1"])

        # Calcul panier moyen par commande
        panier_commande = df.groupby("numero_commande").agg(
            total_commande=("prix_total_ht", "sum")
        ).reset_index()

        # Rejoint pour chaque produit
        df = df.merge(panier_commande, on="numero_commande", how="left")

        # Groupement par produit
        df_grouped = df.groupby(["code_produit", "libelle", "famille"]).agg(
            nb_commandes=("numero_commande", "nunique"),
            qte_vendue=("quantite", "sum"),
            ca_total=("prix_total_ht", "sum"),
            prix_moyen=("prix_total_ht", "mean"),
            panier_moyen=("total_commande", "mean")
        ).reset_index()

        # Application seuils
        df_filtered = df_grouped[
            (df_grouped["nb_commandes"] >= seuil_ventes) &
            (df_grouped["panier_moyen"] >= seuil_panier_moyen) &
            (df_grouped["ca_total"] >= seuil_ca)
        ]

        st.dataframe(df_filtered)
        export_excel(df_filtered, "panier_moyen.xlsx")

# ==================== PAGE STATISTIQUES FAMILLE ====================
elif page == "Statistiques Famille":
    st.header("📊 Statistiques par Famille")

    # Dates persistantes
    if "date_debut_fam" not in st.session_state:
        st.session_state["date_debut_fam"] = datetime.date(2025, 1, 1)
    if "date_fin_fam" not in st.session_state:
        st.session_state["date_fin_fam"] = datetime.date.today()

    col1, col2 = st.columns(2)
    with col1:
        date_debut = st.date_input(
            "Date de début (jj/mm/aaaa)",
            value=st.session_state["date_debut_fam"],
            format="DD/MM/YYYY",
            key="date_debut_fam"
        )
    with col2:
        date_fin = st.date_input(
            "Date de fin (jj/mm/aaaa)",
            value=st.session_state["date_fin_fam"],
            format="DD/MM/YYYY",
            key="date_fin_fam"
        )

    # Initialisation filtres familles persistants
    for k in ["f1", "f2", "f3", "f4"]:
        if k not in st.session_state:
            st.session_state[k] = []

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
          AND DATE(c.date_validation) BETWEEN "{date_debut}" AND "{date_fin}"
        """
        df = bq_query(query)

        # Escalade familles
        df["famille"] = df["famille4"].fillna(df["famille3"]).fillna(df["famille2"]).fillna(df["famille1"])
        df["url"] = df["famille4_url"].fillna(df["famille3_url"]).fillna(df["famille2_url"]).fillna(df["famille1_url"])

        # Sélecteurs persistants
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            choix_f1 = st.multiselect("Famille 1", sorted(df["famille1"].dropna().unique().tolist()),
                                      default=st.session_state["f1"])
            st.session_state["f1"] = choix_f1
        with col2:
            choix_f2 = st.multiselect("Famille 2", sorted(df["famille2"].dropna().unique().tolist()),
                                      default=st.session_state["f2"])
            st.session_state["f2"] = choix_f2
        with col3:
            choix_f3 = st.multiselect("Famille 3", sorted(df["famille3"].dropna().unique().tolist()),
                                      default=st.session_state["f3"])
            st.session_state["f3"] = choix_f3
        with col4:
            choix_f4 = st.multiselect("Famille 4", sorted(df["famille4"].dropna().unique().tolist()),
                                      default=st.session_state["f4"])
            st.session_state["f4"] = choix_f4

        # Application filtres
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

        st.dataframe(df_grouped)
        export_excel(df_grouped, "stats_famille.xlsx")
