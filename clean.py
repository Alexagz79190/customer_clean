import streamlit as st
import pandas as pd
import io
import datetime
import bcrypt
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

# Authentification GCP
creds_dict = st.secrets["gcp_service_account"]
credentials_gcp = service_account.Credentials.from_service_account_info(creds_dict)
client = bigquery.Client(credentials=credentials_gcp, project=creds_dict["project_id"])


# ==================== FONCTIONS ====================
def bq_query(query: str) -> pd.DataFrame:
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
    df_final = df_final.replace({"nan": pd.NA, "None": pd.NA})
    df_final = df_final.dropna(how="any")

    return df_final


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


# ==================== LOGIN ====================
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

st.sidebar.success(f"✅ Connecté en tant que {st.session_state['name']}")
if st.sidebar.button("Se déconnecter"):
    st.session_state["logged_in"] = False
    st.rerun()

# ==================== NAVIGATION ====================
page = st.sidebar.radio("📂 Navigation", ["Clients", "Panier Moyen", "Statistiques Famille"])

# ==================== PAGE CLIENTS ====================
if page == "Clients":
    st.header("📋 Nettoyage Clients")
    if st.button("📥 Extraire et nettoyer les clients"):
        query = f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.{TABLES['client']}`"
        df_raw = bq_query(query)
        st.write(f"✅ {len(df_raw)} lignes brutes")
        df_clean = clean_clients(df_raw)
        st.write(f"✅ {len(df_clean)} lignes nettoyées")
        st.dataframe(df_clean.head(20))
        export_excel(df_clean, "clients_clean.xlsx")

# ==================== PAGE PANIER MOYEN ====================
elif page == "Panier Moyen":
    st.header("🛒 Analyse Panier Moyen")
    date_debut = st.date_input("Date de début", value=datetime.date(2020, 1, 1))
    date_fin = st.date_input("Date de fin", value=datetime.date.today())

    seuil_ventes = st.number_input("Seuil Ventes", value=2, step=1)
    seuil_panier_moyen = st.number_input("Seuil Panier Moyen", value=250, step=10)
    seuil_chiffre_affaire = st.number_input("Seuil Chiffre d’Affaires", value=180, step=10)

    if st.button("📥 Générer analyse"):
        query = f"""
        SELECT numero_commande, date_validation, code_produit, quantite,
               prix_total_ht, prix_achat
        FROM `{PROJECT_ID}.{DATASET_ID}.{TABLES['commande']}`
        WHERE date_validation IS NOT NULL
          AND DATE(date_validation) BETWEEN "{date_debut}" AND "{date_fin}"
        """
        df = bq_query(query)

        ventes = df.groupby("code_produit")["numero_commande"].nunique().reset_index(name="ventes")
        qte = df.groupby("code_produit")["quantite"].sum().reset_index(name="quantite_totale")
        ca = df.groupby("code_produit")["prix_total_ht"].sum().reset_index(name="chiffre_affaire")
        prix_moy = df.groupby("code_produit")["prix_total_ht"].mean().reset_index(name="prix_vente")

        df_merge = ventes.merge(qte, on="code_produit").merge(ca, on="code_produit").merge(prix_moy, on="code_produit")

        panier = df.groupby("numero_commande")["prix_total_ht"].sum().mean()
        df_merge["panier_moyen"] = round(panier, 2)

        # Appliquer les seuils
        df_merge = df_merge[
            (df_merge["ventes"] >= seuil_ventes)
            & (df_merge["panier_moyen"] >= seuil_panier_moyen)
            & (df_merge["chiffre_affaire"] >= seuil_chiffre_affaire)
        ]

        st.dataframe(df_merge.head(20))
        export_excel(df_merge, "panier_moyen.xlsx")

# ==================== PAGE STATISTIQUES FAMILLE ====================
elif page == "Statistiques Famille":
    st.header("📊 Statistiques par Famille")

    # --- Gestion des dates dans session_state ---
    if "date_debut_fam" not in st.session_state:
        st.session_state["date_debut_fam"] = datetime.date(2025, 1, 1)
    if "date_fin_fam" not in st.session_state:
        st.session_state["date_fin_fam"] = datetime.date.today()

    col_date1, col_date2 = st.columns(2)
    with col_date1:
        date_debut = st.date_input("Date de début", value=st.session_state["date_debut_fam"], key="date_debut_fam")
    with col_date2:
        date_fin = st.date_input("Date de fin", value=st.session_state["date_fin_fam"], key="date_fin_fam")

    # --- Bouton génération ---
    if st.button("📥 Générer statistiques"):
        # Conversion format français pour affichage
        date_debut_str = date_debut.strftime("%d/%m/%Y")
        date_fin_str = date_fin.strftime("%d/%m/%Y")

        st.info(f"📅 Période sélectionnée : du {date_debut_str} au {date_fin_str}")

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

        # Escalade famille
        df["famille"] = df["famille4"].fillna(df["famille3"]).fillna(df["famille2"]).fillna(df["famille1"])
        df["url"] = df["famille4_url"].fillna(df["famille3_url"]).fillna(df["famille2_url"]).fillna(df["famille1_url"])

        # --- Filtres multi-niveaux avec session_state ---
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            filtre_f1 = st.multiselect("Famille 1", sorted(df["famille1"].dropna().unique().tolist()), key="f1")
        with col2:
            filtre_f2 = st.multiselect("Famille 2", sorted(df["famille2"].dropna().unique().tolist()), key="f2")
        with col3:
            filtre_f3 = st.multiselect("Famille 3", sorted(df["famille3"].dropna().unique().tolist()), key="f3")
        with col4:
            filtre_f4 = st.multiselect("Famille 4", sorted(df["famille4"].dropna().unique().tolist()), key="f4")

        if filtre_f1:
            df = df[df["famille1"].isin(filtre_f1)]
        if filtre_f2:
            df = df[df["famille2"].isin(filtre_f2)]
        if filtre_f3:
            df = df[df["famille3"].isin(filtre_f3)]
        if filtre_f4:
            df = df[df["famille4"].isin(filtre_f4)]

        # Calculs
        df["marge_calc"] = df["prix_total_ht"] - (df["prix_achat"] * df["quantite"])
        df_grouped = df.groupby(["famille", "url"]).agg(
            ca_total=("prix_total_ht", "sum"),
            marge=("marge_calc", "sum")
        ).reset_index()
        df_grouped["%marge"] = (df_grouped["marge"] / df_grouped["ca_total"] * 100).round(2)

        st.dataframe(df_grouped.head(20))
        export_excel(df_grouped, "stats_famille.xlsx")

