import streamlit as st
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
import io
import datetime

# ==================== CONFIG ====================
PROJECT_ID = "datalake-380714"
DATASET_ID = "pole_agri"
TABLES = {
    "client": "client web_agrizone_client",
    "produit": "produit web_agrizone_produit_description",
    "commande": "commande web_agrizone_commande",
}

# Auth GCP
creds_dict = st.secrets["gcp_service_account"]
credentials_gcp = service_account.Credentials.from_service_account_info(creds_dict)
client = bigquery.Client(credentials=credentials_gcp, project=creds_dict["project_id"])

# ==================== UTILS ====================
def bq_query(query: str) -> pd.DataFrame:
    job = client.query(query)
    return job.result().to_dataframe()

def export_excel(df: pd.DataFrame, filename: str, key: str):
    """Téléchargement Excel avec séparateur virgule et décimales FR."""
    buffer = io.BytesIO()
    df.to_excel(buffer, index=False, engine="openpyxl")
    buffer.seek(0)
    st.download_button(
        label=f"⬇️ Télécharger {filename}",
        data=buffer,
        file_name=filename,
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        key=key,
    )

def multiselect_persistant(label, options, key):
    if key not in st.session_state:
        st.session_state[key] = []
    current = st.session_state[key]
    selected = st.multiselect(label, options, default=current, key=f"{key}_widget")
    st.session_state[key] = selected
    return selected

# ==================== MENU ====================
st.sidebar.title("📑 Menu")
page = st.sidebar.radio("Navigation", ["Clients", "Panier Moyen", "Statistiques Famille"])

# ==================== PAGE CLIENTS ====================
if page == "Clients":
    st.header("👥 Export Clients")
    if st.button("📥 Générer export"):
        query = f"""
        SELECT *
        FROM `{PROJECT_ID}.{DATASET_ID}.{TABLES['client']}`
        """
        df = bq_query(query)
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
        df_final = df_final.replace({r"^\s*$": pd.NA}, regex=True).dropna(how="any")
        st.write(f"✅ {len(df_final)} lignes exportées")
        st.dataframe(df_final.head(20))
        export_excel(df_final, "clients_clean.xlsx", "download_clients")

# ==================== PAGE PANIER MOYEN ====================
elif page == "Panier Moyen":
    st.header("🛒 Analyse Panier Moyen")
    if "date_debut_panier" not in st.session_state:
        st.session_state["date_debut_panier"] = datetime.date(2020, 1, 1)
    if "date_fin_panier" not in st.session_state:
        st.session_state["date_fin_panier"] = datetime.date.today()
    col1, col2 = st.columns(2)
    with col1:
        st.date_input("Date de début", key="date_debut_panier", format="DD/MM/YYYY")
    with col2:
        st.date_input("Date de fin", key="date_fin_panier", format="DD/MM/YYYY")
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
          INNER JOIN `{PROJECT_ID}.{DATASET_ID}.{TABLES['produit']}` p
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
        df = df[
            (df["nb_commandes"] >= seuil_ventes)
            & (df["panier_moyen"].astype(float) >= seuil_panier_moyen)
            & (df["ca"].astype(float) >= seuil_ca)
        ]
        st.write(f"✅ {len(df)} produits analysés")
        st.dataframe(df.head(20))
        export_excel(df, "panier_moyen_complet.xlsx", "download_panier_moyen")
        sup800 = df[df["prix_vente"] > 800]
        if not sup800.empty:
            export_excel(sup800, "panier_moyen_prix_sup800.xlsx", "download_panier_sup800")
        inf800 = df[df["prix_vente"] <= 800]
        if not inf800.empty:
            export_excel(inf800, "panier_moyen_prix_inf_ou_egal800.xlsx", "download_panier_inf800")

# ==================== PAGE STATISTIQUES FAMILLE ====================
elif page == "Statistiques Famille":
    st.header("📊 Statistiques par Famille")

    # Dates
    if "date_debut_fam" not in st.session_state:
        st.session_state["date_debut_fam"] = datetime.date(2025, 1, 1)
    if "date_fin_fam" not in st.session_state:
        st.session_state["date_fin_fam"] = datetime.date.today()
    col1, col2 = st.columns(2)
    with col1:
        date_debut = st.date_input("Date de début", key="date_debut_fam", format="DD/MM/YYYY")
    with col2:
        date_fin = st.date_input("Date de fin", key="date_fin_fam", format="DD/MM/YYYY")

    # Charger les données des commandes et des produits séparément
    query_commandes = f"""
    SELECT
        numero_commande,
        date_validation,
        code_produit,
        quantite,
        prix_total_ht,
        prix_achat
    FROM `{PROJECT_ID}.{DATASET_ID}.{TABLES['commande']}`
    WHERE date_validation IS NOT NULL
      AND DATE(date_validation) BETWEEN "{date_debut}" AND "{date_fin}"
    """
    df_commandes = bq_query(query_commandes)

    query_produits = f"""
    SELECT
        code,
        libelle,
        famille1, famille1_url,
        famille2, famille2_url,
        famille3, famille3_url,
        famille4, famille4_url
    FROM `{PROJECT_ID}.{DATASET_ID}.{TABLES['produit']}`
    """
    df_produits = bq_query(query_produits)

    # Jointure avec pandas (LEFT JOIN pour détecter les commandes sans famille)
    df = pd.merge(
        df_commandes,
        df_produits,
        left_on="code_produit",
        right_on="code",
        how="left",
        indicator=True
    )

    # **Détecter toutes les commandes sans famille valide**
    # Une commande est "sans famille" si :
    # 1. Le produit n'existe pas (LEFT JOIN sans correspondance)
    # 2. Le produit existe mais n'a aucune famille définie
    commandes_sans_famille = df[
        (df["_merge"] == "left_only") |  # Produit inconnu
        (df["famille1"].isna() & df["famille2"].isna() & df["famille3"].isna() & df["famille4"].isna())
    ]

    # **Calculer le CA total des commandes sans famille**
    ca_sans_famille = commandes_sans_famille["prix_total_ht"].sum()
    st.write(f"🔍 **CA total des commandes sans famille : {ca_sans_famille:,.2f} €**")

    # **Exporter le détail des commandes sans famille**
    if not commandes_sans_famille.empty:
        st.error(f"❌ {len(commandes_sans_famille)} commandes ont un produit sans famille ou un produit inconnu.")
        export_excel(commandes_sans_famille, "commandes_sans_famille.xlsx", "download_commandes_sans_famille")

    # **Filtrer strictement pour ne garder que les commandes avec une famille valide**
    df_valide = df[
        (df["_merge"] == "both") &  # Produit existe
        (~df["famille1"].isna() | ~df["famille2"].isna() | ~df["famille3"].isna() | ~df["famille4"].isna())
    ]

    # **Calculer le CA total des commandes valides**
    ca_valide = df_valide["prix_total_ht"].sum()
    st.write(f"✅ **CA total des commandes avec famille valide : {ca_valide:,.2f} €**")

    # **Vérification : la somme des deux CA doit correspondre au CA total des commandes**
    ca_total_commandes = df_commandes["prix_total_ht"].sum()
    st.write(f"📊 **CA total des commandes (toutes) : {ca_total_commandes:,.2f} €**")

    # **Vérification de la cohérence**
    if abs(ca_sans_famille + ca_valide - ca_total_commandes) > 1:
        st.error("❌ Incohérence détectée : la somme des CA (sans famille + valide) ne correspond pas au CA total des commandes.")
    else:
        st.success("✅ Cohérence vérifiée : la somme des CA correspond au CA total des commandes.")

    # Déterminer la famille principale (uniquement sur les données valides)
    df_valide["famille"] = df_valide["famille4"].fillna(df_valide["famille3"]).fillna(df_valide["famille2"]).fillna(df_valide["famille1"])
    df_valide["url"] = df_valide["famille4_url"].fillna(df_valide["famille3_url"]).fillna(df_valide["famille2_url"]).fillna(df_valide["famille1_url"])

    # Sélecteurs persistants pour les familles (uniquement sur les données valides)
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        choix_f1 = multiselect_persistant("Famille 1", sorted(df_valide["famille1"].dropna().unique().tolist()), "choix_f1")
    with col2:
        choix_f2 = multiselect_persistant("Famille 2", sorted(df_valide["famille2"].dropna().unique().tolist()), "choix_f2")
    with col3:
        choix_f3 = multiselect_persistant("Famille 3", sorted(df_valide["famille3"].dropna().unique().tolist()), "choix_f3")
    with col4:
        choix_f4 = multiselect_persistant("Famille 4", sorted(df_valide["famille4"].dropna().unique().tolist()), "choix_f4")

    if st.button("📥 Générer statistiques"):
        # Appliquer les filtres sur les données valides
        if choix_f1:
            df_valide = df_valide[df_valide["famille1"].isin(choix_f1)]
        if choix_f2:
            df_valide = df_valide[df_valide["famille2"].isin(choix_f2)]
        if choix_f3:
            df_valide = df_valide[df_valide["famille3"].isin(choix_f3)]
        if choix_f4:
            df_valide = df_valide[df_valide["famille4"].isin(choix_f4)]

        # Calculs (uniquement sur les données valides)
        df_valide["marge_calc"] = df_valide["prix_total_ht"] - (df_valide["prix_achat"] * df_valide["quantite"])
        df_grouped = df_valide.groupby(["famille", "url"]).agg(
            ca_total=("prix_total_ht", "sum"),
            marge=("marge_calc", "sum")
        ).reset_index()
        df_grouped["%marge"] = (df_grouped["marge"] / df_grouped["ca_total"] * 100).round(2)

        st.write(f"✅ **CA total des commandes avec famille valide (après filtres) : {df_valide['prix_total_ht'].sum():,.2f} €**")
        st.write(f"✅ {len(df_grouped)} familles analysées")
        st.dataframe(df_grouped)
        export_excel(df_grouped, "stats_famille.xlsx", "download_stats_famille")
