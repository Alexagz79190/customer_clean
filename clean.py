import streamlit as st
import pandas as pd
import io
import bcrypt
from google.cloud import bigquery
from google.oauth2 import service_account

# ==================== AUTH GCP ====================
creds_dict = st.secrets["gcp_service_account"]
credentials_gcp = service_account.Credentials.from_service_account_info(creds_dict)
client = bigquery.Client(credentials=credentials_gcp, project=creds_dict["project_id"])

# ==================== CONFIG ====================
PROJECT_ID = "datalake-380714"
DATASET_ID = "pole_agri"

# ==================== FONCTIONS ====================
def clean_clients(df: pd.DataFrame) -> pd.DataFrame:
    df = df.dropna(subset=["email_client"]).drop_duplicates(subset=["email_client"])
    df["Email"] = df["email_client"].astype(str).str.strip()
    df["First Name"] = df["prenom_client"].astype(str).str.strip().str.title()
    df["Last Name"] = df["nom_client"].astype(str).str.strip().str.title()
    df["Country"] = df["libelle_lg_pays"].astype(str).str.strip().str[:2].str.upper()
    df["Zip"] = (
        df["code_postal_adr_client"].astype(str)
        .str.replace(r"[\s.]", "", regex=True).str.strip().str[:5]
    )
    df["Zip"] = df["Zip"].where(df["Zip"].str.fullmatch(r"\d{5}") == True, pd.NA)
    digits = df["portable_client"].astype(str).str.replace(r"\D", "", regex=True)
    df["N° de mobile"] = "+33" + digits.str[-9:]
    df = df[df["N° de mobile"].str.len() == 12]
    cols = ["Email", "First Name", "Last Name", "Country", "Zip", "N° de mobile"]
    return df[cols].dropna(how="any")

def query_panier_moyen(commandes_filtre=None):
    extra_filter = ""
    if commandes_filtre:
        extra_filter = f"AND numero_commande IN ({','.join(map(str, commandes_filtre))})"

    QUERY = f"""
    WITH commandes AS (
      SELECT
        numero_commande,
        code_produit,
        quantite,
        prix_total_ht,
        SUM(prix_total_ht) OVER (PARTITION BY numero_commande) AS total_commande
      FROM `{PROJECT_ID}.{DATASET_ID}.commande web_agrizone_commande`
      WHERE SAFE.PARSE_DATE('%Y-%m-%d', date_validation) > DATE '2020-12-31'
        AND date_validation IS NOT NULL
        {extra_filter}
    )
    SELECT
      c.code_produit,
      p.libelle AS libelle_produit,
      COALESCE(NULLIF(p.famille4, ''), NULLIF(p.famille3, ''), NULLIF(p.famille2, ''), p.famille1) AS famille_finale,
      p.prix_vente_ht AS prix_vente,
      COUNT(DISTINCT c.numero_commande) AS nb_commandes,
      SUM(c.quantite) AS quantite_totale,
      SUM(c.prix_total_ht) AS chiffre_affaire,
      ROUND(SUM(c.total_commande) / COUNT(DISTINCT c.numero_commande), 2) AS panier_moyen
    FROM commandes c
    LEFT JOIN `{PROJECT_ID}.{DATASET_ID}.produit web_agrizone_produit_description` p
      ON c.code_produit = p.code
    GROUP BY c.code_produit, libelle_produit, famille_finale, prix_vente
    """
    return client.query(QUERY).result().to_dataframe()

# ==================== LOGIN ====================
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

# ==================== APP MULTIPAGE ====================
st.sidebar.title(f"Bienvenue {st.session_state['name']} 👋")
page = st.sidebar.radio("Navigation", ["Clients", "Panier Moyen Produits"])

if st.sidebar.button("Se déconnecter"):
    st.session_state["logged_in"] = False
    st.rerun()

# ---------- PAGE CLIENTS ----------
if page == "Clients":
    st.header("📧 Export Clients")
    if st.button("📥 Extraire et nettoyer les clients"):
        df_raw = client.query(
            f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.client web_agrizone_client`"
        ).result().to_dataframe()
        df_clean = clean_clients(df_raw)
        st.success(f"{len(df_clean)} clients nettoyés")
        st.dataframe(df_clean.head(20))

        buffer = io.BytesIO()
        df_clean.to_excel(buffer, index=False, engine="openpyxl")
        buffer.seek(0)
        st.download_button(
            "⬇️ Télécharger Clients",
            data=buffer,
            file_name="clients_clean.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

# ---------- PAGE PANIER MOYEN ----------
elif page == "Panier Moyen Produits":
    st.header("🛒 Analyse Panier Moyen Produits")

    commandes_test = st.text_input("Filtrer par numéros de commande (séparés par ,)", "")

    if st.button("📥 Extraire commandes"):
        commandes_filtre = [int(x.strip()) for x in commandes_test.split(",") if x.strip().isdigit()]
        df = query_panier_moyen(commandes_filtre if commandes_filtre else None)

        st.success(f"{len(df)} lignes récupérées")
        st.dataframe(df.head(20))

        seuil_ventes = 2
        seuil_panier_moyen = 250
        seuil_chiffre_affaire = 180

        df_filtered = df[
            (df["nb_commandes"] >= seuil_ventes) &
            (df["panier_moyen"] >= seuil_panier_moyen) &
            (df["chiffre_affaire"] >= seuil_chiffre_affaire)
        ]

        # Export principal
        buffer = io.BytesIO()
        df_filtered.to_excel(buffer, index=False, engine="openpyxl")
        buffer.seek(0)
        st.download_button(
            "⬇️ Télécharger résultats filtrés",
            data=buffer,
            file_name="resultats_paniers_eleves.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

        # Sup à 800
        df_sup = df_filtered[df_filtered["prix_vente"] > 800]
        buf_sup = io.BytesIO()
        df_sup.to_excel(buf_sup, index=False, engine="openpyxl")
        buf_sup.seek(0)
        st.download_button(
            "⬇️ Résultats prix_vente > 800",
            data=buf_sup,
            file_name="resultats_prix_vente_superieur_800.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

        # Inf ou égal à 800
        df_inf = df_filtered[df_filtered["prix_vente"] <= 800]
        buf_inf = io.BytesIO()
        df_inf.to_excel(buf_inf, index=False, engine="openpyxl")
        buf_inf.seek(0)
        st.download_button(
            "⬇️ Résultats prix_vente <= 800",
            data=buf_inf,
            file_name="resultats_prix_vente_inferieur_ou_egal_800.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
