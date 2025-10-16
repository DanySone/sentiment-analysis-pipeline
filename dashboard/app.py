# dashboard/app.py (Version sécurisée)

import streamlit as st
import pandas as pd
import psycopg2
import time
import os # <-- Importer la librairie pour lire les variables d'environnement

# Utiliser le cache de Streamlit pour ne pas recharger les données à chaque interaction
@st.cache_data(ttl=60) # Rafraîchir les données toutes les 60 secondes
def get_data():
    """Se connecte à la BDD Postgres en utilisant les variables d'environnement."""
    try:
        # ** LA CORRECTION **: Lire les identifiants depuis les variables d'environnement
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST"),
            database=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            port=os.getenv("DB_PORT")
        )
        df = pd.read_sql("SELECT * FROM public.dim_daily_sentiment_by_source ORDER BY article_date", conn)
        return df
    except Exception as e:
        st.warning(f"En attente des données de la base... ({e})")
        return pd.DataFrame()

# --- Configuration de la page ---
st.set_page_config(page_title="Analyse de Sentiments", layout="wide")

# --- Affichage du dashboard ---
st.title("📊 Tableau de Bord - Analyse de Sentiments des Articles")
st.write("Ce tableau de bord se met à jour automatiquement après chaque exécution du pipeline Airflow.")

# Boucle pour tenter de charger les données jusqu'à ce qu'elles soient disponibles
with st.spinner("Connexion à la base de données..."):
    df = get_data()
    # On attend un peu pour que la base de données soit prête au premier lancement
    if df.empty:
        time.sleep(5)
        df = get_data()

if df.empty:
    st.error("Impossible de charger les données. Veuillez lancer le DAG Airflow et rafraîchir la page.")
else:
    st.success("Données chargées avec succès !")

    # Afficher le graphique principal
    st.subheader("Évolution du Sentiment Moyen par Source")
    st.line_chart(df, x="article_date", y="average_sentiment", color="source_name")

    # Afficher les données brutes dans un tableau
    st.subheader("Détail des Données Agrégées")
    st.dataframe(df)