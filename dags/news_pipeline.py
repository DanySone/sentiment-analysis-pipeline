# Fichier : dags/news_pipeline.py (Version sécurisée)

import requests
from datetime import datetime

# Imports Airflow
from airflow.models.dag import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.postgres.hooks.postgres import (
    PostgresHook,
)  # <-- Import sécurisé
from airflow.models import Variable

# Imports pour les fonctions Python
from psycopg2 import extras
from textblob import TextBlob

# =============================================================================
# SECTION 1: FONCTIONS PYTHON
# =============================================================================


# Fichier news_pipeline.py


def fetch_and_load_articles():
    """Récupère les articles et les charge dans la table "raw_articles" EXISTANTE."""
    api_key = Variable.get("NEWS_API_KEY")
    url = f"https://newsapi.org/v2/everything?q=technology&language=en&sortBy=publishedAt&pageSize=100&apiKey={api_key}"

    response = requests.get(url)
    data = response.json()
    articles = data.get("articles", [])

    if not articles:
        print("Aucun article trouvé.")
        return

    # Utilisation du Hook pour une connexion sécurisée gérée par Airflow
    hook = PostgresHook(postgres_conn_id="postgres_default")
    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            # ON A SUPPRIMÉ LE "CREATE TABLE" D'ICI

            for article in articles:
                author = article.get("author")
                if author and len(author) > 250:
                    author = author[:250]
                cur.execute(
                    "INSERT INTO raw_articles (source_name, author, title, description, url, published_at, content) VALUES (%s, %s, %s, %s, %s, %s, %s);",
                    (
                        article["source"]["name"],
                        author,
                        article.get("title"),
                        article.get("description"),
                        article.get("url"),
                        article.get("publishedAt"),
                        article.get("content"),
                    ),
                )
            conn.commit()
    print(f"{len(articles)} articles ont été insérés avec succès.")


def analyze_sentiment():
    """Lit stg_articles, analyse le sentiment, et charge les résultats dans article_sentiments."""
    hook = PostgresHook(postgres_conn_id="postgres_default")
    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS public.article_sentiments (
                    article_id INTEGER PRIMARY KEY, sentiment_score FLOAT,
                    analyzed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """
            )
            cur.execute("SELECT article_id, title FROM public.stg_articles;")
            articles = cur.fetchall()

            if not articles:
                print("Aucun article à analyser.")
                return

            results = []
            for article_id, title in articles:
                if title:
                    sentiment_score = TextBlob(title).sentiment.polarity
                    results.append((article_id, sentiment_score))

            extras.execute_values(
                cur,
                "INSERT INTO public.article_sentiments (article_id, sentiment_score) VALUES %s ON CONFLICT (article_id) DO UPDATE SET sentiment_score = EXCLUDED.sentiment_score;",
                results,
            )
            conn.commit()
    print(f"{len(results)} articles ont été analysés avec succès.")


# =============================================================================
# SECTION 2: DÉFINITION DU DAG AIRFLOW
# =============================================================================

with DAG(
    dag_id="news_sentiment_pipeline",
    start_date=datetime(2023, 10, 26),
    schedule="@daily",
    catchup=False,
    tags=["news", "dbt"],
) as dag:
    fetch_articles_task = PythonOperator(
        task_id="fetch_and_load_articles", python_callable=fetch_and_load_articles
    )

    # COMBINER les commandes dbt en une seule tâche
    run_dbt_task = BashOperator(
        task_id="run_dbt_models",
        bash_command="cd /usr/local/airflow/dbt_project && dbt clean && dbt deps && dbt run --select staging --profiles-dir .",
    )

    analyze_sentiment_task = PythonOperator(
        task_id="analyze_sentiment", python_callable=analyze_sentiment
    )

    # Mettez à jour la chaîne de dépendances
    fetch_articles_task >> run_dbt_task >> analyze_sentiment_task
