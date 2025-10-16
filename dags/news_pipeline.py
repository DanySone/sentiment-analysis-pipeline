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
    """Récupère les articles de NewsAPI et les charge en masse dans "raw_articles"."""
    api_key = Variable.get("NEWS_API_KEY")
    url = f"https://newsapi.org/v2/everything?q=technology&language=en&sortBy=publishedAt&pageSize=100&apiKey={api_key}"
    response = requests.get(url)
    data = response.json()
    articles = data.get("articles", [])

    if not articles:
        print("Aucun article trouvé.")
        return

    # Préparer les données pour une insertion en masse
    insert_data = []
    for article in articles:
        author = article.get("author")
        if author and len(author) > 250:
            author = author[:250]
        insert_data.append(
            (
                article["source"]["name"],
                author,
                article.get("title"),
                article.get("description"),
                article.get("url"),
                article.get("publishedAt"),
                article.get("content"),
            )
        )

    hook = PostgresHook(postgres_conn_id="postgres_default")
    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            # Étape 1: Créer la table
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS public.raw_articles (
                    id SERIAL PRIMARY KEY, source_name VARCHAR(255), author VARCHAR(255),
                    title TEXT, description TEXT, url TEXT, published_at TIMESTAMP,
                    content TEXT, inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """
            )
            # **LA CORRECTION** : On "enregistre" la création de la table immédiatement.
            conn.commit()

            # Étape 2: Insérer les données
            extras.execute_values(
                cur,
                "INSERT INTO raw_articles (source_name, author, title, description, url, published_at, content) VALUES %s;",
                insert_data,
            )
            # On "enregistre" l'insertion
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

    # Tâche pour exécuter dbt clean, dbt deps, et dbt run
    fetch_articles_task = PythonOperator(
        task_id="fetch_and_load_articles", python_callable=fetch_and_load_articles
    )

    # Tâche pour exécuter dbt clean et dbt deps
    run_dbt_clean_deps_task = BashOperator(
        task_id="run_dbt_clean_deps",
        bash_command="cd /usr/local/airflow/dbt_project && dbt clean && dbt deps",
    )

    # Tâche pour exécuter uniquement les modèles staging
    run_dbt_staging_task = BashOperator(
        task_id="run_dbt_staging_models",
        bash_command="cd /usr/local/airflow/dbt_project && dbt run --select staging --profiles-dir .",
    )

    # Tâche pour analyser le sentiment
    analyze_sentiment_task = PythonOperator(
        task_id="analyze_sentiment", python_callable=analyze_sentiment
    )
    
    # Tâche pour exécuter uniquement les modèles marts
    run_dbt_marts_task = BashOperator(
        task_id="run_dbt_marts_models",
        bash_command="cd /usr/local/airflow/dbt_project && dbt run --exclude staging --profiles-dir .",
    )

    # Tâche supplémentaire pour exécuter les tests dbt
    run_dbt_tests_task = BashOperator(
        task_id="run_dbt_tests",
        bash_command="cd /usr/local/airflow/dbt_project && dbt test --profiles-dir .",
    )

    # Tâche supplémentaire pour générer la documentation dbt
    generate_dbt_docs_task = BashOperator(
        task_id="generate_dbt_docs",
        bash_command="cd /usr/local/airflow/dbt_project && dbt docs generate --profiles-dir .",
    )
    # Mettez à jour la chaîne de dépendances
    (
        fetch_articles_task
        >> run_dbt_clean_deps_task
        >> run_dbt_staging_task
        >> analyze_sentiment_task
        >> run_dbt_marts_task
        >> run_dbt_tests_task
        >> generate_dbt_docs_task
    )
