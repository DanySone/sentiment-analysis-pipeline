# Utiliser une image Python légère
FROM python:3.11-slim

# Définir le répertoire de travail
WORKDIR /app

# Copier et installer les dépendances
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copier le code de l'application
COPY app.py .

# Exposer le port par défaut de Streamlit
EXPOSE 8501

# La commande pour lancer l'application
CMD ["streamlit", "run", "app.py"]