FROM astrocrpublic.azurecr.io/runtime:3.1-2

# On passe en utilisateur root pour pouvoir installer des paquets
USER root

# On met à jour la liste des paquets et on installe git
# L'option -y répond "oui" automatiquement à toutes les questions
RUN apt-get update && apt-get install -y git