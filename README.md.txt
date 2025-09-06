# Football Prediction API

API de prédiction de matchs de football utilisant un modèle Bayesian hiérarchique.

## Déploiement

Cette API est conçue pour être déployée sur Render.com

## Endpoints

- `/` - Informations sur l'API
- `/predict` - Prédiction pour un match
- `/predict/batch` - Prédiction pour tous les matchs à venir
- `/model/info` - Informations sur le modèle
- `/model/retrain` - Réentraîner le modèle

## Configuration

Variables d'environnement requises :
- `PHP_BRIDGE_URL` : URL du bridge PHP
- `PHP_SECRET` : Clé secrète pour l'authentification