"""
API de prédiction football avec modèle Bayesian hiérarchique
Version optimisée pour Render.com avec gestion des restrictions Hostinger
"""

import os
import json
import requests
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import time

# Configuration MySQL avec gestion Hostinger
import mysql.connector
from mysql.connector import pooling, Error

# FastAPI et dépendances
from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import uvicorn

# Machine Learning léger (sans pandas ni sklearn)
from scipy.stats import poisson
import pickle

# ==================== CONFIGURATION ====================

# Configuration MySQL Hostinger avec paramètres optimisés
MYSQL_CONFIG = {
    'host': 'srv1043.hstgr.io',
    'database': 'u827503784_appdevfoot',
    'user': 'u827503784_BnFnX7',
    'password': 'TestApFootProno7+',
    'port': 3306,
    'raise_on_warnings': False,
    'use_pure': True,  # Important : utilise l'implémentation Python pure (plus compatible)
    'autocommit': True,
    'pool_reset_session': False,  # Évite de réinitialiser la session (économise des ressources)
    'connect_timeout': 10,  # Timeout de connexion en secondes
    'connection_timeout': 10,
    'auth_plugin': 'mysql_native_password'  # Plugin d'authentification compatible
}

# Configuration depuis les variables d'environnement
PHP_BRIDGE_URL = os.getenv('PHP_BRIDGE_URL', 'https://appdevfoot.leselixirsdedamenature.fr/api_bridge_enhanced.php')
PHP_SECRET = os.getenv('PHP_SECRET', 'TonSecret2024')
PORT = int(os.getenv('PORT', 8000))

# ==================== GESTION DE CONNEXION MYSQL ROBUSTE ====================

class MySQLConnectionManager:
    """
    Gestionnaire de connexion MySQL robuste pour gérer les restrictions Hostinger
    Implémente la reconnexion automatique et le pooling intelligent
    """
    
    def __init__(self, config: Dict):
        self.config = config
        self.pool = None
        self.last_connection_time = None
        self.connection_count = 0
        self.max_retries = 3
        self.retry_delay = 2  # secondes entre les tentatives
        
        # Tenter de créer un pool de connexions
        self._initialize_pool()
    
    def _initialize_pool(self):
        """Initialise le pool de connexions avec gestion d'erreur"""
        try:
            # Configuration du pool avec des paramètres conservateurs
            # Hostinger limite souvent à 10-30 connexions simultanées
            pool_config = self.config.copy()
            pool_config.update({
                'pool_name': 'football_pool',
                'pool_size': 3,  # Seulement 3 connexions dans le pool (conservateur)
                'pool_reset_session': False
            })
            
            self.pool = mysql.connector.pooling.MySQLConnectionPool(**pool_config)
            print("✅ Pool de connexions MySQL créé avec succès")
            return True
            
        except Error as e:
            print(f"⚠️ Impossible de créer le pool: {e}")
            print("Utilisation de connexions directes à la place")
            self.pool = None
            return False
    
    def get_connection(self, retry_count=0):
        """
        Obtient une connexion avec retry automatique
        Gère les cas où Hostinger bloque temporairement les connexions
        """
        
        # Limiter le nombre de connexions par minute (protection contre le rate limiting)
        if self.last_connection_time:
            time_since_last = time.time() - self.last_connection_time
            if time_since_last < 0.1:  # Pas plus de 10 connexions par seconde
                time.sleep(0.1 - time_since_last)
        
        try:
            # Essayer d'obtenir une connexion du pool
            if self.pool:
                try:
                    conn = self.pool.get_connection()
                    self.last_connection_time = time.time()
                    self.connection_count += 1
                    
                    # Vérifier que la connexion est valide
                    if not conn.is_connected():
                        conn.reconnect(attempts=3, delay=2)
                    
                    return conn
                    
                except Error as pool_error:
                    print(f"⚠️ Erreur pool: {pool_error}")
                    # Si le pool échoue, essayer une connexion directe
                    self.pool = None
            
            # Connexion directe si pas de pool ou si le pool a échoué
            conn = mysql.connector.connect(**self.config)
            self.last_connection_time = time.time()
            self.connection_count += 1
            
            return conn
            
        except mysql.connector.errors.DatabaseError as db_error:
            # Erreurs spécifiques à la base de données
            error_code = db_error.errno if hasattr(db_error, 'errno') else None
            
            # Codes d'erreur MySQL courants
            if error_code == 1040:  # Too many connections
                print("⚠️ Trop de connexions simultanées, attente...")
                time.sleep(5)
                
            elif error_code == 1045:  # Access denied
                print("❌ Accès refusé - vérifier les credentials")
                raise
                
            elif error_code == 2003:  # Can't connect to MySQL server
                print(f"⚠️ Serveur MySQL inaccessible, tentative {retry_count + 1}/{self.max_retries}")
                
            # Retry logic
            if retry_count < self.max_retries:
                time.sleep(self.retry_delay * (retry_count + 1))
                return self.get_connection(retry_count + 1)
            else:
                raise HTTPException(status_code=503, detail="Base de données temporairement indisponible")
                
        except Exception as e:
            print(f"❌ Erreur connexion inattendue: {e}")
            if retry_count < self.max_retries:
                time.sleep(self.retry_delay)
                return self.get_connection(retry_count + 1)
            else:
                raise
    
    def execute_query(self, query: str, params: tuple = None, fetch_all: bool = True):
        """
        Exécute une requête avec gestion automatique de la connexion
        Ferme toujours la connexion après usage pour éviter l'épuisement
        """
        
        conn = None
        cursor = None
        result = None
        
        try:
            # Obtenir une connexion
            conn = self.get_connection()
            cursor = conn.cursor(dictionary=True)
            
            # Exécuter la requête
            cursor.execute(query, params or ())
            
            # Récupérer les résultats si c'est un SELECT
            if query.strip().upper().startswith('SELECT'):
                if fetch_all:
                    result = cursor.fetchall()
                else:
                    result = cursor.fetchone()
            else:
                # Pour INSERT/UPDATE/DELETE, commiter les changements
                conn.commit()
                result = cursor.lastrowid if query.strip().upper().startswith('INSERT') else cursor.rowcount
            
            return result
            
        except mysql.connector.Error as err:
            print(f"❌ Erreur MySQL lors de l'exécution: {err}")
            if conn:
                conn.rollback()
            raise
            
        finally:
            # IMPORTANT : Toujours fermer les connexions pour Hostinger
            # Hostinger limite le nombre de connexions simultanées
            if cursor:
                cursor.close()
            if conn and conn.is_connected():
                conn.close()
            
            # Log pour debug
            if self.connection_count % 100 == 0:
                print(f"📊 {self.connection_count} connexions utilisées jusqu'à présent")

# Instance globale du gestionnaire de connexion
db_manager = MySQLConnectionManager(MYSQL_CONFIG)

# ==================== MODÈLES DE DONNÉES ====================

class MatchFeatures(BaseModel):
    """Features d'un match pour la prédiction"""
    home_form: float = 0.5
    away_form: float = 0.5
    h2h_home_wins: int = 0
    h2h_draws: int = 0
    h2h_away_wins: int = 0
    home_goals_avg: float = 1.5
    away_goals_avg: float = 1.2
    home_defense: float = 1.0
    away_defense: float = 1.3
    days_since_last_home: int = 3
    days_since_last_away: int = 3
    league_position_home: int = 10
    league_position_away: int = 12

class PredictionRequest(BaseModel):
    """Requête de prédiction"""
    match_id: int
    home_team: str
    away_team: str
    competition: str = "Unknown"
    features: Optional[MatchFeatures] = None
    odds_consensus: Optional[Dict] = None

# ==================== MODÈLE BAYESIAN OPTIMISÉ ====================

class BayesianPoissonModel:
    """
    Modèle Bayesian Hiérarchique optimisé pour économiser la mémoire
    Utilise les requêtes MySQL agrégées pour éviter de charger toutes les données
    """
    
    def __init__(self, alpha_prior=1.5, beta_prior=1.5):
        self.alpha_prior = alpha_prior
        self.beta_prior = beta_prior
        self.team_strengths = {}
        self.league_effects = {}
        self.home_advantage_global = 1.148  # Valeur historique moyenne
        self.is_trained = False
        self.training_date = None
        
    def fit_from_db(self):
        """
        Entraîne le modèle directement depuis MySQL
        Utilise des requêtes agrégées pour économiser la mémoire
        """
        
        print("📊 Début de l'entraînement du modèle Bayésien...")
        
        try:
            # Étape 1 : Calculer les effets par ligue
            print("📈 Calcul des effets par ligue...")
            
            query_leagues = """
            SELECT 
                competition_name,
                COUNT(*) as nb_matchs,
                AVG(home_score + away_score) as moy_buts_total,
                AVG(home_score) as moy_buts_domicile,
                AVG(away_score) as moy_buts_exterieur
            FROM matches
            WHERE home_score IS NOT NULL 
                AND away_score IS NOT NULL
                AND competition_name IS NOT NULL
            GROUP BY competition_name
            HAVING nb_matchs >= 10
            """
            
            leagues_data = db_manager.execute_query(query_leagues)
            
            if leagues_data:
                for league in leagues_data:
                    # Normaliser autour de 2.5 buts (moyenne mondiale)
                    facteur = league['moy_buts_total'] / 2.5 if league['moy_buts_total'] else 1.0
                    self.league_effects[league['competition_name']] = {
                        'facteur_buts': facteur,
                        'nb_matchs': league['nb_matchs']
                    }
                
                print(f"✅ {len(self.league_effects)} ligues analysées")
            
            # Étape 2 : Calculer les forces d'équipes
            print("⚽ Calcul des forces d'équipes...")
            
            # Requête optimisée qui calcule tout en une passe
            query_teams = """
            SELECT 
                team_name,
                SUM(matchs_joues) as total_matchs,
                SUM(buts_marques) as total_buts_pour,
                SUM(buts_encaisses) as total_buts_contre,
                AVG(buts_marques) as moy_buts_marques,
                AVG(buts_encaisses) as moy_buts_encaisses
            FROM (
                SELECT 
                    home_team as team_name,
                    COUNT(*) as matchs_joues,
                    SUM(home_score) as buts_marques,
                    SUM(away_score) as buts_encaisses
                FROM matches
                WHERE home_score IS NOT NULL
                GROUP BY home_team
                
                UNION ALL
                
                SELECT 
                    away_team as team_name,
                    COUNT(*) as matchs_joues,
                    SUM(away_score) as buts_marques,
                    SUM(home_score) as buts_encaisses
                FROM matches
                WHERE away_score IS NOT NULL
                GROUP BY away_team
            ) as stats
            GROUP BY team_name
            HAVING total_matchs >= 5
            """
            
            teams_data = db_manager.execute_query(query_teams)
            
            if teams_data:
                for team in teams_data:
                    n_matchs = int(team['total_matchs'])
                    
                    # Calculs Bayésiens
                    alpha_attaque = self.alpha_prior + float(team['total_buts_pour'] or 0)
                    beta_attaque = self.beta_prior + n_matchs
                    
                    alpha_defense = self.alpha_prior + float(team['total_buts_contre'] or 0)
                    beta_defense = self.beta_prior + n_matchs
                    
                    # Force avec shrinkage
                    confiance = min(1.0, n_matchs / 30)
                    
                    force_attaque = alpha_attaque / beta_attaque
                    force_defense = alpha_defense / beta_defense
                    
                    # Appliquer le shrinkage vers la moyenne
                    force_attaque = confiance * force_attaque + (1 - confiance) * 1.5
                    force_defense = confiance * force_defense + (1 - confiance) * 1.5
                    
                    self.team_strengths[team['team_name']] = {
                        'attack': round(force_attaque, 3),
                        'defense': round(force_defense, 3),
                        'matches': n_matchs,
                        'confidence': round(confiance, 3)
                    }
                
                print(f"✅ {len(self.team_strengths)} équipes analysées")
            
            self.is_trained = True
            self.training_date = datetime.now()
            print("✅ Modèle Bayésien entraîné avec succès!")
            
        except Exception as e:
            print(f"❌ Erreur lors de l'entraînement : {e}")
            self.is_trained = False
    
    def predict_match(self, home_team: str, away_team: str, competition: str = None):
        """
        Prédit un match avec le modèle Bayésien
        Retourne les probabilités et statistiques détaillées
        """
        
        # Récupérer les forces des équipes
        home_stats = self.team_strengths.get(home_team, {
            'attack': 1.5, 'defense': 1.5, 'confidence': 0
        })
        away_stats = self.team_strengths.get(away_team, {
            'attack': 1.3, 'defense': 1.3, 'confidence': 0
        })
        
        # Effet de ligue si disponible
        league_factor = 1.0
        if competition and competition in self.league_effects:
            league_factor = self.league_effects[competition]['facteur_buts']
        
        # Calculer les lambdas pour la distribution de Poisson
        home_lambda = home_stats['attack'] * away_stats['defense'] * self.home_advantage_global * league_factor
        away_lambda = away_stats['attack'] * home_stats['defense'] * league_factor
        
        # Limiter les valeurs pour éviter les calculs extrêmes
        home_lambda = min(max(home_lambda, 0.5), 5.0)
        away_lambda = min(max(away_lambda, 0.5), 5.0)
        
        # Calculer les probabilités pour différents scores
        max_goals = 6  # Limiter pour économiser les calculs
        
        home_win_prob = 0
        draw_prob = 0
        away_win_prob = 0
        
        for i in range(max_goals):
            for j in range(max_goals):
                prob = poisson.pmf(i, home_lambda) * poisson.pmf(j, away_lambda)
                if i > j:
                    home_win_prob += prob
                elif i == j:
                    draw_prob += prob
                else:
                    away_win_prob += prob
        
        # Statistiques additionnelles
        over_25 = 1 - sum([poisson.pmf(i, home_lambda) * poisson.pmf(j, away_lambda) 
                          for i in range(3) for j in range(3) if i + j <= 2])
        
        btts = 1 - (poisson.pmf(0, home_lambda) + poisson.pmf(0, away_lambda) - 
                   poisson.pmf(0, home_lambda) * poisson.pmf(0, away_lambda))
        
        # Score le plus probable
        most_likely_home = int(round(home_lambda))
        most_likely_away = int(round(away_lambda))
        
        # Calculer la confiance
        confidence = max([home_win_prob, draw_prob, away_win_prob])
        
        return {
            'probabilities': {
                'home': round(home_win_prob, 3),
                'draw': round(draw_prob, 3),
                'away': round(away_win_prob, 3)
            },
            'expected_goals': {
                'home': round(home_lambda, 2),
                'away': round(away_lambda, 2),
                'total': round(home_lambda + away_lambda, 2),
                'over_2.5': round(over_25, 3),
                'btts': round(btts, 3)
            },
            'most_likely_score': f"{most_likely_home}-{most_likely_away}",
            'confidence': round(confidence, 3),
            'model': 'bayesian_poisson'
        }

# ==================== INSTANCE GLOBALE DU PRÉDICTEUR ====================

predictor = BayesianPoissonModel()

# ==================== APPLICATION FASTAPI ====================

app = FastAPI(
    title="Football Prediction API",
    description="API de prédiction avec modèle Bayésien optimisé pour Render et Hostinger",
    version="2.2"
)

# Configuration CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ==================== ENDPOINTS ====================

@app.on_event("startup")
async def startup_event():
    """Initialisation au démarrage de l'API"""
    print("🚀 Démarrage de l'API Football Prediction...")
    print(f"📡 Configuration MySQL : {MYSQL_CONFIG['host']}/{MYSQL_CONFIG['database']}")
    
    # Tester la connexion MySQL
    try:
        # Test simple de connexion
        result = db_manager.execute_query("SELECT COUNT(*) as count FROM matches", fetch_all=False)
        
        if result:
            count = result['count']
            print(f"✅ Connexion MySQL OK - {count} matchs en base")
            
            # Entraîner le modèle si on a assez de données
            if count > 50:
                predictor.fit_from_db()
            else:
                print("⚠️ Pas assez de données pour entraîner le modèle")
        else:
            print("⚠️ La table matches semble vide")
            
    except Exception as e:
        print(f"❌ Erreur au démarrage : {e}")
        print("L'API continue mais sans modèle entraîné")
    
    print("✅ API prête à recevoir des requêtes!")

@app.get("/")
def root():
    """Point d'entrée principal - Informations sur l'API"""
    return {
        "status": "online",
        "service": "Football Prediction API",
        "version": "2.2",
        "model": {
            "type": "bayesian_poisson",
            "trained": predictor.is_trained,
            "teams_count": len(predictor.team_strengths),
            "leagues_count": len(predictor.league_effects)
        },
        "database": {
            "host": MYSQL_CONFIG['host'],
            "status": "configured"
        }
    }

@app.get("/health")
def health_check():
    """Health check pour Render - Vérifie l'état du service"""
    
    # Vérifier la connexion base de données
    db_status = "unknown"
    try:
        result = db_manager.execute_query("SELECT 1 as test", fetch_all=False)
        db_status = "connected" if result else "error"
    except:
        db_status = "disconnected"
    
    return {
        "status": "healthy",
        "database": db_status,
        "model_trained": predictor.is_trained,
        "memory_usage_mb": get_memory_usage()
    }

def get_memory_usage():
    """Obtenir l'utilisation mémoire actuelle en MB"""
    try:
        import resource
        usage = resource.getrusage(resource.RUSAGE_SELF)
        return round(usage.ru_maxrss / 1024, 2)  # Conversion en MB
    except:
        return 0

@app.post("/predict")
async def predict_match(request: PredictionRequest):
    """
    Endpoint principal de prédiction pour un match
    Accepte les données du match et retourne les probabilités
    """
    
    try:
        # Si le modèle n'est pas entraîné, le faire maintenant
        if not predictor.is_trained:
            print("⚠️ Modèle non entraîné, entraînement en cours...")
            predictor.fit_from_db()
            
            if not predictor.is_trained:
                raise HTTPException(
                    status_code=503,
                    detail="Modèle non disponible - pas assez de données d'entraînement"
                )
        
        # Faire la prédiction
        prediction = predictor.predict_match(
            request.home_team,
            request.away_team,
            request.competition
        )
        
        # Ajouter les informations du match
        prediction['match_info'] = {
            'match_id': request.match_id,
            'home_team': request.home_team,
            'away_team': request.away_team,
            'competition': request.competition
        }
        
        # Déterminer le résultat le plus probable
        probs = prediction['probabilities']
        if probs['home'] > probs['draw'] and probs['home'] > probs['away']:
            prediction['predicted_outcome'] = 'HOME'
        elif probs['draw'] > probs['away']:
            prediction['predicted_outcome'] = 'DRAW'
        else:
            prediction['predicted_outcome'] = 'AWAY'
        
        # Optionnel : Sauvegarder la prédiction en base
        try:
            query = """
            INSERT INTO predictions 
            (match_id, home_win_prob, draw_prob, away_win_prob, confidence, created_at)
            VALUES (%s, %s, %s, %s, %s, NOW())
            ON DUPLICATE KEY UPDATE
            home_win_prob = VALUES(home_win_prob),
            draw_prob = VALUES(draw_prob),
            away_win_prob = VALUES(away_win_prob),
            confidence = VALUES(confidence),
            updated_at = NOW()
            """
            
            db_manager.execute_query(query, (
                request.match_id,
                probs['home'],
                probs['draw'],
                probs['away'],
                prediction['confidence']
            ))
            
            prediction['saved'] = True
            
        except Exception as save_error:
            print(f"⚠️ Impossible de sauvegarder la prédiction : {save_error}")
            prediction['saved'] = False
        
        return prediction
        
    except Exception as e:
        print(f"❌ Erreur lors de la prédiction : {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/model/retrain")
async def retrain_model():
    """
    Force le réentraînement du modèle avec les dernières données
    Utile après l'ajout de nouveaux matchs
    """
    
    try:
        predictor.fit_from_db()
        
        if predictor.is_trained:
            return {
                "status": "success",
                "message": "Modèle réentraîné avec succès",
                "teams_count": len(predictor.team_strengths),
                "leagues_count": len(predictor.league_effects)
            }
        else:
            raise HTTPException(
                status_code=500,
                detail="Échec du réentraînement - vérifier les données"
            )
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/model/info")
def get_model_info():
    """
    Retourne des informations détaillées sur le modèle entraîné
    Utile pour le debug et la compréhension du système
    """
    
    if not predictor.is_trained:
        return {"status": "not_trained", "message": "Le modèle n'est pas encore entraîné"}
    
    # Top 5 équipes par force d'attaque
    top_attack = sorted(
        predictor.team_strengths.items(),
        key=lambda x: x[1]['attack'],
        reverse=True
    )[:5]
    
    # Top 5 équipes par force défensive (plus bas = meilleur)
    top_defense = sorted(
        predictor.team_strengths.items(),
        key=lambda x: x[1]['defense']
    )[:5]
    
    return {
        "model_status": {
            "trained": predictor.is_trained,
            "training_date": predictor.training_date.isoformat() if predictor.training_date else None,
            "total_teams": len(predictor.team_strengths),
            "total_leagues": len(predictor.league_effects)
        },
        "top_attack_teams": [
            {"team": team, "attack": stats['attack'], "confidence": stats['confidence']}
            for team, stats in top_attack
        ],
        "top_defense_teams": [
            {"team": team, "defense": stats['defense'], "confidence": stats['confidence']}
            for team, stats in top_defense
        ],
        "leagues": list(predictor.league_effects.keys())
    }

# ==================== LANCEMENT DE L'APPLICATION ====================

if __name__ == "__main__":
    # Utiliser le port fourni par Render (ou 8000 en local)
    uvicorn.run(app, host="0.0.0.0", port=PORT)
