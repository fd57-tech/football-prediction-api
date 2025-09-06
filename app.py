import os
import json
import requests
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import mysql.connector
from mysql.connector import pooling

# FastAPI et dépendances
from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import uvicorn

# Machine Learning (sans pandas ni sklearn pour économiser la mémoire)
from scipy.stats import poisson
import pickle

# Configuration MySQL depuis Hostinger
MYSQL_CONFIG = {
    'host': 'srv1043.hstgr.io',
    'database': 'u827503784_appdevfoot',
    'user': 'u827503784_BnFnX7',
    'password': 'TestApFootProno7+',
    'port': 3306,
    'raise_on_warnings': False,
    'use_pure': True,  # Important pour la compatibilité
    'autocommit': True,
    'pool_size': 3,  # Limiter les connexions pour économiser la mémoire
    'pool_name': 'mypool'
}

# Configuration depuis les variables d'environnement
PHP_BRIDGE_URL = os.getenv('PHP_BRIDGE_URL', 'https://appdevfoot.leselixirsdedamenature.fr/api_bridge_enhanced.php')
PHP_SECRET = os.getenv('PHP_SECRET', 'TonSecret2024')
PORT = int(os.getenv('PORT', 8000))

# Pool de connexions MySQL (plus efficace)
try:
    connection_pool = mysql.connector.pooling.MySQLConnectionPool(**MYSQL_CONFIG)
    print("✅ Pool de connexions MySQL créé avec succès")
except Exception as e:
    print(f"❌ Erreur création pool MySQL: {e}")
    connection_pool = None

# Créer l'application FastAPI
app = FastAPI(
    title="Football Prediction API",
    description="API de prédiction avec modèle Bayesian optimisé pour Render",
    version="2.1"
)

# Configuration CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

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

# ==================== FONCTIONS UTILITAIRES MYSQL ====================

def get_db_connection():
    """Obtenir une connexion depuis le pool"""
    if connection_pool:
        try:
            return connection_pool.get_connection()
        except Exception as e:
            print(f"Erreur obtention connexion: {e}")
            # Tenter une connexion directe en fallback
            return mysql.connector.connect(**{k: v for k, v in MYSQL_CONFIG.items() 
                                           if k not in ['pool_size', 'pool_name']})
    else:
        # Si pas de pool, connexion directe
        return mysql.connector.connect(**{k: v for k, v in MYSQL_CONFIG.items() 
                                       if k not in ['pool_size', 'pool_name']})

def execute_query(query: str, params: tuple = None, fetch_all: bool = True):
    """Exécuter une requête MySQL de manière optimisée"""
    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        cursor.execute(query, params)
        
        if fetch_all:
            result = cursor.fetchall()
        else:
            result = cursor.fetchone()
        
        return result
        
    except Exception as e:
        print(f"Erreur requête MySQL: {e}")
        return None
        
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

# ==================== MODÈLE BAYESIAN OPTIMISÉ ====================

class BayesianPoissonModelLite:
    """
    Version allégée du modèle Bayesian pour économiser la mémoire
    Utilise uniquement numpy et scipy (pas de pandas)
    """
    
    def __init__(self, alpha_prior=1.5, beta_prior=1.5):
        self.alpha_prior = alpha_prior
        self.beta_prior = beta_prior
        self.team_strengths = {}
        self.league_effects = {}
        
    def fit_from_db(self):
        """
        Entraîne le modèle directement depuis la base MySQL
        Plus efficace en mémoire que de charger toutes les données
        """
        print("📊 Entraînement du modèle depuis MySQL...")
        
        # Récupérer les statistiques agrégées par équipe (plus efficace)
        query_teams = """
        SELECT 
            team_name,
            COUNT(*) as matches_played,
            AVG(goals_scored) as avg_goals_scored,
            AVG(goals_conceded) as avg_goals_conceded,
            AVG(CASE WHEN is_home = 1 THEN goals_scored ELSE goals_conceded END) as home_performance,
            AVG(CASE WHEN is_home = 0 THEN goals_scored ELSE goals_conceded END) as away_performance
        FROM (
            SELECT 
                home_team as team_name,
                home_score as goals_scored,
                away_score as goals_conceded,
                1 as is_home
            FROM matches
            WHERE home_score IS NOT NULL
            UNION ALL
            SELECT 
                away_team as team_name,
                away_score as goals_scored,
                home_score as goals_conceded,
                0 as is_home
            FROM matches
            WHERE away_score IS NOT NULL
        ) as team_stats
        GROUP BY team_name
        HAVING matches_played >= 5
        """
        
        teams_data = execute_query(query_teams)
        
        if teams_data:
            for team in teams_data:
                n_matches = team['matches_played']
                
                # Mise à jour Bayésienne
                alpha_attack = self.alpha_prior + team['avg_goals_scored'] * n_matches
                beta_attack = self.beta_prior + n_matches
                
                alpha_defense = self.alpha_prior + team['avg_goals_conceded'] * n_matches
                beta_defense = self.beta_prior + n_matches
                
                # Facteur de confiance
                confidence = min(1.0, n_matches / 20)
                
                # Force avec shrinkage
                attack_strength = (alpha_attack / beta_attack) * confidence + 1.5 * (1 - confidence)
                defense_strength = (alpha_defense / beta_defense) * confidence + 1.5 * (1 - confidence)
                
                self.team_strengths[team['team_name']] = {
                    'attack': attack_strength,
                    'defense': defense_strength,
                    'matches': n_matches,
                    'confidence': confidence
                }
            
            print(f"✅ Modèle entraîné sur {len(self.team_strengths)} équipes")
        else:
            print("⚠️ Aucune donnée d'entraînement disponible")
        
        # Récupérer les effets de ligue
        query_leagues = """
        SELECT 
            competition_name,
            AVG(home_score + away_score) as avg_total_goals,
            COUNT(*) as matches_count
        FROM matches
        WHERE competition_name IS NOT NULL
        GROUP BY competition_name
        HAVING matches_count >= 20
        """
        
        leagues_data = execute_query(query_leagues)
        
        if leagues_data:
            for league in leagues_data:
                # Normaliser autour de 2.5 buts (moyenne standard)
                self.league_effects[league['competition_name']] = league['avg_total_goals'] / 2.5
    
    def predict_match(self, home_team: str, away_team: str, competition: str = None):
        """
        Prédit un match avec le modèle Bayesian
        """
        # Récupérer les forces des équipes ou utiliser les valeurs par défaut
        home_stats = self.team_strengths.get(home_team, {
            'attack': 1.5, 'defense': 1.5, 'confidence': 0
        })
        away_stats = self.team_strengths.get(away_team, {
            'attack': 1.3, 'defense': 1.3, 'confidence': 0
        })
        
        # Effet de ligue
        league_factor = self.league_effects.get(competition, 1.0) if competition else 1.0
        
        # Lambda pour Poisson (avec avantage domicile)
        home_lambda = home_stats['attack'] * away_stats['defense'] * 1.148 * league_factor
        away_lambda = away_stats['attack'] * home_stats['defense'] * 0.87 * league_factor
        
        # Limiter les lambdas pour éviter les débordements
        home_lambda = min(home_lambda, 5.0)
        away_lambda = min(away_lambda, 5.0)
        
        # Calculer les probabilités de manière efficace
        max_goals = 6  # Réduire pour économiser la mémoire
        
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
        
        # Calculer les statistiques supplémentaires
        over_25 = 1 - sum([poisson.pmf(i, home_lambda) * poisson.pmf(j, away_lambda) 
                          for i in range(3) for j in range(3) if i + j <= 2])
        
        btts = 1 - (poisson.pmf(0, home_lambda) + poisson.pmf(0, away_lambda) - 
                   poisson.pmf(0, home_lambda) * poisson.pmf(0, away_lambda))
        
        # Score le plus probable
        most_likely_home = int(home_lambda)
        most_likely_away = int(away_lambda)
        
        # Calculer la confiance
        confidence = max([home_win_prob, draw_prob, away_win_prob])
        if home_team in self.team_strengths and away_team in self.team_strengths:
            confidence = confidence * 0.6 + min(home_stats['confidence'], away_stats['confidence']) * 0.4
        
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
            'model': 'bayesian_lite'
        }

# ==================== INSTANCE GLOBALE ====================

predictor = BayesianPoissonModelLite()

# ==================== ENDPOINTS DE L'API ====================

@app.on_event("startup")
async def startup_event():
    """
    Initialisation au démarrage
    """
    print("🚀 Démarrage de l'API Football Prediction...")
    print(f"📡 Configuration MySQL: {MYSQL_CONFIG['host']}/{MYSQL_CONFIG['database']}")
    
    # Tester la connexion MySQL
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM matches")
        count = cursor.fetchone()[0]
        cursor.close()
        conn.close()
        print(f"✅ Connexion MySQL OK - {count} matchs en base")
        
        # Entraîner le modèle si des données sont disponibles
        if count > 100:
            predictor.fit_from_db()
        else:
            print("⚠️ Pas assez de données pour l'entraînement")
            
    except Exception as e:
        print(f"❌ Erreur connexion MySQL: {e}")
    
    print("✅ API prête!")

@app.get("/")
def root():
    """
    Point d'entrée principal
    """
    return {
        "status": "online",
        "service": "Football Prediction API",
        "version": "2.1",
        "model": {
            "type": "bayesian_lite",
            "teams_count": len(predictor.team_strengths),
            "leagues_count": len(predictor.league_effects)
        }
    }

@app.get("/health")
def health_check():
    """
    Health check pour Render
    """
    # Vérifier la connexion MySQL
    db_status = "unknown"
    try:
        conn = get_db_connection()
        conn.close()
        db_status = "connected"
    except:
        db_status = "disconnected"
    
    return {
        "status": "healthy",
        "database": db_status,
        "memory_usage_mb": get_memory_usage()
    }

def get_memory_usage():
    """
    Obtenir l'utilisation mémoire actuelle
    """
    try:
        import resource
        usage = resource.getrusage(resource.RUSAGE_SELF)
        return round(usage.ru_maxrss / 1024, 2)  # En MB
    except:
        return 0

@app.post("/predict")
async def predict_match(request: PredictionRequest):
    """
    Prédire un match
    """
    try:
        # Si le modèle n'est pas entraîné, le faire maintenant
        if not predictor.team_strengths:
            predictor.fit_from_db()
        
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
        
        # Sauvegarder en base si possible
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
            
            conn = get_db_connection()
            cursor = conn.cursor()
            cursor.execute(query, (
                request.match_id,
                probs['home'],
                probs['draw'],
                probs['away'],
                prediction['confidence']
            ))
            conn.commit()
            cursor.close()
            conn.close()
            
            prediction['saved'] = True
            
        except Exception as e:
            print(f"Erreur sauvegarde: {e}")
            prediction['saved'] = False
        
        return prediction
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/predict/batch")
async def predict_batch():
    """
    Prédire les matchs à venir
    """
    try:
        # Récupérer les matchs des 48 prochaines heures
        query = """
        SELECT 
            match_id,
            home_team,
            away_team,
            competition_name
        FROM matches
        WHERE match_date BETWEEN NOW() AND DATE_ADD(NOW(), INTERVAL 48 HOUR)
        AND home_score IS NULL
        """
        
        matches = execute_query(query)
        
        if not matches:
            return {"message": "Aucun match à venir"}
        
        predictions = []
        for match in matches:
            try:
                pred = predictor.predict_match(
                    match['home_team'],
                    match['away_team'],
                    match['competition_name']
                )
                pred['match_id'] = match['match_id']
                predictions.append(pred)
            except Exception as e:
                print(f"Erreur prédiction match {match['match_id']}: {e}")
        
        return {
            "status": "success",
            "predictions_count": len(predictions),
            "predictions": predictions
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/model/info")
def get_model_info():
    """
    Informations sur le modèle
    """
    # Top équipes par force d'attaque
    top_attack = sorted(
        predictor.team_strengths.items(),
        key=lambda x: x[1]['attack'],
        reverse=True
    )[:10]
    
    # Top équipes par défense (plus bas = meilleur)
    top_defense = sorted(
        predictor.team_strengths.items(),
        key=lambda x: x[1]['defense']
    )[:10]
    
    return {
        "model": "bayesian_lite",
        "statistics": {
            "total_teams": len(predictor.team_strengths),
            "total_leagues": len(predictor.league_effects)
        },
        "top_attack": [
            {"team": team, "strength": round(stats['attack'], 2)}
            for team, stats in top_attack
        ],
        "top_defense": [
            {"team": team, "strength": round(stats['defense'], 2)}
            for team, stats in top_defense
        ]
    }

@app.post("/model/retrain")
async def retrain_model():
    """
    Réentraîner le modèle
    """
    try:
        predictor.fit_from_db()
        return {"status": "success", "teams_trained": len(predictor.team_strengths)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=PORT)

