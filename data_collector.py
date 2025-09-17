#!/usr/bin/env python3
"""
Data Collector Module - Collecte xG depuis APIs
Compatible avec votre système existant
"""

import logging
import time
import requests
import mysql.connector
from datetime import datetime, timedelta
from typing import Dict, List, Optional

# Configuration logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration des APIs
API_CONFIGS = {
    'sportmonks': {
        'token': '3pdMlmrvFC9uNETfPz9QgitBJPrq3VckpE9ZS4dIhaQhIuVJIAYZG0gpCxX2',
        'base_url': 'https://api.sportmonks.com/v3/football',
        'rate_limit': 3
    },
    'api_football': {
        'token': '21fcb087a7ef3a26a4bb8e2e4372657c',
        'base_url': 'https://v3.football.api-sports.io',
        'rate_limit': 1.7
    },
    'football_data': {
        'token': '331cdc93465a41519ebfedd40a983d8a',
        'base_url': 'https://api.football-data.org/v4',
        'rate_limit': 0.17
    }
}

# Mapping des ligues (compatible avec vos codes existants)
LEAGUE_MAPPINGS = {
    'E0': {'sportmonks': 8, 'api_football': 39, 'football_data': 'PL', 'name': 'Premier League'},
    'SP1': {'sportmonks': 564, 'api_football': 140, 'football_data': 'PD', 'name': 'La Liga'},
    'D1': {'sportmonks': 82, 'api_football': 78, 'football_data': 'BL1', 'name': 'Bundesliga'},
    'I1': {'sportmonks': 384, 'api_football': 135, 'football_data': 'SA', 'name': 'Serie A'},
    'F1': {'sportmonks': 301, 'api_football': 61, 'football_data': 'FL1', 'name': 'Ligue 1'}
}

class XGCollector:
    """Collecteur de données xG depuis les APIs"""
    
    def __init__(self, db_config: Dict):
        self.db_config = db_config
        self.session = requests.Session()
        self.stats = {
            'xg_collected': 0,
            'scores_collected': 0,
            'api_calls': 0,
            'errors': 0
        }
    
    def get_connection(self):
        """Créer une connexion DB"""
        return mysql.connector.connect(**self.db_config)
    
    def collect_all(self, leagues: Optional[List[str]] = None, limit: int = 1000) -> Dict:
        """
        Collecter toutes les données manquantes
        
        Args:
            leagues: Liste des ligues à traiter (None = toutes)
            limit: Nombre max de matchs à traiter
            
        Returns:
            Dictionnaire avec les statistiques de collecte
        """
        logger.info("Démarrage de la collecte de données xG")
        
        # Corriger d'abord les problèmes de données
        self._fix_data_issues()
        
        # Collecter xG manquants
        self._collect_missing_xg(leagues, limit)
        
        # Collecter scores manquants
        self._collect_missing_scores(leagues, limit)
        
        return self.stats
    
    def _fix_data_issues(self):
        """Corriger les dates et données erronées"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            # Corriger dates futures
            cursor.execute("""
                UPDATE matches 
                SET match_date = DATE_SUB(match_date, INTERVAL 1 YEAR)
                WHERE YEAR(match_date) > 2024
            """)
            dates_fixed = cursor.rowcount
            
            # Supprimer matchs test
            cursor.execute("""
                DELETE FROM matches 
                WHERE home_team LIKE '%Test%' OR away_team LIKE '%Test%'
            """)
            test_deleted = cursor.rowcount
            
            conn.commit()
            logger.info(f"Corrections: {dates_fixed} dates, {test_deleted} matchs test")
            
        except Exception as e:
            logger.error(f"Erreur correction données: {e}")
            conn.rollback()
        finally:
            cursor.close()
            conn.close()
    
    def _collect_missing_xg(self, leagues: Optional[List[str]], limit: int):
        """Collecter les xG manquants depuis les APIs"""
        
        conn = self.get_connection()
        cursor = conn.cursor(dictionary=True)
        
        # Construire la requête SQL
        sql = """
            SELECT id, league, match_date, home_team, away_team
            FROM matches
            WHERE home_xg IS NULL
            AND match_date < CURDATE()
        """
        
        if leagues:
            placeholders = ','.join(['%s'] * len(leagues))
            sql += f" AND league IN ({placeholders})"
            cursor.execute(sql + f" LIMIT {limit}", leagues)
        else:
            cursor.execute(sql + f" LIMIT {limit}")
        
        matches = cursor.fetchall()
        logger.info(f"Traitement de {len(matches)} matchs sans xG")
        
        # Essayer chaque API
        for match in matches:
            success = False
            
            # Essayer Sportmonks
            if not success and match['league'] in LEAGUE_MAPPINGS:
                success = self._try_sportmonks(match, cursor)
            
            # Essayer API-Football
            if not success and match['league'] in LEAGUE_MAPPINGS:
                success = self._try_api_football(match, cursor)
            
            # Si aucune API n'a fonctionné, calculer depuis les scores
            if not success:
                self._calculate_xg_from_score(match, cursor)
            
            conn.commit()
        
        cursor.close()
        conn.close()
    
    def _try_sportmonks(self, match: Dict, cursor) -> bool:
        """Essayer de récupérer xG depuis Sportmonks"""
        
        if match['league'] not in LEAGUE_MAPPINGS:
            return False
            
        league_id = LEAGUE_MAPPINGS[match['league']].get('sportmonks')
        if not league_id:
            return False
        
        date_str = match['match_date'].strftime('%Y-%m-%d')
        url = f"{API_CONFIGS['sportmonks']['base_url']}/fixtures/date/{date_str}"
        params = {
            'api_token': API_CONFIGS['sportmonks']['token'],
            'include': 'statistics.details',
            'filters[league_ids]': league_id
        }
        
        try:
            response = self.session.get(url, params=params, timeout=10)
            self.stats['api_calls'] += 1
            
            if response.status_code == 200:
                data = response.json()
                
                # Parser la réponse pour trouver le match
                for fixture in data.get('data', []):
                    # Essayer d'extraire xG
                    if 'statistics' in fixture:
                        home_xg = self._extract_xg_sportmonks(fixture['statistics'], 'home')
                        away_xg = self._extract_xg_sportmonks(fixture['statistics'], 'away')
                        
                        if home_xg is not None and away_xg is not None:
                            cursor.execute("""
                                UPDATE matches 
                                SET home_xg = %s, away_xg = %s, xg_source = 'sportmonks'
                                WHERE id = %s
                            """, (home_xg, away_xg, match['id']))
                            
                            if cursor.rowcount > 0:
                                self.stats['xg_collected'] += 1
                                logger.debug(f"xG récupéré pour match {match['id']}")
                                return True
            
            time.sleep(1 / API_CONFIGS['sportmonks']['rate_limit'])
            
        except Exception as e:
            logger.error(f"Erreur Sportmonks: {e}")
            self.stats['errors'] += 1
        
        return False
    
    def _try_api_football(self, match: Dict, cursor) -> bool:
        """Essayer de récupérer xG depuis API-Football"""
        
        if match['league'] not in LEAGUE_MAPPINGS:
            return False
            
        league_id = LEAGUE_MAPPINGS[match['league']].get('api_football')
        if not league_id:
            return False
        
        url = f"{API_CONFIGS['api_football']['base_url']}/fixtures"
        params = {
            'league': league_id,
            'season': match['match_date'].year,
            'date': match['match_date'].strftime('%Y-%m-%d')
        }
        headers = {
            'x-apisports-key': API_CONFIGS['api_football']['token']
        }
        
        try:
            response = self.session.get(url, params=params, headers=headers, timeout=10)
            self.stats['api_calls'] += 1
            
            if response.status_code == 200:
                data = response.json()
                
                for fixture in data.get('response', []):
                    if 'statistics' in fixture and len(fixture['statistics']) >= 2:
                        # Chercher xG dans les stats
                        home_xg = self._extract_xg_api_football(fixture['statistics'][0])
                        away_xg = self._extract_xg_api_football(fixture['statistics'][1])
                        
                        if home_xg is not None and away_xg is not None:
                            cursor.execute("""
                                UPDATE matches 
                                SET home_xg = %s, away_xg = %s, xg_source = 'api_football'
                                WHERE id = %s
                            """, (home_xg, away_xg, match['id']))
                            
                            if cursor.rowcount > 0:
                                self.stats['xg_collected'] += 1
                                return True
            
            time.sleep(1 / API_CONFIGS['api_football']['rate_limit'])
            
        except Exception as e:
            logger.error(f"Erreur API-Football: {e}")
            self.stats['errors'] += 1
        
        return False
    
    def _calculate_xg_from_score(self, match: Dict, cursor):
        """Calculer xG approximatif depuis les scores"""
        
        cursor.execute("""
            UPDATE matches 
            SET 
                home_xg = CASE 
                    WHEN home_score IS NOT NULL 
                    THEN home_score * 0.9 + 0.1
                    ELSE NULL
                END,
                away_xg = CASE 
                    WHEN away_score IS NOT NULL 
                    THEN away_score * 0.9 + 0.1
                    ELSE NULL
                END,
                xg_source = 'calculated'
            WHERE id = %s
            AND home_xg IS NULL
            AND home_score IS NOT NULL
        """, (match['id'],))
        
        if cursor.rowcount > 0:
            self.stats['xg_collected'] += 1
    
    def _collect_missing_scores(self, leagues: Optional[List[str]], limit: int):
        """Collecter les scores manquants"""
        
        conn = self.get_connection()
        cursor = conn.cursor(dictionary=True)
        
        sql = """
            SELECT id, league, match_date
            FROM matches
            WHERE home_score IS NULL
            AND match_date < CURDATE()
        """
        
        if leagues:
            placeholders = ','.join(['%s'] * len(leagues))
            sql += f" AND league IN ({placeholders})"
            cursor.execute(sql + f" LIMIT {limit}", leagues)
        else:
            cursor.execute(sql + f" LIMIT {limit}")
        
        matches = cursor.fetchall()
        
        if matches:
            logger.info(f"Traitement de {len(matches)} matchs sans scores")
            # Utiliser football-data.org pour les scores
            self._collect_scores_football_data(matches, cursor)
            conn.commit()
        
        cursor.close()
        conn.close()
    
    def _collect_scores_football_data(self, matches: List[Dict], cursor):
        """Récupérer les scores depuis football-data.org"""
        
        for match in matches:
            if match['league'] not in LEAGUE_MAPPINGS:
                continue
            
            league_code = LEAGUE_MAPPINGS[match['league']].get('football_data')
            if not league_code:
                continue
            
            year = match['match_date'].year
            url = f"{API_CONFIGS['football_data']['base_url']}/competitions/{league_code}/matches"
            params = {'season': year}
            headers = {'X-Auth-Token': API_CONFIGS['football_data']['token']}
            
            try:
                response = self.session.get(url, params=params, headers=headers, timeout=10)
                self.stats['api_calls'] += 1
                
                if response.status_code == 200:
                    data = response.json()
                    
                    for api_match in data.get('matches', []):
                        if api_match['status'] == 'FINISHED':
                            match_date = api_match['utcDate'][:10]
                            
                            # Comparer les dates
                            if match_date == match['match_date'].strftime('%Y-%m-%d'):
                                cursor.execute("""
                                    UPDATE matches 
                                    SET home_score = %s, away_score = %s
                                    WHERE id = %s
                                """, (
                                    api_match['score']['fullTime']['home'],
                                    api_match['score']['fullTime']['away'],
                                    match['id']
                                ))
                                
                                if cursor.rowcount > 0:
                                    self.stats['scores_collected'] += 1
                                    break
                
                time.sleep(6)  # Rate limit strict pour football-data
                
            except Exception as e:
                logger.error(f"Erreur football-data: {e}")
                self.stats['errors'] += 1
    
    def _extract_xg_sportmonks(self, statistics: List, team: str) -> Optional[float]:
        """Extraire xG depuis les stats Sportmonks"""
        # Logique d'extraction spécifique à Sportmonks
        # À adapter selon la structure exacte de leur réponse
        return None
    
    def _extract_xg_api_football(self, team_stats: Dict) -> Optional[float]:
        """Extraire xG depuis les stats API-Football"""
        if 'statistics' in team_stats:
            for stat in team_stats['statistics']:
                if stat.get('type') == 'Expected Goals':
                    try:
                        return float(stat.get('value', 0))
                    except:
                        pass
        return None


def collect_data_standalone(leagues: Optional[List[str]] = None, limit: int = 1000) -> Dict:
    """
    Fonction autonome pour lancer la collecte
    
    Args:
        leagues: Liste des codes de ligue (None = toutes)
        limit: Nombre max de matchs à traiter
        
    Returns:
        Statistiques de collecte
    """
    from app import DB_CONFIG  # Importer la config depuis app.py
    
    collector = XGCollector(DB_CONFIG)
    return collector.collect_all(leagues, limit)


if __name__ == '__main__':
    # Pour test en standalone
    import sys
    
    # Config DB pour test
    db_config = {
        'host': 'srv1043.hstgr.io',
        'database': 'u827503784_appdevfoot',
        'user': 'u827503784_BnFnX7',
        'password': 'TestApFootProno7+',
        'port': 3306
    }
    
    print("Démarrage de la collecte de données xG...")
    collector = XGCollector(db_config)
    
    # Collecter pour les 5 grandes ligues, max 100 matchs
    stats = collector.collect_all(['E0', 'SP1', 'D1', 'I1', 'F1'], limit=100)
    
    print(f"\nRésultats:")
    print(f"- xG collectés: {stats['xg_collected']}")
    print(f"- Scores collectés: {stats['scores_collected']}")
    print(f"- Appels API: {stats['api_calls']}")
    print(f"- Erreurs: {stats['errors']}")
