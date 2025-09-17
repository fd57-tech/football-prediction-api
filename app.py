import os
from flask import Flask, jsonify, request, send_file
from flask_cors import CORS
import mysql.connector
from datetime import datetime
import threading

# Try to import the scraper utilities.  These are optional: if the
# module isn't present, the /scrape endpoint will return an error.
try:
    from scraper_fbref import scrape_all, LEAGUE_META  # type: ignore
except ImportError:
    scrape_all = None
    LEAGUE_META = {}

# Import du nouveau collecteur de données
try:
    from data_collector import XGCollector
except ImportError:
    XGCollector = None


app = Flask(__name__)
CORS(app)

# Configuration de la base de données
DB_CONFIG = {
    'host': 'srv1043.hstgr.io',
    'database': 'u827503784_appdevfoot',
    'user': 'u827503784_BnFnX7',
    'password': 'TestApFootProno7+',
    'port': 3306,
    'autocommit': True,
    'pool_size': 3,
    'pool_reset_session': True
}

# État du collecteur
collector_status = {
    'running': False,
    'last_run': None,
    'stats': {}
}


def get_db_connection():
    """Crée une connexion sécurisée à la base de données"""
    try:
        return mysql.connector.connect(**DB_CONFIG)
    except Exception as e:
        print(f"Erreur connexion DB: {e}")
        return None


@app.route('/')
def home():
    """Point d'entrée principal de l'API"""
    return jsonify({
        'status': 'online',
        'api': 'Football Prediction System',
        'version': '2.1',
        'endpoints': {
            '/health': 'Vérification santé système',
            '/stats': 'Statistiques globales',
            '/stats/detailed': 'Statistiques détaillées (NOUVEAU)',
            '/matches/recent': 'Matchs récents',
            '/predict/<match_id>': 'Prédiction pour un match',
            '/scrape': 'Scrape FBref fixtures (POST)',
            '/collect': 'Collecter xG depuis APIs (POST) - NOUVEAU',
            '/collect/status': 'Statut de la collecte (GET) - NOUVEAU',
            '/export': 'Exporter données ML (GET) - NOUVEAU'
        }
    })


@app.route('/health')
def health():
    """Vérifie que tout fonctionne"""
    db_status = 'offline'
    tables_count = 0

    conn = get_db_connection()
    if conn:
        try:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT COUNT(*) FROM information_schema.tables "
                "WHERE table_schema = 'u827503784_appdevfoot'"
            )
            tables_count = cursor.fetchone()[0]
            db_status = 'online'
            cursor.close()
            conn.close()
        except Exception:
            pass

    return jsonify({
        'api': 'healthy',
        'database': db_status,
        'tables': tables_count,
        'timestamp': datetime.now().isoformat()
    })


@app.route('/stats')
def get_stats():
    """Retourne les statistiques de votre système"""
    conn = get_db_connection()
    if not conn:
        return jsonify({'error': 'Database connection failed'}), 500

    try:
        cursor = conn.cursor(dictionary=True)
        # Récupération des stats essentielles
        stats_queries = {
            'total_matches': "SELECT COUNT(*) as count FROM matches",
            'matches_with_xg': "SELECT COUNT(*) as count FROM matches WHERE home_xg IS NOT NULL",
            'matches_with_scores': "SELECT COUNT(*) as count FROM matches WHERE home_score IS NOT NULL",
            'total_predictions': "SELECT COUNT(*) as count FROM predictions IF EXISTS",
            'teams': "SELECT COUNT(DISTINCT home_team) as count FROM matches"
        }

        results = {}
        for key, query in stats_queries.items():
            try:
                cursor.execute(query)
                results[key] = cursor.fetchone()['count']
            except:
                results[key] = 0

        cursor.close()
        conn.close()

        return jsonify({
            'success': True,
            'stats': results,
            'quality_indicators': {
                'xg_coverage': f"{(results.get('matches_with_xg', 0) / max(results.get('total_matches', 1), 1)) * 100:.1f}%",
                'score_coverage': f"{(results.get('matches_with_scores', 0) / max(results.get('total_matches', 1), 1)) * 100:.1f}%"
            }
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/stats/detailed')
def get_detailed_stats():
    """Statistiques détaillées par source et ligue"""
    conn = get_db_connection()
    if not conn:
        return jsonify({'error': 'Database connection failed'}), 500

    try:
        cursor = conn.cursor(dictionary=True)
        
        # Stats par source xG
        cursor.execute("""
            SELECT xg_source, COUNT(*) as count
            FROM matches
            WHERE xg_source IS NOT NULL
            GROUP BY xg_source
            ORDER BY count DESC
        """)
        by_source = cursor.fetchall()
        
        # Stats par ligue
        cursor.execute("""
            SELECT 
                league,
                COUNT(*) as total,
                COUNT(home_xg) as with_xg,
                COUNT(home_score) as with_scores,
                ROUND(100 * COUNT(home_xg) / COUNT(*), 1) as xg_percentage
            FROM matches
            GROUP BY league
            ORDER BY total DESC
        """)
        by_league = cursor.fetchall()
        
        # Matchs sans xG
        cursor.execute("""
            SELECT COUNT(*) as count
            FROM matches
            WHERE home_xg IS NULL
            AND match_date < CURDATE()
        """)
        missing_xg = cursor.fetchone()['count']
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'by_source': by_source,
            'by_league': by_league,
            'missing_xg': missing_xg,
            'timestamp': datetime.now().isoformat()
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/matches/recent')
def get_recent_matches():
    """Récupère les 20 derniers matchs avec leurs prédictions"""
    conn = get_db_connection()
    if not conn:
        return jsonify({'error': 'Database connection failed'}), 500

    try:
        cursor = conn.cursor(dictionary=True)
        cursor.execute(
            """
                SELECT 
                    m.id,
                    m.home_team,
                    m.away_team,
                    m.home_score,
                    m.away_score,
                    m.home_xg,
                    m.away_xg,
                    m.xg_source,
                    DATE_FORMAT(m.match_date, '%Y-%m-%d %H:%i') as match_date,
                    m.status
                FROM matches m
                ORDER BY m.match_date DESC
                LIMIT 20
            """
        )
        matches = cursor.fetchall()
        cursor.close()
        conn.close()
        return jsonify({
            'success': True,
            'matches': matches,
            'count': len(matches)
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/predict/<int:match_id>')
def predict_match(match_id):
    """Génère une prédiction basique pour un match"""
    conn = get_db_connection()
    if not conn:
        return jsonify({'error': 'Database connection failed'}), 500
    try:
        cursor = conn.cursor(dictionary=True)
        # Vérifie si une prédiction existe déjà
        cursor.execute(
            """
                SELECT * FROM predictions 
                WHERE match_id = %s 
                ORDER BY created_at DESC 
                LIMIT 1
            """,
            (match_id,)
        )
        existing = cursor.fetchone()
        if existing:
            cursor.close()
            conn.close()
            return jsonify({
                'success': True,
                'prediction': existing,
                'source': 'database'
            })
        # Sinon, génère une prédiction simple basée sur xG
        cursor.execute(
            """
                SELECT 
                    m.*,
                    m.home_xg,
                    m.away_xg
                FROM matches m
                WHERE m.id = %s
            """,
            (match_id,)
        )
        match = cursor.fetchone()
        if not match:
            return jsonify({'error': 'Match not found'}), 404
        prediction = {
            'match_id': match_id,
            'method': 'xG-based' if match['home_xg'] else 'basic',
            'home_win_prob': 0,
            'draw_prob': 0,
            'away_win_prob': 0
        }
        if match['home_xg'] and match['away_xg']:
            # Calcul basé sur xG (méthode simplifiée)
            home_advantage = 0.1
            home_strength = float(match['home_xg']) + home_advantage
            away_strength = float(match['away_xg'])
            total = home_strength + away_strength + 0.5  # 0.5 pour le draw
            prediction['home_win_prob'] = round((home_strength / total) * 100, 2)
            prediction['draw_prob'] = round((0.5 / total) * 100, 2)
            prediction['away_win_prob'] = round((away_strength / total) * 100, 2)
        cursor.close()
        conn.close()
        return jsonify({
            'success': True,
            'prediction': prediction,
            'source': 'calculated'
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# ---------------------------------------------------------------------------
# Nouveau endpoint pour lancer le scraping FBref
# ---------------------------------------------------------------------------
@app.route('/scrape', methods=['POST'])
def scrape_data():
    """Déclenche le scraping des fixtures FBref pour des ligues et saisons données.

    JSON attendu (champs facultatifs) :
        {
            "leagues": ["E0", "F1", ...],
            "start_season": "2000-2001",
            "end_season": "2024-2025"
        }

    Si le module scraper n'est pas disponible, retourne une erreur.
    """
    if scrape_all is None:
        return jsonify({'error': 'Scraping module not available'}), 500
    payload = request.get_json(silent=True) or {}
    leagues = payload.get('leagues')
    start_season = payload.get('start_season', '2000-2001')
    end_season = payload.get('end_season', '2024-2025')
    # Vérifie les codes de ligue fournis
    if leagues is not None:
        invalid = [code for code in leagues if code not in LEAGUE_META]
        if invalid:
            return jsonify({'error': f'Unknown league codes: {invalid}'}), 400
    try:
        results = scrape_all(leagues, start_season, end_season, DB_CONFIG)
        return jsonify({'success': True, 'inserted': results}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# ---------------------------------------------------------------------------
# NOUVEAUX ENDPOINTS pour la collecte de données xG
# ---------------------------------------------------------------------------

@app.route('/collect', methods=['POST'])
def collect_xg_data():
    """
    Lancer la collecte de données xG depuis les APIs
    
    JSON attendu (optionnel):
        {
            "leagues": ["E0", "SP1", ...],  # Ligues à traiter
            "limit": 1000                   # Nombre max de matchs
        }
    """
    global collector_status
    
    if XGCollector is None:
        return jsonify({'error': 'Data collector module not available'}), 500
    
    if collector_status['running']:
        return jsonify({
            'error': 'Collection already in progress',
            'status': collector_status
        }), 409
    
    payload = request.get_json(silent=True) or {}
    leagues = payload.get('leagues')
    limit = payload.get('limit', 1000)
    
    def run_collector():
        global collector_status
        collector_status['running'] = True
        collector_status['last_run'] = datetime.now().isoformat()
        
        try:
            collector = XGCollector(DB_CONFIG)
            stats = collector.collect_all(leagues, limit)
            collector_status['stats'] = stats
            collector_status['success'] = True
        except Exception as e:
            collector_status['stats'] = {'error': str(e)}
            collector_status['success'] = False
        finally:
            collector_status['running'] = False
    
    # Lancer dans un thread séparé
    thread = threading.Thread(target=run_collector)
    thread.daemon = True
    thread.start()
    
    return jsonify({
        'success': True,
        'message': 'Collection started in background',
        'status': collector_status
    })


@app.route('/collect/status')
def get_collector_status():
    """Obtenir le statut de la collecte en cours"""
    return jsonify(collector_status)


@app.route('/export')
def export_ml_data():
    """Exporter les données pour Machine Learning"""
    conn = get_db_connection()
    if not conn:
        return jsonify({'error': 'Database connection failed'}), 500
    
    try:
        # Créer le fichier CSV
        import csv
        import io
        
        cursor = conn.cursor(dictionary=True)
        cursor.execute("""
            SELECT 
                m.id,
                m.league,
                m.season,
                m.match_date,
                m.home_team,
                m.away_team,
                m.home_score,
                m.away_score,
                m.home_xg,
                m.away_xg,
                m.xg_source
            FROM matches m
            WHERE m.home_score IS NOT NULL
            AND m.home_xg IS NOT NULL
            ORDER BY m.match_date DESC
            LIMIT 10000
        """)
        
        matches = cursor.fetchall()
        cursor.close()
        conn.close()
        
        # Créer le CSV en mémoire
        output = io.StringIO()
        if matches:
            writer = csv.DictWriter(output, fieldnames=matches[0].keys())
            writer.writeheader()
            writer.writerows(matches)
        
        # Convertir en bytes pour l'envoi
        output.seek(0)
        
        # Créer la réponse
        from flask import Response
        response = Response(output.getvalue(), mimetype='text/csv')
        response.headers['Content-Disposition'] = f'attachment; filename=football_data_{datetime.now().strftime("%Y%m%d")}.csv'
        
        return response
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/fix', methods=['POST'])
def fix_data():
    """Corriger rapidement les problèmes de données"""
    conn = get_db_connection()
    if not conn:
        return jsonify({'error': 'Database connection failed'}), 500
    
    try:
        cursor = conn.cursor()
        fixes = []
        
        # Fix dates 2025
        cursor.execute("""
            UPDATE matches 
            SET match_date = DATE_SUB(match_date, INTERVAL 1 YEAR)
            WHERE YEAR(match_date) > 2024
        """)
        if cursor.rowcount > 0:
            fixes.append(f"Corrigé {cursor.rowcount} dates futures")
        
        # Supprimer matchs test
        cursor.execute("""
            DELETE FROM matches 
            WHERE home_team LIKE '%Test%' 
            OR away_team LIKE '%Test%'
        """)
        if cursor.rowcount > 0:
            fixes.append(f"Supprimé {cursor.rowcount} matchs test")
        
        # Calculer xG manquants depuis scores
        cursor.execute("""
            UPDATE matches 
            SET 
                home_xg = home_score * 0.9 + 0.1,
                away_xg = away_score * 0.9 + 0.1,
                xg_source = 'calculated'
            WHERE home_xg IS NULL 
            AND home_score IS NOT NULL
            AND match_date < '2014-01-01'
        """)
        if cursor.rowcount > 0:
            fixes.append(f"Calculé {cursor.rowcount} valeurs xG")
        
        conn.commit()
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'fixes': fixes,
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)
