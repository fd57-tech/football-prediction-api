import os
from flask import Flask, jsonify, request
from flask_cors import CORS
import mysql.connector
from datetime import datetime

# Try to import the scraper utilities.  These are optional: if the
# module isn't present, the /scrape endpoint will return an error.
try:
    from scraper_fbref import scrape_all, LEAGUE_META  # type: ignore
except ImportError:
    scrape_all = None
    LEAGUE_META = {}


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
        'version': '2.0',
        'endpoints': {
            '/health': 'Vérification santé système',
            '/stats': 'Statistiques globales',
            '/matches/recent': 'Matchs récents',
            '/predict/<match_id>': 'Prédiction pour un match',
            '/scrape': 'Scrape FBref fixtures (POST)'  # new endpoint
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
            'matches_with_xg': "SELECT COUNT(*) as count FROM match_xg_stats",
            'total_predictions': "SELECT COUNT(*) as count FROM predictions",
            'teams': "SELECT COUNT(*) as count FROM teams"
        }

        results = {}
        for key, query in stats_queries.items():
            cursor.execute(query)
            results[key] = cursor.fetchone()['count']

        cursor.close()
        conn.close()

        return jsonify({
            'success': True,
            'stats': results,
            'quality_indicators': {
                'xg_coverage': f"{(results['matches_with_xg'] / max(results['total_matches'], 1)) * 100:.1f}%"
            }
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
                    DATE_FORMAT(m.match_date, '%Y-%m-%d %H:%i') as match_date,
                    m.status,
                    mx.home_xg,
                    mx.away_xg
                FROM matches m
                LEFT JOIN match_xg_stats mx ON m.id = mx.match_id
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
                    mx.home_xg,
                    mx.away_xg
                FROM matches m
                LEFT JOIN match_xg_stats mx ON m.id = mx.match_id
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

    JSON attendu (champs facultatifs) :
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


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)