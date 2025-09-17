import os
from flask import Flask, jsonify, request
from flask_cors import CORS
import mysql.connector
from datetime import datetime

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
        'endpoints': [
            '/health',
            '/stats', 
            '/collect',
            '/fix'
        ]
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
            cursor.execute("SELECT COUNT(*) FROM matches")
            tables_count = cursor.fetchone()[0]
            db_status = 'online'
            cursor.close()
            conn.close()
        except Exception as e:
            print(f"Erreur health: {e}")
    
    return jsonify({
        'api': 'healthy',
        'database': db_status,
        'matches_count': tables_count,
        'timestamp': datetime.now().isoformat()
    })

@app.route('/stats')
def get_stats():
    """Retourne les statistiques de votre système"""
    conn = get_db_connection()
    if not conn:
        return jsonify({'error': 'Database connection failed'}), 500
    
    try:
        cursor = conn.cursor()
        
        # Total matchs
        cursor.execute("SELECT COUNT(*) FROM matches")
        total_matches = cursor.fetchone()[0]
        
        # Matchs avec xG
        cursor.execute("SELECT COUNT(*) FROM matches WHERE home_xg IS NOT NULL")
        matches_with_xg = cursor.fetchone()[0]
        
        # Matchs avec scores
        cursor.execute("SELECT COUNT(*) FROM matches WHERE home_score IS NOT NULL")
        matches_with_scores = cursor.fetchone()[0]
        
        # Stats par ligue
        cursor.execute("""
            SELECT league, COUNT(*) as total,
                   COUNT(home_xg) as with_xg,
                   COUNT(home_score) as with_scores
            FROM matches
            GROUP BY league
            ORDER BY total DESC
            LIMIT 10
        """)
        
        leagues = []
        for row in cursor.fetchall():
            leagues.append({
                'league': row[0],
                'total': row[1],
                'with_xg': row[2],
                'with_scores': row[3]
            })
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'total_matches': total_matches,
            'matches_with_xg': matches_with_xg,
            'matches_with_scores': matches_with_scores,
            'xg_coverage': round(matches_with_xg / max(total_matches, 1) * 100, 1),
            'score_coverage': round(matches_with_scores / max(total_matches, 1) * 100, 1),
            'leagues': leagues
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/collect', methods=['POST'])
def collect_xg():
    """Lance la collecte de données xG"""
    try:
        # Import du collecteur
        from data_collector import XGCollector
        
        # Paramètres
        payload = request.get_json() or {}
        leagues = payload.get('leagues', ['E0'])
        limit = payload.get('limit', 100)
        
        # Lancer la collecte
        collector = XGCollector(DB_CONFIG)
        stats = collector.collect_all(leagues, limit)
        
        return jsonify({
            'success': True,
            'stats': stats
        })
    except ImportError:
        return jsonify({
            'error': 'Data collector module not found',
            'message': 'Please ensure data_collector.py is uploaded'
        }), 500
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
            fixes.append(f"Fixed {cursor.rowcount} future dates")
        
        # Remove test matches
        cursor.execute("""
            DELETE FROM matches 
            WHERE home_team LIKE '%Test%' 
            OR away_team LIKE '%Test%'
        """)
        if cursor.rowcount > 0:
            fixes.append(f"Removed {cursor.rowcount} test matches")
        
        # Calculate missing xG from scores
        cursor.execute("""
            UPDATE matches 
            SET 
                home_xg = home_score * 0.9 + 0.1,
                away_xg = away_score * 0.9 + 0.1,
                xg_source = 'calculated'
            WHERE home_xg IS NULL 
            AND home_score IS NOT NULL
            AND match_date < '2014-01-01'
            LIMIT 1000
        """)
        if cursor.rowcount > 0:
            fixes.append(f"Calculated {cursor.rowcount} xG values")
        
        conn.commit()
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'fixes': fixes,
            'timestamp': datetime.now().isoformat()
        })
    except Exception as e:
        conn.rollback()
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)
