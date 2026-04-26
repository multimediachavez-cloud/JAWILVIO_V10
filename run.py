import os

from app import create_app

app = create_app()

if __name__ == "__main__":
    debug_enabled = os.getenv('FLASK_DEBUG', '0').strip().lower() in {'1', 'true', 'yes', 'on'}
    port = int(os.getenv('PORT', '5000'))
    app.run(host='0.0.0.0', port=port, debug=debug_enabled)
