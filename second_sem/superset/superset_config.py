import os


SECRET_KEY = os.getenv("SUPERSET_SECRET_KEY", "second-sem-superset-secret-key")
SQLALCHEMY_DATABASE_URI = "sqlite:////app/superset_home/superset.db"
WTF_CSRF_ENABLED = True
