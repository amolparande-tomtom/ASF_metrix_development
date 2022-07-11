import os


class Config:
    # Flask settings
    FLASK_SERVER_NAME = 'localhost:8081'
    FLASK_ENV = 'development'

    # Flask-Restplus settings
    SWAGGER_UI_DOC_EXPANSION = 'list'
    RESTPLUS_VALIDATE = True
    RESTPLUS_MASK_SWAGGER = False
    ERROR_404_HELP = False


class DeveloperConfig(Config):
    FLASK_DEBUG = True
    LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")
