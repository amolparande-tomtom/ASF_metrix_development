from flask import Flask, Blueprint

from src.asf_service.Commons.api import api
from src.asf_service.controller.swagger_controller import ns

app = Flask(__name__)


def configure_app(app):
    app.config["ENV"] = "dev"
    if app.config["ENV"] == "dev":
        app.config.from_object("config.DeveloperConfig")
    else:
        app.config.from_object("config")


def initialize_app(flask_app):
    configure_app(flask_app)
    blueprint = Blueprint('api', __name__)
    api.init_app(blueprint)
    api.add_namespace(ns)
    flask_app.register_blueprint(blueprint)


def main():
    initialize_app(app)
    app.run(use_reloader=False, port=9091, host='localhost')


if __name__ == '__main__':
    main()
