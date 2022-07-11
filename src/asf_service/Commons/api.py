# encoding: utf-8
"""
Base class for having API configuration.
-----------------------------
"""

from flask_restx import Api

from flask import current_app as app

api = Api(version='1.0', title='Sample API', description="Perform sample actions")


@api.errorhandler
def default_error_handler(e):
    message = 'An unhandled exception occurred.'

    if not app.config['FLASK_DEBUG']:
        return {'message': message}, 500


@api.errorhandler(ValueError)
def incorrect_input_error_handler(e):
    return {'message': e.args[0]}, 404
