# encoding: utf-8
"""
Parsing all inputs for the web service
-----------------------------
"""
from flask_restx import reqparse

file_upload_request_parameters = reqparse.RequestParser()
file_upload_request_parameters.add_argument('mnr_db_url', required=True, help='Enter your MNR DB url')
file_upload_request_parameters.add_argument('vad_db_url', required=True, help='Enter your VAD DB url')
file_upload_request_parameters.add_argument('input_path', required=True, help='Enter your input file Path')
file_upload_request_parameters.add_argument('output_path', required=True, help='Enter your output file Path')
file_upload_request_parameters.add_argument('mnr_filename', required=True, help='Enter your mnr file name')
file_upload_request_parameters.add_argument('mnr_schema', required=True, help='Enter your mnr schema name')
file_upload_request_parameters.add_argument('vad_filename', required=True, help='Enter your vad file name')
file_upload_request_parameters.add_argument('vad_schema', required=True, help='Enter your vad schema name')
file_upload_request_parameters.add_argument('language_codes', required=True, help='Enter your vad schema name')