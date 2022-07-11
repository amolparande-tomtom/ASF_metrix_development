# encoding: utf-8
"""
Parsing all inputs for the web service
-----------------------------
"""
from flask_restx import reqparse
from werkzeug.datastructures import FileStorage

from src.asf_service.Commons.mnrserver import MnrServer

parser = reqparse.RequestParser()
parser.add_argument('mnr_db_url', required=True, help='Select your MNR DB url',
                    choices=[e.name for e in MnrServer])
parser.add_argument('mnr_schema', required=True, help='Enter your mnr schema name',
                    choices=("eur_cas", "nam", "lam_mea_oce_sea", "ind_ind", "isr_isr", "s_o_s_o", "indoor"))
parser.add_argument('vad_schema', required=True, help='Enter your vad schema name')
parser.add_argument('input_file_path', type=FileStorage, location='files',
                    required=True, help='Enter your input file Path')
parser.add_argument('language_codes', action="append", help='Enter languages', required=True)

