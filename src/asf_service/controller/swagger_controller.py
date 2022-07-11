# encoding: utf-8
import datetime
import os

from flask_restx import Resource

from src.asf_service import mnr_operations, vad_operations
from src.asf_service.Commons.Utility import Utility
from src.asf_service.Commons.api import api
from src.asf_service.Commons.mnrserver import MnrServer
from src.asf_service.asf_class_functions import AsfProcess
from src.asf_service.controller.input import input_parser
from src.asf_service.controller.input.input_parser import parser

ns = api.namespace('', description='Operations related to Sample')


@ns.route('/')
@api.response(200, 'Done.')
@api.response(400, 'Invalid.')
class SampleController(Resource):

    def __init__(self, api=None, *args, **kwargs):
        super().__init__(api, *args, **kwargs)

    @api.expect(parser, validate=True)
    def post(self):
        """
        Get ASF Metrics
        """
        request_parameter = input_parser.parser.parse_args()
        mnr_url = MnrServer.__getitem__(request_parameter['mnr_db_url']).value
        mnr_schema = request_parameter['mnr_schema']
        vad_schema = request_parameter['vad_schema']
        language_codes = request_parameter['language_codes']

        input_file = request_parameter['input_file_path']
        input_path = "../" + input_file.filename
        saved_file = input_file.save(input_path)

        mnr_filename = "MNR_Output_" + str(datetime.datetime.now())
        vad_filename = "VAD_Output_" + str(datetime.datetime.now())
        output_path = "../"
        vad_url = "postgresql://vad3g-prod.openmap.maps.az.tt3.com/ggg?user=ggg_ro&password=ggg_ro"

        self.process_start(input_path, output_path, mnr_filename, vad_filename,
                           mnr_url, mnr_schema, vad_url, vad_schema, language_codes)
        if os.path.exists(input_path):
            os.remove(input_path)
        if os.path.exists(output_path + mnr_filename):
            os.remove(output_path + mnr_filename)
        if os.path.exists(output_path + vad_filename):
            os.remove(output_path + vad_filename)
        return "Processing done"

    @staticmethod
    def process_start(input_path, output_path, output_mnr_filename, output_vad_filename,
                      mnr_db_url, mnr_schema, vad_db_url, vad_schema, language_codes):
        asf_cls = AsfProcess(mnr_db_url, vad_db_url)
        mnr_db_conn = asf_cls.postgres_db_connection(mnr_db_url)
        vad_db_conn = asf_cls.postgres_db_connection(vad_db_url)

        csv_gdf = Utility.create_points_from_input_csv(input_path)
        # process mnr data
        mnr_operations.mnr_csv_buffer_db_apt_fuzzy_matching(csv_gdf, mnr_schema, output_path, output_mnr_filename,
                                                            mnr_db_conn)
        # process vad data
        for language_code in language_codes:
            vad_operations.vad_csv_buffer_db_apt_fuzzy_matching(csv_gdf, vad_schema, output_path, output_vad_filename,
                                                                language_code, vad_db_conn)
