# encoding: utf-8

from flask_restx import Resource

from src.asf_service import mnr_operations, vad_operations
from src.asf_service.Commons.Utility import Utility
from src.asf_service.Commons.api import api
from src.asf_service.asf_class_functions import AsfProcess
from src.asf_service.controller.input import input_parser
from src.asf_service.controller.input.input_parser import file_upload_request_parameters

ns = api.namespace('', description='Operations related to Sample')


@ns.route('/')
@api.response(204, 'Done.')
@api.response(400, 'Invalid.')
class SampleController(Resource):

    def __init__(self, api=None, *args, **kwargs):
        super().__init__(api, *args, **kwargs)

    @api.expect(file_upload_request_parameters, validate=True)
    def post(self):
        """
        Get ASF Metrics
        """
        request_parameter = input_parser.file_upload_request_parameters.parse_args()
        mnr_url = request_parameter['mnr_db_url']
        vad_url = request_parameter['vad_db_url']
        input_path = request_parameter['input_path']
        output_path = request_parameter['output_path']
        mnr_filename = request_parameter['mnr_filename']
        mnr_schema = request_parameter['mnr_schema']
        vad_filename = request_parameter['vad_filename']
        vad_schema = request_parameter['vad_schema']
        language_codes = request_parameter['language_codes']

        self.process_start(input_path, output_path, mnr_filename, vad_filename,
                           mnr_url, mnr_schema, vad_url, vad_schema, language_codes)
        return mnr_url

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
