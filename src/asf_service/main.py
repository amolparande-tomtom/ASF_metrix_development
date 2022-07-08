from src.asf_service.asf_class_functions import AsfMnrProcess as asf

if __name__ == '__main__':
    mnr_db_url = "postgresql://caprod-cpp-pgmnr-005.flatns.net/mnr?user=mnr_ro&password=mnr_ro"
    vad_db_url = "postgresql://vad3g-prod.openmap.maps.az.tt3.com/ggg?user=ggg_ro&password=ggg_ro"
    csv_path = '/Users/parande/Documents/4_ASF_Metrix/0_input_csv/1_BEL/'
    filename = 'BEL_ASF_logs copy.csv'
    outputpath = '/Users/parande/Documents/4_ASF_Metrix/2_output/BEL/'
    mnr_filename = 'MNR_ASF_output_BEL_NL.csv'
    vad_filename = 'VAD_ASF_output_BEL_NL.csv'
    mnr_schema_name = 'eur_cas'
    vad_schema_name = 'eur_bel_20220521_cw20'
    country_language_code = ['nl', 'de', 'fr']


    csv_gdb = asf.create_points_from_input_csv(csv_path)

    asf.main_mnr_csv_buffer_db_apt_fuzzy_matching(csv_gdb, db_url, mnr_schema_name, filename, outputpath)





