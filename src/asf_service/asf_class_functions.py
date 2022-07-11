import psycopg2


class AsfProcess:
    def __init__(self, mnr_db_url, vad_db_url):
        self.mnr_db_url = mnr_db_url
        self.vad_db_url = vad_db_url

    def postgres_db_connection(self, db_url):
        """
        :param db_url: Postgres Server
        :return: DB Connection
        """
        try:
            return psycopg2.connect(db_url)
        except Exception as error:
            print("Oops! An exception has occurred:", error)
            print("Exception TYPE:", type(error))
