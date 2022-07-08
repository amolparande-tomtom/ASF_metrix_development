from sqlalchemy import create_engine
import pandas as pd

df = pd.DataFrame({
    'Company': ['A', 'A', 'A', 'B', 'B', 'B', 'B'],
    'Model': ['A1', 'A2', 'A3', 'B1', 'B2', 'B3', 'B4'],
    'Year': [2019, 2020, 2021, 2018, 2019, 2020, 2021],
    'Transmission': ['Manual', 'Automatic', 'Automatic', 'Manual', 'Automatic', 'Automatic', 'Manual'],
    'EngineSize': [1.4, 2.0, 1.4, 1.5, 2.0, 1.5, 1.5],
    'MPG': [55.4, 67.3, 58.9, 52.3, 64.2, 68.9, 83.1]
})

# DB Connection URL   {db_connection_url_5 = "postgresql://username:password@localhost:5433/database"}
# db_connection_url_5 = "postgresql://username:password@localhost:5433/database"

engine = create_engine('postgresql://postgres:postgres@localhost:5433/test')

# DataFream to Posgres

# if_exists : {'fail', 'replace', 'append'}
# df.to_sql('test',engine, if_exists = 'append')


group_max = df.groupby(['Company', 'Transmission'])['MPG'].max()

group_max.to_sql('group_max', engine, if_exists='append')
