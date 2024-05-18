
from sqlalchemy import create_engine, Integer, VARCHAR
import pandas as pd
from datetime import datetime

# Decodificando arquivo
pd.set_option('display.max_columns', None)
caminho_do_arquivo = r"C:\Documentos\Curso SGBDS\Python SGBDS\Postgree\1.Scripts\V_OCORRENCIA_AMPLA.json"
df = pd.read_json(caminho_do_arquivo, encoding='utf-8-sig')

# Alterando nome para retirar caracteres especiais
colunas = ["Numero_da_Ocorrencia", "Classificacao_da_Ocorrência", "Data_da_Ocorrencia","Municipio","UF","Regiao","Nome_do_Fabricante","Modelo"]
df = df[colunas]
df.rename( columns={  'Classificacao_da_Ocorrência' : 'Classificacao_da_Ocorrencia'  } ,inplace=True )

# Alterando o type da variável para datetime
df['Data_da_Ocorrencia'] = pd.to_datetime(df['Data_da_Ocorrencia']) 
ano_atual = datetime.now().year
df = df[df['Data_da_Ocorrencia'].dt.year == ano_atual]

# Parâmetro de conexão
dbname   = 'python'
user     = 'postgres'
password = '206170'
host     = 'localhost'
port     = '5432' 

conexao_str = f'postgresql://{user}:{password}@{host}:{port}/{dbname}'
engine = create_engine(conexao_str)

nome_tabela = 'anac_sqlalchemy' 

# Deletar registros com base no ano atual
cursor=engine.connect() 
delete = text(f'delete from public.{nome_tabela} where extract(year from "Data_da_Ocorrencia") = {ano_atual}')
cursor.execute(delete)
cursor.commit()

# Enviar DataFrame para o banco de dados
df.to_sql(nome_tabela, engine, index=False, if_exists='append',
                     dtype={ 
                           'Numero_da_Ocorrencia' :   Integer ,
                           'Classificacao_da_Ocorrencia': VARCHAR(50),
                           })

engine.dispose()
cursor.close()


