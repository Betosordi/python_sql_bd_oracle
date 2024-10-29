import cx_Oracle
import pandas as pd
import psycopg2  # Biblioteca para conexão com PostgreSQL
import pywhatkit as kit


# Conectar ao banco de dados Oracle
dsn = cx_Oracle.makedsn("IP onde esta o banco de dados", 1521, service_name="Seu serviço")
connection = cx_Oracle.connect(user="Usuario de leitura", password="Sua senha", dsn=dsn)
#********************************************************************************************8
# Conectar ao banco de dados PostgreSQL

# Configurações do banco de dados
DB_PARAMS = {
    'dbname': 'Tabela do banco de dados Postgree',
    'user': 'postgres',
    'password': 'Sua senha',
    'host': 'O IP onde esta instalado o Postgree',
    'port': '5432'
}
# Estabelecendo a conexão
connection_postgre = psycopg2.connect(
    dbname=DB_PARAMS['dbname'],
    user=DB_PARAMS['user'],
    password=DB_PARAMS['password'],
    host=DB_PARAMS['host'],
    port=DB_PARAMS['port']
)

consulta_bd_app = """
SELECT * FROM qualidade_producao;
"""

# Executar a consulta e armazenar os resultados em um DataFrame do pandas
lista_csv_producao = pd.read_sql(consulta_bd_app, connection_postgre)

lista_csv_producao.to_csv("N://PYTHON/arquivos_tratados_fonte_de_dados/Produtos Produzidos/producao_maquinas_csv/producao.csv", index=False, sep=';')

##################-------------------------------------------------------------------------------------
# Saldo dos Acabados
# Definir a consulta SQL
query = """
SELECT 
    ap.*,
    p.REFERENCIA_PRODUTO,
    p.NOME_PRODUTO,
    p.TP_PRODUTO
FROM 
    SEVEN.ALMOXARIFADO_PRODUTO ap
JOIN 
    SEVEN.PRODUTO p
ON 
    ap.PRODUTOID = p.PRODUTOID
WHERE 
    ap.ALMOXARIFADOID = 5
    AND p.TP_PRODUTO IN ('ACABADO')
"""

#try:
    # Executar a consulta e armazenar os resultados em um DataFrame do pandas
planilha_saldo_produtos_acabados = pd.read_sql(query, connection)
    
    # Gerar um arquivo Excel com os resultados
planilha_saldo_produtos_acabados.to_excel("N://PYTHON/arquivos_tratados_fonte_de_dados/Produtos_Acabados_Saldo_SQL/produtos_acabados_saldo_expedição.xlsx", index=False)
                                                    
#finally:
    # Fechar a conexão
   # connection.close()

#####################################################################################################################

# Pedidos, NF e materiais faturados
# Consulta SQL 
procx_pdvenda_no_pdvendaitem = """
    SELECT 
    pvi.PEDIDOVENDAID,
    pvi.PRODUTOID,
    pvi.REFERENCIA_PEDIDOVENDA_ITEM,
    pvi.QT_PEDIDOVENDA_ITEM,
    pv.VENDEDOR1ID,
    pv.DT_PEDIDOVENDA,
    pv.STATUS_PEDIDOVENDA,
    pv.DT_FATURAMENTO_PEDIDOVENDA,
    pv.USUARIOID,
    pv.NR_NOTA_PEDIDOVENDA,
    pv.DT_SAIDA_PEDIDOVENDA,
    pv.ORIGEM_DIGITACAO_PEDIDOVENDA,
    pvi.QT_DESPACHO,
    pvi.DT_DESPACHO,
    pvi.USUARIOID_DESPACHO,
    pvi.DS_PRODUTO_PEDIDOVENDA_ITEM,
    pvi.QT_PECAS_PEDIDOVENDA_ITEM,
    pvi.DT_DIGITACAO_PEDIDOVENDA_ITEM,
    pv.VL_TOTALPROD_PEDIDOVENDA
    
    FROM 
        SEVEN.PEDIDOVENDA_ITEM pvi
    JOIN 
        SEVEN.PEDIDOVENDA pv
    ON 
        pvi.PEDIDOVENDAID = pv.PEDIDOVENDAID
    WHERE 
        pv.DT_PEDIDOVENDA BETWEEN TO_DATE('01-01-2024', 'DD-MM-YYYY') AND SYSDATE
        AND pv.STATUS_PEDIDOVENDA IN ('ABERTO', 'BLOQUEADO', 'PARCIAL', 'IMPORTADO', 'FATURADO')
"""

# Executar a consulta e armazenar os resultados em um DataFrame do pandas
planilha_pedidos_nf = pd.read_sql(procx_pdvenda_no_pdvendaitem, connection)

planilha_pedidos_nf['QT_PEDIDOVENDA_ITEM'] = planilha_pedidos_nf['QT_PEDIDOVENDA_ITEM'].astype(int)

planilha_pedidos_nf['QT_DESPACHO'] = planilha_pedidos_nf['QT_DESPACHO'].astype(int)

# Substitui valores NaN por 0 (ou outro valor de sua escolha)
planilha_pedidos_nf['USUARIOID_DESPACHO'].fillna(0, inplace=True)

# Converte a coluna para int
planilha_pedidos_nf['USUARIOID_DESPACHO'] = planilha_pedidos_nf['USUARIOID_DESPACHO'].astype(int)


# Salvando o DataFrame em um arquivo Excel
caminho_pedidos_SQL = "N://PYTHON/arquivos_tratados_fonte_de_dados/Pedidos_NF_QTD_SQL/Pedidos_NF_QTD_SQL.csv"

# Salvando com o índice removido
planilha_pedidos_nf.to_csv(caminho_pedidos_SQL, index=False,sep=';')
########################################################################################################################


# Pedidos faturados e pendentes
# Consulta SQL 
consulta_pedidos_sql = """
SELECT 
  PEDIDOVENDAID,
  VENDEDOR1ID,
  VL_TOTALPROD_PEDIDOVENDA,
  STATUS_PEDIDOVENDA,
  DT_PEDIDOVENDA,
  DT_FATURAMENTO_PEDIDOVENDA,
  NR_NOTA_PEDIDOVENDA,
  CADCFTVID
  
FROM 
    SEVEN.PEDIDOVENDA
WHERE 
    DT_PEDIDOVENDA BETWEEN TO_DATE('01-01-2023', 'DD-MM-YYYY') AND SYSDATE
    AND STATUS_PEDIDOVENDA IN ('ABERTO', 'BLOQUEADO', 'PARCIAL', 'IMPORTADO', 'FATURADO')
    
"""
# Executar a consulta e armazenar os resultados em um DataFrame do pandas
csv_pedidos_status = pd.read_sql(consulta_pedidos_sql, connection)

pedidos_status_caminho_sql = "N://PYTHON/arquivos_tratados_fonte_de_dados/Pedidos_Status_SQL/pedidos_status_sql.csv"

# Salvando com o índice removido
csv_pedidos_status.to_csv(pedidos_status_caminho_sql, index=False,sep=';')
#########################################################################################################################


# Consulta SQL para entradas de produção
consulta_entrada_sql = """
SELECT K.*, P.DS_PRODUTO, P.REFERENCIA_PRODUTO
FROM SEVEN.KARDEX K
JOIN SEVEN.PRODUTO P ON K.PRODUTOID = P.PRODUTOID
WHERE 
    EXTRACT(YEAR FROM K.DT_KARDEX) = EXTRACT(YEAR FROM SYSDATE) 
    AND K.TIPO_KERDEX = 'ENTRADA'
"""

# Executa a consulta SQL e transforma o resultado em um DataFrame
csv_entradas_producao = pd.read_sql(consulta_entrada_sql, connection)

csv_entradas_producao['QT_KARDEX'] = csv_entradas_producao['QT_KARDEX'].astype(int)

# Caminho para salvar o arquivo CSV
producao_caminho_sql = "N://PYTHON/arquivos_tratados_fonte_de_dados/Entrada_producao_SQL/entrada_producao_sql.csv"

# Salva o DataFrame em CSV com o índice removido e separador ponto e vírgula
csv_entradas_producao.to_csv(producao_caminho_sql, index=False, sep=';')
####################################################################################################################

# Usuarios login
usuarios_consulta_sql = """
SELECT USUARIOID, NOME_USUARIO
FROM SEVEN.USUARIO
"""
#-------------------------------------------------------

# Executa a consulta SQL e transforma o resultado em um DataFrame
csv_usuarios = pd.read_sql(usuarios_consulta_sql, connection)

# Caminho para salvar o arquivo CSV
csv_caminho_sql = "N://PYTHON/arquivos_tratados_fonte_de_dados/Usuarios/usuarios_sql.csv"

# Salva o DataFrame em CSV com o índice removido e separador ponto e vírgula
csv_usuarios.to_csv(csv_caminho_sql, index=False, sep=';')
####################################################################################################################

# Produtos acabados

produtos_acabados_referencia = """

SELECT PRODUTOID, SUBGRUPOPRODUTOID, DS_PRODUTO, TP_PRODUTO 
FROM SEVEN.PRODUTO p 
WHERE TP_PRODUTO = 'ACABADO'

"""
#-------------------------------------------------------

# Executa a consulta SQL e transforma o resultado em um DataFrame
csv_produtos_acabados = pd.read_sql(produtos_acabados_referencia, connection)

csv_produtos_acabados['PRODUTOID'] = csv_produtos_acabados['PRODUTOID'].astype(int)

# Caminho para salvar o arquivo CSV
csv_produtos_acabados_sql = "N://PYTHON/arquivos_tratados_fonte_de_dados/Produtos acabados sql/produtos_acabados_sql.csv"

# Salva o DataFrame em CSV com o índice removido e separador ponto e vírgula
csv_produtos_acabados.to_csv(csv_produtos_acabados_sql, index=False, sep=';')
####################################################################################################################

# Clientes 
clientes_sql = """
SELECT 
    pv.PEDIDOVENDAID, 
    c.CADCFTVID, 
    c.CNPJCPF_CADCFTV, 
    c.NOME_CADCFTV
FROM 
    SEVEN.PEDIDOVENDA pv
JOIN 
    SEVEN.CADCFTV c 
ON 
    pv.CADCFTVID = c.CADCFTVID
"""
# Executa a consulta SQL e transforma o resultado em um DataFrame
csv_clientes = pd.read_sql(clientes_sql, connection)

csv_clientes['CADCFTVID'] = csv_clientes['CADCFTVID'].astype(int)

# Caminho para salvar o arquivo CSV
csv_caminho_clientes_sql = "N://PYTHON/arquivos_tratados_fonte_de_dados/Clientes_sql/csv_clientes.csv"

# Salva o DataFrame em CSV com o índice removido e separador ponto e vírgula
csv_clientes.to_csv(csv_caminho_clientes_sql, index=False, sep=';')
####################################################################################################################
