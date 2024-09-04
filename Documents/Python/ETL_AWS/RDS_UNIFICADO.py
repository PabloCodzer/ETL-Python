# -----------------------------------------------------------
#  ** CONSTRUCTO EM UM FILE, BI SENFINS **
# -----------------------------------------------------------
from PyInstaller.utils.hooks import collect_submodules
from PyInstaller.utils.hooks import collect_data_files
import schedule
import datetime as data
import os
import re
import time
from threading import Thread
import concurrent.futures
import glob as gb
import traceback
import pyodbc
import pandas as pd
from datetime import date, datetime, timedelta
import requests
from bcb import sgs
from bcb import currency

server_selecao = 0

# ----------------------------------------------------------
#
#   *** PARTE REFERENTE AO DOMINIO ***
#
# ----------------------------------------------------------
def conecta_dom():
    connection_string = "Driver={SQL Anywhere 16};UID=externo;PWD=externo;links=tcpip(host=192.168.193.100);ServerName=srvnew;port=2638"
    try:
        connection = pyodbc.connect(connection_string)
        return connection
    except Exception as err:
        print("Error occurred in making connection …")
        print(err)
        traceback.print_exc()


def consulta_plano(codi_emp):
    conn = conecta_dom()
    cursor = conn.cursor()
    cursor.execute(f"""SELECT first codi_emp_plano FROM bethadba.ctlancto WHERE codi_emp = {codi_emp}""")

    lista = cursor.fetchone()

    if lista is not None:
        return lista[0]
    else:
        return 0


def consulta_nome(codi_emp):
    conn = conecta_dom()
    cursor = conn.cursor()
    cursor.execute(
        f"""select nome from bethadba.fofiliais where codi_emp = {codi_emp}""")
    nome = cursor.fetchone()
    return nome[0]


def consulta_ct_contas_todas(codi_emp):
    ctcontas = []
    conn = conecta_dom()
    cursor = conn.cursor()
    cursor.execute(f"""select * from bethadba.ctcontas where codi_emp = {codi_emp}""")
    ctcontas = cursor.fetchall()

    if len(ctcontas) == 0:
        ctcontas = [0]
    return ctcontas


def consulta_ct_contas(codi_emp):
    conn = conecta_dom()
    cursor = conn.cursor()
    cursor.execute(
        f"select * from bethadba.ctcontas where codi_emp = {codi_emp}")
    ctcontas = cursor.fetchall()
    return ctcontas


def consulta_ct_lancamentos(codi_emp, periodo_ini, periodo_fim):
    ct_lancamentos = []
    debito = []
    credito = []

    sql_ctlancamentos = f"""
        select
            codi_emp,
            nume_lan,
            data_lan,
            vlor_lan,
            cdeb_lan,
            ccre_lan,
            codi_his,
            chis_lan,
            orig_lan,
            fili_lan 
            from bethadba.ctlancto
            where codi_emp in ( {codi_emp} )
            AND data_lan BETWEEN '{periodo_ini}' and  '{periodo_fim}'
            """
    conn = conecta_dom()
    cursor = conn.cursor()
    cursor.execute(sql_ctlancamentos)
    ct_lancamentos = cursor.fetchall()
    return ct_lancamentos


def pega_centro_de_custo(codi_emp, periodo_ini, periodo_fim):
    sql_ctc = f"""
                SELECT 
                    c.codi_emp,
                    c.nume_ccu as nume_lan,
                    c.data_ccu as data_lan,
                    case c.cdeb_ccu 
                    when 0 then c.vlor_ccu * -1
                    else c.vlor_ccu * 1 end as vlor_lan,
                    c.codi_cta,
                    'RATEIO DE CENTRO DE CUSTO' as chis_lan,
                    --ct.chis_lan as numero_1,
                    c.fili_ccu as fili_lan,
                    (c.cdeb_ccu + c.ccre_ccu) as ccu,
                    ct.orig_lan,
                    case c.cdeb_ccu
                    when 0 then 'crédito' else 'débito' end as tipo,
                    cc.desc_ccu 
                from bethadba.ctclancto c 
                inner join bethadba.ctlancto ct on ct.codi_emp = c.codi_emp and ct.nume_lan = c.nume_ccu 
                INNER JOIN bethadba.ctccusto cc ON cc.codi_emp = c.codi_emp and (cc.codi_ccu = c.cdeb_ccu or cc.codi_ccu = c.ccre_ccu)
                WHERE c.codi_emp in  (  {codi_emp}  ) 
                AND c.data_ccu BETWEEN '{periodo_ini}' and  '{periodo_fim}'
    """
    conn = conecta_dom()
    cursor = conn.cursor()
    cursor.execute(sql_ctc)
    ccu = cursor.fetchall()
    return ccu


def busca_descricoes_ccu(codi_emp, periodo_ini, periodo_fim):
    busca_descicoes = f"""
                       select 
                           c.codi_emp,
                           c.nume_ccu as nume_lan,
                           cc.desc_ccu 
                       from bethadba.ctclancto c 
                       inner join bethadba.ctlancto ct on ct.codi_emp = c.codi_emp and ct.nume_lan = c.nume_ccu 
                       INNER JOIN bethadba.ctccusto cc ON cc.codi_emp = c.codi_emp and (cc.codi_ccu = c.cdeb_ccu or cc.codi_ccu = c.ccre_ccu)
                       WHERE c.codi_emp in ({codi_emp})
                       AND c.data_ccu BETWEEN '{periodo_ini}' and '{periodo_fim}'
                       """
    conn = conecta_dom()
    cursor = conn.cursor()
    cursor.execute(busca_descicoes)
    return cursor.fetchall()


def verifica_periodo_empresa(codi_emp):
    sql_periodo = f"""
        SELECT       
                p.codi_emp,            
                DATEFORMAT(dateadd(day,-1,p.dini_par), 'YYYY-MM-DD') as atualizado_ate,
                DATEFORMAT(p.dini_par, 'YYYY-MM-DD') as abertura_periodo,            
                DATEFORMAT(p.dfin_par, 'YYYY-MM-DD') as abertura_fim,            
                'A' as status,            
                IF p.codi_pad is null THEN p.codi_emp ELSE p.codi_pad end if as codi_emp_plano            
                FROM bethadba.ctparmto p            
                WHERE p.codi_emp = {codi_emp}"""
    conn = conecta_dom()
    cursor = conn.cursor()
    cursor.execute(sql_periodo)
    return cursor.fetchone()


def busca_filiais(codi_emp):
    sql_filiais = f"""
        SELECT CODI_EMP_ATENDIMENTO, nome  FROM bethadba.fofiliais
        WHERE codi_emp in ({codi_emp})
        """
    conn = conecta_dom()
    cursor = conn.cursor()
    cursor.execute(sql_filiais)
    return cursor.fetchall()


#-----------------------------------------------------------------------------------------------------------------------
# **** FUNÇÃO QUE VERIFICA DATA FECHAMENTO ****
#-----------------------------------------------------------------------------------------------------------------------
# def atualizacao_continua_lancametos():
#     todas_as_empresas_geral = busca_todas_empresas_bi()
#
#     for empresa in todas_as_empresas_geral:
#         print("*******************************************************************")
#         print(empresa[0])
#         max_lan = busca_max_data_lancamentos(empresa[0])
#         print(f"EMPRESA ATUALIZADA ATÉ: {max_lan}")
#
#         consulta_empresa = verifica_periodo_empresa(empresa[0])
#         data_fechamento_dominio = data.datetime.strptime(consulta_empresa[2], "%Y-%m-%d").date()
#
#         data_fechamento_compara = data_fechamento_dominio - data.timedelta(hours=24)
#
#         data_fechamento_str = data_fechamento_dominio - data.timedelta(hours=24)
#         data_fechamento_str = str(data_fechamento_str)
#         max_lan_date = data.datetime.strptime(str(max_lan), "%Y-%m-%d").date()
#         print(f"FECHAMENTO DE PERIODO: {data_fechamento_compara}")
#
#         if data_fechamento_compara != max_lan_date:
#             print("Precisa atualizar")
#             try:
#                 busca_lancamentos(empresa[0])
#                 atualiza_status_lancamanetos_empresa(empresa[0])
#                 deleta_dados_lancamentos(empresa[0])
#             except Exception as err:
#                 print(err)
#         # deleta ct_lancamentos
#
#         print("*******************************************************************")


# ---------------------------------------------------
# Orçado, busca de acordo com ano
# ---------------------------------------------------
def consulta_orcado(codi_emp, ano):
    sql_consulta_orcado = f"""
                        SELECT orc.*,cta.nome_cta,cta.tipo_cta, cta.clas_cta,left(cta.clas_cta,2) as mask 
                        FROM bethadba.ctorcado orc 
                        INNER join bethadba.ctcontas cta on cta.codi_emp = orc.codi_emp and cta.codi_cta = orc.conta 
                        WHERE orc.codi_emp = {codi_emp}
                        AND orc.ano = {ano}
                        """
    conn = conecta_dom()
    cursor = conn.cursor()
    cursor.execute(sql_consulta_orcado)

    return cursor.fetchall()


# parte de consulta especialmente para o postgres


def consulta_ct_lancamentos_postgres(codi_emp, periodo_ini, periodo_fim):
    ct_lancamentos = []
    debito = []
    credito = []

    sql_ctlancamentos = """
        select
            codi_emp,
            nume_lan ,
            data_lan,
            vlor_lan,
            cdeb_lan ,
            ccre_lan ,
            codi_his ,
            chis_lan,
            codi_usu,
            orig_lan ,
            ndoc_lan,
            fili_lan ,
            origem_reg ,
            codi_lote,
            dorig_lan,
            codi_pad,
            conciliado_deb,
            conciliado_cre,
            codi_emp_plano,
            cpf_beneficiario_servico,
            data_lan_busca,
            data_ocorrencia,
            DATA_OCORRENCIA
            from bethadba.ctlancto
            where codi_emp in (  """ + str(codi_emp) + """  )
            AND data_lan BETWEEN '""" + periodo_ini + """' and  '""" + periodo_fim + """'
            """

    conn = conecta_dom()
    cursor = conn.cursor()
    cursor.execute(sql_ctlancamentos)
    ct_lancamentos = cursor.fetchall()
    return ct_lancamentos


def busca_credito_postgres(codi_emp, data_init, data_fim):
    credito = []
    sql_credito = """

    SELECT
        cm.codi_emp,
        cm.nume_lan,
        cm.data_lan,
        (cm.vlor_lan * -1) as vlor_lan,
        cm.ccre_lan as codi_cta,
        cm.chis_lan,
        cm.fili_lan,
        null,
        cm.orig_lan,
        'crédito' as tipo,
        null as desc_ccu
        FROM public.ct_lancamento cm
        WHERE cm.codi_emp in  (""" + str(codi_emp) + """)
        AND cm.data_lan BETWEEN ' """ + str(data_init) + """ ' and '""" + str(data_fim) + """'
        group by codi_emp ,nume_lan, data_lan ,vlor_lan , codi_cta ,chis_lan ,fili_lan ,orig_lan
    """
    conn = conecta_db_postgres()
    cursor = conn.cursor()
    cursor.execute(sql_credito)
    credito = cursor.fetchall()
    return credito


def busca_debito_postgres(codi_emp, data_init, data_fim):
    debito = []
    sql_debito = """
    select 
        cm.codi_emp,
        cm.nume_lan,
        cm.data_lan,
        cm.vlor_lan,
        cm.cdeb_lan as codi_cta,
        cm.chis_lan,
        cm.fili_lan,
        null,
        cm.orig_lan,
        'débito' as tipo,
        null as desc_ccu
        FROM public.ct_lancamento cm
        WHERE cm.codi_emp in  (""" + str(codi_emp) + """)
        AND cm.data_lan BETWEEN ' """ + str(data_init) + """ ' and '""" + str(data_fim) + """'
        group by cm.codi_emp,cm.nume_lan,cm.data_lan,vlor_lan,codi_cta,cm.chis_lan,cm.fili_lan,cm.orig_lan
    """
    conn = conecta_db_postgres()
    cursor = conn.cursor()
    cursor.execute(sql_debito)
    debito = cursor.fetchall()
    return debito


def busca_bi_ctccusto(codi_emp, periodo_ini, periodo_fim):
    conn = conecta_dom()
    cursor = conn.cursor()
    sql_bi_ctccusto = f"""
        select
            l.codi_emp,
            l.nume_ccu,
            c.desc_ccu
        from bethadba.ctclancto l
        inner join bethadba.ctccusto c on (c.codi_emp = l.codi_emp  and c.codi_ccu  = (l.cdeb_ccu + l.ccre_ccu))
        where l.codi_emp in ({codi_emp})
        AND l.data_ccu BETWEEN '{periodo_ini}' and '{periodo_fim}'
    """
    cursor.execute(sql_bi_ctccusto)
    desk_ccu = cursor.fetchall()
    return desk_ccu


# -----------------------------------------------------------------------------------------------------------------------
# ----------------------------------------------------------
#
#   *** PARTE REFERENTE AO SQL ***
#       passar parametro global: 1 - geral, 2 - senfins, 3 - bruneto e 4 - tag
#
# ----------------------------------------------------------
def conecta_db_sql():
    server = ''
    database = ''
    username = ''
    password = ''

    # GERAL
    if server_selecao == 0:
        server = "db-bi-contabil.cuens8xulkur.sa-east-1.rds.amazonaws.com"
        database = 'db_contabil'
        username = 'admin'
        password = 'kvxWRRIKZlTNzGSzf8P6'

    # SENFINS
    if server_selecao == 1:
        server = 'db-bi-semfins.cuens8xulkur.sa-east-1.rds.amazonaws.com'
        database = 'db_contabil'
        username = 'admin'
        password = 'fmyZWJi4G0C1PHpVCPmc'

    # BRUNETTO
    if server_selecao == 2:
        server = 'db-bi-brunetto.cuens8xulkur.sa-east-1.rds.amazonaws.com'
        database = 'db_contabil'
        username = 'admin'
        password = 'YC8wvTFbJCxbsu15KsFK'

    # TAG
    if server_selecao == 3:
        server = 'db-bi-tag.cuens8xulkur.sa-east-1.rds.amazonaws.com'
        database = 'db_contabil'
        username = 'admin'
        password = '2FetJsUgNmHHJXBqKVRE'

    # PROSPECÇÃO
    if server_selecao == 4:
        server = 'bi-prospeccao.cuens8xulkur.sa-east-1.rds.amazonaws.com'
        database = 'db_contabil'
        username = 'admin'
        password = 'Qj0yOzsL13c4rBtbswzV'

    # GNC
    if server_selecao == 5:
        server = 'db-bi-gnc.cuens8xulkur.sa-east-1.rds.amazonaws.com'
        database = 'db_contabil'
        username = 'admin'
        password = 'V0p3kEm4b6NhRU4GzOSm'

    if server_selecao == 6:
        server = 'bi-prospeccao.cuens8xulkur.sa-east-1.rds.amazonaws.com'
        database = 'dbo_contabil'
        username = 'admin'
        password = 'Qj0yOzsL13c4rBtbswzV'

    if server_selecao == 7:
        server = 'bi-construtoras.cuens8xulkur.sa-east-1.rds.amazonaws.com'
        database = 'db_contabil'
        username = 'admin'
        password = 'FTnWKoeAEAv0Lhs8dela'

    if server_selecao == 8:
        server = 'db-bi-contabil-acesso-direto.cuens8xulkur.sa-east-1.rds.amazonaws.com'
        database = 'db_contabil'
        username = 'admin'
        password = 'bjbN1OpBtnUOVbaOme6y'



    try:
        conexao = pyodbc.connect(
            'DRIVER={ODBC Driver 18 for SQL Server};SERVER=' +
            server + ';DATABASE=' + database + ';ENCRYPT=no;UID=' +
            username + ';PWD=' + password + ';port=' + '1433')
        return conexao
    except Exception as err:
        print("Error occurred in making connection …")
        print(err)
        traceback.print_exc()


# ----------------------------------------------------
# Busca todas as empresas do bi_empresas
# ----------------------------------------------------
def busca_todas_empresas_bi():
    busca_bi_empresas_bi = """
        SELECT codi_emp
        FROM dbo.bi_empresa 
        WHERE grupo_lançamento = 'G'
        AND status = 'A'
    """
    conn = conecta_db_sql()
    cursor = conn.cursor()
    cursor.execute(busca_bi_empresas_bi)
    bi_empresas_lista = cursor.fetchall()

    return bi_empresas_lista


# ----------------------------------------------------
# Pesquisa ate que data foi atualizado
# ----------------------------------------------------
def busca_max_data_lancamentos(codi_emp):
    consulta = "select max(data_lan) from dbo.bi_lancamentos where codi_emp =" + str(codi_emp) + "group by codi_emp"
    conn = conecta_db_sql()
    cursor = conn.cursor()
    cursor.execute(consulta)
    max = cursor.fetchone()

    if max:
        return max[0]
    else:
        return datetime.strptime("2000-01-01", "%Y-%m-%d").date()

def consulta_fechamento_oficial(codi_emp):
    consulta = f"select atualizado_ate from dbo.bi_empresa where codi_emp ={codi_emp}"
    conn = conecta_db_sql()
    cursor = conn.cursor()
    cursor.execute(consulta)
    atualizado_ate = cursor.fetchone()

    if atualizado_ate:
        return atualizado_ate[0]
    else:
        return datetime.strptime("2000-01-01", "%Y-%m-%d").date()

# ----------------------------------------------------
# Pesquisa ate que data foi atualizado notas
# ----------------------------------------------------
def busca_max_data_notas(codi_emp):
    consulta = f"select max([data]) from dbo.bi_notas_fiscais  where codi_emp = {codi_emp}  group by codi_emp"
    conn = conecta_db_sql()
    cursor = conn.cursor()
    cursor.execute(consulta)
    max = cursor.fetchone()

    if max:
        return max[0]
    else:
        return datetime.strptime("2000-01-01", "%Y-%m-%d").date()

# ----------------------------------------------------
# Deleta lançamentos, e reimporta
# ----------------------------------------------------

def deleta_dados_lancamentos(codi_emp):
    sql_delete_ct_lancamentos = f"DELETE FROM dbo.ct_lancamento WHERE codi_emp = {codi_emp}"

    conn = conecta_db_sql()
    cursor = conn.cursor()
    cursor.execute(sql_delete_ct_lancamentos)
    cursor.commit()

def deleta_dados_lancamentos_periodo(codi_emp, dataIni, dataFim):
    sql_delete_bi_lancamentos = f"DELETE FROM dbo.bi_lancamentos WHERE codi_emp = {codi_emp} and data_lan BETWEEN  '{dataIni}' and '{dataFim}' "
    conn = conecta_db_sql()
    cursor = conn.cursor()
    cursor.execute(sql_delete_bi_lancamentos)
    cursor.commit()

def deleta_dados_bi_creditodebito(codi_emp):
    sql_dados_bi_creditodebito = """
            DELETE FROM dbo.bi_lancamentos WHERE codi_emp = """ + str(codi_emp)
    conn = conecta_db_sql()
    cursor = conn.cursor()
    cursor.execute(sql_dados_bi_creditodebito)
    cursor.commit()

def deleta_dados_bi_credito_periodo(codi_emp, data_ini, data_fim):
    sql_dados_bicredito = f"""
            DELETE FROM dbo.bi_lancamentos WHERE codi_emp = {codi_emp}
            AND data_lan BETWEEN '{data_ini}' and '{data_fim}'
    """
    conn = conecta_db_sql()
    cursor = conn.cursor()
    cursor.execute(sql_dados_bicredito)
    cursor.commit()

def insere_debito_credto(debito, credito):
    conexao = conecta_db_sql()
    cursor = conexao.cursor()
    cursor.fast_executemany = True

    sql_isere_debito_credito = """
        INSERT INTO dbo.bi_lancamentos( 
           codi_emp, nume_lan, data_lan, vlor_lan, 
           codi_cta, chis_lan, fili_lan, ccu, orig_lan, tipo, desc_ccu
        )
        VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""

    print("*** INSERE DEBITO ***")
    tamanho = 0
    tamanho = len(debito)
    if tamanho >= 56843:

        splited = [debito[i::100] for i in range(100)]
        for lista in splited:
            cursor.executemany(sql_isere_debito_credito, lista)
            cursor.commit()
    else:
        cursor.executemany(sql_isere_debito_credito, debito)
        cursor.commit()

    print("*** INSERE CREDITO ***")
    tamanho = 0
    tamanho = len(credito)
    if tamanho >= 56843:

        splited = [credito[i::100] for i in range(100)]
        for lista in splited:
            cursor.executemany(sql_isere_debito_credito, lista)
            cursor.commit()
    else:
        cursor.executemany(sql_isere_debito_credito, credito)
        cursor.commit()

    conexao.close()


def insere_ct_contas(ct_lancamentos):
    conexao = conecta_db_sql()
    cursor = conexao.cursor()
    cursor.fast_executemany = True

    sql_insert_query = """ 
        INSERT INTO dbo.ct_lancamento( 
           codi_emp,
           nume_lan,
           data_lan,
           vlor_lan,
           cdeb_lan, 
           ccre_lan,
           codi_his,
           chis_lan,
           orig_lan,
           fili_lan
        )
        VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""

    tamanho = 0
    tamanho = len(ct_lancamentos)
    if tamanho >= 56843:
        print("maior ou igual ")
        splited = [ct_lancamentos[i::10] for i in range(10)]
        for lista in splited:
            cursor.executemany(sql_insert_query, lista)
            cursor.commit()
    else:
        cursor.executemany(sql_insert_query, ct_lancamentos)
        cursor.commit()
    conexao.close()


def busca_credito(codi_emp, data_init, data_fim):
    credito = []
    sql_credito = """
    SELECT
        codi_emp,
        nume_lan,
        data_lan,
        (vlor_lan * -1) as vlor_lan,
        ccre_lan,
        chis_lan,
        fili_lan,
        null,
        orig_lan,
        'crédito' as tipo,
        null as desc_ccu
        FROM dbo.ct_lancamento
        WHERE codi_emp in  (""" + str(codi_emp) + """)
        AND data_lan BETWEEN '""" + str(data_init) + """ ' and '""" + str(data_fim) + """'
        group by codi_emp ,nume_lan, data_lan ,vlor_lan , ccre_lan ,chis_lan ,fili_lan ,orig_lan"""
    conn = conecta_db_sql()
    cursor = conn.cursor()
    cursor.execute(sql_credito)
    credito = cursor.fetchall()
    return credito


def busca_debito(codi_emp, data_init, data_fim):
    debito = []
    sql_debito = """
    SELECT 
        codi_emp,
        nume_lan,
        data_lan,
        vlor_lan,
        cdeb_lan,
        chis_lan,
        fili_lan,
        null,
        orig_lan,
        'débito' as tipo,
        null as desc_ccu
    FROM dbo.ct_lancamento
    WHERE codi_emp in (""" + str(codi_emp) + """)
    AND data_lan BETWEEN ' """ + str(data_init) + """ ' and '""" + str(data_fim) + """'
    group by codi_emp,nume_lan,data_lan,vlor_lan,cdeb_lan,chis_lan,fili_lan,orig_lan"""
    conn = conecta_db_sql()
    cursor = conn.cursor()
    cursor.execute(sql_debito)
    debito = cursor.fetchall()
    return debito


def insere_ccu(ccu):
    insere_ccu = """
            INSERT INTO dbo.bi_lancamentos( 
               codi_emp, nume_lan, data_lan, vlor_lan, 
               codi_cta, chis_lan, fili_lan, ccu, orig_lan, tipo, desc_ccu
            )
            VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""
    conexao = conecta_db_sql()
    cursor = conexao.cursor()
    cursor.fast_executemany = True

    tamanho = 0
    tamanho = len(ccu)
    if tamanho >= 1000:
        splited = [ccu[i::10] for i in range(10)]
        for lista in splited:
            cursor.executemany(insere_ccu, lista)
            cursor.commit()


    if tamanho < 1000:
        for ccui in ccu:
            cursor.execute(insere_ccu, ccui)
            conexao.commit()

    # cursor.executemany(insere_ccu, ccu)
    # conexao.commit()
    # conexao.close()
    return 1


def insere_desc_ccu(codi_emp, periodo_ini, periodo_fim):
    busca_descicoes = []
    insere_desc_ccu = """
              INSERT INTO dbo.bi_ctccusto( 
                 codi_emp, codi_ccu, desc_ccu
              )
              VALUES ( ?, ?, ? )"""
    conexao = conecta_db_sql()
    cursor = conexao.cursor()
    cursor.fast_executemany = True

    busca_descicoes = busca_descricoes_ccu(codi_emp, periodo_ini, periodo_fim)

    tamanho = 0
    tamanho = len(busca_descicoes)
    if tamanho >= 10000:
        splited = [busca_descicoes[i::100] for i in range(100)]
        for lista in splited:
            try:
                cursor.executemany(insere_desc_ccu, lista)
                cursor.commit()
            except Exception as err:
                print(err)
    else:
        for lista in busca_descicoes:
            try:
                cursor.execute(insere_desc_ccu, lista)
                cursor.commit()
            except Exception as err:
                print(err)
    conexao.close()
    return "fois"


def busca_insere_ccu(codi_emp, periodo_ini, periodo_fim):
    ccu = []
    ccu = pega_centro_de_custo(codi_emp, periodo_ini, periodo_fim)
    if len(ccu) > 0:
        insere_ccu(ccu)
    else:
        print("* NENHUM CCU DISPONÍVEL *")


def exclui_ccu(codi_emp):
    sql_dados_bi_ccu = f"""
                DELETE FROM dbo.bi_ctccusto WHERE codi_emp = {codi_emp}"""
    conn = conecta_db_sql()
    cursor = conn.cursor()
    cursor.execute(sql_dados_bi_ccu)
    cursor.commit()

def busca_credito(codi_emp, data_init, data_fim):
    credito = []
    sql_credito = """
    SELECT
        codi_emp,
        nume_lan,
        data_lan,
        (vlor_lan * -1) as vlor_lan,
        ccre_lan,
        chis_lan,
        fili_lan,
        null,
        orig_lan,
        'crédito' as tipo,
        null as desc_ccu
        FROM dbo.ct_lancamento
        WHERE codi_emp in  (""" + str(codi_emp) + """)
        AND data_lan BETWEEN '""" + str(data_init) + """ ' and '""" + str(data_fim) + """'
        group by codi_emp ,nume_lan, data_lan ,vlor_lan , ccre_lan ,chis_lan ,fili_lan ,orig_lan"""
    conn = conecta_db_sql()
    cursor = conn.cursor()
    cursor.execute(sql_credito)
    credito = cursor.fetchall()

    return credito


def busca_debito(codi_emp, data_init, data_fim):
    debito = []
    sql_debito = """
    SELECT 
        codi_emp,
        nume_lan,
        data_lan,
        vlor_lan,
        cdeb_lan,
        chis_lan,
        fili_lan,
        null,
        orig_lan,
        'débito' as tipo,
        null as desc_ccu
    FROM dbo.ct_lancamento
    WHERE codi_emp in (""" + str(codi_emp) + """)
    AND data_lan BETWEEN ' """ + str(data_init) + """ ' and '""" + str(data_fim) + """'
    group by codi_emp,nume_lan,data_lan,vlor_lan,cdeb_lan,chis_lan,fili_lan,orig_lan"""
    conn = conecta_db_sql()
    cursor = conn.cursor()
    cursor.execute(sql_debito)
    debito = cursor.fetchall()

    return debito


def insere_ct_contas(ct_lancamentos):
    conexao = conecta_db_sql()
    cursor = conexao.cursor()
    cursor.fast_executemany = True

    sql_insert_query = """ 
        INSERT INTO dbo.ct_lancamento( 
           codi_emp,
           nume_lan,
           data_lan,
           vlor_lan,
           cdeb_lan, 
           ccre_lan,
           codi_his,
           chis_lan,
           orig_lan,
           fili_lan
        )
        VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""

    tamanho = 0
    tamanho = len(ct_lancamentos)
    if tamanho >= 50000:
        splited = [ct_lancamentos[i::100] for i in range(100)]
        for lista in splited:
            cursor.executemany(sql_insert_query, lista)
            cursor.commit()
    else:
        splited = [ct_lancamentos[i::50] for i in range(50)]

        for lista in splited:
            if len(lista) > 0:
                cursor.executemany(sql_insert_query, lista)
                cursor.commit()


# ----------------------------------------------------
#
#  ***** PARTE REFERENTE A CTC LANCTO ******
#
# ----------------------------------------------------
def insere_bi_ctccusto(codi_emp, periodo_ini, periodo_fim):
    conexao = conecta_db_sql()
    cursor = conexao.cursor()
    cursor.fast_executemany = True

    sql_ctclancto = """ INSERT INTO 
                           dbo.bi_ctccusto(codi_emp, codi_ccu, desc_ccu)     
                           VALUES (?, ?, ?)"""

    print("EXCLUI BI_CTCCUSTO")
    sql_ctclancto_delete = f"""  delete from dbo.bi_ctccusto where codi_emp = {codi_emp}  """
    cursor.execute(sql_ctclancto_delete)
    conexao.commit()

    ccu_desk = busca_bi_ctccusto(codi_emp, periodo_ini, periodo_fim)

    li = []
    lii = []
    liii = []
    for ccu in ccu_desk:
        li.append(ccu[0])
        lii.append(ccu[1])
        liii.append(ccu[2])

    ccu_desk_frame = pd.DataFrame({
        'codi_emp' : li,
        'codi_ccu' : lii,
        'desk_ccu' : liii
    })



    ccu_limpo = ccu_desk_frame.drop_duplicates(subset = ['codi_ccu'], keep='last')

    print("INSERE NOVO BI_CTCCUSTO")

    print(len(ccu_limpo.values.tolist()))

    tamanho = 0
    tamanho = len(ccu_limpo.values.tolist())
    if tamanho >= 50000:
        splited = [ccu_limpo.values.tolist()[i::100] for i in range(100)]
        for lista in splited:
            cursor.executemany(sql_ctclancto, lista)
            cursor.commit()
    else:
        splited = [ccu_limpo.values.tolist()[i::50] for i in range(50)]

        for lista in splited:
            if len(lista) > 0:
                cursor.executemany(sql_ctclancto, lista)
                cursor.commit()

# ----------------------------------------------------
#
#  ***** PARTE REFERENTE AO ORÇADO E UPDATE *****
#
# ----------------------------------------------------
def insere_orcado(orcado):
    sql_insre = """
                INSERT INTO dbo.bi_orcado(
                    codi_emp, conta, ano, janeiro, fevereiro, marco, abril,
	                maio, junho, julho, agosto, setembro, outubro, novembro,
	                dezembro, FILIAL, nome_cta, tipo_cta, clas_cta, mask 
                 )
                VALUES (  ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? )
                """
    conexao = conecta_db_sql()
    cursor = conexao.cursor()
    cursor.fast_executemany = True

    if len(orcado) > 0:
        cursor.executemany(sql_insre, orcado)
        conexao.commit()
    else:
        print("** Nenhum Orçado dispoivel **")
    conexao.close()


def apaga_orcado(codi_emp):
    sql_apaga_orcado = f"""
        DELETE FROM dbo.bi_orcado WHERE codi_emp = {codi_emp} 
    """
    conexao = conecta_db_sql()
    cursor = conexao.cursor()
    cursor.execute(sql_apaga_orcado)
    conexao.commit()
    conexao.close()


def pega_mask_depara(codi_emp):
    conexao = conecta_db_sql()
    cursor = conexao.cursor()
    cursor.execute(f"SELECT mask, classe FROM dbo.bi_depara WHERE codi_emp = {codi_emp}")
    select = cursor.fetchall()
    return select


# ----------------------------------------------------
#
#  ***** PARTE REFERENTE AO DEPARA EXCEÇÕES ******
#
# ----------------------------------------------------

def varifica_data_hora(codi_emp, T_stamp):

    data_ultima_atualizacao = consulta_valida_depara_excecoes(codi_emp)
    data_arquivo = datetime.strptime( T_stamp , '%Y-%m-%d %H:%M:%S')


    if data_arquivo > data_ultima_atualizacao:
        print("DATA DO ARQUIVO MAIOR Q NO SQL")
        print(f" Data Arquivo:  {data_arquivo}")
        print(f" Data Banco:  {data_ultima_atualizacao}")
        #insere_data_ultima_atualizacao(codi_emp, data_arquivo)
        return 1

    elif data_arquivo == data_ultima_atualizacao:

        print("DATA DO ARQUIVO IGUAL SQL")
        print(data_arquivo)
        return 0
    else:
        print("DATA DO ARQUIVO MENOR Q SQL")
        print(data_arquivo)
        return 0



def insere_data_ultima_atualizacao(codi_emp, data_ultima_atualizacao):
    existe_verifica = []
    data_arquivo = datetime.strptime(data_ultima_atualizacao, '%Y-%m-%d %H:%M:%S')

    sql_insere = """  
            INSERT INTO dbo.valida_depara_excecoes_plano(codi_emp, data_atualizacao_arquivo)
            VALUES( ?, ?)
     """
    sql_update = f"""  
               UPDATE dbo.valida_depara_excecoes_plano SET data_atualizacao_arquivo = (?) 
               WHERE codi_emp = (?)
        """
    sql_verifica_existencia = f"""SELECT codi_emp FROM dbo.valida_depara_excecoes_plano WHERE codi_emp = {codi_emp} """

    dados = [codi_emp, data_arquivo]

    conexao = conecta_db_sql()
    cursor = conexao.cursor()
    cursor.execute(sql_verifica_existencia)
    existe_verifica = cursor.fetchall()

    if len( existe_verifica ) > 0:
        cursor.execute(sql_update, (data_arquivo, codi_emp,))
        conexao.commit()


    else:
        cursor.execute(sql_insere, dados)
        conexao.commit()



def consulta_valida_depara_excecoes(codi_emp):

    sql_valida = f"""SELECT data_atualizacao_arquivo  FROM  dbo.valida_depara_excecoes_plano WHERE codi_emp = {codi_emp}"""
    conexao = conecta_db_sql()
    cursor = conexao.cursor()
    cursor.execute(sql_valida)
    data_banco = cursor.fetchall()

    if data_banco:
        return data_banco[0][0]
    else:
        return datetime.strptime("2000-01-01 01:01:00", '%Y-%m-%d %H:%M:%S')

def corrige_descri(desc):
    if 'CAIXAS' in desc:
        return desc.replace('CAIXAS', 'CAIXA')

    if 'AMORTIZAÇÃO ACUMULADA' in desc:
        return desc.replace('AMORTIZAÇÃO ACUMULADA', 'AMORTIZAÇÕES ACUMULADAS')

    if 'AMORTIZAÇÕES ACUMULADA' in desc:
        return desc.replace('AMORTIZAÇÕES ACUMULADA', 'AMORTIZAÇÕES ACUMULADAS')

    if 'BANCOS CONTA APLICAÇÂO' in desc:
        return desc.replace('BANCOS CONTA APLICAÇÂO', 'BANCOS CONTA APLICAÇÃO')

    if 'BANCOS CONTA APLICACAO' in desc:
        return desc.replace('BANCOS CONTA APLICACAO', 'BANCOS CONTA APLICAÇÃO')

    if 'TITULOS A RECEBER A LONGO PRAZO' in desc:
        return desc.replace('TITULOS A RECEBER A LONGO PRAZO', 'TÍTULOS A RECEBER A LONGO PRAZOO')

    if 'PARCELAMENTO IMPOSTOS FEDERAIS' in desc:
        return desc.replace('PARCELAMENTO IMPOSTOS FEDERAIS', 'PARCELAMENTO DE IMPOSTOS FEDERAIS')

    if 'PARCELAMENTO DE IMPOSTOS ESTADUAIS' in desc:
        return desc.replace('PARCELAMENTO DE IMPOSTOS ESTADUAIS', 'PARCELAMENTOS DE IMPOSTOS ESTADUAIS')

    if 'PARCELAMENTO DE IMPOSTOS ESTADUAIS' in desc:
        return desc.replace('PARCELAMENTO DE IMPOSTOS ESTADUAIS', 'PARCELAMENTOS DE IMPOSTOS ESTADUAIS')

    if 'PARCELAMENTO DE IMPOSTOS MUNICIPAIS' in desc:
        return desc.replace('PARCELAMENTO DE IMPOSTOS MUNICIPAIS', 'PARCELAMENTOS DE IMPOSTOS MUNICIPAIS')

    return desc


def importa_modelo(codi_emp, arquivo_excel):
    mask_coluna = []
    classe_coluna = []
    descr_coluna = []
    meta_coluna = []
    mascara_coluna = []
    tipo_coluna = []
    codiemp_coluna = []
    empresa_coluna = []

    modelo_ti = pd.read_excel(arquivo_excel, "modelo TI")

    # -----------------------------------------------------
    #   CODI_EMP
    # -----------------------------------------------------
    for codi in modelo_ti['empresa']:
        check = str(codi)
        codiemp_coluna.append(int(codi_emp))
        empresa_coluna.append(str(codi_emp))

    # -----------------------------------------------------
    #   MASK
    # -----------------------------------------------------
    for mask in modelo_ti['mask']:
        mask_str = str(mask)

        mask_str = acrescenta_pontos_mask_dpara(mask_str)
        mask_coluna.append(mask_str)


    # -----------------------------------------------------
    #   CLASSE
    # -----------------------------------------------------
    for classe in modelo_ti['classe']:
        clas_str = str(classe)
        clas_str_sp = clas_str.replace(".", '')
        clas_str_sn = re.sub('[^0-9]', '', clas_str_sp)
        classe_coluna.append(clas_str_sn[0:8])

    # ---------------------------------------------------
    # DESCR
    # ---------------------------------------------------
    for descr in modelo_ti['descr']:
        desc_maiusculo = str(descr).strip().upper()
        descr_coluna.append(corrige_descri(desc_maiusculo))

    # ---------------------------------------------------
    # META
    # ---------------------------------------------------
    for meta in modelo_ti['meta']:
        if meta == 0.0:
            meta_coluna.append(0)
        else:
            meta_ponto = str(meta)
            metal_virgula = meta_ponto.replace(".", ",")
            meta_coluna.append(metal_virgula)

    # ---------------------------------------------------
    # CLASSE
    # ---------------------------------------------------
    for chave in modelo_ti['classe']:
        mascara = ''
        clas_str = str(chave)
        clas_str_sp = clas_str.replace(".", '')
        clas_str_sn = re.sub('[^0-9]', '', clas_str_sp)
        clas_str_sZ_sp = clas_str_sn[0:8]
        mascara = str(codi_emp) + "-" + clas_str_sZ_sp
        mascara_coluna.append(mascara)

    # ---------------------------------------------------
    # Formata tipo
    # ---------------------------------------------------
    for mask in modelo_ti['mask']:
        mask_str = str(mask)
        if mask_str[0] == '4':
            tipo_coluna.append("3")
        else:
            tipo_coluna.append(mask_str[0])

    depara = []
    depara_limpo = []
    depara = list(zip(
        mask_coluna,
        classe_coluna,
        descr_coluna,
        mascara_coluna,
        meta_coluna,
        tipo_coluna,
        empresa_coluna,
        codiemp_coluna
    ))

    for linha in depara:
        if 'nan' not in linha:
            depara_limpo.append(linha)

    insere_depara(depara_limpo, codi_emp)   


def insere_depara(depara, codi_emp):
    conexao = conecta_db_sql()
    cursor = conexao.cursor()

    if consulta_empresa(str(codi_emp)):

        print('EMPRESA JA CADASTRADA !')

    else:

        print('PRECISA CADASTRAR EMPRESA !')
        e_plano = consulta_plano(codi_emp)
        print(f'PLANO DA EMPRESA :{e_plano}')
        nome_empresa = consulta_nome(codi_emp)

        insere_empresa(codi_emp, e_plano, nome_empresa)

        importa_filiais(busca_filiais(codi_emp))

    if consulta_depara(codi_emp):

        print('APARAGA DEPARA')
        apaga_depara(codi_emp)

    else:

        print('NÃO PRECISA APAGAR DEPARA')

    sql_insert_query = """ INSERT INTO dbo.bi_depara( mask, classe,
       descr, chave, meta, tipo, empresa, codi_emp)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?)"""

    for i in depara:
        try:
            cursor.execute(sql_insert_query, i)
            conexao.commit()
        except Exception as err:
            print(err)
    return 1


def insere_excecoes(excecoes, codi_emp):
    conexao = conecta_db_sql()
    cursor = conexao.cursor()

    if (consulta_excecoes(codi_emp)):
        print('PRECISA APAGAR EXCEÇÕES')
        apaga_excecoes(codi_emp)
    else:
        print('NÃO PRECISA APAGAR EXCEÇÕES')

    postgres_insert_query = """ INSERT INTO dbo.bi_excecoes(codi_emp, chave,
    conta, nome, tipo, estrutural, dep )
    VALUES (?, ?, ?, ?, ?, ?, ?)"""
    for i in excecoes:
        try:
            cursor.execute(postgres_insert_query, i)
            conexao.commit()
        except Exception as err:
            print(err)
    conexao.close()
    return 1


def consulta_excecoes(codi_emp):
    select = []
    conexao = conecta_db_sql()
    cursor = conexao.cursor()
    cursor.execute(f"""SELECT * FROM dbo.bi_excecoes WHERE codi_emp = {codi_emp}""")
    select = cursor.fetchall()
    conexao.close()
    if len(select) == 0:
        return 0
    else:
        return 1


def apaga_excecoes(codi_emp):
    conexao = conecta_db_sql()
    cursor = conexao.cursor()
    cursor.execute(f"""DELETE FROM dbo.bi_excecoes WHERE codi_emp ={codi_emp}""")
    conexao.commit()
    conexao.close()


def consulta_empresa(cod_emp):
    select = []
    conexao = conecta_db_sql()
    cursor = conexao.cursor()
    cursor.execute(f"""SELECT * FROM dbo.bi_empresa WHERE codi_emp ={cod_emp}""")
    select = cursor.fetchall()

    if len(select) == 0:
        return 0
    else:
        return 1

def consulta_empresa_status(cod_emp):
    conexao = conecta_db_sql()
    cursor = conexao.cursor()
    cursor.execute(f"""SELECT * FROM dbo.bi_empresa WHERE codi_emp ={cod_emp}""")
    return cursor.fetchone()
    
def insere_empresa(codi_emp, plano, nome_emp):
    conexao = conecta_db_sql()
    cursor = conexao.cursor()
    dados = [
        str(codi_emp),  #codi_emp
        str(nome_emp).upper().replace('.', ''), #nome
        data.datetime.strptime('2000-01-01', "%Y-%m-%d").date(),          # atualizado ate
        data.datetime.strptime('2000-01-01', "%Y-%m-%d").date(),          # fechamento
        'A',               
        data.datetime.strptime('2000-01-01', "%Y-%m-%d").date(),          #
        'G', 
        str(plano)
        ]
    sql_insert_query = """ INSERT INTO dbo.bi_empresa(codi_emp, razao, atualizado_ate,
    abertura_periodo, status, fechamento_periodo, grupo_lançamento, codi_emp_plano )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)"""
    cursor.execute(sql_insert_query, dados)
    conexao.commit()


def consulta_depara(cod_emp):
    select = []
    conexao = conecta_db_sql()
    cursor = conexao.cursor()
    cursor.execute(f"""SELECT * FROM dbo.bi_depara WHERE codi_emp = {cod_emp}""")
    select = cursor.fetchall()

    if len(select) == 0:
        return 0
    else:
        return 1


def apaga_depara(codi_emp):
    conexao = conecta_db_sql()
    cursor = conexao.cursor()
    cursor.execute(f""" DELETE FROM dbo.bi_depara WHERE codi_emp ={codi_emp}""")
    conexao.commit()


def acrescenta_pontos_mask_dpara(mask_str):
    mask_str_dot = ""

    if '.' not in mask_str[1] and len(mask_str) >= 5:
        mask_str_dot = mask_str.replace(".", '')
        mask_str = mask_str_dot[0:3]

        if mask_str[0] in '3':

            mask_str_ = ""
            mask_str_ += mask_str[0] if mask_str[0] != "." else  ''
            mask_str_ += '.' if mask_str[1] != "." else  mask_str[1]
            mask_str_ += mask_str[1] 
            mask_str_ += mask_str[2]
            mask_str = mask_str_

        else:
            mask_str_ = ""
            mask_str_ += mask_str[0] if mask_str[0] != "." else  ''
            mask_str_ += '.' if mask_str[1] != "." else  mask_str[1]
            mask_str_ += mask_str[1] 
            mask_str_ += '.' if mask_str[2] != "." else  mask_str[2]
            mask_str_ += mask_str[2]
            mask_str = mask_str_

    if '.' not in mask_str[1] and len(mask_str) >= 4:
        mask_str_dot = mask_str.replace(".", '')
        mask_str = mask_str_dot[0:2]

        if mask_str[0] == '3':
            mask_str_ = ""
            mask_str_ += mask_str[0] if mask_str[0] != "." else  ''
            mask_str_ += '.' if mask_str[1] != "." else  mask_str[1]
            mask_str_ += mask_str[1] 
            mask_str = mask_str_
                
        else:
            mask_str_ = ""
            mask_str_ += mask_str[0] if mask_str[0] != "." else  ''
            mask_str_ += '.' if mask_str[1] != "." else  mask_str[1]
            mask_str_ += mask_str[1] 
    
    return mask_str

def importa_excecoes(codi_emp, arquivo_excel):
    excecoes = pd.read_excel(arquivo_excel, "Exceções", skiprows=range(0, 1))
    numero_linhas = len(excecoes['Cod Dominio - 6'])

    conta = []
    nome = []
    estrutural = []

    # ---------------------------------------------------
    # Formata Cod Dominio ( conta )
    # ---------------------------------------------------
    for cod6 in excecoes['Cod Dominio - 6']:
        clas_str = str(cod6)
        clas_str_sp = clas_str.replace(".0", '')
        clas_str_sn = re.sub('[^0-9]', '', clas_str)
        conta.append(clas_str_sp[0:8])

    # ---------------------------------------------------
    # Formata Nome
    # ---------------------------------------------------
    for nomeE in excecoes['Nome Dominio']:
        nome.append(nomeE)

    # ---------------------------------------------------
    # Formata Plano Excel ( strutural )
    # ---------------------------------------------------
    for para in excecoes['Para - Plano Excel - 5']:

        clas_str = str(para)
        clas_str_sp = clas_str.replace(".", '')
        clas_str_sn = re.sub('[^0-9]', '', clas_str_sp)
        estrutural.append(clas_str_sn[0:8])

    execoes_lista = [[0 for _ in range(7)] for _ in range(numero_linhas)]

    for linha in range(numero_linhas):
        coluna = 0
        execoes_lista[linha][coluna] = codi_emp
        coluna += 1
        execoes_lista[linha][coluna] = str(codi_emp) + '-' + str(conta[linha])
        coluna += 1
        execoes_lista[linha][coluna] = conta[linha]
        coluna += 1
        execoes_lista[linha][coluna] = nome[linha]
        coluna += 1

        if (estrutural[linha] == '44444444'):
            execoes_lista[linha][coluna] = 3
            coluna += 1
        else:
            # execoes_lista[linha][coluna] = estrutural[linha][0]
            execoes_lista[linha][coluna] = 2
            coluna += 1

        execoes_lista[linha][coluna] = estrutural[linha]
        coluna += 1

        execoes_lista[linha][coluna] = str(codi_emp) + '-' + estrutural[linha]
        coluna += 1

    # ---------------------------------------------------
    # Remove Nan da tabela
    # ---------------------------------------------------
    excecoes_frame = pd.DataFrame(execoes_lista).dropna().drop_duplicates()

    # SUBIR SIMULTANEAMENTE NO POSTGRES E SQLSERVER
    insere_excecoes(excecoes_frame.values.tolist(), codi_emp)
    busca_plano(codi_emp)


# ----------------------------------------------------
#
#  ***** PARTE REFERENTE AO PLANO ******
#
# ----------------------------------------------------
def apaga_plano(cod_emp):
    conexao = conecta_db_sql()
    cursor = conexao.cursor()
    cursor.execute(f"""DELETE FROM dbo.bi_plano WHERE codi_emp = {cod_emp}""")
    conexao.commit()
    conexao.close()

def apaga_plano_test(cod_emp):
    conexao = conecta_db_sql()
    cursor = conexao.cursor()
    cursor.execute(f"""DELETE FROM dbo.bi_plano_test WHERE codi_emp = {cod_emp}""")
    conexao.commit()
    conexao.close()

def busca_plano(codi_emp):
    sql = f""" SELECT *  FROM dbo.bi_plano WHERE codi_emp = {str(codi_emp)} """
    conn = conecta_db_sql()
    cursor = conn.cursor()
    cursor.execute(sql)
    return cursor.fetchall()


def importa_plano():
    todas_as_empresas = busca_todas_empresas_bi()

    for empresa in todas_as_empresas:
        filtro_emp = empresa[0]
        print(filtro_emp)
        codi_emp_int = int(consulta_plano(filtro_emp))
        if codi_emp_int != 0:
            migra_plano(filtro_emp)
        else:
            print(f"PLANO INVALIDO PARA A EMPRESA: {filtro_emp} USA PLANO: {codi_emp_int}")

def importa_plano_test():
    todas_as_empresas = busca_todas_empresas_bi()

    for empresa in todas_as_empresas:
        filtro_emp = empresa[0]
        print(filtro_emp)
        codi_emp_int = int(consulta_plano(filtro_emp))
        if codi_emp_int != 0:
            migra_plano_test(filtro_emp)
        else:
            print(f"PLANO INVALIDO PARA A EMPRESA: {filtro_emp} USA PLANO: {codi_emp_int}")


def importa_plano_umepresa(codi_emp):

    filtro_emp = codi_emp
    print(filtro_emp)
    codi_emp_int = int(consulta_plano(filtro_emp))
    if codi_emp_int != 0:
        migra_plano(filtro_emp)
    else:
        print(f"PLANO INVALIDO PARA A EMPRESA: {filtro_emp} USA PLANO: {codi_emp_int}")

def migra_plano(codi_emp):

    plano = int(consulta_plano(codi_emp))
    codi_emp_int = int(codi_emp)
    plano = int(consulta_plano(codi_emp)) 
    ctcontas = consulta_ct_contas_todas(codi_emp)

    if (plano != codi_emp_int):
        print("*** PLANO COMPARTILHADO ***")
        plano_compartilhado = monta_plano_compartilhado(codi_emp)
        plano_compartilhado_corrigido = exclui_conta_duplicada_pc(codi_emp, plano_compartilhado)
        apaga_plano(codi_emp)
        insere_plano_contas(codi_emp, plano_compartilhado_corrigido)

    else:
        print("*** PLANO PROPRIO !! ***")
        plano_propio = monta_plano_proprio(ctcontas)
        plano_proprio_corrigido = exclui_conta_duplicada(codi_emp, plano_propio)
        apaga_plano(codi_emp)
        insere_plano_contas(codi_emp, plano_proprio_corrigido)

def migra_plano_sem_excecoes(codi_emp):

    plano = int(consulta_plano(codi_emp))
    codi_emp_int = int(codi_emp)
    plano = int(consulta_plano(codi_emp)) 
    ctcontas = consulta_ct_contas_todas(codi_emp)

    if (plano != codi_emp_int):
        print("*** PLANO COMPARTILHADO ***")
        plano_compartilhado = monta_plano_compartilhado(codi_emp)
        plano_compartilhado_corrigido = exclui_conta_duplicada_pc(codi_emp, plano_compartilhado)
        apaga_plano(codi_emp)
        insere_plano_contas_sem_exc(codi_emp, plano_compartilhado_corrigido)

    else:
        print("*** PLANO PROPRIO !! ***")
        plano_propio = monta_plano_proprio(ctcontas)
        plano_proprio_corrigido = exclui_conta_duplicada(codi_emp, plano_propio)
        apaga_plano(codi_emp)
        insere_plano_contas_sem_exc(codi_emp, plano_proprio_corrigido)

def migra_plano_test(codi_emp):
    # verifica se o plano é compartilhado ou próprio
    plano = int(consulta_plano(codi_emp))
    codi_emp_int = int(codi_emp)
    plano = int(consulta_plano(codi_emp))  # ou a empresa que compartilha o plano

    ctcontas = consulta_ct_contas_todas(codi_emp)

    if (plano != codi_emp_int):
        print("*** PLANO COMPARTILHADO ***")
        plano_compartilhado = monta_plano_compartilhado(codi_emp)
        plano_compartilhado_corrigido = exclui_conta_duplicada_pc(codi_emp, plano_compartilhado)
        apaga_plano_test(codi_emp)
        insere_plano_contas_teste(codi_emp, plano_compartilhado_corrigido)

    else:
        print("*** PLANO PROPRIO !! ***")
        plano_propio = monta_plano_proprio(ctcontas)
        plano_proprio_corrigido = exclui_conta_duplicada(codi_emp, plano_propio)
        apaga_plano_test(codi_emp)
        insere_plano_contas_teste(codi_emp, plano_proprio_corrigido)




def monta_plano_compartilhado(codi_emp):
    codi_emp = str(codi_emp)
    print("Empresa : " + str(codi_emp))
    plano_compartilhdo = consulta_plano(codi_emp)
    print("USA PLANO: ")
    print(plano_compartilhdo)

    plano = []
    contas_plano_compartilhado = consulta_ct_contas(
        str(plano_compartilhdo))

    if (len(contas_plano_compartilhado) > 0):
        for linha in contas_plano_compartilhado:
            plano.append([
                codi_emp,
                codi_emp + '-' + str(linha[1]),
                linha[1],
                str(linha[2]),
                linha[3][0],
                linha[3][0:8],
                codi_emp + '-' + linha[3][0:8]
            ])
    return plano


def monta_plano_proprio(ctcontas):
    plano = []

    if (len(ctcontas) > 0) and ctcontas is not None:
        for linha in ctcontas:
            plano.append([
                linha[0],
                str(linha[0]) + '-' + str(linha[1]),
                linha[1],
                str(linha[2]),
                linha[3][0],
                linha[3][0:8],
                str(linha[0]) + '-' + linha[3][0:8]
            ])

    return plano


def exclui_conta_duplicada_pc(codi_emp, plano_compartilhado):
    excecoes = []
    sql_consulta_conta = f"""SELECT conta FROM dbo.bi_excecoes WHERE codi_emp ={codi_emp}"""
    conexao = conecta_db_sql()
    cursor = conexao.cursor()
    cursor.execute(sql_consulta_conta)
    excecoes = cursor.fetchall()

    total = 0
    plano_contas_compartilhado = []

    excecoes_lista = []
    for linha in excecoes:
        excecoes_lista.append(linha[0])

    for p in plano_compartilhado:
        if p[2] not in excecoes_lista:
            total += 1
            plano_contas_compartilhado.append(p)

    print("Total de contas compartilhadas : ")
    print(total)

    return plano_contas_compartilhado


def exclui_conta_duplicada(codi_emp, plano):
    excecoes = []
    # apaga_plano(codi_emp)
    sql_consulta_conta = f"""SELECT conta FROM dbo.bi_excecoes WHERE codi_emp  = {codi_emp}"""
    conexao = conecta_db_sql()
    cursor = conexao.cursor()
    cursor.execute(sql_consulta_conta)
    excecoes = cursor.fetchall()

    total = 0
    plano_contas = []

    excecoes_lista = []
    for linha in excecoes:
        excecoes_lista.append(linha[0])

    for p in plano:
        if p[2] not in excecoes_lista:
            total += 1
            plano_contas.append(p)

    print("Total de contas: ")
    print(total)

    return plano_contas


def insere_plano_contas(codi_emp, plano_corrigido):
    apaga_plano(codi_emp)
    sql_insert_plano = """ INSERT INTO 
                               dbo.bi_plano(codi_emp, chave,
                               conta, nome, tipo, estrutural, dep )     
                               VALUES (?, ?, ?, ?, ?, ?, ?)"""

    sql_consulta_excecoes = f" SELECT * FROM dbo.bi_excecoes WHERE codi_emp  ={codi_emp}"

    conexao = conecta_db_sql()
    cursor = conexao.cursor()
    cursor.fast_executemany = True
    cursor.execute(sql_consulta_excecoes)
    excecoes = cursor.fetchall()

    print("Insere Exceções no Plano!! ")
    for li in excecoes:
        cursor.execute(sql_insert_plano, li)
        conexao.commit()
    print()
    print("Insere Plano !! ")
    # if(len(plano_corrigido) > 7000):
    #     splited = [plano_corrigido[i::200] for i in range(200)]
    #     for lista in splited:
    #         cursor.executemany(sql_insert_plano, lista)
    #         cursor.commit()
    # else:
    for plano in plano_corrigido:
        cursor.execute(sql_insert_plano, plano)
        conexao.commit()



def insere_plano_contas_sem_exc(codi_emp, plano_corrigido):
    apaga_plano(codi_emp)
    sql_insert_plano = """ INSERT INTO 
                               dbo.bi_plano(codi_emp, chave,
                               conta, nome, tipo, estrutural, dep )     
                               VALUES (?, ?, ?, ?, ?, ?, ?)"""

    conexao = conecta_db_sql()
    cursor = conexao.cursor()

    print("Insere Plano !! ")
    for plano in plano_corrigido:
        cursor.execute(sql_insert_plano, plano)
        conexao.commit()


def insere_plano_contas_teste(codi_emp, plano_corrigido):
    apaga_plano_test(codi_emp)
    sql_insert_plano = """ INSERT INTO 
                               dbo.bi_plano_test(codi_emp, chave,
                               conta, nome, tipo, estrutural, dep )     
                               VALUES (?, ?, ?, ?, ?, ?, ?)"""

    sql_consulta_excecoes = f" SELECT * FROM dbo.bi_excecoes WHERE codi_emp  ={codi_emp}"

    conexao = conecta_db_sql()
    cursor = conexao.cursor()
    cursor.fast_executemany = True
    cursor.execute(sql_consulta_excecoes)
    excecoes = cursor.fetchall()

    print("Insere Exceções no Plano!! ")
    for li in excecoes:
        cursor.execute(sql_insert_plano, li)
        conexao.commit()
    print()
    print("Insere Plano !! ")
    # if(len(plano_corrigido) > 1000):
    #     splited = [plano_corrigido[i::10] for i in range(10)]

    #     for lista in splited:
    #         cursor.executemany(sql_insert_plano, lista)
    #         cursor.commit()
    # else:
    for plano in plano_corrigido:
            cursor.execute(sql_insert_plano, plano)
            conexao.commit()


def importa_filiais(filiais):
    sql_insere_filiais = """INSERT INTO dbo.bi_filiais(codi_emp, apel_emp)VALUES (?, ?)"""
    conexao = conecta_db_sql()
    cursor = conexao.cursor()
    if(len(filiais) > 0):
        print("INSERINDO FILIAIS")
        for l in filiais:
            cursor.execute(f"DELETE FROM dbo.bi_filiais WHERE codi_emp = {l[0]}")
            conexao.commit()
   
        try:
            for i in filiais:
                cursor.execute(sql_insere_filiais, i )
                conexao.commit()
        except:
            print("\n Filiar ja cadastrada \n")
    else:
        print("NÃO TEM FILIAIS")

# -----------------------------------------------------------------------------------------------------------------------
# ----------------------------------------------------------
#
#   *** PARTE LANCAMENTOS ***
#
# ----------------------------------------------------------
def busca_lancamentos(codi_emp):
    print("deleta ct_lancamentos")
    deleta_dados_lancamentos(codi_emp)

    deleta_dados_bi_creditodebito(codi_emp)

    # pg.deleta_dados_lancamentos(codi_emp)
    # pg.deleta_dados_bi_creditodebito(codi_emp)

    print("exclui ccu")
    exclui_ccu(codi_emp)

    # pg.exclui_ccu(codi_emp)

    ct_lancamentos = []
    ccu = []

    consulta_empresa = verifica_periodo_empresa(codi_emp)
    data_fechamento_dominio = data.datetime.strptime(consulta_empresa[2], "%Y-%m-%d").date()
    data_fechamento_str = data_fechamento_dominio - data.timedelta(hours=24)
    data_fechamento_str = str(data_fechamento_str)

    ct_lancamentos = consulta_ct_lancamentos(codi_emp, '2000-01-01', data_fechamento_str)
    ccu = pega_centro_de_custo(codi_emp, '2000-01-01', data_fechamento_str)

    if len(ct_lancamentos) > 0:
        print("Insere STAGE : ")
        insere_ct_contas(ct_lancamentos)
        insere_desc_ccu(codi_emp, '2000-01-01', data_fechamento_str)

        print("Insere debito e credito: ")
        credito = busca_credito(codi_emp, '2000-01-01', '2023-09-30')
        debito = busca_debito(codi_emp, '2000-01-01', '2023-09-30')

        insere_debito_credto(debito, credito)

    if len(ccu) > 0:
        print("insere CCU")
        insere_ccu(ccu)
        # pg.insere_ccu(ccu)
        print("ctc_lancto")
        insere_bi_ctccusto(codi_emp, '2000-01-01', data_fechamento_str)



def busca_lancamentos_data_especifica(codi_emp, data_ini, data_fim):
    print("deleta ct_lancamentos")
    #deleta_dados_lancamentos(codi_emp)

    deleta_dados_lancamentos_periodo(codi_emp, data_ini, data_fim)

    print("exclui ccu")
    exclui_ccu(codi_emp)

    ct_lancamentos = []
    ccu = []

    ct_lancamentos = consulta_ct_lancamentos(codi_emp, data_ini, data_fim)
    ccu = pega_centro_de_custo(codi_emp, data_ini, data_fim)

    if len(ct_lancamentos) > 0:
        print("Insere STAGE : ")
        insere_ct_contas(ct_lancamentos)
        insere_desc_ccu(codi_emp, data_ini, data_fim)

        print("Insere debito e credito: ")
        credito = busca_credito(codi_emp, data_ini, data_fim)
        debito = busca_debito(codi_emp, data_ini, data_fim)

        print(f"Linhas DEBITO {len(debito)}")
        print(f"Linhas CREDITO {len(credito)}")

        insere_debito_credto(debito, credito)
        deleta_dados_lancamentos(codi_emp)

    print(len(ccu))
    if len(ccu) > 0:
        print("insere CCU" )
        # filtra_lanccu_ctlancamentos(ccu, ct_lancamentos)
        insere_ccu(ccu)
        print("ctc_lancto")
        insere_bi_ctccusto(codi_emp, data_ini, data_fim)



def filtra_lanccu_ctlancamentos(ccu_lan, ct_lancamentos):
    print("ccu")
    print(len(ccu_lan))
    print("ct_lancto")
    print(len(ct_lancamentos))

    codi_empccu = []
    nume_lan = []
    data_lan = []
    vlor_lan = []
    codi_cta = []
    chis_lan = []
    fili_lan = []
    ccu = []
    orig_lan = []
    tipo = []
    desc_ccu = []

    for c in ccu_lan:
        codi_empccu.append(c[0])
        nume_lan.append(int(c[1]))
        data_lan.append(c[2])
        vlor_lan.append(c[3])
        codi_cta.append(c[4])
        chis_lan.append(c[5])
        fili_lan.append(c[6])
        ccu.append(c[7])
        orig_lan.append(c[8])
        tipo.append(c[9])
        desc_ccu.append(c[10])

    ccu_frame = pd.DataFrame({
        'codi_emp'  : codi_empccu,
        'nume_lan'  : nume_lan,
        'data_lan ' : data_lan,
        'vlor_lan'  : vlor_lan,
        'codi_cta'  : codi_cta,
        'chis_lan'  : chis_lan,
        'fili_lan'  : fili_lan,
        'ccu'       : ccu,
        'orig_lan'  : orig_lan,
        'tipo'      : tipo,
        'desc_ccu'  : desc_ccu,
    })

    codi_emp = []
    nume_lan = []
    data_lan = []
    vlor_lan = []
    cdeb_lan = []
    ccre_lan = []
    codi_his = []
    chis_lan = []
    orig_lan = []
    fili_lan = []

    for l in ct_lancamentos:
        codi_emp.append(l[0])
        nume_lan.append(l[1])
        data_lan.append(l[2])
        vlor_lan.append(l[3])
        cdeb_lan.append(l[4])
        ccre_lan.append(l[5])
        codi_his.append(l[6])
        chis_lan.append(l[7])
        orig_lan.append(l[8])
        fili_lan.append(l[9])

    ctlan_frame = pd.DataFrame({
        'codi_emp' : codi_emp,
        'nume_lan' : nume_lan,
        'data_lan' : data_lan,
        'vlor_lan' : vlor_lan,
        'cdeb_lan' : cdeb_lan,
        'ccre_lan' : ccre_lan,
        'codi_his' : codi_his,
        'chis_lan' : chis_lan,
        'orig_lan' : orig_lan,
        'fili_lan' : fili_lan,
    })


    result = pd.merge(ccu_frame,ctlan_frame['nume_lan'], on='nume_lan', how='inner' )

    print(len(result.values))  
    print(len(ccu_frame.values))  

    insere_ccu = """
            INSERT INTO dbo.bi_lancamentos( 
               codi_emp, nume_lan, data_lan, vlor_lan, 
               codi_cta, chis_lan, fili_lan, ccu, orig_lan, tipo, desc_ccu
            )
            VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""
    conexao = conecta_db_sql()
    cursor = conexao.cursor()
    cursor.fast_executemany = True


    for ah in ccu_frame.values: 
        if ah[1] in ctlan_frame['nume_lan']:
            print("INSERE")
            print(ah)
            cursor.execute(insere_ccu, list(ah))
            conexao.commit()
        else:
            print(f"Não : {ah[1]}")

def busca_lancamentos_incremental(codi_emp, data_ini, data_fim):

    ct_lancamentos = []
    ccu = []

    ct_lancamentos = consulta_ct_lancamentos(codi_emp, str(data_ini), str(data_fim))
    ccu = pega_centro_de_custo(codi_emp, str(data_ini), str(data_fim))

    # if len(ct_lancamentos) > 0:
    #     print("Insere STAGE : ")
    #     insere_ct_contas(ct_lancamentos)
    #     insere_desc_ccu(codi_emp, str(data_ini), str(data_fim))
    #
    #     print("Insere debito e credito: ")
    #     credito = busca_credito(codi_emp, str(data_ini), str(data_fim))
    #     debito = busca_debito(codi_emp, str(data_ini), str(data_fim))
    #
    #     insere_debito_credto(debito, credito)
    #
    # if len(ccu) > 0:
    #     print("insere CCU")
    #     insere_ccu(ccu)
    #     print("ctc_lancto")
    #     insere_bi_ctccusto(codi_emp, str(data_ini), str(data_fim))
    #

# ----------------------------------------------------
#  Atualiza status da tabela bi_empresa
# ----------------------------------------------------
def atualiza_status_lancamanetos_empresa(codi_emp, abertura_periodo ,abertura_periodo_trabalho, fechamento_periodo_trabalho):

    sql_update = f"""
                    UPDATE dbo.bi_empresa SET atualizado_ate = (?), abertura_periodo = (?), fechamento_periodo = (?) 
                    WHERE codi_emp = (?)
                """


    print(f"  DATA PERIODO FECHADO: {abertura_periodo}")
    print(f"  DATA ABERTURA PERIODO TRABALHO: {abertura_periodo_trabalho}")
    print(f"  DATA FECHAMENTO PERIODO TRABALHO: {fechamento_periodo_trabalho}")
    conexao = conecta_db_sql()
    cursor = conexao.cursor()
    cursor.execute(sql_update, (abertura_periodo, abertura_periodo_trabalho, fechamento_periodo_trabalho, codi_emp,))
    conexao.commit()


# -----------------------------------------------------------------------------------------------------------------------
# ----------------------------------------------------------
#
#   *** PARTE REFERENTE AS CONSULTAS DA B3 ***
#
# ----------------------------------------------------------
def consulta_b3(data_hj):
    data_consulta = str(data_hj)

    # Investimentos
    SELIC = sgs.get({"selic": 432}, start=data_consulta).values;
    CDI = sgs.get({"CDI": 12}, start=data_consulta).values;
    IGPM = sgs.get({"igpm": 189}, start=data_consulta).values;
    IPCA = sgs.get({"ipca": 433}, start=data_consulta).values;
    TBAN = sgs.get({"TBAN": 423}, start=data_consulta).values;
    IBOVESPA = sgs.get({"IBOVESPA (%)": 7832}, start=data_consulta).values;
    TBC = sgs.get({"TBC": 422}, start=data_consulta).values;
    SALARIO_MINIMO = sgs.get({"SALARIO MÍNIMO": 1619}, start=data_consulta).values;

    # moedas extrangeiras
    moedas = consulta_cotacoes(data_hj)
    USD = moedas[0][0]
    EUR = moedas[0][1]
    GBP = moedas[0][2]

    # bitcoin
    BTC_USD = get_btc(data_hj);
    ETH_USD = get_btc(data_hj);

    BTC_BRL = round(BTC_USD * USD, 2);
    ETH_BRL = round(ETH_USD * USD, 2);

    export_indicesB3 = [];

    export_indicesB3.append([
        USD,
        EUR,
        GBP,
        BTC_BRL,
        ETH_BRL,
        float(ETH_USD),
        float(BTC_USD),
        IBOVESPA[0][0],
        CDI[0][0],
        SELIC[0][0],
        IPCA[0][0],
        IGPM[0][0],
        float(SALARIO_MINIMO[0][0])
    ])

    return export_indicesB3


def consulta_cotacoes(data_hj):
    data_consulta = str(data_hj)
    data_ontem = data_hj - timedelta(days=1)
    cotacoes = currency.get(['USD', 'EUR', 'GBP'], start=data_ontem, end=data_consulta)
    return cotacoes.values


def get_btc(data_hj):
    data_consulta = str(data_hj)
    data_ontem = data_hj - timedelta(days=1)

    product_id = 'BTC-USD'
    url = f"https://api.exchange.coinbase.com/products/{product_id}/candles"

    granularity = 86400
    start_date = "2023-11-06"
    end_date = "2023-11-07"

    params = {
        "start": data_ontem,
        "end": data_consulta,
        "granularity": granularity
    }

    # Define request headers
    headers = {"content-type": "application/json"}

    data = requests.get(url, params=params, headers=headers)
    data = data.json()

    columns = ['timestamp', "low", "high", "open", "close", "volume"]

    df = pd.DataFrame(data=data, columns=columns)

    df['datetime'] = pd.to_datetime(df['timestamp'], unit='s')
    df.set_index('datetime', inplace=True)
    df.drop('timestamp', axis=1, inplace=True)
    max_value_btc = df['high'].max()

    # print(f"Max BTC price between {start_date[:10]} and {end_date[:10]} was ${max_value_btc}")
    return max_value_btc


def get_eth(data_hj):
    data_consulta = str(data_hj)
    data_ontem = data_hj - timedelta(days=1)

    product_id = 'ETH-USD'
    url = f"https://api.exchange.coinbase.com/products/{product_id}/candles"

    granularity = 86400
    start_date = "2023-11-06"
    end_date = "2023-11-07"

    params = {
        "start": data_ontem,
        "end": data_consulta,
        "granularity": granularity
    }

    # Define request headers
    headers = {"content-type": "application/json"}

    data = requests.get(url, params=params, headers=headers)
    data = data.json()

    columns = ['timestamp', "low", "high", "open", "close", "volume"]

    df = pd.DataFrame(data=data, columns=columns)

    df['datetime'] = pd.to_datetime(df['timestamp'], unit='s')
    df.set_index('datetime', inplace=True)
    df.drop('timestamp', axis=1, inplace=True)
    max_value_eth = df['high'].max()

    # print(f"Max ETH price between {start_date[:10]} and {end_date[:10]} was ${max_value_eth}")
    return max_value_eth


def importa_indices_rds(consulta_b3):
    conexao = conecta_db_sql()
    cursor = conexao.cursor()
    sql_bi_indices = """
         INSERT INTO  dbo.bi_indices(usd_brl, eur_brl, gbp_brl, btc_brl, eth_brl, eth_usd, btc_usd, ibovespa, cdi, selic, ipca, igpm, salario_minimo)
         VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
    cursor.execute(sql_bi_indices, consulta_b3[0])
    conexao.commit()


# -----------------------------------------------------------------------------------------------------------------------
# ----------------------------------------------------------
#
#   *** PARTE REFERENTE AO BACKUP DE BANCO ***
#
# ----------------------------------------------------------
def insere_os_balancos():
    balanco_b2 = []
    balanco_b1 = []
    balanco_b0 = []

    balanco_b0.append([1, 'Ativo', 1])
    balanco_b0.append([2, 'Passivo e Patrimonio Liquido', 2])

    balanco_b1.append([2, '1.3', 'Ativo Não Circulante', 1])
    balanco_b1.append([3, '1.4', 'Realizável a Longo Prazo', 1])
    balanco_b1.append([4, '1.5', 'Investimentos', 1])
    balanco_b1.append([5, '1.6', 'Imobilizado', 1])
    balanco_b1.append([6, '1.7', 'Intangível', 1])
    balanco_b1.append([7, '2.1', 'Passivo Circulante', 2])
    balanco_b1.append([8, '2.2', 'Passivo Não Circulante', 2])
    balanco_b1.append([9, '2.3', 'Patrimônio Líquido', 2])
    balanco_b1.append([1, '1.2', 'Ativo Circulante', 1])
    balanco_b1.append([10, '3.1', 'Resultado do Exercicio', 2])

    balanco_b2.append([25, '2.1.1', 'Fornecedores Nacionais', '2.1'])
    balanco_b2.append([34, '2.2.3', 'Financiamentos e Emprestimos', '2.2'])
    balanco_b2.append([35, '2.2.4', 'Impostos Parcelados', '2.2'])
    balanco_b2.append([36, '2.2.5', 'Pessoas Ligadas', '2.2'])
    balanco_b2.append([37, '2.2.6', 'Provisoes de Perdas', '2.2'])
    balanco_b2.append([38, '2.2.7', 'Impostos Diferidos', '2.2'])
    balanco_b2.append([39, '2.2.8', 'Outras Obrigacoes', '2.2'])
    balanco_b2.append([40, '2.3.1', 'Capital Social', '2.3'])
    balanco_b2.append([41, '2.3.2', 'AFAC', '2.3'])

    balanco_b2.append([42, '2.3.3', 'Reservas', '2.3'])
    balanco_b2.append([43, '2.3.4', 'Ajustes de Exercicios Anteriores', '2.3'])
    balanco_b2.append([44, '2.3.5', 'Lucros ou Prejuizos ', '2.3'])
    balanco_b2.append([45, '2.3.6', 'Lucros Distribuidos', '2.3'])
    balanco_b2.append([22, '1.6.4', 'Bens em Comodato', '1.6'])
    balanco_b2.append([46, '2.3.7', 'Ajuste de Avaliação Patrimonial', '2.3'])


# ----------------------------------------------------------------------------------------------------------------------
# ----------------------------------------------------------
#
#   *** PARTE REFERENTE DO CONTOLE ***
#
# ----------------------------------------------------------
def importa_notas(codi_emp, data_ini, data_fim):

    notasDominio = []
    sql_notas = f"""
                    SELECT ent.codi_emp,ent.ddoc_ent as data,COUNT(ent.ddoc_ent) as quantidade,'ENTRADAS' as tipo
                    FROM bethadba.efentradas ent
                    WHERE ent.codi_emp IN ({codi_emp})
                    AND ent.ddoc_ent BETWEEN '2017-01-01' and '2023-09-30'
                    GROUP BY ent.codi_emp,ent.ddoc_ent
                    union 
                    SELECT sai.codi_emp,sai.ddoc_sai as data,COUNT(sai.ddoc_sai) as quantidade,'SAIDAS' AS tipo
                    FROM bethadba.efsaidas sai
                    WHERE sai.codi_emp IN ({codi_emp})
                    AND sai.ddoc_sai BETWEEN '{data_ini}' and '{data_fim}'
                    GROUP by sai.codi_emp,sai.ddoc_sai
                    order by 1,2
                """

    sql_notas_insert = f"""
                        INSERT INTO db_contabil.dbo.bi_notas_fiscais(codi_emp,  [data], quantidade, TIPO)
                        VALUES(?, ?, ?, ?)
                        """

    sql_delete_periodo = f""" 
                          DELETE 
                          FROM db_contabil.dbo.bi_notas_fiscais 
                          WHERE codi_emp = {codi_emp} 
                          AND [data] BETWEEN '{data_ini}' and '{data_fim}'
                          """

    domm = conecta_dom()
    cursorDom = domm.cursor()
    cursorDom.execute(sql_notas)
    notasDominio = cursorDom.fetchall()
    # return notas

    conexao = conecta_db_sql()
    cursor = conexao.cursor()
    cursor.fast_executemany = True

    cursor.execute(sql_delete_periodo)
    conexao.commit()

    print(len(notasDominio))
    tamanho = 0
    tamanho = len(notasDominio)
    if tamanho >= 1000:
        splited = [notasDominio[i::100] for i in range(100)]
        for lista in splited:
            cursor.executemany(sql_notas_insert, lista)
            cursor.commit()
    else:
        splited = [notasDominio[i::50] for i in range(50)]

        for lista in splited:
            if len(lista) > 0:
                cursor.executemany(sql_notas_insert, lista)
                cursor.commit()

# ----------------------------------------------------------------------------------------------------------------------
# ----------------------------------------------------------
#
#   *** PARTE REFERENTE DO CONTOLE ***
#
# ----------------------------------------------------------

def importa_planos_todos():
    print(busca_todas_empresas_bi())
    importa_plano()

def importa_planos_todos_teste():
    print(busca_todas_empresas_bi())
    importa_plano_test()


def importa_indicesB3():
    data_hj = date.today()
    print(data_hj)
    B3_HJ = consulta_b3(data_hj)
    print(B3_HJ)
    importa_indices_rds(B3_HJ)

def busca_ultima_linhaB3():
    sqlUltimaLinha = f"""
                       SELECT *  FROM dbo.bi_indices ORDER BY data_atualizacao DESC
                     """
    conexao = conecta_db_sql()
    cursor = conexao.cursor()
    cursor.execute(sqlUltimaLinha)
    antiga = cursor.fetchone()
    insere_novaLinha = []

    insere_novaLinha.append([
        antiga[0],
        antiga[1],
        antiga[2],
        antiga[3],
        antiga[4],
        antiga[5],
        antiga[6],
        antiga[7],
        antiga[8],
        antiga[9],
        antiga[10],
        antiga[11],
        antiga[12],
    ])

    sql_bi_indices = """
         INSERT INTO  dbo.bi_indices(usd_brl, eur_brl, gbp_brl, btc_brl, eth_brl, eth_usd, btc_usd, ibovespa, cdi, selic, ipca, igpm, salario_minimo)
         VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
    # cursor.execute(sql_bi_indices, insere_novaLinha[0])
    # conexao.commit()
    return  insere_novaLinha

def consulta_ultima_linha_B3():
    global server_selecao
    for i in [0, 1, 2, 3, 6, 5]:
        server_selecao = i
        try:
            importa_indicesB3()
        except Exception as err:
            print("erro na busca B3: REIMPORTANDO DIA ANTERIOR")
            print(busca_ultima_linhaB3())
            print(err)

# ----------------------------------------------------------
#   *** INDIFERENTE DO BANCO, SOBE O DEPARA E EXCEÇÕES ***
# ----------------------------------------------------------
def insere_um_arquivo_depara(codi_emp, nome_arquivo):
    arquivo_depara_original = fr"C:\Users\pablo.britto\Desktop\{nome_arquivo}.xlsx"
    arquivo_excel = pd.ExcelFile(arquivo_depara_original, engine='openpyxl')
    importa_modelo(codi_emp, arquivo_excel)
    importa_excecoes(codi_emp, arquivo_excel)
    migra_plano(codi_emp)
    print(arquivo_depara_original)

# ----------------------------------------------------------
#   *** FILTRA EMPRESAS DIFERENTES DO BANCO ***
# ----------------------------------------------------------
def empresas_naopertercentes(banco):
    global server_selecao
    elas_não = []
    BANCOS = [0,1,2,3,6,5]
    SUB = set([banco])
    dbs = [i for i in BANCOS if i not in SUB]

    todas_empresas = []

    for i in dbs:
        server_selecao = i
        for j in busca_todas_empresas_bi():
            todas_empresas.append(j[0])
    return todas_empresas

# ----------------------------------------------------------
#   *** LISTA TODOS ARQIVOS E COMPARA COM DATA EM BANCO ***
# ----------------------------------------------------------
def lista_arquivos_depara_geral():
    global server_selecao
    server_selecao = 0

    elas_não = empresas_naopertercentes(server_selecao)

    deparas = fr"R:\Processos Internos\Contabilidade\Controles Internos\COORDENAÇÃO\14. REGRAS TI\PROJETO BI\Contabil\De-Para"

    arquivos = os.walk(deparas)

    for raiz, pasta, excel_arquivo in arquivos:
        for depara_excecoes in excel_arquivo:
            arquivo_depara_original = os.path.join(deparas, depara_excecoes)

            if len( gb.glob( arquivo_depara_original)) > 0 :
                try:
                    if re.search('\\b.xlsx\\b', arquivo_depara_original, re.IGNORECASE):

                        print("\n")

                        data_alteracao = os.path.getmtime(arquivo_depara_original)
                        data_alteracao_formatado =   time.ctime( data_alteracao )

                        t_obj = time.strptime(data_alteracao_formatado)
                        T_stamp = time.strftime("%Y-%m-%d %H:%M:%S", t_obj)

                        codi_emp = str(depara_excecoes).split()
                        arquivo_excel = pd.ExcelFile(arquivo_depara_original, engine='openpyxl')
                        print(arquivo_depara_original)
                        print(codi_emp[0])
                        server_selecao = 0
                        if( codi_emp[0] not in elas_não ):
                            if varifica_data_hora(codi_emp[0], T_stamp):
                                print("Atualiza")
                                importa_modelo(codi_emp[0], arquivo_excel)
                                importa_excecoes(codi_emp[0], arquivo_excel)
                                migra_plano(codi_emp[0])
                                insere_data_ultima_atualizacao(codi_emp[0], T_stamp)

                                # on_message(codi_emp[0])

                except Exception as err:
                    print(f"Error de permissão ou arquivo aberto :{excel_arquivo}")
                    print(err)



def lista_arquivos_depara_senfins():
    global server_selecao
    server_selecao = 1
    elas_não = empresas_naopertercentes(server_selecao)
    deparas = fr"R:\Processos Internos\Contabilidade\Controles Internos\COORDENAÇÃO\14. REGRAS TI\PROJETO BI\Contabil\De-Para\SEMFINS"

    arquivos = os.walk(deparas)

    for raiz, pasta, excel_arquivo in arquivos:
        for depara_excecoes in excel_arquivo:
            arquivo_depara_original = os.path.join(deparas, depara_excecoes)

            if len( gb.glob( arquivo_depara_original)) > 0 :
                try:
                    if re.search('\\b.xlsx\\b', arquivo_depara_original, re.IGNORECASE):

                        print("\n")

                        data_alteracao = os.path.getmtime(arquivo_depara_original)
                        data_alteracao_formatado =   time.ctime( data_alteracao )

                        t_obj = time.strptime(data_alteracao_formatado)
                        T_stamp = time.strftime("%Y-%m-%d %H:%M:%S", t_obj)

                        codi_emp = str(depara_excecoes).split()
                        arquivo_excel = pd.ExcelFile(arquivo_depara_original, engine='openpyxl')
                        print(arquivo_depara_original)
                        print(codi_emp[0])
                        server_selecao = 1
                        if( codi_emp[0] not in elas_não ):
                            if varifica_data_hora(codi_emp[0], T_stamp):
                                importa_modelo(codi_emp[0], arquivo_excel)
                                importa_excecoes(codi_emp[0], arquivo_excel)
                                migra_plano(codi_emp[0])
                                insere_data_ultima_atualizacao(codi_emp[0], T_stamp)

                except Exception as err:
                    print(f"Error de permissão ou arquivo aberto :{excel_arquivo}")
                    print(err)

def lista_arquivos_depara_brunetto():
    global server_selecao
    server_selecao = 2
    elas_não = empresas_naopertercentes(server_selecao)
    deparas = fr"R:\Processos Internos\Contabilidade\Controles Internos\COORDENAÇÃO\14. REGRAS TI\PROJETO BI\Contabil\De-Para\BRUNETTO"

    arquivos = os.walk(deparas)

    for raiz, pasta, excel_arquivo in arquivos:
        for depara_excecoes in excel_arquivo:
            arquivo_depara_original = os.path.join(deparas, depara_excecoes)

            if len( gb.glob( arquivo_depara_original)) > 0 :
                try:
                    if re.search('\\b.xlsx\\b', arquivo_depara_original, re.IGNORECASE):

                        print("\n")

                        data_alteracao = os.path.getmtime(arquivo_depara_original)
                        data_alteracao_formatado =   time.ctime( data_alteracao )

                        t_obj = time.strptime(data_alteracao_formatado)
                        T_stamp = time.strftime("%Y-%m-%d %H:%M:%S", t_obj)

                        codi_emp = str(depara_excecoes).split()
                        arquivo_excel = pd.ExcelFile(arquivo_depara_original, engine='openpyxl')
                        print(arquivo_depara_original)
                        print(codi_emp[0])
                        server_selecao = 2
                        if( codi_emp[0] not in elas_não ):
                            if varifica_data_hora(codi_emp[0], T_stamp):

                                importa_modelo(codi_emp[0], arquivo_excel)
                                importa_excecoes(codi_emp[0], arquivo_excel)
                                migra_plano(codi_emp[0])
                                insere_data_ultima_atualizacao(codi_emp[0], T_stamp)

                except Exception as err:
                    print(f"Error de permissão ou arquivo aberto :{excel_arquivo}")
                    print(err)



def lista_arquivos_depara_tag():
    global server_selecao
    server_selecao = 3
    elas_não = empresas_naopertercentes(server_selecao)
    deparas = fr"R:\Processos Internos\Contabilidade\Controles Internos\COORDENAÇÃO\14. REGRAS TI\PROJETO BI\Contabil\De-Para\TAG"

    arquivos = os.walk(deparas)

    for raiz, pasta, excel_arquivo in arquivos:
        for depara_excecoes in excel_arquivo:
            arquivo_depara_original = os.path.join(deparas, depara_excecoes)

            if len( gb.glob( arquivo_depara_original)) > 0 :
                try:
                    if re.search('\\b.xlsx\\b', arquivo_depara_original, re.IGNORECASE):

                        print("\n")

                        data_alteracao = os.path.getmtime(arquivo_depara_original)
                        data_alteracao_formatado =   time.ctime( data_alteracao )

                        t_obj = time.strptime(data_alteracao_formatado)
                        T_stamp = time.strftime("%Y-%m-%d %H:%M:%S", t_obj)

                        codi_emp = str(depara_excecoes).split()
                        arquivo_excel = pd.ExcelFile(arquivo_depara_original, engine='openpyxl')
                        print(arquivo_depara_original)
                        print(codi_emp[0])
                        server_selecao = 3
                        if( codi_emp[0] not in elas_não ):
                            if varifica_data_hora(codi_emp[0], T_stamp):
                                importa_modelo(codi_emp[0], arquivo_excel)
                                importa_excecoes(codi_emp[0], arquivo_excel)
                                migra_plano(codi_emp[0])
                                insere_data_ultima_atualizacao(codi_emp[0], T_stamp)

                except Exception as err:
                    print(f"Error de permissão ou arquivo aberto :{excel_arquivo}")
                    print(err)


def lista_arquivos_depara_gnc():
    global server_selecao
    server_selecao = 5
    elas_não = empresas_naopertercentes(server_selecao)
    deparas = fr"R:\Processos Internos\Contabilidade\Controles Internos\COORDENAÇÃO\14. REGRAS TI\PROJETO BI\Contabil\De-Para\GNC"

    arquivos = os.walk(deparas)

    for raiz, pasta, excel_arquivo in arquivos:
        for depara_excecoes in excel_arquivo:
            arquivo_depara_original = os.path.join(deparas, depara_excecoes)

            if len( gb.glob( arquivo_depara_original)) > 0 :
                try:
                    if re.search('\\b.xlsx\\b', arquivo_depara_original, re.IGNORECASE):

                        print("\n")

                        data_alteracao = os.path.getmtime(arquivo_depara_original)
                        data_alteracao_formatado =   time.ctime( data_alteracao )

                        t_obj = time.strptime(data_alteracao_formatado)
                        T_stamp = time.strftime("%Y-%m-%d %H:%M:%S", t_obj)

                        codi_emp = str(depara_excecoes).split()
                        arquivo_excel = pd.ExcelFile(arquivo_depara_original, engine='openpyxl')
                        print(arquivo_depara_original)
                        server_selecao = 5
                        if( codi_emp[0] not in elas_não ):
                            if varifica_data_hora(codi_emp[0], T_stamp):
                                importa_modelo(codi_emp[0], arquivo_excel)
                                importa_excecoes(codi_emp[0], arquivo_excel)
                                migra_plano(codi_emp[0])
                                insere_data_ultima_atualizacao(codi_emp[0], T_stamp)

                except Exception as err:
                    print(f"Error de permissão ou arquivo aberto :{excel_arquivo}")
                    print(err)


def lista_arquivos_depara_tecnova():
    global server_selecao
    server_selecao = 8

    elas_não = empresas_naopertercentes(server_selecao)

    deparas = fr"R:\Processos Internos\Contabilidade\Controles Internos\COORDENAÇÃO\14. REGRAS TI\PROJETO BI\Contabil\De-Para\TECNOVA"

    arquivos = os.walk(deparas)

    for raiz, pasta, excel_arquivo in arquivos:
        for depara_excecoes in excel_arquivo:
            arquivo_depara_original = os.path.join(deparas, depara_excecoes)

            if len( gb.glob( arquivo_depara_original)) > 0 :
                try:
                    if re.search('\\b.xlsx\\b', arquivo_depara_original, re.IGNORECASE):

                        print("\n")

                        data_alteracao = os.path.getmtime(arquivo_depara_original)
                        data_alteracao_formatado =   time.ctime( data_alteracao )

                        t_obj = time.strptime(data_alteracao_formatado)
                        T_stamp = time.strftime("%Y-%m-%d %H:%M:%S", t_obj)

                        codi_emp = str(depara_excecoes).split()
                        arquivo_excel = pd.ExcelFile(arquivo_depara_original, engine='openpyxl')
                        print(arquivo_depara_original)
                        print(codi_emp[0])
                        server_selecao = 8
                        if( codi_emp[0] not in elas_não ):
                            if varifica_data_hora(codi_emp[0], T_stamp):
                                print("Atualiza")
                                importa_modelo(codi_emp[0], arquivo_excel)
                                importa_excecoes(codi_emp[0], arquivo_excel)
                                migra_plano(codi_emp[0])
                                insere_data_ultima_atualizacao(codi_emp[0], T_stamp)

                                # on_message(codi_emp[0])

                except Exception as err:
                    print(f"Error de permissão ou arquivo aberto :{excel_arquivo}")
                    print(err)




def lista_arquivos_depara_construtoras():
    global server_selecao
    server_selecao = 7
    elas_não = empresas_naopertercentes(server_selecao)
    deparas = fr"R:\Processos Internos\Contabilidade\Controles Internos\COORDENAÇÃO\14. REGRAS TI\PROJETO BI\Contabil\De-Para\CONSTRUTORAS"

    arquivos = os.walk(deparas)

    for raiz, pasta, excel_arquivo in arquivos:
        for depara_excecoes in excel_arquivo:
            arquivo_depara_original = os.path.join(deparas, depara_excecoes)

            if len( gb.glob( arquivo_depara_original)) > 0 :
                try:
                    if re.search('\\b.xlsx\\b', arquivo_depara_original, re.IGNORECASE):

                        print("\n")

                        data_alteracao = os.path.getmtime(arquivo_depara_original)
                        data_alteracao_formatado =   time.ctime( data_alteracao )

                        t_obj = time.strptime(data_alteracao_formatado)
                        T_stamp = time.strftime("%Y-%m-%d %H:%M:%S", t_obj)

                        codi_emp = str(depara_excecoes).split()
                        arquivo_excel = pd.ExcelFile(arquivo_depara_original, engine='openpyxl')
                        print(arquivo_depara_original)
                        server_selecao = 7
                        if( codi_emp[0] not in elas_não ):
                            if varifica_data_hora(codi_emp[0], T_stamp):
                                importa_modelo(codi_emp[0], arquivo_excel)
                                importa_excecoes(codi_emp[0], arquivo_excel)
                                migra_plano(codi_emp[0])
                                insere_data_ultima_atualizacao(codi_emp[0], T_stamp)

                except Exception as err:
                    print(f"Error de permissão ou arquivo aberto :{excel_arquivo}")
                    print(err)

def lista_arquivos_depara_financeiras():
    
    global server_selecao
    server_selecao = 4
    elas_não = empresas_naopertercentes(server_selecao)
    deparas = fr"R:\Processos Internos\Contabilidade\Controles Internos\COORDENAÇÃO\14. REGRAS TI\PROJETO BI\Contabil\De-Para\Instituição Financeira"

    arquivos = os.walk(deparas)

    for raiz, pasta, excel_arquivo in arquivos:
        for depara_excecoes in excel_arquivo:
            arquivo_depara_original = os.path.join(deparas, depara_excecoes)

            if len( gb.glob( arquivo_depara_original)) > 0 :
                try:
                    if re.search('\\b.xlsx\\b', arquivo_depara_original, re.IGNORECASE):

                        print("\n")

                        data_alteracao = os.path.getmtime(arquivo_depara_original)
                        data_alteracao_formatado =   time.ctime( data_alteracao )

                        t_obj = time.strptime(data_alteracao_formatado)
                        T_stamp = time.strftime("%Y-%m-%d %H:%M:%S", t_obj)

                        codi_emp = str(depara_excecoes).split()
                        arquivo_excel = pd.ExcelFile(arquivo_depara_original, engine='openpyxl')
                        print(arquivo_depara_original)
                        server_selecao = 4
                        if( codi_emp[0] not in elas_não ):
                            if varifica_data_hora(codi_emp[0], T_stamp):
                                importa_modelo(codi_emp[0], arquivo_excel)
                                importa_excecoes(codi_emp[0], arquivo_excel)
                                migra_plano(codi_emp[0])
                                insere_data_ultima_atualizacao(codi_emp[0], T_stamp)

                except Exception as err:
                    print(f"Error de permissão ou arquivo aberto :{excel_arquivo}")
                    print(err)



def sobe_todos_os_planos():
    global server_selecao
    for i in [2,1,0,6,5,7,8]:
         server_selecao = i
         print(busca_todas_empresas_bi())
         try:
            importa_planos_todos()
         except Exception as err:
             print(err)



def sobe_todos_os_planos_test():
    global server_selecao
    for i in [ 0,1,2,3,6,5]:
         server_selecao = i
         print(busca_todas_empresas_bi())
         try:
            importa_planos_todos_teste()
         except Exception as err:
             print(err)

#-----------------------------------------------------------------------------------------------------------------------
#
#   ***** VRIFICA FECHAMENTO E ATUALIZA CASO A DATA SEJA DIFERENTE DO MAX_LAN *******
#
#-----------------------------------------------------------------------------------------------------------------------
def verifica_fechamento_atualiza():
    print(" VERIFICA ATUALIZAÇÃO ")

    todas_as_empresas_geral = busca_todas_empresas_bi()
    for empresa in todas_as_empresas_geral:
        print(empresa[0])
        max_lan = busca_max_data_lancamentos(empresa[0])
        max_lan_date = data.datetime.strptime(str(max_lan), "%Y-%m-%d").date()
        max_data_1d = max_lan_date + data.timedelta(hours=24)
        max_data_1d = str(max_data_1d)
        consulta_empresa = verifica_periodo_empresa(empresa[0])

        fechamento_dominio = data.datetime.strptime(consulta_empresa[1], "%Y-%m-%d").date()
        fechamento_rds = consulta_fechamento_oficial(empresa[0])

        # fechamento_rds = data.datetime.strptime(fechamento_rds, "%Y-%m-%d").date()

        print(f"LANÇAMENTOS EMPRESA ATÉ: {max_lan}")
        print(f"FECHAMENTO DOMINIO: {fechamento_dominio}")
        print(f"FECHAMENTO RDS: {fechamento_rds}")

        if fechamento_dominio > fechamento_rds:
            try:
                print("DATA DE FECHAMENTO MAIOR:  ATUALIZAÇÃO INCREMENTAL")
                print(f"MAXLAN + 1D sendo inserido {max_data_1d}")
                busca_lancamentos_incremental(empresa[0], max_data_1d, fechamento_dominio)
                atualiza_status_lancamanetos_empresa(empresa[0], fechamento_dominio, consulta_empresa[2],consulta_empresa[3])
                deleta_dados_lancamentos(empresa[0])
            except Exception as err:
                print(err)

        if fechamento_dominio < fechamento_rds:
            print("\n\n**** Apaga e atualiza **** \n\n")
            print("DATA DE FECHAMENTO MENOR Q ATUAL NO BANCO")
            try:
                busca_lancamentos(empresa[0])
                atualiza_status_lancamanetos_empresa(empresa[0], fechamento_dominio, consulta_empresa[2],consulta_empresa[3])
                deleta_dados_lancamentos(empresa[0])
            except Exception as err:
                    print(err)

        print("*******************************************************************")


#-----------------------------------------------------------------------------------------------------------------------
#
#   ***** ATUALIZAÇÃO CONTINUA SOBRE NOTAS *******
#
#-----------------------------------------------------------------------------------------------------------------------
def atualizacao_notas_continua():

    print("ATUALIZAÇÃO NOTAS: ")
    global server_selecao
    for i in [0,1,2,3,6,5]:
         server_selecao = i
         print(busca_todas_empresas_bi())
         try:
            todas_as_empresas_geral = busca_todas_empresas_bi()

            for empresa in todas_as_empresas_geral:
                print("*******************************************************************")
                print(empresa[0])
                max_lan = busca_max_data_notas(empresa[0])
                print(f"EMPRESA ATUALIZADA ATÉ: {max_lan}")
                #
                consulta_empresa = verifica_periodo_empresa(empresa[0])
                data_fechamento_dominio = data.datetime.strptime(consulta_empresa[2], "%Y-%m-%d").date()
                #
                data_fechamento_compara = data_fechamento_dominio - data.timedelta(hours=24)
                #
                data_fechamento_str = data_fechamento_dominio - data.timedelta(hours=24)
                data_fechamento_str = str(data_fechamento_str)
                max_lan_date = data.datetime.strptime(str(max_lan), "%Y-%m-%d").date()
                max_data_1d = max_lan_date + data.timedelta(hours=24)

                print(f"FECHAMENTO DE PERIODO: {data_fechamento_compara}")

                if data_fechamento_compara > max_lan_date:
                    try:
                        print("DATA DE FECHAMENTO MAIOR:  ATUALIZAÇÃO INCREMENTAL")
                        print(f"busca incremental. {empresa[0]} : data_ini:{max_data_1d}, data_fim: {data_fechamento_compara}")
                        importa_notas(empresa[0], max_data_1d, data_fechamento_compara)

                    except Exception as err:
                        print(err)

                if data_fechamento_compara < max_lan_date:
                    print("DATA DE FECHAMENTO MENOR Q ATUAL NO BANCO")
                    try:
                        print("apaga tudo e atualiza ! ")
                        importa_notas(empresa[0], '2000-01-01', data_fechamento_compara)
                    except Exception as err:
                            print(err)

                print("*******************************************************************")
         except Exception as err:
                print(err)


#-----------------------------------------------------------------------------------------------------------------------



#-----------------------------------------------------------------------------------------------------------------------
#
#   ***** THREADS *******
#
#-----------------------------------------------------------------------------------------------------------------------
def PROCESSOS_LANCAMENTOS():

    print(" VERIFICA ATUALIZAÇÃO LANCS")
    global server_selecao
    for i in [1,0,2,3,5]:
        server_selecao = i
        G = Thread(target=checaPeriodo_lancs_e_checaDiferenca)
        G.start()
        G.join()


def buscafiliaisRDS(codi_emp, dataIni, dataFim):

    sql = f"""
        SELECT fili_lan FROM dbo.bi_lancamentos 
        WHERE data_lan
        BETWEEN '{dataIni}' AND '{dataFim}' 
        AND codi_emp = {codi_emp}
        GROUP BY fili_lan
    """

    conn = conecta_db_sql()
    cursor = conn.cursor()
    cursor.execute(sql)
    return cursor.fetchall()

def classesDeparaRDS(codi_emp, periodoIni, periodoFim):
    sql = f"""
        SELECT distinct bp.estrutural
        FROM dbo.bi_lancamentos bl
        INNER JOIN dbo.bi_plano bp on bp.codi_emp = bl.codi_emp and bp.conta = bl.codi_cta
        WHERE bl.codi_emp = {codi_emp}
        AND bl.data_lan BETWEEN '{periodoIni}' AND '{periodoFim}'
        AND NOT EXISTS (SELECT 1 FROM dbo.bi_depara bd
        WHERE bd.classe = bp.estrutural
        AND bd.codi_emp = bl.codi_emp)
        """

    conn = conecta_db_sql()
    cursor = conn.cursor()
    cursor.execute(sql)
    return cursor.fetchall()

def B3_report_diario():
    print(" B3 REPORT DIARIO ")
    global server_selecao
    for i in [1,0,2,3,5]:
        server_selecao = i
        data_hj = date.today()
        print(data_hj)
        B3_HJ = []
        B3_HJ = consulta_b3(data_hj)
        if len(B3_HJ) > 0:
            print(len(B3_HJ))
        else:
            print( busca_ultima_linhaB3() )


def contasPlanoRDS(codi_emp, periodoIni, periodoFim):
    sql = f"""
            SELECT distinct(lan.codi_cta)
            FROM db_contabil.dbo.bi_lancamentos lan 
            WHERE lan.codi_emp = {codi_emp}
            AND lan.data_lan BETWEEN '{periodoIni}' AND '{periodoFim}'
            AND lan.codi_cta <> 0
            AND NOT EXISTS (
                SELECT 1
                FROM db_contabil.dbo.bi_plano bd 
                WHERE bd.codi_emp = lan.codi_emp
                AND bd.conta = lan.codi_cta )
    """
    conn = conecta_db_sql()
    cursor = conn.cursor()
    cursor.execute(sql)
    return cursor.fetchall()

def valorLancamentosDom(codi_emp, periodoIni, periodoFim):
    sql = f"""
            SELECT bl.codi_emp,
                YEAR(bl.data_lan) as ano,
                MONTH(bl.data_lan) as mes,
                sum(bl.vlor_lan) as valor
            FROM bethadba.ctlancto bl
            WHERE bl.codi_emp = {codi_emp}
            AND bl.data_lan BETWEEN  '{periodoIni}' AND '{periodoFim}'
            GROUP BY bl.codi_emp, YEAR(bl.data_lan),MONTH(bl.data_lan)
            ORDER BY YEAR(bl.data_lan),MONTH(bl.data_lan)
            """
    conn = conecta_dom()
    cursor = conn.cursor()
    cursor.execute(sql)
    return cursor.fetchall()

def ValoresCentroCustoDOM(codi_emp, periodoIni, periodoFim):
    sql =f"""
            SELECT ct.codi_ccu,ct.desc_ccu,sum(lan.vlor_ccu) as valor_credito
			FROM bethadba.ctclancto lan 
			INNER JOIN bethadba.ctccusto ct ON ct.codi_emp = lan.codi_emp and (ct.codi_ccu = lan.cdeb_ccu)
			WHERE lan.codi_emp ={codi_emp}
			AND lan.data_ccu BETWEEN '{periodoIni}' AND '{periodoFim}'
			GROUP BY ct.codi_ccu,ct.desc_ccu,YEAR(lan.data_ccu)
			ORDER BY YEAR(lan.data_ccu)
    """
    conn = conecta_dom()
    cursor = conn.cursor()
    cursor.execute(sql)
    return cursor.fetchall()


def valor_dom_por_filial(codi_emp, periodoIni, periodoFim):
    sql = f"""
            SELECT bl.codi_emp, YEAR(bl.data_lan) as ano, MONTH(bl.data_lan) as mes, sum(bl.vlor_lan) as valor, bl.fili_lan 
            FROM bethadba.ctlancto bl
            WHERE bl.codi_emp ={codi_emp}
            AND bl.data_lan BETWEEN  '{periodoIni}' AND '{periodoFim}'
            GROUP BY bl.codi_emp, YEAR(bl.data_lan),MONTH(bl.data_lan), bl.fili_lan 
            ORDER BY YEAR(bl.data_lan),MONTH(bl.data_lan), bl.fili_lan ASC
    """
    conn = conecta_dom()
    cursor = conn.cursor()
    cursor.execute(sql)
    return cursor.fetchall()

def ValoresCentroCustoRDS(codi_emp, periodoIni, periodoFim):
    sql =f"""
            SELECT bl.ccu,bl.desc_ccu,sum(bl.vlor_lan) as valor
			FROM db_contabil.dbo.bi_lancamentos bl
			WHERE bl.codi_emp ={codi_emp}
			AND bl.data_lan BETWEEN '{periodoIni}' AND '{periodoFim}'
			AND bl.vlor_lan > 0
			AND bl.ccu is not null
			GROUP BY bl.ccu,bl.desc_ccu
			ORDER BY bl.ccu,bl.desc_ccu
    """
    conn = conecta_db_sql()
    cursor = conn.cursor()
    cursor.execute(sql)
    return cursor.fetchall()

def valorLanctosPorPeriodoRDS(codi_emp, periodoIni, periodoFim):
    sql = f""" 
            SELECT bl.codi_emp, YEAR(bl.data_lan) as ano,MONTH(bl.data_lan) as mes,sum(bl.vlor_lan) as valor
            FROM db_contabil.dbo.bi_lancamentos bl
            WHERE bl.codi_emp = {codi_emp}
            AND bl.vlor_lan > 0
            AND bl.ccu is null
            AND bl.data_lan BETWEEN '{periodoIni}' AND '{periodoFim}'
            GROUP BY bl.codi_emp, YEAR(bl.data_lan),MONTH(bl.data_lan)
            ORDER BY YEAR(bl.data_lan),MONTH(bl.data_lan)
        """
    conn = conecta_db_sql()
    cursor = conn.cursor()
    cursor.execute(sql)
    return cursor.fetchall()


def valor_rds_por_filial(codi_emp, periodoIni, periodoFim):
    sql = f""" 
        SELECT bl.codi_emp, YEAR(bl.data_lan) as ano,MONTH(bl.data_lan) as mes,sum(bl.vlor_lan) as valor, bl.fili_lan 
        FROM db_contabil.dbo.bi_lancamentos bl
        WHERE bl.codi_emp = {codi_emp}
        AND bl.vlor_lan > 0
        AND bl.ccu is null
        AND bl.data_lan BETWEEN '{periodoIni}' AND '{periodoFim}'
        GROUP BY bl.codi_emp, YEAR(bl.data_lan),MONTH(bl.data_lan), bl.fili_lan 
        ORDER BY YEAR(bl.data_lan),MONTH(bl.data_lan), bl.fili_lan ASC
        """
    conn = conecta_db_sql()
    cursor = conn.cursor()
    cursor.execute(sql)
    return cursor.fetchall()


def identifca_diferenca(lancaRDS, lancaDOM):
    D = 0
    for elemA, elemB in zip(lancaDOM, lancaRDS):
        print(f"{elemA[3]} - {elemB[3]}")
        D += elemA[3] - elemB[3]
    return D

def identifca_diferenca_fili(lancaRDS, lancaDOM):
    D = 0
    for elemA, elemB in zip(lancaDOM, lancaRDS):
        print(f"{elemA[4]} - {elemB[4]}")

def checklist():

    periodoEmpresa = []
    periodoIni = []
    periodoFim = []
    atualizadoAte = []
    D = 0

    print("CHELIST EMPRESAS BI")
    global server_selecao
    for i in [0]:
        server_selecao = i
        todas_as_empresas_geral = busca_todas_empresas_bi()
        for empresa in todas_as_empresas_geral:
            print(f"*** {empresa[0]} ***")
            periodoEmpresa = verifica_periodo_empresa(empresa[0])
            periodoIni = '2023-01-01'
            periodoFim = data.datetime.strptime(periodoEmpresa[2], "%Y-%m-%d").date() - data.timedelta(hours=24)
            atualizadoAte = periodoEmpresa[1]

            valLancDom = valorLancamentosDom(empresa[0], periodoIni, periodoFim)
            valLancRDS = valorLanctosPorPeriodoRDS(empresa[0], periodoIni, periodoFim)

            ccustoDom = ValoresCentroCustoDOM(empresa[0], periodoIni, periodoFim)
            ccustoRDS = ValoresCentroCustoRDS(empresa[0], periodoIni, periodoFim)

            print('\n**************************************************')

            print(f"Atualizado ate:        {atualizadoAte}")
            print(f"Periodo Ini:           {periodoIni}   ")
            print(f"Periodo Fim:           {periodoFim}   ")
            print(f"valorLancamentosDom:    {len(valLancDom)}")
            print(f"valorLancamentosRDS:    {len(valLancRDS)}")

            if len(valLancRDS) == len(valLancDom):
                print("VERIFICA VALORES ENTRE RDS E DOMINIO:")
                D = identifca_diferenca(valLancRDS, valLancDom)
                if D != 0 :
                    print(f" SOBE NOVAMENTE: {empresa[0]}, ini: {periodoIni}, fim: {periodoFim}")
                    # busca_lancamentos_data_especifica(empresa[0], str(periodoIni), str(periodoFim))
                    print(f"codi_emp:  {empresa[0]}  {periodoEmpresa[1]}, {periodoEmpresa[2]}, {periodoEmpresa[3]}")
                    # atualiza_status_lancamanetos_empresa(empresa[0], periodoEmpresa[1], periodoEmpresa[2],periodoEmpresa[1])
            else:
                print("SOBE LANCS CONFORME PERIODO")
                # busca_lancamentos_data_especifica(empresa[0], '2018-01-01', str(periodoFim))
                print(f"codi_emp:  {empresa[0]}, atualiza ate: {periodoEmpresa[1]}, abertura: {periodoEmpresa[2]}, fechamento: {periodoEmpresa[3]}")
                # atualiza_status_lancamanetos_empresa(empresa[0], periodoEmpresa[1], periodoEmpresa[2], periodoEmpresa[1])
            print('**************************************************\n')



def hlancs_geral():
    global server_selecao
    server_selecao = 0
    for codi_emp in busca_todas_empresas_bi():

        periodoEmpresa = verifica_periodo_empresa(codi_emp[0])

        data_fechamento_dominio = data.datetime.strptime(periodoEmpresa[2], "%Y-%m-%d").date()
        #
        data_fechamento_compara = data_fechamento_dominio - data.timedelta(hours=24)

        busca_lancamentos_data_especifica(codi_emp[0], '2018-01-01', periodoEmpresa[1])
        # atualiza_status_lancamanetos_empresa(codi_emp[0], periodoEmpresa[1],'01-01-2023', periodoEmpresa[1])
        atualiza_status_lancamanetos_empresa(codi_emp[0], periodoEmpresa[1], periodoEmpresa[2], periodoEmpresa[1])
        importa_plano_umepresa(codi_emp[0])

def verificaDiferencas_mesmoPeriodo(codi_emp):

    periodoEmpresa = []
    periodoIni = []
    periodoFim = []
    atualizadoAte = []
    D = 0
    periodoEmpresa = verifica_periodo_empresa(codi_emp)
    periodoIni = '2023-01-01'

    periodoFim = data.datetime.strptime(periodoEmpresa[2], "%Y-%m-%d").date() - data.timedelta(hours=24)
    atualizadoAte = periodoEmpresa[1]

    valLancDom = valorLancamentosDom(codi_emp, periodoIni, periodoFim)
    valLancRDS = valorLanctosPorPeriodoRDS(codi_emp, periodoIni, periodoFim)

    # print(f"Atualizado ate:        {atualizadoAte}")
    # print(f"valorLancamentosDom:    {len(valLancDom)}")
    # print(f"valorLancamentosRDS:    {len(valLancRDS)}")    

    D = identifca_diferenca(valLancRDS, valLancDom)
    print(f"DIFERENÇA ENTRE VAL LANCS DA EMPRESA: {codi_emp}, DIFERENCA: {D}")
    print(f"QUANTIDADE DE LANCS: {codi_emp}, DOM = {len(valLancDom)} RDS = {len(valLancRDS)}")

    if D != 0 :
        print(f"DIFERENCA DE VALORES: {D}, NA EMPRESA: {codi_emp}")
        busca_lancamentos_data_especifica(codi_emp, str(periodoIni), str(periodoFim))
        print(f"EMPRESA:  {codi_emp}  {periodoEmpresa[1]}, {periodoEmpresa[2]}, {periodoEmpresa[3]}")
        atualiza_status_lancamanetos_empresa(codi_emp, periodoEmpresa[1], periodoEmpresa[2],periodoEmpresa[1])
        atualiza_data_atualizado_bi_empresas_prospec(codi_emp, periodoEmpresa[1])
        valLancDom = valorLancamentosDom(codi_emp, str(periodoIni), str(periodoFim))
        valLancRDS = valorLanctosPorPeriodoRDS(codi_emp,  str(periodoIni), str(periodoFim))
        atualiz_log_lancs(codi_emp, valLancDom, valLancRDS )

    if len(valLancRDS) != len(valLancDom):
        print(f"DIFERENCA DE QUANTIDADE DE REGISTROS: DOM = {len(valLancDom)} RDS = {len(valLancRDS)}")
        busca_lancamentos_data_especifica(codi_emp, '2022-01-01', str(periodoFim))
        print(f"codi_emp:  {codi_emp}, atualiza ate: {periodoEmpresa[1]}, abertura: {periodoEmpresa[2]}, fechamento: {periodoEmpresa[3]}")
        atualiza_status_lancamanetos_empresa(codi_emp, periodoEmpresa[1], periodoEmpresa[2], periodoEmpresa[1])
        atualiza_data_atualizado_bi_empresas_prospec(codi_emp, periodoEmpresa[1])
        valLancDom = valorLancamentosDom(codi_emp, str(periodoIni), str(periodoFim))
        valLancRDS = valorLanctosPorPeriodoRDS(codi_emp,  str(periodoIni), str(periodoFim))
        atualiz_log_lancs(codi_emp, valLancDom, valLancRDS )


def verificaDiferencas_mesmoPeriodo_porfiliais(codi_emp):
    periodoEmpresa = []
    periodoIni = []
    periodoFim = []
    atualizadoAte = []
    D = 0
    periodoEmpresa = verifica_periodo_empresa(codi_emp)
    periodoIni = '2023-01-01'

    periodoFim = data.datetime.strptime(periodoEmpresa[2], "%Y-%m-%d").date() - data.timedelta(hours=24)
    atualizadoAte = periodoEmpresa[1]

    valLancDom = valorLancamentosDom(codi_emp, periodoIni, periodoFim)
    valLancRDS = valorLanctosPorPeriodoRDS(codi_emp, periodoIni, periodoFim)

    # print(f"Atualizado ate:        {atualizadoAte}")
    # print(f"valorLancamentosDom:    {len(valLancDom)}")
    # print(f"valorLancamentosRDS:    {len(valLancRDS)}")    

    D = identifca_diferenca(valLancRDS, valLancDom)
    print(f"DIFERENÇA ENTRE VAL LANCS DA EMPRESA: {codi_emp}, DIFERENCA: {D}")
    print(f"QUANTIDADE DE LANCS: {codi_emp}, DOM = {len(valLancDom)} RDS = {len(valLancRDS)}")

def verifica_fechamento_periodo(codi_emp):

    max_lan = busca_max_data_notas(codi_emp)
    empAtualizadaAte = consulta_empresa_status(codi_emp)
    print(f"EMPRESA ATUALIZADA ATÉ: {empAtualizadaAte[2]}")

    consulta_empresaDOM = verifica_periodo_empresa(codi_emp)
    data_fechamento_dominio = data.datetime.strptime(consulta_empresaDOM[2], "%Y-%m-%d").date()

    data_fechamento_compara = data_fechamento_dominio - data.timedelta(hours=24)

    data_fechamento_str = data_fechamento_dominio - data.timedelta(hours=24)
    data_fechamento_str = str(data_fechamento_str)
    max_lan_date = data.datetime.strptime(str(max_lan), "%Y-%m-%d").date()
    max_data_1d = max_lan_date + data.timedelta(hours=24)

    print(f"FECHAMENTO DE PERIODO: {data_fechamento_compara}")

    print(f"EMPRESA {codi_emp} ATUALIZADA ATÉ: {empAtualizadaAte[2]} FECHAMENTO DA DOMINIO: {data_fechamento_compara}")

    if data_fechamento_compara > empAtualizadaAte[2]:
        print("*** DATA DE FECHAMENTO MAIOR:  ATUALIZAÇÃO INCREMENTAL ***")
        busca_lancamentos_data_especifica(codi_emp, max_data_1d, data_fechamento_compara)
        atualiza_status_lancamanetos_empresa(codi_emp, consulta_empresaDOM[1], consulta_empresaDOM[2],consulta_empresaDOM[1])
        atualiza_data_atualizado_bi_empresas_prospec(codi_emp, consulta_empresaDOM[1])
        valLancDom = valorLancamentosDom(codi_emp,  max_data_1d, data_fechamento_compara)
        valLancRDS = valorLanctosPorPeriodoRDS(codi_emp,  max_data_1d, data_fechamento_compara)
        atualiz_log_lancs(codi_emp, valLancDom, valLancRDS )
        
    if data_fechamento_compara < empAtualizadaAte[2]:
        print("*** DATA DE FECHAMENTO MENOR Q ATUAL NO BANCO ***")
        deleta_dados_lancamentos_periodo(codi_emp, '2020-01-01', empAtualizadaAte[2])
        busca_lancamentos_data_especifica(codi_emp, '2020-01-01', data_fechamento_compara)
        atualiza_status_lancamanetos_empresa(codi_emp, consulta_empresaDOM[1], consulta_empresaDOM[2],consulta_empresaDOM[1])
        atualiza_data_atualizado_bi_empresas_prospec(codi_emp, consulta_empresaDOM[1])
        valLancDom = valorLancamentosDom(codi_emp,  '2020-01-01', data_fechamento_compara)
        valLancRDS = valorLanctosPorPeriodoRDS(codi_emp,  '2020-01-01', data_fechamento_compara)
        atualiz_log_lancs(codi_emp, valLancDom, valLancRDS )

def checaPeriodo_lancs_e_checaDiferenca():
    global server_selecao
    for i in [0,1,2,5,7,8]:
        server_selecao = i
        todas_as_empresas_geral = busca_todas_empresas_bi()
        
        for empresa in todas_as_empresas_geral:
            codi_emp = empresa[0]
            print('\n**************************************************')

            print(f"EMPRESA : {codi_emp}") 
            print(f"VERIFICA PERIODO: {codi_emp}")
            verifica_fechamento_periodo(codi_emp)
            print("VERIFICA DIFERENCAS: ")
            verificaDiferencas_mesmoPeriodo(codi_emp)

            print('\n**************************************************')

def checaPeriodo_lancs_e_checaDiferenca_TAG():
    global server_selecao
    server_selecao = 3
    todas_as_empresas_geral = busca_todas_empresas_bi()
        
    for empresa in todas_as_empresas_geral:
        codi_emp = empresa[0]
        print('\n**************************************************')

        print(f"EMPRESA : {codi_emp}") 
        print(f"VERIFICA PERIODO: {codi_emp}")
        verifica_fechamento_periodo(codi_emp)
        print("VERIFICA DIFERENCAS: ")
        verificaDiferencas_mesmoPeriodo(codi_emp)

        print('\n**************************************************')

def atualiza_bi_orcado():

    global server_selecao
    
    data_hj = date.today()
    data_hj_str = str(data_hj)

    dia = data_hj_str[8:10]
    ano = int(data_hj_str[0:4])
    mes = int(data_hj_str[5:7])
    print(f" dia: {dia}, mes:{mes}, ano:{ano}")

    if dia == '01':
        server_selecao = 1
        todas_as_empresas_geral = busca_todas_empresas_bi()
                
        for empresa in todas_as_empresas_geral:
            codi_emp = empresa[0]
            contador = 2018
            apaga_orcado(codi_emp)
            while (contador <= ano):
                print(f"emp : {codi_emp} - ano: {contador}")
                orc = consulta_orcado(codi_emp, contador)
                filai = []
                nome_cta = []
                tipo_cta = []
                clas_cta = []
                Codi_Emp = []
                conta = []
                anoA = []
                janeiro  = []
                fevereiro  = []
                marco  = []
                abril  = []
                maio  = []
                junho  = []
                julho  = []
                agosto  = []
                setembro  = []
                outubro  = []
                novembro  = []
                dezembro  = []
                msk = []
                df_unique = []

                for linha in orc:
                    mask = linha[19][0] +"."+linha[19][1]


                    Codi_Emp.append( linha[0] )
                    conta.append( linha[1] )
                    anoA.append( linha[2] )
                    janeiro.append( linha[3] )
                    fevereiro.append( linha[4] )
                    marco.append( linha[5] )
                    abril.append( linha[6] )
                    maio.append( linha[7] )
                    junho.append( linha[8] )
                    julho.append( linha[9] )
                    agosto.append( linha[10] )
                    setembro.append( linha[11] )
                    outubro.append( linha[12] )
                    novembro.append( linha[13] )
                    dezembro.append( linha[14] )
                    filai.append( linha[15] )
                    nome_cta.append( linha[16] )
                    tipo_cta.append( linha[17] )
                    clas_cta.append( linha[18][0:8] )
                    msk.append(mask)
    
                orcado_frame = pd.DataFrame({
                            'codi_emp' : Codi_Emp,
                            'conta'    : conta,
                            'ano'      : anoA,
                            'janeiro'  : janeiro,
                            'fevereiro': fevereiro,
                            'marco'    : marco,
                            'abril'    : abril,
                            'maio'     : maio,
                            'junho'    : junho,
                            'julho'    : julho,
                            'agosto'   : agosto,
                            'setembro' : setembro,
                            'outubro'  : outubro,
                            'novembro' : novembro,
                            'dezembro' : dezembro,
                            'filal'    : filai,
                            'nome_cta' : nome_cta,
                            'tipo_cta' : tipo_cta,
                            'clas_cta' : clas_cta,
                            'mask'     : msk
                        })   

                df_unique = orcado_frame

                insere_orcado(df_unique.values.tolist())
                contador += 1

    else:
        print("AGUARDANDO PROXIMO DIA 01")

#-----------------------------------------------------------------------------------------------------------------------
#    *** MISCELANIUS ***            
#-----------------------------------------------------------------------------------------------------------------------

def conecta_bi_prospec():

    server = 'bi-prospeccao.cuens8xulkur.sa-east-1.rds.amazonaws.com'
    database = 'dbo_contabil'
    username = 'admin'
    password = 'Qj0yOzsL13c4rBtbswzV'
    try:
        conexao = pyodbc.connect(
            'DRIVER={ODBC Driver 18 for SQL Server};SERVER=' +
            server + ';DATABASE=' + database + ';ENCRYPT=no;UID=' +
            username + ';PWD=' + password + ';port=' + '1433')
        return conexao
    except Exception as err:
        print("Error occurred in making connection …")
        print(err)
        traceback.print_exc()

def atualiz_log_lancs(codi_emp, valLancDom, valLancRDS ):

    razao = consulta_nome(codi_emp)
    empAtualizadaAte = consulta_empresa_status(codi_emp)
    periodoEmpresa = verifica_periodo_empresa(codi_emp)
    # print( razao )
    # print( valLancDom[0][3] )
    # print( valLancRDS[0][3] )
    # print( empAtualizadaAte[2] )
    # print( periodoEmpresa[1] )

    try:
        valDom = valLancDom[0][3] if valLancDom[0][3] is not None else 0
        valRDS = valLancRDS[0][3] if valLancRDS[0][3] is not None else 0
    except :
        valDom = 0
        valRDS = 0


    log = [codi_emp, razao, empAtualizadaAte[2], periodoEmpresa[1], valDom, valRDS]

    sql_log = """
    INSERT INTO dbo_contabil.dbo.historico_fechamento_bi_empresas(
        codi_emp, razao, atualizado_ate_rds, atualizado_dominio, Lancamentos_dom, Lancamentos_rds
	   )
        values(?,?,?,?,?,?)"""

    conn = conecta_bi_prospec()
    cursor = conn.cursor()
    cursor.execute(sql_log, log)
    cursor.commit()


def rotina_que_o_elto_pediu_conjunto():
    conn_prospec = conecta_bi_prospec()
    sql_prospec = """ 
                    UPDATE dbo_BI.dbo.empresas_contabil SET atualizado_ate = (?) 
                    WHERE codi_emp = (?)
                  """
    
    conn_prospec_c = conn_prospec.cursor()
    
    global server_selecao
    server_selecao = 0
    conn = conecta_db_sql()
    sql_geral = f"select codi_emp ,atualizado_ate from db_contabil.dbo.bi_empresa"
    geral_cursor = conn.cursor()
    atualizado_geral = geral_cursor.execute(sql_geral)

    for data in atualizado_geral:
        conn_prospec_c.execute(sql_prospec, (data[1], data[0]) )
        conn_prospec_c.commit()
        

def atualiza_data_atualizado_bi_empresas_prospec(codi_emp, atualizado_ate):
    conn_prospec = conecta_bi_prospec()
    sql_prospec = """ 
                    UPDATE dbo_BI.dbo.empresas_contabil SET atualizado_ate = (?) 
                    WHERE codi_emp = (?)
                  """
    conn_prospec_c = conn_prospec.cursor()
    conn_prospec_c.execute(sql_prospec, ( atualizado_ate, codi_emp ) )
    conn_prospec_c.commit()

def busca_usu_cg():
    su_cg_std = "select * from dbo.bi_usuario_standart"
    conn = conecta_bi_prospec()
    cursor = conn.cursor()
    cursor.execute(su_cg_std)
    usu_cg = cursor.fetchall()
    return usu_cg

#-----------------------------------------------------------------------------------------------------------------------
#    *** ESCALONADOR E AGENDAMENTOS ***            
#-----------------------------------------------------------------------------------------------------------------------
# schedule.every().day.at("04:01").do(sobe_todos_os_planos)
# schedule.every().day.at("12:01").do(sobe_todos_os_planos)
# schedule.every().day.at("16:01").do(sobe_todos_os_planos)

schedule.every().day.at("15:00").do(atualizacao_notas_continua)

# schedule.every().day.at("20:10").do(lista_arquivos_depara_geral)
# schedule.every().day.at("18:00").do(lista_arquivos_depara_geral)

# schedule.every().day.at("12:10").do(lista_arquivos_depara_tag)
# schedule.every().day.at("19:40").do(lista_arquivos_depara_tag)

# schedule.every().day.at("12:50").do(lista_arquivos_depara_construtoras)
# schedule.every().day.at("19:50").do(lista_arquivos_depara_construtoras)

# schedule.every().day.at("12:20").do(lista_arquivos_depara_brunetto)
# schedule.every().day.at("19:50").do(lista_arquivos_depara_brunetto)

# schedule.every().day.at("12:30").do(lista_arquivos_depara_senfins)
# schedule.every().day.at("20:00").do(lista_arquivos_depara_senfins)

# schedule.every().day.at("12:40").do(lista_arquivos_depara_geral)  
# schedule.every().day.at("12:50").do(lista_arquivos_depara_construtoras)

# schedule.every().hour.do(checaPeriodo_lancs_e_checaDiferenca)
schedule.every().day.at("00:01").do(atualiza_bi_orcado)

#schedule.every().hour.do(lista_arquivos_depara_tag)
#-----------------------------------------------------------------------------------------------------------------------            
if __name__ == '__main__':
    print("RDS UNIFICADO")
    # while True:
    #     schedule.run_pending() 
    #     time.sleep(1)
    #   
    # sobe_todos_os_planos()

    server_selecao = 4
    codi_emp = 2174
    conn = conecta_dom()
    cursor = conn.cursor()
    cursor.execute(f"""SELECT * FROM bethadba.ctcontas WHERE codi_emp = {codi_emp} """)
    finaceira_plano = cursor.fetchall()
    plano = []
    for linha in finaceira_plano:
        # tamanho_menos_1 = len(linha[3]) - 1
        tamanho_menos_1 = 7
        # tamanho_menos_1 = 8
        plano.append([
                linha[0],
                str(linha[0]) + '-' + str(linha[1]),
                linha[1],
                str(linha[2]),
                linha[3][0],
                linha[3][0:tamanho_menos_1],
                str(linha[0]) + '-' + linha[3][0:tamanho_menos_1]
            ])
        
    for p in plano:
        print(plano)

    #plano_proprio_corrigido = exclui_conta_duplicada(codi_emp, plano)
    apaga_plano(codi_emp)
    insere_plano_contas_sem_exc(codi_emp, plano)
