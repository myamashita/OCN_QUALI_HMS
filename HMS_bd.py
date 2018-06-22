# -*- coding: utf-8 -*-

# Márcio
# Camila
# inicio:28/05/2018
# Create "HMS.db" base de dados de Atitude
# PYTHON 2/3 compatible code

import os
import re
import pyocnp
import sqlite3
import getpass
import datetime as dtm
from zlib import decompress, error


class CheckBd(object):
    """docstring for CheckBd
    Create a DataBase if nonexistent and erase inserts before 120 days."""

    def __init__(self, bd):
        super(CheckBd, self).__init__()
        self.user = getpass.getuser()
        new_db = not os.path.isfile(bd)
        self.conn = sqlite3.connect(
            bd, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
        self.curs = self.conn.cursor()
        self.create_basebd() if new_db else self.erase_olddata()
        self.exec_user()

    def create_basebd(self):
        """ Create a DataBase."""
        qry = ("{0} TB_EXECUCAO (CD_EXECUCAO {1} AUTOINCREMENT, DT_EXECUCAO "
               "{2} {3}, USER_EXECUCAO {4} {3}); {0} TB_IMPORTACOES "
               "(IMPO_NR_IMPORTACAO {1} AUTOINCREMENT, DT_IMPORTACAO "
               "{2} {3}, CD_LOCAL {4} {3} , DT_AQUISICAO {2} {3} );").format(
            'CREATE TABLE', 'INTEGER PRIMARY KEY',
            'timestamp', 'NOT NULL', 'INT')
        self.conn.executescript(qry)
        self.conn.commit()

    def create_ucdtb(self, ucd):
        """ Create a UCDs table if nonexistent to insert data."""
        ucd = ''.join(x for x in ucd if x.isalnum())
        qry = ("CREATE TABLE IF NOT EXISTS TB_UCD_{0} (DT_AQUISICAO timestamp "
               "{2}, CATEGORIA_AERONAVE {1}, DIA_NOITE {1}, POUSO_PERMITIDO "
               "{1}, PITCH {3}, PITCH_DM {3}, PITCH_UM {3}, ROLL {3}, ROLL_PM "
               "{3}, ROLL_SM {3}, INCL {3}, INCL_M {3}, HEAVE {3}, HEAVE_M "
               "{3}, HEAVE_PER {3}, HEAVE_VEL_M {3}); ").format(
            ucd, 'INT', 'NOT NULL', 'REAL')
        self.conn.executescript(qry)
        self.conn.commit()
        return 'TB_UCD_{}'.format(ucd)

    def erase_olddata(self):
        """ Erase old data before 120 days in all UCDs tables."""
        old_dt = dtm.datetime.now() - dtm.timedelta(days=120)
        [self.curs.execute('DELETE FROM {0} WHERE {0}.DT_AQUISICAO < ?'.format(
            x[0]), [old_dt]) for x in self.list_ucdtb()]
        self.curs.execute('DELETE FROM {0} WHERE {0}.DT_AQUISICAO < ?'.format(
            'TB_IMPORTACOES'), [old_dt])
        self.conn.commit()

    def check_impo(self, dt_aquisicao, cd_local):
        """ Run a select query against TB_IMPORTACOES
            to see if any record exists"""
        self.curs.execute("""SELECT DT_IMPORTACAO
                          FROM TB_IMPORTACOES
                          WHERE DT_AQUISICAO=?
                          AND CD_LOCAL=?""",
                          (dt_aquisicao, cd_local))
        return self.curs.fetchone()

    def exec_user(self):
        """ Insert User name that run a script."""
        qry_exec = ("INSERT INTO TB_EXECUCAO "
                    "(DT_EXECUCAO, "
                    "USER_EXECUCAO) "
                    "VALUES(?,?);")
        qry_exec_insert = [dtm.datetime.now(), self.user]
        self.curs.execute(qry_exec, qry_exec_insert)
        self.conn.commit()

    def list_ucdtb(self):
        """ Just see a list of UCDs tables existents."""
        self.curs.execute("SELECT name FROM sqlite_master WHERE "
                          "type='table' AND name LIKE '%UCD%';")
        return self.curs.fetchall()

    def insert_data(self, data):
        """ Just insert data in respective UCD table."""
        if not data:
            return
        tb = self.create_ucdtb(data['UCD'])
        impo_dt = dtm.datetime.now()
        qry_impo = ("INSERT INTO TB_IMPORTACOES "
                    "(DT_IMPORTACAO, "
                    "CD_LOCAL, "
                    "DT_AQUISICAO) "
                    "VALUES(?,?,?);")
        qry_impo_insert = [impo_dt, data['CD_LOCAL'], data['DT_AQUISICAO']]
        self.curs.execute(qry_impo, qry_impo_insert)
        qry_data = ("INSERT INTO {0} (DT_AQUISICAO, CATEGORIA_AERONAVE, "
                    "DIA_NOITE, POUSO_PERMITIDO, {1}, {1}_DM, {1}_UM, {2}, "
                    "{2}_PM, {2}_SM, {3}, {3}_M, {4}, {4}_M, {4}_PER, "
                    "{4}_VEL_M) VALUES({5}?);").format(
            tb, 'PITCH', 'ROLL', 'INCL', 'HEAVE', 15 * '?,')
        qry_data_insert = list(zip(data['DT_SAMPLE'],
                                   data['CATEGORIA_AERONAVE'],
                                   data['DIA_NOITE'], data['POUSO_PERMITIDO'],
                                   data['PITCH'], data['PITCH_DM'],
                                   data['PITCH_UM'], data['ROLL'],
                                   data['ROLL_PM'], data['ROLL_SM'],
                                   data['INCL'], data['INCL_M'],
                                   data['HEAVE'], data['HEAVE_M'],
                                   data['HEAVE_PER'], data['HEAVE_VEL_M']))
        self.curs.executemany(qry_data, qry_data_insert)
        self.conn.commit()


class AtitudeData(object):
    """docstring for AtitudeData
    Get compacted HMS data and insert in SQLITE DataBase"""

    def __init__(self, ucd):
        super(AtitudeData, self).__init__()
        self.path, self.id = self.getinfo_hms(ucd)
        self.ucd = ucd

    def getinfo_hms(self, ucd):
        """ Get information about path and id of UCD."""
        DESV = pyocnp.DESV_DBACCESS
        PROD = pyocnp.PROD_DBACCESS
        id_desv = pyocnp.ucdid_byname_ocndb(ucd, flt_tol=.9, str_dbaccess=DESV)
        id_prod = pyocnp.ucdid_byname_ocndb(ucd, flt_tol=.9, str_dbaccess=PROD)
        if id_desv[0] is None:
            return (None, ucd_name)
        dbqry = ("SELECT {0}.PAIN_TX_PATH_ARQ FROM {0}"
                 " WHERE {0}.LOIN_CD_LOCAL = {1}"
                 " AND {0}.EQUI_CD_EQUIPAMENT = 72").format(
            'UE6RK.TB_PARAMETROS_INST', id_desv[0])
        qryresults = pyocnp.odbqry_all(dbqry, pyocnp.asciidecrypt(DESV))
        path = qryresults[0][0] if qryresults else u' Sem cadastro de HMS '
        return (path, id_prod[0])

    def get_data(self, file):
        """ Get data from compacted file."""
        if not file:
            return
        str_fmt = '{}HMS%Y-%m-%d-%H-%M.hms_gz'.format(self.id)
        if file.startswith('{}'.format(self.id)):
            self.data = {'UCD': self.ucd, 'CD_LOCAL': self.id,
                         'DT_AQUISICAO': dtm.datetime.strptime(file, str_fmt),
                         'DT_SAMPLE': [], 'CATEGORIA_AERONAVE': [],
                         'DIA_NOITE': [], 'POUSO_PERMITIDO': [], 'PITCH': [],
                         'PITCH_DM': [], 'PITCH_UM': [], 'ROLL': [],
                         'ROLL_PM': [], 'ROLL_SM': [], 'INCL': [], 'INCL_M': [],
                         'HEAVE': [], 'HEAVE_M': [], 'HEAVE_PER': [],
                         'HEAVE_VEL_M': []}
            self.open_file(os.path.join(self.path, file))
            return self.data
        else:
            print('<<Info Error>> id vs filename:')

    def open_file(self, file):
        """ Attempt to open / read the file."""
        try:
            with open(file, 'rb') as filegz:
                txtgz = filegz.read()
            self.unpack_file(txtgz)
        except IOError as err:
            print('<<Reading Error>>')

    def unpack_file(self, txtgz):
        """ Attempt to unpack contents of the file."""
        try:
            txtugz = decompress(txtgz).decode('cp1252')
            self.clear_file(txtugz)
        except error as err:
            print('<<Decompressing Error>>')

    def clear_file(self, txtugz):
        """ Cleanning content."""
        txtugz = txtugz.replace(u"\t", u" ")
        txtugz = txtugz.replace(u"*.*", u" --")
        txtugz = txtugz.replace(u"*", u" ")
        txtugz = txtugz.replace(u" --", u" NaN")
        txtlns = txtugz.splitlines()
        self.parse_data(txtlns)

    def parse_data(self, txtlns):
        """ Parsing data."""
        try:
            # Identificação da data de aquisição.
            aqs = " ".join(txtlns[0].split(u" ")[-2:])
            d = dtm.datetime.strptime(aqs, "%d/%m/%Y %H:%M:%S")
            dsample = d.date() if d.hour else (d - dtm.timedelta(1)).date()
            if self.data['DT_AQUISICAO'] == d:
                for (nln, ln) in enumerate(txtlns):
                    if u"m/s" in ln:
                        nln2 = nln + 2
                        break
                while nln2 < len(txtlns):
                    lndata = txtlns[nln2].split()
                    hsample = dtm.datetime.strptime(lndata[0],
                                                    "%H:%M:%S").time()
                    DT_AQS = d.combine(
                        dsample, hsample) if hsample else d.combine(
                        d.date(), hsample)
                    self.data['DT_SAMPLE'].append(DT_AQS)
                    self.data['CATEGORIA_AERONAVE'].append(lndata[-15])
                    self.data['DIA_NOITE'].append(lndata[-14])
                    self.data['POUSO_PERMITIDO'].append(lndata[-13])
                    self.data['PITCH'].append(lndata[-12])
                    self.data['PITCH_DM'].append(lndata[-11])
                    self.data['PITCH_UM'].append(lndata[-10])
                    self.data['ROLL'].append(lndata[-9])
                    self.data['ROLL_PM'].append(lndata[-8])
                    self.data['ROLL_SM'].append(lndata[-7])
                    self.data['INCL'].append(lndata[-6])
                    self.data['INCL_M'].append(lndata[-5])
                    self.data['HEAVE'].append(lndata[-4])
                    self.data['HEAVE_M'].append(lndata[-3])
                    self.data['HEAVE_PER'].append(lndata[-2])
                    self.data['HEAVE_VEL_M'].append(lndata[-1])
                    nln2 += 1
            else:
                print('<<Parse Error (Name time or Acquisition time)>>')
                return
        except ValueError:
            print('<<Parse Error (Acquisition time)>>')


def list_arq(bd, hms, start, dt_t=0):
    """ Generate list of files to be inserted."""
    now = dtm.datetime.utcnow()
    past120days = now - dtm.timedelta(days=120)
    if start < past120days:
        print('<Data inicial anterior a 120 dias>')
        return
    dict_full = {'{:{fmt}}'.format(start - dtm.timedelta(hours=x),
                                   fmt="%Y-%m-%d %H:00:00"):
                 '{}HMS{:{fmt}}.hms_gz'.format(hms.id,
                                               start - dtm.timedelta(hours=x),
                                               fmt="%Y-%m-%d-%H-00")
                 for x in range(dt_t + 1)
                 if start - dtm.timedelta(hours=x) > past120days}
    return [y for x, y in dict_full.items() if bd.check_impo(x, hms.id) is None]


if __name__ == '__main__':

    ini = dtm.datetime.now()
    ucd_hms = ['P-07', 'P-08', 'P-09', 'P-12', 'P-15', 'P-18', 'P-19',
               'P-20', 'P-25', 'P-26', 'P-31', 'P-32', 'P-33', 'P-35',
               'P-37', 'P-38', 'P-40', 'P-43', 'P-47', 'P-48', 'P-50',
               'P-51', 'P-52', 'P-53', 'P-54', 'P-55', 'P-56', 'P-57',
               'P-58', 'P-61', 'P-62', 'P-63', 'P-65', 'P-66', 'P-74',
               ]

    start = dtm.datetime.utcnow()
    # bd = CheckBd('HMS.db')
    # for i in ucd_hms:
    #     hmd = AtitudeData(i)
    #     list_file = list_arq(bd, hms, start, dt_t=300)
    #     [bd.insert_data(hms.get_data(x)) for x in list_file]
    # bd.conn.close()

    bd = CheckBd('HMS.db')
    i = 'P-08'
    hms = AtitudeData(i)

    # criar lista
    list_file = list_arq(bd, hms, start, dt_t=24)
    [bd.insert_data(hms.get_data(x)) for x in list_file]
    fim = dtm.datetime.now() - ini
    print('Tempo usado: {}'.format(fim))
    bd.conn.close()
