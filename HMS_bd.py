# -*- coding: utf-8 -*-
# Márcio
# Camila
# start:28/May/2018
# update: 06/Feb/2019
# Create "HMS.db" DataBase
# PYTHON 2/3 compatible code
import re
import sys
import json
import jsonschema
import logging
import argparse
import threading
import sqlite3 as sql3
import datetime as dtm
from os import getpid, path
from getpass import getuser
import multiprocessing as mp
from inspect import getabsfile
from zlib import decompress, error
from platform import python_version
from pyocnp import asciidecrypt, odbqry_all, ucdid_byname_ocndb, ucdname_byid_ocndb

parser = argparse.ArgumentParser(prog='{}'.format(sys.argv[0]),
                                 usage='%(prog)s [options]',
                                 description=('Create and populate DataBase'
                                              ' in sqlite3.'))
parser.add_argument('-d', '--debug', action="store_true",
                    help='Enable Debugging mode.')
args = parser.parse_args()
if args.debug:
    fmt = '%(asctime)s - %(lineno)d - %(levelname)s: %(message)s'
    logging.basicConfig(filename='HMS_bd.py_DEBUG.log', format=fmt,
                        datefmt='%b/%d/%Y %H:%M:%S', level=logging.DEBUG)
    d_msg = ('{}\nStart Debugging @ {}\nPython version: {}\n'
             'User: {}\n{}').format(45 * '#', sys.argv[0], python_version(),
                                    getuser(), 80 * '#')
    logging.debug(d_msg)

if sys.version_info.major >= 3:
    basestring = str
else:
    basestring = basestring

fmt = "%d/%m/%Y %H:%M"


class UcdThread(threading.Thread):
    def __init__(self, DBpath, ucd, initial_time, end_time, erase):
        """ constructor, setting initial variables """
        self._thread_ucd = ucd
        self._bd = DatabaseHms(path.join(DBpath, ucd + '.db'), erase)
        self._hms = AtitudeData(ucd)
        self._list_arq = list_arq(self._bd, self._hms, initial_time, end_time)
        threading.Thread.__init__(self, name=ucd)

    def run(self):
        """ main control loop """
        logging.info('Thread @ process: {}'.format(getpid()))
        if not self._list_arq:
            logging.info('Files in date range already exists in Database.')
            self._bd.conn.close()
            logging.info('{} Database closed.'.format(self._thread_ucd))
            return
        setattr(self, 'thread{}'.format(getpid()), mp.Pool())
        logging.info('List_arq size: {}'.format(len(self._list_arq)))
        [getattr(self, 'thread{}'.format(getpid())).apply_async(
            read, args=(i, self._hms),
            callback=self._bd.insert_data) for i in self._list_arq]
        getattr(self, 'thread{}'.format(getpid())).close()
        getattr(self, 'thread{}'.format(getpid())).join()
        logging.info('End List_arq.')
        self._bd.conn.close()
        logging.info('{} Database closed.'.format(self._thread_ucd))


class DatabaseHms(object):
    """docstring for DatabaseHms
    Create a DataBase if nonexistent."""

    def __init__(self, db=[], erase=None):
        super(DatabaseHms, self).__init__()
        logging.debug('*** Call Class DatabaseHms.')
        self.db = db
        self.erase = erase
        self.conn = self.db
        self.exec_user()
        self.tables = self.list_ucdtb()

    @property
    def db(self):
        return self._db

    @db.setter
    def db(self, d):
        assert d, critical('DataBase not specified.')
        logging.debug('@property of database (db).')
        logging.info('input >> {}'.format(d))
        dname, fname = path.split(d)
        if not dname:
            self._db = path.join(get_script_dir(), fname)
        else:
            if not path.isdir(dname):
                critical('Directory path not valid.')
            self._db = d

    @property
    def conn(self):
        return self._conn

    @conn.setter
    def conn(self, d):
        logging.debug('@property of connection (conn).')
        if not path.isfile(d):
            logging.info('Try Open Database file >> {}.'.format(d))
            self._build_connection(d)
            self._create_base_schema()
        else:
            logging.info('Try Connect with Database file >> {}.'.format(d))
            self._build_connection(d)
            # self._check_db_integrity()
            self.erase_olddata(self.erase)

    def _check_db_integrity(self):
        logging.debug('** Function _check_db_integrity.')
        try:
            self.conn.execute('PRAGMA quick_check;')
            logging.info('DataBase integrity ok.')
        except Exception as err:
            critical(err)

    def _build_connection(self, db):
        logging.debug('** Function _build_connection.')
        try:
            sql3.threadsafety = 2
            self._conn = sql3.connect(
                db, check_same_thread=False,
                detect_types=sql3.PARSE_DECLTYPES | sql3.PARSE_COLNAMES)
            self.curs = self._conn.cursor()
            logging.info('Connection started.')
        except Exception as err:
            critical(err)

    def _create_base_schema(self):
        logging.debug('** Function _create_base_schema.')
        qry = ("{0} TB_EXECUCAO (DT_EXECUCAO {1} {2}, "
               "USER_EXECUCAO {3} {2}); {0} TB_IMPORTACOES "
               "(DT_IMPORTACAO {1} {2}, "
               "UCD {3} {2} , DT_AQUISICAO {1} {2} );").format(
            'CREATE TABLE', 'timestamp', 'NOT NULL', 'TEXT')
        self.conn.executescript(qry)
        self.conn.commit()

    def create_ucdtb(self, ucd):
        """ Create a UCDs table if nonexistent to insert data."""
        logging.debug('** Function create_ucdtb.')
        qry = ("CREATE TABLE IF NOT EXISTS TB_UCD_{0} (DT_AQUISICAO timestamp "
               "{2}, CATEGORIA_AERONAVE {1}, DIA_NOITE {1}, POUSO_PERMITIDO "
               "{1}, PITCH {3}, PITCH_DM {3}, PITCH_UM {3}, ROLL {3}, ROLL_PM "
               "{3}, ROLL_SM {3}, INCL {3}, INCL_M {3}, HEAVE {3}, HEAVE_M "
               "{3}, HEAVE_PER {3}, HEAVE_VEL_M {3}); ").format(
            ''.join(x for x in ucd if x.isalnum()), 'INT', 'NOT NULL', 'REAL')
        self.conn.executescript(qry)
        self.conn.commit()
        self.tables = self.list_ucdtb()

    def erase_olddata(self, erase=None):
        logging.debug('** Function erase_olddata.')
        if erase is None:
            logging.info('Skipping erase_olddata.')
            return
        [self.curs.execute('DELETE FROM {0} WHERE {0}.DT_AQUISICAO < ?'.format(
            x[0]), [erase]) for x in self.list_ucdtb()]
        self.curs.execute('DELETE FROM {0} WHERE {0}.DT_AQUISICAO < ?'.format(
            'TB_IMPORTACOES'), [erase])
        self.conn.commit()
        self.conn.isolation_level = None
        self.curs.execute('VACUUM;')
        self.conn.isolation_level = ''
        self.conn.commit()
        logging.info('Erased Data older than {}'.format(erase))

    def check_impo(self, dt_aquisicao, ucd):
        """ Run a select query against TB_IMPORTACOES
            to see if any record exists"""
        self.curs.execute("""SELECT DT_IMPORTACAO
                          FROM TB_IMPORTACOES
                          WHERE DT_AQUISICAO=?
                          AND UCD=?""",
                          (dt_aquisicao, ucd))
        return self.curs.fetchone()

    def exec_user(self):
        logging.debug('** Function exec_user.')
        qry_exec = ("INSERT INTO TB_EXECUCAO "
                    "(DT_EXECUCAO, USER_EXECUCAO) "
                    "VALUES(?,?);")
        qry_exec_insert = [dtm.datetime.now(), getuser()]
        self.curs.execute(qry_exec, qry_exec_insert)
        self.conn.commit()
        logging.info('User running script.')

    def list_ucdtb(self):
        """ Just see a list of UCDs tables existents."""
        self.curs.execute("SELECT name FROM sqlite_master WHERE "
                          "type='table' AND name LIKE '%UCD%';")
        return self.curs.fetchall()

    def insert_data(self, data):
        logging.debug('** Function insert_data.')
        if not data:
            return logging.error('No data to insert in DataBase.')
        str_ucd = ''.join(x for x in data['UCD'] if x.isalnum())
        tb = 'TB_UCD_{}'.format(str_ucd)
        if tb not in (tb[0] for tb in self.tables):
            self.create_ucdtb(data['UCD'])
            logging.info('Created {} @ DataBase.'.format(tb))
        impo_dt = dtm.datetime.now()
        qry_impo = ("INSERT INTO TB_IMPORTACOES "
                    "(DT_IMPORTACAO, UCD, DT_AQUISICAO) VALUES(?,?,?);")
        qry_impo_insert = [impo_dt, data['UCD'], data['DT_AQUISICAO']]
        self.curs.execute(qry_impo, qry_impo_insert)
        self.conn.commit()
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
        logging.info('{} inserted in {}.'.format(data['DT_AQUISICAO'], tb))


class AtitudeData(object):
    """docstring for AtitudeData
    Get Atitude data from compacted HMS files."""

    def __init__(self, ucd=[]):
        super(AtitudeData, self).__init__()
        logging.debug('*** Call Class AtitudeData.')
        self.ucd = ucd

    @property
    def ucd(self):
        return self._ucd

    @ucd.setter
    def ucd(self, d):
        assert d, critical('UCD name can not be empty.')
        logging.debug('@property of ucd.')
        logging.info('input >> {}'.format(d))
        DESV = 'eNoLdTUL8tYPBZEGBg4ursFhAC4gBNw='
        PROD = 'eNoLdTUL8g6Id3cJ1nd0DTIJMfNwdwgI8ncBAFU2BsQ='
        id_d, id_p = self._get_ucdid(d, DESV), self._get_ucdid(d, PROD)
        if id_d[0] is None:
            critical('{} not exist @ DB'.format(d))
        logging.info('id @ DESV: {}; id @ PROD: {}'.format(id_d[0], id_p[0]))
        dbqry = ("SELECT {0}.PAIN_TX_PATH_ARQ FROM {0}"
                 " WHERE {0}.LOIN_CD_LOCAL = {1}"
                 " AND {0}.EQUI_CD_EQUIPAMENT = 72").format(
            'UE6RK.TB_PARAMETROS_INST', id_d[0])
        qryresults = odbqry_all(dbqry, asciidecrypt(DESV))
        if not qryresults:
            critical('HMS of {} not registered @ DB.'.format(d))
        else:
            self._ucd = d
            self.id = id_p[0]
            self.path = qryresults[0][0]

    def _get_ucdid(self, name, dbaccess):
        logging.debug('** Function _get_ucdid')
        try:
            return ucdid_byname_ocndb(name, flt_tol=.9, str_dbaccess=dbaccess)
        except Exception:
            critical('Can not access Oracle DataBase.')

    def get_data(self, fname):
        logging.debug('** Function get_data. @ P:{}'.format(getpid()))
        assert fname, critical('File not specified.')
        logging.info('input >> {}'.format(fname))
        idx = fname.find('HMS')
        dt = self._get_datetime(fname[idx:], 'HMS%Y-%m-%d-%H-%M.hms_gz')
        if (idx is -1) or not isinstance(dt, dtm.datetime):
            logging.error(('Wrong time format in filename pattern '
                           '"{}HMSYYYY-MM-DD-HH-MM.hms_gz".').format(self.id))
            return
        if fname.startswith('{}'.format(self.id)):
            return self._open_file(path.join(self.path, fname))
        else:
            logging.error('Prefix of filename expected: {}.'.format(self.id))

    def _open_file(self, file):
        """ Attempt to open / read the file."""
        logging.debug('** Function _open_file.')
        assert file, critical('File not specified.')
        logging.info('input >> {}'.format(file))
        dname, fname = path.split(file)
        if not dname:
            file = path.join(get_script_dir(), fname)
        else:
            if not path.isdir(dname):
                critical('Directory path not valid.')
        try:
            with open(file, 'rb') as filegz:
                txtgz = filegz.read()
            logging.info('Read Gzip file.')
            return self._unpack_file(txtgz, fname)
        except IOError as err:
            critical(err)

    # def _get_ucdname(self, id):
    #     logging.debug('** Function _get_ucdname.')
    #     try:
    #         return ucdname_byid_ocndb(id)
    #     except Exception:
    #         critical('Can not access Oracle DataBase.')

    def _unpack_file(self, txtgz, fname):
        logging.debug('** Function _unpack_file.')
        """ Attempt to unpack contents of the file."""
        try:
            logging.info('Decompressing file.')
            txtugz = decompress(txtgz).decode('cp1252')
            return self._clear_file(txtugz, fname)
        except error as err:
            critical('Decompressing Error.')

    def _clear_file(self, txtugz, fname):
        logging.debug('** Function _clear_file.')
        txtugz = txtugz.replace("\t", " ")
        txtugz = txtugz.replace("*.*", " --")
        txtugz = txtugz.replace(" * ", " --")
        txtugz = txtugz.replace("*", " ")
        txtugz = txtugz.replace("#.#", " --")
        txtugz = txtugz.replace("#", " ")
        txtugz = txtugz.replace(" --", " NaN")
        txtlns = txtugz.splitlines()
        logging.info('Content cleaned.')
        return self._parse_data(txtlns, fname)

    def _parse_data(self, txtlns, fname):
        logging.debug('** Function _parse_data.')
        self.data = {'UCD': [], 'DT_AQUISICAO': [], 'FNAME': fname,
                     'DT_SAMPLE': [], 'CATEGORIA_AERONAVE': [],
                     'DIA_NOITE': [], 'POUSO_PERMITIDO': [], 'PITCH': [],
                     'PITCH_DM': [], 'PITCH_UM': [], 'ROLL': [], 'ROLL_PM': [],
                     'ROLL_SM': [], 'INCL': [], 'INCL_M': [], 'HEAVE': [],
                     'HEAVE_M': [], 'HEAVE_PER': [], 'HEAVE_VEL_M': []}
        logging.info('Check acquisiton time.')
        try:
            aqs = " ".join(txtlns[0].split()[-2:])
        except Exception as err:
            critical(err)
        d = self._get_datetime(aqs, "%d/%m/%Y %H:%M:%S")
        msg = 'Acquisition time does not match format "%d/%m/%Y %H:%M:%S".'
        assert isinstance(d, dtm.datetime), critical(msg)
        dsample = d.date() if d.hour else (d - dtm.timedelta(1)).date()
        logging.info('Acquisition time in file {}.'.format(d))
        self.data['DT_AQUISICAO'] = d
        for (nln, ln) in enumerate(txtlns, start=2):
            if 'Missao:' in ln:
                logging.info('Checking UCD name.')
                logging.info('UCD name in file {}.'.format(ln.split()[-1]))
                self.data['UCD'] = ln.split()[-1]
            if "m/s" in ln:
                cols = 25 if nln > 10 else 24
                logging.info('Parsing data from line {}.'.format(nln + 1))
                gen = (g.split() for g in txtlns[nln:])
                break
        if not self.data['UCD']:
            self.data['UCD'] = self.ucd
        for nln2, lndata in enumerate(gen, start=nln + 1):
            hsample = self._get_datetime(lndata[0], '%H:%M:%S')
            if len(lndata) < cols:
                msg = ('Skipping line {}, which has {} columns. '
                       '(expected {}).'.format(nln2, len(lndata), cols))
                logging.warning(msg)
                pass
            elif hsample is None:
                msg = ('Skipping line {}, which time data {} does not match '
                       'format "%H:%M:%S".').format(nln2, lndata[0])
                logging.warning(msg)
                pass
            else:
                DT_AQS = d.combine(dsample, hsample.time()) if not hsample.time() == dtm.time(0, 0)\
                    else d.combine(d.date(), hsample.time())
                self.data['DT_SAMPLE'].append(DT_AQS)
                self.data['CATEGORIA_AERONAVE'].append(float(lndata[-15]))
                self.data['DIA_NOITE'].append(float(lndata[-14]))
                self.data['POUSO_PERMITIDO'].append(float(lndata[-13]))
                self.data['PITCH'].append(float(lndata[-12]))
                self.data['PITCH_DM'].append(float(lndata[-11]))
                self.data['PITCH_UM'].append(float(lndata[-10]))
                self.data['ROLL'].append(float(lndata[-9]))
                self.data['ROLL_PM'].append(float(lndata[-8]))
                self.data['ROLL_SM'].append(float(lndata[-7]))
                self.data['INCL'].append(float(lndata[-6]))
                self.data['INCL_M'].append(float(lndata[-5]))
                self.data['HEAVE'].append(float(lndata[-4]))
                self.data['HEAVE_M'].append(float(lndata[-3]))
                self.data['HEAVE_PER'].append(float(lndata[-2]))
                self.data['HEAVE_VEL_M'].append(float(lndata[-1]))
        logging.info('Last data to parse at line {}.'.format(nln2))
        return self.data

    def _get_datetime(self, str, fmt):
        try:
            return dtm.datetime.strptime(str, fmt)
        except ValueError:
            pass


def daterange(start, end):
    logging.debug('Create generator of daterange.')
    while start <= end:
        yield start
        start = start + dtm.timedelta(hours=1)


def list_arq(bd, hms, start, end):
    logging.debug('** Function list_arq.')
    """ Generate list of files to be inserted."""
    msg = 'Wrong instance of the type.'
    assert isinstance(bd, DatabaseHms), critical(msg)
    assert isinstance(hms, AtitudeData), critical(msg)
    assert start <= end, critical(
        'Start {:{fmt}} must be prior to End {:{fmt}}'.format(start, end))
    return ['{}HMS{:{fmt}}.hms_gz'.format(
        hms.id, x, fmt="%Y-%m-%d-%H-00")
            for x in daterange(start, end) if bd.check_impo(x, hms.ucd) is None]


def get_params(json_file=None):
    logging.debug('** Function get_params.')
    logging.info('input >> {} **'.format(json_file))
    if json_file is None:
        logging.warning('Missing specific json_file.')
        json_file = path.join(get_script_dir(), 'HMS_bd.json')
        logging.info('Using {}'.format(json_file))
    if not path.isfile(json_file):
        critical('{} is not a regular file.'.format(json_file))
    try:
        with open(json_file, 'r') as f:
            ini_file = json.load(f)
    except Exception as err:
        logging.critical("Poorly-formed text, not JSON:")
        critical(err)
    logging.info('Json file loaded.')
    schema = {"$schema": "http://json-schema.org/draft-06/schema#",
              "$ref": "#/definitions/HMSBd",
              "definitions": {
                  "HMSBd": {
                      "type": "object",
                      "additionalProperties": True,
                      "properties": {
                          "DBpath": {"type": ["string", "null"]},
                          "initial_time": {"type": ["string", "null"]},
                          "end_time": {"type": ["string", "null"]},
                          "erase": {"type": ["string", "null"]},
                          "ucds_hms": {"type": "array",
                                       "items": {"type": "string"}}},
                      "required": ["DBpath", "end_time", "erase",
                                   "initial_time", "ucds_hms"],
                      "title": "HMSBd"}}}
    try:
        jsonschema.validate(ini_file, schema)
    except jsonschema.exceptions.ValidationError as e:
        logging.critical("Well-formed but invalid JSON:")
        critical(e)
    check_json(ini_file)
    return ini_file


def get_script_dir(follow_symlinks=True):
    if getattr(sys, 'frozen', False):
        directory = path.abspath(sys.executable)
    else:
        directory = getabsfile(get_script_dir)
    if follow_symlinks:
        directory = path.realpath(directory)
    return path.dirname(directory)


def get_date(str_in, now, gap=0):
    logging.debug('** Function get_date')
    logging.info('input >> {}'.format(str_in))
    if str_in:
        try:
            date = dtm.datetime.strptime(str_in, fmt)
        except ValueError:
            critical('Unexpected format of date string.')
    else:
        date = (now - dtm.timedelta(hours=gap))
    logging.info('Using >> {:{fmt}}'.format(date, fmt=fmt))
    return date


def check_json(f_json):
    logging.debug('** Function check_json.')
    now = dtm.datetime.utcnow()
    [f_json.pop(m) for m in list(f_json) if m.startswith('__')]
    logging.info('Check DataBase path.')
    if f_json['DBpath']:
        if not path.isdir(f_json['DBpath']):
            critical('Directory path not valid.')
    else:
        f_json['DBpath'] = get_script_dir()
    logging.info('Get initial time.')
    f_json['initial_time'] = get_date(f_json['initial_time'], now, 72)
    logging.info('Get end time.')
    f_json['end_time'] = get_date(f_json['end_time'], now)
    logging.debug('Check the time interval.')
    assert (f_json['initial_time'] <= f_json['end_time']), \
        critical('Start {:{fmt}} must be prior to End {:{fmt}}'.format(
            f_json['initial_time'], f_json['end_time'], fmt=fmt))
    del_data = f_json['erase']
    logging.info('Get erase time.')
    if del_data is not None:
        assert isinstance(del_data, basestring), \
            critical('Assert erase time failed.')
        for i in ['hours', 'days', 'weeks']:
            if del_data.find(i) is not -1:
                k = int(''.join(c for c in del_data if c in '1234567890'))
                f_json['erase'] = now - dtm.timedelta(**{i: k})
                break
        if not isinstance(f_json['erase'], dtm.datetime):
            f_json['erase'] = get_date(f_json['erase'], now)
        if not f_json['erase'] <= f_json['initial_time']:
            logging.error('Erase date must be prior to Start date insert.')
            f_json['erase'] = f_json['initial_time']
            logging.warning('Overwrite Erase date to: {:{fmt}}'.format(
                f_json['initial_time'], fmt=fmt))
        logging.info('Erase old data before {:{fmt}}'.format(
            f_json['erase'], fmt=fmt))
    else:
        logging.info('Set to not erase old data.')
    logging.info('Check list of UCDs with HMS.')
    assert isinstance(f_json['ucds_hms'], list), \
        critical('Assert ucds_hms failed.')
    [critical('item ({}) is not string at ucds_hms list.'.format(i)) for i
        in f_json['ucds_hms'] if not isinstance(i, basestring)]
    if sys.version_info.major == 2:
        f_json['ucds_hms'] = [i.encode('utf-8') for i in f_json['ucds_hms']]
    logging.info('JsonObject was parsed successfully.')


def populate_bd(DBpath, ucds_hms, initial_time, end_time, erase):
    logging.debug('** Function populate_bd.')
    for j in ucds_hms:
        DBname = path.join(DBpath, j + '.db')
        bd = DatabaseHms(DBname, erase)
        hms = AtitudeData(j)
        arq = list_arq(bd, hms, initial_time, end_time)
        if not arq:
            logging.info('Files in date range already exists in Database.')
            pass
        for i in arq:
            try:
                bd.insert_data(hms.get_data(i))
            except Exception:
                logging.error('skipping {}'.format(i))
                pass
        bd.conn.close()
    logging.info('Close connection with {}'.format(DBname))


def populate_bd_Pool(DBpath, ucds_hms, initial_time, end_time, erase):
    logging.debug('** Function populate_bd_Pool.')
    for j in ucds_hms:
        DBname = path.join(DBpath, j + '.db')
        bd = DatabaseHms(DBname, erase)
        hms = AtitudeData(j)
        arq = list_arq(bd, hms, initial_time, end_time)
        logging.info('List_arq size: {}'.format(len(arq)))
        if not arq:
            logging.info('Files in date range already exists in Database.')
        else:
            pool = mp.Pool()
            [pool.apply_async(read, args=(i, hms),
                              callback=bd.insert_data) for i in arq]
            pool.close()
            pool.join()
            logging.info('End List_arq.')
        bd.conn.close()
        logging.info('{} Database closed.'.format(j))


def populate_bd_2thread(DBpath, ucds_hms, initial_time, end_time, erase):
    logging.debug('** Function populate_bd_2thread.')
    ucds = iter(ucds_hms)
    for k in ucds:
        logging.debug('Thread_1 for {}.'.format(k))
        thread_1 = UcdThread(DBpath, k, initial_time, end_time, erase)
        thread_1.start()
        try:
            A = next(ucds)
        except Exception:
            thread_1.join()
            logging.info('Thread_1 for {} finished.'.format(k))
            return
        logging.debug('Thread_2 for {}.'.format(A))
        thread_2 = UcdThread(DBpath, A, initial_time, end_time, erase)
        thread_2.start()
        thread_1.join()
        logging.info('Thread_1 for {} finished.'.format(k))
        thread_2.join()
        logging.info('Thread_2 for {} finished.'.format(A))
    return


def read(i, hms):
    logging.info('Process #: {}'.format(getpid()))
    dado = hms.get_data(i)
    logging.info('End process #: {}, file: {}'.format(getpid(), dado['FNAME']))
    return dado


def critical(msg):
    logging.critical(msg)
    raise Exception


def main(json_file=None):
    ini_file = get_params(json_file)
    # populate_bd(**ini_file)
    # populate_bd_Pool(**ini_file)
    populate_bd_2thread(**ini_file)


if __name__ == '__main__':
    ini = dtm.datetime.now()
    main('HMS_bd.json')
    fim = dtm.datetime.now() - ini
    print('Execution Time: {}'.format(fim))
