# -*- coding: utf-8 -*-
# Márcio
# Camila
# start:17/May/2018
# update: 17/Aug/2018
# Quality Assurance HMS
# PYTHON 2/3 compatible code

import os
import sys
import json
import logging
import argparse
import datetime as dtm
from zlib import compress
from getpass import getuser
from inspect import getabsfile
from platform import python_version
from pyocnp import asciidecrypt, odbqry_all, ucdid_byname_ocndb

parser = argparse.ArgumentParser(prog='{}'.format(sys.argv[0]),
                                 usage='%(prog)s [options]',
                                 description='Checking files in server.')
parser.add_argument('-d', '--debug', action="store_true",
                    help='Enable Debugging mode.')
args = parser.parse_args()
if args.debug:
    fmt = '%(asctime)s - %(lineno)d - %(levelname)s: %(message)s'
    logging.basicConfig(filename='DEBUG.log', format=fmt,
                        datefmt='%b/%d/%Y %H:%M:%S', level=logging.DEBUG)
    d_msg = ('{}\nStart Debugging @ {}\nPython version: {}\n'
             'User: {}\n{}').format(45 * '#', sys.argv[0], python_version(),
                                    getuser(), 80 * '#')
    logging.debug(d_msg)

if sys.version_info.major >= 3:
    basestring = str
else:
    basestring = basestring


def load_json(json_file=None):
    logging.debug('** Function load_json.')
    logging.info('input >> {} **'.format(json_file))
    if json_file is None:
        logging.warning('Missing specific json_file.')
        json_file = os.path.join(get_script_dir(), 'HMS_QA.json')
    if not os.path.isfile(json_file):
        logging.critical('{} is not a regular file.'.format(json_file))
        raise Exception('{} is not a regular file.'.format(json_file))
    try:
        with open(json_file, 'r') as f:
            ini_file = json.load(f)
            logging.info('Using {}'.format(json_file))
    except Exception:
        critical('{} is not a regular json file.'.format(json_file))
    logging.info('Json file loaded.')
    check_json(ini_file)
    return ini_file


def get_script_dir(follow_symlinks=True):
    if getattr(sys, 'frozen', False):
        path = os.path.abspath(sys.executable)
    else:
        path = getabsfile(get_script_dir)
    if follow_symlinks:
        path = os.path.realpath(path)
    return os.path.dirname(path)


def check_json(f_json):
    logging.debug('** Function check_json.')
    keys = ['output', 'interval', 'ucds_hms']
    [critical('{} not avaiable.'.format(i)) for i in keys if i not in f_json]
    assert isinstance(f_json['output'], basestring), \
        critical('Assert output failed.')
    assert isinstance(f_json['interval'], int), \
        critical('Assert interval failed.')
    assert isinstance(f_json['ucds_hms'], list), \
        critical('Assert ucds_hms failed.')
    [critical('item ({}) is not string at ucds_hms list.'.format(i)) for i
        in f_json['ucds_hms'] if not isinstance(i, basestring)]
    logging.info('input >> {output: str; interval: int; ucds_hms: list} **')
    if sys.version_info.major == 2:
        f_json['output'] = f_json['output'].encode('utf-8')
        f_json['ucds_hms'] = [i.encode('utf-8') for i in f_json['ucds_hms']]


def start_header(output, t_range):
    logging.debug('** Function start_header.')
    logging.info('input >> output: {}; t_range: {} **'.format(output, t_range))
    fmt = ' Log @: %d-%b-%Y %H:%M UTC '
    data = dtm.datetime.utcnow()
    run_time = '{:#^46}\n'.format('{:{}}'.format(data, fmt))
    interval = ('# Check last file @:\n' if t_range == 0 else
                '# Check {} lastest files @:\n'.format(t_range + 1))
    Buffer = [run_time, interval]
    return Buffer, data


def getinfo_hms(ucd_name):
    logging.debug('** Function getinfo_hms.')
    logging.info('input >> {} **'.format(ucd_name))
    DESV = 'eNoLdTUL8tYPBZEGBg4ursFhAC4gBNw='
    PROD = 'eNoLdTUL8g6Id3cJ1nd0DTIJMfNwdwgI8ncBAFU2BsQ='
    try:
        id_desv = ucdid_byname_ocndb(ucd_name, flt_tol=.9, str_dbaccess=DESV)
        id_prod = ucdid_byname_ocndb(ucd_name, flt_tol=.9, str_dbaccess=PROD)
    except Exception:
        critical('Can not access Oracle DataBase.')
    if id_desv[0] is None:
        return (None, ucd_name)
    logging.info('id @ DESV: {}; id @ PROD: {}'.format(id_desv[0], id_prod[0]))
    dbqry = ("SELECT {0}.PAIN_TX_PATH_ARQ FROM {0}"
             " WHERE {0}.LOIN_CD_LOCAL = {1}"
             " AND {0}.EQUI_CD_EQUIPAMENT = 72").format(
        'UE6RK.TB_PARAMETROS_INST', id_desv[0])
    qryresults = odbqry_all(dbqry, asciidecrypt(DESV))
    path = qryresults[0][0] if qryresults else 'without HMS register'
    return (path, id_prod[0])


def list_arq(id, start, dt_t=0):
    logging.debug('** Function list_arq')
    logging.info('input >> id: {}, start: {}, dt_t: {} **'.format(id,
                                                                  start, dt_t))
    list_file = ['{}HMS{:{fmt}}.hms_gz'.format(id, start, fmt="%Y-%m-%d-%H-00")
                 ] if dt_t == 0 else ['{}HMS{:{fmt}}.hms_gz'.format(
                     id, start - dtm.timedelta(hours=x),
                     fmt="%Y-%m-%d-%H-00") for x in range(dt_t)]
    return list_file


def check_file(f_name, path):
    logging.debug('** Function check_file')
    logging.info('input >> {}. **'.format(f_name))
    arq = os.path.join(path, f_name)
    if os.path.isfile(arq):
        if os.stat(arq).st_size < 700:
            logging.info('Truncated File.')
            st = check_unpack(arq[:-3])
            return '{} - Truncated\n{}'.format(f_name, st)
        logging.info('File Found.')
    else:
        logging.warning('Non Existent File.')
        st = check_unpack(arq[:-3])
        return '{} - Not Found\n{}'.format(f_name, st)


def check_unpack(f_name):
    name = os.path.basename(f_name)
    logging.debug('** Function check_unpack')
    logging.info('input >> {}. **'.format(name))
    if os.path.isfile(f_name):
        if os.stat(f_name).st_size > 700:
            logging.info('File Found.')
            msg = '{} - Found\n'.format(name)
            st_unpack = compress_gz(f_name)
            return '{}{}'.format(msg, st_unpack)
        logging.warning('Truncated decompressed file.')
        return '{}\n'.format('Truncated decompressed file.')
    else:
        logging.warning('Non Existent File.')
        return '{} - Not Found\n'.format(name)


def compress_gz(f_name):
    logging.debug('** Function compress_gz')
    try:
        file = open(f_name, 'r')
        file = file.read()
        file = file.replace('\n', '\r\n')
    except IOError:
        logging.error('Unable to read file.')
        return '{}'.format('Unable to read file.\n')
    else:
        filegz = open('{}_gz'.format(f_name), 'wb')
        textgz = compress(file)
        filegz.write(textgz)
        filegz.close()
        logging.info('Compacted.')
        return '{:*^46}\n'.format('Compacted')


def critical(msg):
    logging.critical(msg)
    raise Exception


def write_log(arq, b_tmp):
    logging.debug('** Function write_log')
    if os.path.isfile(arq):
        logging.info('Reading and writing output file.')
        with open(arq, 'r+') as f:
            log = f.read()
            f.seek(0, 0)
            [f.write(i) for i in b_tmp]
            f.write(log)
    else:
        logging.info('Writing output file.')
        f = open(arq, 'a')
        [f.write(i) for i in b_tmp]
    f.close()
    logging.info('Output file saved and closed.\n')


def main(json_file=None):
    ini_file = load_json(json_file)
    Out, data = start_header(ini_file['output'], ini_file['interval'])
    logging.debug('Start loop in ucds_list.')
    for i in ini_file['ucds_hms']:
        logging.info('{0:-<45}'.format(i))
        Out.append('{0:-<46}\n'.format(i))
        path, id = getinfo_hms(i)
        if not path:
            msg = 'Skipping {} which did not exist @ DB'.format(i)
            logging.error(msg)
            Out.append('{}\n'.format(msg))
        elif 'without' in path:
            msg = 'Skipping {} which HMS is not registered @ DB.'.format(i)
            logging.error(msg)
            Out.append('{}\n'.format(msg))
        elif os.path.exists(path):
            logging.info('path:\n {}'.format(path))
            list_file = list_arq(id, data, ini_file['interval'])
            [Out.append(x) for x in (check_file(j, path)
                                     for j in list_file) if isinstance(x, str)]
        else:
            logging.error('Path refers to a not existing path.')
            Out.append('Path refers to a not existing path\n')
    logging.info('Loop in ucds_list finished.')
    Out.append('{:#^46}\n'.format(' End Log '))
    write_log(ini_file['output'], Out)


if __name__ == '__main__':

    main()
