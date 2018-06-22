# -*- coding: utf-8 -*-

# Márcio
# Camila
# inicio:17/05/2018
# Quality Assurance HMS
# PYTHON 2/3 compatible code

import os
import pyocnp
import datetime as dtm
from zlib import compress

# Criando lista com as unidades que possuem HMS
ucd_hms = ['P-07', 'P-08', 'P-09', 'P-12', 'P-15', 'P-18', 'P-19',
           'P-20', 'P-25', 'P-26', 'P-31', 'P-32', 'P-33', 'P-35',
           'P-37', 'P-38', 'P-40', 'P-43', 'P-47', 'P-48', 'P-50',
           'P-51', 'P-52', 'P-53', 'P-54', 'P-55', 'P-56', 'P-57',
           'P-58', 'P-61', 'P-62', 'P-63', 'P-65', 'P-66', 'P-74',
           ]


def getinfo_hms(ucd_name):
    # Banco de dados disponível e chave criptografada de acesso.
    DESV = pyocnp.DESV_DBACCESS
    PROD = pyocnp.PROD_DBACCESS
    id_desv = pyocnp.ucdid_byname_ocndb(ucd_name, flt_tol=.9, str_dbaccess=DESV)
    id_prod = pyocnp.ucdid_byname_ocndb(ucd_name, flt_tol=.9, str_dbaccess=PROD)
    if id_desv[0] is None:
        return (None, ucd_name)
    dbqry = ("SELECT {0}.PAIN_TX_PATH_ARQ FROM {0}"
             " WHERE {0}.LOIN_CD_LOCAL = {1}"
             " AND {0}.EQUI_CD_EQUIPAMENT = 72").format(
        'UE6RK.TB_PARAMETROS_INST', id_desv[0])
    qryresults = pyocnp.odbqry_all(dbqry, pyocnp.asciidecrypt(DESV))
    path = qryresults[0][0] if qryresults else u' Sem cadastro de HMS '
    return (path, id_prod[0])


def list_arq(id, start, dt_t=0):
    list_file = ['{}HMS{:{fmt}}.hms_gz'.format(id, start, fmt="%Y-%m-%d-%H-00")
                 ] if dt_t == 0 else ['{}HMS{:{fmt}}.hms_gz'.format(
                     id, start - dtm.timedelta(hours=x),
                     fmt="%Y-%m-%d-%H-00") for x in range(dt_t)]
    return list_file


def check_file(f_name, path):
    # Verificação do dado
    data = os.path.join(path, f_name)
    if os.path.exists(path):
        if os.path.isfile(data):
            if os.stat(data).st_size < 700:
                st = check_unpack(data[:-3])
                return '{} - Truncado\n{}'.format(f_name, st)
        else:
            st = check_unpack(data[:-3])
            return '{} - Inexistente\n{}'.format(f_name, st)
    else:
        return '{} - Inexistente\n'.format(path)


def check_unpack(f_name):
    name = os.path.basename(f_name)
    if os.path.isfile(f_name):
        if os.stat(f_name).st_size > 700:
            msg = '{} - Existente\n'.format(name)
            st_unpack = compress_gz(f_name)
            return '{}{}'.format(msg, st_unpack)
    else:
        return '{} - Inexistente\n'.format(name)


def compress_gz(f_name):
    try:
        file = open(f_name, 'r')
        file = file.read()
        file = file.replace('\n', '\r\n')
    except IOError:
        return '{}'.format('Leitura impossível\n')
    else:
        filegz = open('{}_gz'.format(f_name), 'wb')
        textgz = compress(file)
        filegz.write(textgz)
        filegz.close()
        return '{:*^46}\n'.format('Compactado')


def write_log(arq, b_tmp):
    if os.path.isfile(arq):
        with open(arq, 'r+') as f:
            log = f.read()
            f.seek(0, 0)
            [f.write(i) for i in b_tmp]
            f.write(log)
    else:
        f = open(arq, 'a')
        [f.write(i) for i in b_tmp]
    f.close()


if __name__ == '__main__':
    arq_log = 'log.dat'
    delta_t = 30  # verificando delta_t arquivos

    fmt = ' Log em: %d-%b-%Y %H:%M UTC '
    data = dtm.datetime.utcnow()
    run_time = '{:#^46}\n'.format('{:{}}'.format(data, fmt))
    interval = ('# Check último arquivo em:\n' if delta_t == 0 else
                '# Check {} último(s) arquivo(s) em:\n'.format(delta_t))
    B_tmp = [run_time, interval]
    for i in ucd_hms:
        B_tmp.append('{0:-<46}\n'.format(i))
        path, id = getinfo_hms(i)
        if not path:
            B_tmp.append(
                '{:*^46}\n'.format(' Id de {} inexistente '.format(id)))
        elif u'cadastro' in path:
            B_tmp.append('{:*^46}\n'.format(path))
        else:
            list_file = list_arq(id, data, delta_t)
            [B_tmp.append(x) for x in (check_file(j, path)
                                       for j in list_file) if isinstance(x, str)]
    B_tmp.append('{:#^46}\n'.format(' Log Finalizado '))
    write_log(arq_log, B_tmp)
