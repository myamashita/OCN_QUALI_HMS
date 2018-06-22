# -*- coding: utf-8 -*-
# purpose:  check atitude data from database "HMS.db" in sqlite3
# author:   Márcio Yamashita
# created:  05-June-2018
# PYTHON 2/3 compatible code
# modified:

import pyocnp
import sqlite3
import pandas as pd
from numpy import asarray
from collections import OrderedDict
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
from matplotlib.dates import DateFormatter, YearLocator, MonthLocator
from matplotlib.dates import DayLocator, HourLocator, MinuteLocator

degree = u"\u00b0"
plt.rcParams['axes.grid'] = True


class Read_Db(object):
    """docstring for Read_Db
    Connect to a DataBase."""
    def __init__(self, bd):
        super(Read_Db, self).__init__()
        self.conn = sqlite3.connect(
            bd, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
        self.curs = self.conn.cursor()

    def list_ucdtb(self):
        """List of UCDs tables in DataBase."""
        self.curs.execute("SELECT name FROM sqlite_master "
                          "WHERE type='table' AND name LIKE '%UCD%';")
        return self.curs.fetchall()

    def list_vars(self, tb):
        """List of variables in UCD table."""
        self.curs.execute('SELECT * FROM {}'.format(tb))
        return list(map(lambda x: x[0], self.curs.description))

    def sql_qry(func):
        def make_qry(self, ucd, vars=None, ini=None, fim=None):
            """Make query and return the execution."""
            tb = ' TB_UCD_{}'.format(''.join(x for x in ucd if x.isalnum()))
            idx = '{}.{}'.format(tb, 'DT_AQUISICAO')
            sel = mk_selection(tb, vars)
            fil = mk_filter(idx, ini, fim)
            qyr_order = 'ORDER BY {}'.format(idx)
            if sel:
                full_qry = ('{} {} {}'.format(sel, fil, qyr_order)
                            if fil else '{} {}'.format(sel, qyr_order))
            else:
                print('empty selection')
                return
            return exec_qry(full_qry, self.conn)

        def mk_selection(tb, vars):
            """Make select part of query."""
            if isinstance(vars, list):
                qry_selection = 'SELECT {0}.DT_AQUISICAO{1} FROM {0}'.format(
                    tb, ''.join([', {}.{}'.format(tb, x) for x in vars]))
            elif vars == 'all':
                qry_selection = 'SELECT * FROM {0}'.format(tb)
            else:
                print('empty list of variable(s)')
                return
            return qry_selection

        def mk_filter(idx, ini, fim):
            """Make filter part of query."""
            [ini, fim] = test_time_input(ini), test_time_input(fim)
            if isinstance(ini, datetime) and isinstance(fim, datetime):
                qry_filter = ('' if ini > fim
                              else "WHERE {0}>'{1}' AND {0}<'{2}'".format(
                                  idx, ini, fim))
            elif isinstance(ini, datetime) and fim is None:
                qry_filter = "WHERE {0}>'{1}'".format(idx, ini)
            elif ini is None and isinstance(ini, datetime):
                qry_filter = "WHERE {0}<'{1}'".format(idx, fim)
            else:
                qry_filter = ''
            return qry_filter

        def exec_qry(full_qry, conn):
            """Execute query and return a DataFrame."""
            try:
                df = pd.read_sql_query(full_qry, conn,
                                       index_col='DT_AQUISICAO',
                                       parse_dates={'DT_AQUISICAO':
                                                    '%Y-%m-%d %H:%M:%S'})
            except DatabaseError:
                df = None
                print('Execution failed on sql {}'.format(full_qry))
            return df

        def test_time_input(dt):
            """ Teste para input de datas."""
            if dt:
                try:
                    return datetime.strptime(dt, "%d/%m/%Y %H:%M:%S")
                except ValueError:
                    print('Corrigir input DD/MM/YYYY HH:MM:SS')
            return
        return make_qry

    @sql_qry
    def get_data(self, ucd, vars, ini, fim):
        """Get data on DataBase and return a DataFrame."""
        return df


class HMS_QC(object):
    """docstring for HMS_QC"""

    def __init__(self, name):
        super(HMS_QC, self).__init__()
        self.name = name

    def plot_var(self, data, var_name=None, ini=None, fim=None, step=1):
        """ Plot of variables in DataFrame or obey a var_name list."""
        fig_list = list(data.keys()) if var_name is None else var_name
        if isinstance(fig_list, list):
            for k, v in enumerate(fig_list):
                self.make_figure(k)
                ax = getattr(self, 'ax{}'.format(k))
                df = self.get_data(data[v], ini, fim)
                if df.empty:
                    msg = 'Plot request returned an empty object'
                    kw = {'family': 'serif', 'style': 'italic',
                          'ha': 'center', 'wrap': True}
                    ax.text(0.5, 0.5, msg, **kw)
                elif v not in data.keys():
                    print('{} is not in DataFrame'.format(v))
                else:
                    ax.cla()
                    ax.plot(df.index[::step], df[::step])
                    ax.set_title('{} @ {}'.format(df.name, self.name))
                    self.setaxdate(ax, df.index.min(), df.index.max())
        else:
            print('Not a list of variable(s)')

    def plot_full_qc(self, data, ini=None, fim=None):
        """ Plot of variables determinants to close helideck.
        Necessary 'all' variables"""
        dict_fig = OrderedDict([("axPT", [(0, "PITCH"),
                                          (1, "PITCH_DM"),
                                          (1, "PITCH_UM"),
                                          (2, "LIM",
                                           "CATEGORIA_AERONAVE", 3, 4)]),
                                ("axRL", [(0, "ROLL"),
                                          (1, "ROLL_PM"),
                                          (1, "ROLL_SM"),
                                          (2, "LIM",
                                           "CATEGORIA_AERONAVE", 3, 4)]),
                                ("axIN", [(0, "INCL"),
                                          (1, "INCL_M"),
                                          (2, "LIM", "CATEGORIA_AERONAVE",
                                           3.5, 4.5)]),
                                ("axVArf", [(0, "HEAVE_VEL_M"),
                                            (2, "LIM", "DIA_NOITE", 1.3, 1)])])
        fig, (self.axPT, self.axRL, self.axIN, self.axVArf) = plt.subplots(
            nrows=4, facecolor=(1.0, 1.0, 1.0), figsize=(12, 8), sharex=True)
        self.axPT.set_title('Atitudes @ {}'.format(self.name))
        df = self.get_data(data[:], ini, fim)
        if df.empty:
            msg = 'Plot request returned an empty object'
            kw = {'family': 'serif', 'style': 'italic',
                  'ha': 'center', 'wrap': True}
            [getattr(self, i).text(0.5, 0.5, msg, **kw)
             for i in dict_fig.keys()]
        else:
            for i in dict_fig:
                self.plot_sub(df, i, dict_fig[i])
            mask = df['POUSO_PERMITIDO'] == 0
            self.axVArf.plot(df['POUSO_PERMITIDO'][mask], 'ro')
            self.setaxdate(self.axVArf, df.index.min(), df.index.max())
            self.set_full_qc()

    def plot_PRH(self, data, ini=None, fim=None):
        """ Plot of variables PITCH, ROLL and HEAVE."""
        dict_fig = OrderedDict([("axPT", [(0, "PITCH"),
                                          (2, "LIM",
                                           "CATEGORIA_AERONAVE", 3, 4)]),
                                ("axRL", [(0, "ROLL"),
                                          (2, "LIM",
                                           "CATEGORIA_AERONAVE", 3, 4)]),
                                ("axHV", [(0, "HEAVE"),
                                          (2, "LIM", "DIA_NOITE", 5, 4)])])
        fig, (self.axPT, self.axRL, self.axHV) = plt.subplots(
            nrows=3, facecolor=(1.0, 1.0, 1.0), figsize=(12, 8), sharex=True)
        self.axPT.set_title('Atitudes @ {}'.format(self.name))
        df = self.get_data(data[:], ini, fim)
        if df.empty:
            msg = 'Plot request returned an empty object'
            kw = {'family': 'serif', 'style': 'italic',
                  'ha': 'center', 'wrap': True}
            [getattr(self, i).text(0.5, 0.5, msg, **kw)
             for i in dict_fig.keys()]
        else:
            for i in dict_fig:
                self.plot_sub(df, i, dict_fig[i])
            self.setaxdate(self.axHV, df.index.min(), df.index.max())
            self.set_prh()

    def plot_sub(self, data, axes_name, dict_key):
        """ Plot key of dict (inside key type 0,1,2)."""
        ax = getattr(self, axes_name)
        kw = {'facecolor': 'green', 'alpha': 0.2}
        for i in dict_key:
            if i[0] == 1:
                ax.plot(data.index, data[i[1]], ':', color='coral')
            elif i[1] == 'LIM':
                val = asarray([i[4] if j else i[3] for j in data[i[2]]])
                ax.fill_between(data.index, -val, val, **kw)
            elif i[0] == 0:
                ax.plot(data.index, data[i[1]])

    def make_figure(self, k):
        """ Create figure with subplot."""
        kw = {'facecolor': (1.0, 1.0, 1.0), 'figsize': (12, 8)}
        setattr(self, 'fig{}'.format(k), plt.figure(k, **kw))
        setattr(self, 'ax{}'.format(k), getattr(
            self, 'fig{}'.format(k)).add_subplot(1, 1, 1))

    def get_data(self, df, ini, fim):
        """ Get data inside interval."""
        [ini, fim] = self.test_time_input(ini), self.test_time_input(fim)
        data_ = df.loc[ini: fim]
        return data_

    def test_time_input(self, dt):
        """ Test time input, if ok return string."""
        if dt:
            try:
                dt = datetime.strptime(dt, "%d/%m/%Y %H:%M:%S")
                return str(dt)
            except ValueError:
                print('Corrigir input DD/MM/YYYY HH:MM:SS')
        return

    def setaxdate(self, axes, datemn, datemx):
        """ Customizar rótulos de eixo temporal. """
        # Limites inferior e superior do eixo.
        (axes.set_xlim([datemn - timedelta(hours=1), datemx +
                        timedelta(hours=1)]) if datemn == datemx else
         axes.set_xlim([datemn, datemx]))
        axes.fmt_xdata = DateFormatter('%d/%m/%y %H:%M:%S')
        # Time Scale: < 30 minutes.
        if (datemx - datemn) <= timedelta(minutes=30):
            axes.xaxis.set_major_locator(MinuteLocator(byminute=(0, 6, 12, 18,
                                                                 24, 30, 36, 42,
                                                                 48, 54)))
            axes.xaxis.set_major_formatter(DateFormatter('%H:%M'))
            axes.xaxis.set_minor_locator(MinuteLocator(byminute=(3, 9, 15, 21,
                                                                 27, 33, 39, 45,
                                                                 51, 57)))
            axes.xaxis.set_minor_formatter(DateFormatter(''))
        # Time Scale: ]30 minutes, 1[ hour.
        elif timedelta(minutes=30) < (datemx - datemn) < timedelta(hours=1):
            axes.xaxis.set_major_locator(MinuteLocator(byminute=(0, 10, 20, 30,
                                                                 40, 50)))
            axes.xaxis.set_major_formatter(DateFormatter('%H:%M'))
            axes.xaxis.set_minor_locator(MinuteLocator(byminute=(5, 15, 25,
                                                                 35, 45, 55)))
            axes.xaxis.set_minor_formatter(DateFormatter(''))
        # Time Scale: ]1 hous, 3[ horas.
        elif timedelta(hours=1) <= (datemx - datemn) < timedelta(hours=3):
            axes.xaxis.set_major_locator(HourLocator())
            axes.xaxis.set_major_formatter(DateFormatter('%d/%m/%y\n%H:%M'))
            axes.xaxis.set_minor_locator(MinuteLocator(byminute=(15, 30, 45)))
            axes.xaxis.set_minor_formatter(DateFormatter(''))
        # Time Scale: [3 hours, 12[ horas.
        elif timedelta(hours=3) <= (datemx - datemn) < timedelta(hours=12):
            axes.xaxis.set_major_locator(HourLocator(byhour=(0, 3, 6, 9, 12,
                                                             15, 18, 21)))
            axes.xaxis.set_major_formatter(DateFormatter('%d/%m/%y\n%H:%M'))
            axes.xaxis.set_minor_locator(HourLocator())
            axes.xaxis.set_minor_formatter(DateFormatter(''))
        # Time Scale: [12, 24[ hours.
        elif timedelta(hours=12) <= (datemx - datemn) < timedelta(hours=24):
            axes.xaxis.set_major_locator(HourLocator(byhour=(0, 6, 12, 18)))
            axes.xaxis.set_major_formatter(DateFormatter('%d/%m/%y\n%H:%M'))
            axes.xaxis.set_minor_locator(HourLocator())
            axes.xaxis.set_minor_formatter(DateFormatter(''))
        # Time Scale: [24hours, 10[ days.
        elif timedelta(hours=24) <= (datemx - datemn) < timedelta(hours=240):
            axes.xaxis.set_major_locator(DayLocator())
            axes.xaxis.set_major_formatter(DateFormatter('%d/%m/%y'))
            axes.xaxis.set_minor_locator(HourLocator(byhour=(6, 12, 18)))
            axes.xaxis.set_minor_formatter(DateFormatter('%H:%M'))
        # Time Scale: [11, 45[ days.
        elif 10 <= (datemx.toordinal() - datemn.toordinal()) < 45:
            axes.xaxis.set_major_locator(DayLocator(bymonthday=(3, 8, 13,
                                                                18, 23, 28)))
            axes.xaxis.set_major_formatter(DateFormatter('%d/%m/%y'))
            axes.xaxis.set_minor_locator(DayLocator())
            axes.xaxis.set_minor_formatter(DateFormatter(''))
        # Time Scale: [45, 183[ days.
        elif 45 <= (datemx.toordinal() - datemn.toordinal()) < 183:
            axes.xaxis.set_major_locator(MonthLocator())
            axes.xaxis.set_major_formatter(DateFormatter('%m/%Y'))
            axes.xaxis.set_minor_locator(DayLocator(bymonthday=(5, 10,
                                                                15, 20, 25)))
            axes.xaxis.set_minor_formatter(DateFormatter('%d'))
        # Time Scale: >= 365 days.
        else:
            axes.xaxis.set_major_locator(YearLocator())
            axes.xaxis.set_major_formatter(DateFormatter('%Y'))
            axes.xaxis.set_minor_locator(MonthLocator())
            axes.xaxis.set_minor_formatter(DateFormatter('%m'))

        for label in axes.xaxis.get_majorticklabels():
            label.set_rotation(45)
        for label in axes.xaxis.get_minorticklabels():
            label.set_rotation(45)

    def set_full_qc(self):
        """ Set figure made in plot_full_qc."""
        self.axPT.set_ylabel('PITCH [%s]' % degree)
        self.axPT.set_ylim(-4, 4)
        self.clean_ax(self.axPT)
        self.axRL.set_ylabel('ROLL [%s]' % degree)
        self.axRL.set_ylim(-4, 4)
        self.clean_ax(self.axRL)
        self.axIN.set_ylabel('INCLINATION [%s]' % degree)
        self.axIN.set_ylim(0, 4.5)
        self.clean_ax(self.axIN)
        self.axVArf.set_ylabel('VArf [m/s]')
        self.axVArf.set_ylim(0, 4)

    def set_prh(self):
        """ Set figure made in plot_PRH."""
        self.axPT.set_ylabel('PITCH [%s]' % degree)
        self.axPT.set_ylim(-4, 4)
        self.clean_ax(self.axPT)
        self.axRL.set_ylabel('ROLL [%s]' % degree)
        self.axRL.set_ylim(-4, 4)
        self.clean_ax(self.axRL)
        self.axHV.set_ylabel('HEAVE [m]')
        self.axHV.set_ylim(-5, 5)

    def clean_ax(self, axes):
        """ Clear temporal axis."""
        axes.fmt_xdata = DateFormatter('%d/%m/%y %H:%M:%S')
        for label in axes.xaxis.get_majorticklabels():
            label.set_visible(False)
        for label in axes.xaxis.get_minorticklabels():
            label.set_visible(False)


if __name__ == '__main__':

    ucd_hms = ['P-07', 'P-08', 'P-09', 'P-12', 'P-15', 'P-18', 'P-19',
               'P-20', 'P-25', 'P-26', 'P-31', 'P-32', 'P-33', 'P-35',
               'P-37', 'P-38', 'P-40', 'P-43', 'P-47', 'P-48', 'P-50',
               'P-51', 'P-52', 'P-53', 'P-54', 'P-55', 'P-56', 'P-57',
               'P-58', 'P-61', 'P-62', 'P-63', 'P-65', 'P-66', 'P-74',
               ]

    ucd = 'P-19'
    # vars=['HEAVE', 'PITCH', 'ROLL', 'INCL', 'DIA_NOITE', 'HEAVE_VEL_M']

    bd = Read_Db('HMS.db')
    bd.list_ucdtb()
    bd.list_vars('TB_UCD_P19')

    ini = datetime.now()
    df = bd.get_data(ucd, vars='all', ini='10/06/2018 00:00:00', fim='11/06/2018 00:00:00')
    fim = datetime.now() - ini
    print('Tempo gasto na requisição de dados: {}'.format(fim))

    qc = HMS_QC(ucd)
    qc.plot_var(df, var_name=None, ini=None, fim=None, step=1)
    qc.plot_var(df, var_name=['HEAVE', 'PITCH', 'ROLL'],
                ini='10/06/2018 08:30:00', fim='10/06/2018 08:40:00', step=1)

    qc.plot_full_qc(df)
    qc.plot_full_qc(df, ini='19/06/2018 00:00:00')
    qc.plot_full_qc(df, ini='10/06/2018 08:30:00', fim='10/06/2018 08:40:00')
    qc.plot_PRH(df, ini='10/06/2018 08:30:00', fim='10/06/2018 08:40:00')
