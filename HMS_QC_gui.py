# -*- coding: utf-8 -*-
# purpose:  check atitude data from database "HMS.db" in sqlite3
# author:   Márcio Yamashita
# created:  09-Octuber-2018
# PYTHON 2/3 compatible code
# modified:

import sys
import glob
import sqlite3
import pandas as pd
from os import getcwd, path
from numpy import asarray, diff

from time import sleep
import matplotlib as mpl
from collections import OrderedDict
import matplotlib.pyplot as plt
from matplotlib.colors import rgb2hex
from datetime import datetime, timedelta
from matplotlib.dates import DateFormatter, YearLocator, MonthLocator
from matplotlib.dates import DayLocator, HourLocator, MinuteLocator
from matplotlib.ticker import (MultipleLocator, FormatStrFormatter,
                               FuncFormatter, AutoMinorLocator)
if sys.version_info.major >= 3:
    basestring = str
    import tkinter as tki
    import tkinter.ttk as ttk
    from tkinter.filedialog import askdirectory
else:
    basestring = basestring
    import Tkinter as tki
    import ttk
    from tkFileDialog import askdirectory

import threading
degree = u"\u00b0"
plt.rcParams['axes.grid'] = True
mpl.rcParams['path.simplify_threshold'] = 1.0
mpl.rcParams['agg.path.chunksize'] = 100000


class HMS_gui:
    """ Interface Gráfica Principal ao Usuário (GUI) baseada em Tkinter. """

    def __init__(self, root):
        """ Instanciar elementos gráficos (widgets) e ações atreladas. """
        # Definição de Cores ========================================= #
        BGCOLORBOX = rgb2hex((1.000, 1.000, 1.000))
        BGCOLORAPL = rgb2hex((0.392, 0.584, 0.929))
        BGCOLORSLT = rgb2hex((0.031, 0.572, 0.815))
        TXCOLORSTD = rgb2hex((0.000, 0.000, 0.000))  # texto padrão (preto)
        # Definição de Atributos ===================================== #
        self._root = root  # Janela gráfica de origem da aplicação.
        self.fmt = '%d/%m/%Y %H:%M:%S'  # Formato de data/hora usual ocn
        self.fmt_bd = '%Y-%m-%d %H:%M:%S'  # Formato de data/hora SQLite3

        # Frame Principal ============================================ #
        self._mainfrm = tki.Frame(self._root, bg=BGCOLORAPL)

        self._menubar = tki.Menu(self._mainfrm)  # Barra de Opções.

        # Barra/menu "Arquivo".
        menu = tki.Menu(self._menubar, tearoff=0)
        menu.add_command(label="Path BD...", command=self.open_dir)
        menu.add_command(label="Sair", command=lambda rt=self._root:
                         (rt.quit(), rt.destroy(), plt.close('all')))
        self._menubar.add_cascade(label=u"Arquivo", menu=menu)

        # Associação da barra de opções ao frame principal.
        self._mainfrm.master.config(menu=self._menubar)

        # ============================================================ #
        # Frame de Banco de dados #
        # ============================================================ #
        self._bdfrm = tki.Frame(self._mainfrm, bg=BGCOLORAPL,
                                bd=2, relief='groove')
        self._bdfrm.pack(fill='both', padx=6, pady=4, side='top')
        # Subframe de Seleção de BD #
        # ============================================================ #
        self._selbdfrm = tki.Frame(self._bdfrm, bg=BGCOLORAPL,
                                   bd=1, relief='groove')
        self._selbdfrm.pack(padx=1, pady=0, side='left', fill='x', expand=1)

        tki.Label(self._selbdfrm, bg=BGCOLORAPL, fg='white', justify='left',
                  text="Path do Banco de dados:").grid()

        self._bd_var = tki.StringVar()  # Variável mutante do BD
        self._bd_var.set(("//sbcnas03/ussub-gds_ocn/Confidencial"
                          "/Dados_UCDs/DESV/HMS_sqlite3"))
        tki.Label(self._selbdfrm, bg=BGCOLORAPL, fg='white', width=70,
                  wraplength=680, font=('bold'), textvariable=self._bd_var,
                  anchor='w').grid(column=1, row=0, padx=1, pady=1, sticky='w')

        self._rot_ucd = tki.Label(self._selbdfrm, bg='yellow',
                                  fg=BGCOLORAPL, text='UCD:')
        self._rot_ucd.grid(column=0, row=1, sticky='w')

        self._ucd = tki.StringVar()  # Variável mutante da UCD
        self._ucd_chosen = ttk.Combobox(self._selbdfrm, width=18,
                                        textvariable=self._ucd)
        self._ucd_chosen.grid(column=0, row=1, sticky='w', padx=50)

        self._ucd_chosen.bind("<<ComboboxSelected>>", self.bd_meta)
        self._ucd_chosen.state(['readonly'])

        for i, j in enumerate(['Aquisição inicial:', 'Aquisição final:']):
            tki.Label(self._selbdfrm, bg=BGCOLORAPL, fg='white', justify='left',
                      text=j).grid(column=1, row=1, stick='w', padx=i * 250)

        self._lst_dt = []  # lista de datas
        for i in range(2):
            var = tki.StringVar()
            tki.Label(self._selbdfrm, fg='white', bg=BGCOLORAPL, width=15,
                      textvariable=var, justify='center').grid(row=1, column=1,
                          sticky='w', padx=100 + i * 250)
            self._lst_dt.append(var)  # self._lst_dt[0] data inicial do BD
                                      # self._lst_dt[1] data final do BD

        # ============================================================ #
        # Frame de Plots #
        # ============================================================ #
        self._pltfrm = tki.LabelFrame(self._mainfrm, bg=BGCOLORAPL,
            bd=2, relief='groove', fg='white', text=' Plots ')
        self._pltfrm.pack(fill='both', padx=6)

        # Subframe de Período para plots #
        # ============================================================ #
        self._datefrm = tki.Frame(self._pltfrm, bg=BGCOLORAPL,
                                  bd=1, relief='groove')
        self._datefrm.pack(padx=1, side="top", anchor="w", fill='x', expand=1)

        Vars = ['Período:', 'Data Inicial', 'até', 'Data Final',
                'Freq.:', 'Tx Amostral']
        for h, i in enumerate(range(0, 12, 2)):
            r = 0 if i % 4 else 1
            tki.Label(self._datefrm, bd=0, bg=BGCOLORAPL, fg='white',
                  text=Vars[h], font=('Verdana', '8', 'bold'),
                  justify='center').grid(column=i, row=r, padx=6) 

        # Campos de entrada da data inicial e final de consulta
        for i, j in enumerate(['_idatent', '_fdatent']):
            var = tki.StringVar()
            setattr(self, j, tki.Entry(self._datefrm, justify='center',
                                       bd=3, width=19, textvariable=var))
            getattr(self, j).grid(column=2 + i * 4, row=1)
            var.trace("w", lambda vn, en, md,
                dn=getattr(self, j): dn.config(fg=TXCOLORSTD))
            self._lst_dt.append(var)  # self._lst_dt[2] data inicial de consulta
                                      # self._lst_dt[3] data final de consulta

        # Botões de acréscimo e decréscimo das datas: inicial e final
        Di = [self._idatent, self._lst_dt[2], self._lst_dt[3], self._lst_dt[0]]
        Df = [self._fdatent, self._lst_dt[3], self._lst_dt[1], self._lst_dt[2]]
        Dd = [u"\u25B2", u"\u25BC", 'n', 's', 1]
        Dh = [u"\u25B3", u"\u25BD", 'n', 's', 1 / 24.]
        for i in range(1, 8, 2):
            Vl = Di if i < 4 else Df
            Vr = Dd if i in [1, 5] else Dh
            for j, k in enumerate([1, -1]):
                tki.Button(self._datefrm, bd=1, text=Vr[j],
                           font=('Default', '5', 'bold'),
                           command=lambda da=Vl[1], dt=Vr[-1] * k,
                           dlim=Vl[2 + j], loc=Vl[0], md=self.moddatevar:
                           md(da, dt, loc, lim=dlim)).grid(column=i, row=1,
                               pady=3 - 3 * j, sticky=Vr[2 + j])

        MODES = [("1Hz", 1), ("0.02Hz-(50s)", 50), ("0.01Hz-(100s)", 100),
                 ("1.66mHz-(600s)", 600)]
        self.step = tki.IntVar(value=1)

        for i, f in enumerate(MODES, start=9):
            tki.Radiobutton(self._datefrm, selectcolor=BGCOLORAPL, fg='white',
                            value=f[1], bg=BGCOLORAPL, font=('Verdana', '8'),
                            text=f[0], variable=self.step).grid(row=1, column=i)

        # Subframe de Seleção de Plots individuais #
        # ============================================================ #
        self._plt_var = tki.LabelFrame(self._pltfrm, bg=BGCOLORAPL,
            fg='white', bd=1, relief='groove', text=' Individuais ')
        self._plt_var.pack(padx=1, side="left", anchor="w")

        self.dict_var = {'Pouso': 'POUSO_PERMITIDO', 'Pitch': 'PITCH',
                         'Pitch Down Max.': 'PITCH_DM',
                         'Pitch Up Max.': 'PITCH_UM', 'Roll': 'ROLL',
                         'Roll Port Max.': 'ROLL_PM',
                         'Roll Starboard Max.': 'ROLL_SM', 'Incl.': 'INCL',
                         'Incl. Max.': 'INCL_M', 'Heave': 'HEAVE',
                         'Heave Max.': 'HEAVE_M', 'Heave Per.': 'HEAVE_PER',
                         'Heave Vel. Max.': 'HEAVE_VEL_M'}

        self.plt_ind_vars = {}
        r = None
        kw = {'selectcolor': BGCOLORAPL, 'bg': BGCOLORAPL, 'fg': 'white'}
        for i, f in enumerate(self.dict_var):
            self.plt_ind_vars[f] = tki.IntVar(value=0)
            cb = tki.Checkbutton(self._plt_var, variable=self.plt_ind_vars[f],
                                 text=f, **kw)
            if i % 4 == 0:
                r = (r if isinstance(r, int) else -1) + 1
            cb.grid(row=r, column=i % 4, sticky='w')

        self._bt_cb(self._plt_var, 'Limpar', self.plt_ind_vars, 0, 3, 3, 0)
        self._bt_cb(self._plt_var, 'Todos', self.plt_ind_vars, 1, 3, 3, 50)
        # Subframe de Seleção de grupo de Plots  #
        # ============================================================ #
        self._plt_group = tki.LabelFrame(self._pltfrm, bg=BGCOLORAPL,
            fg='white', bd=1, relief='groove', text=' Agrupados ')
        self._plt_group.pack(padx=1, side="left", anchor="w",
                             fill='x', expand=1)

        self.group_list = ['Pitch - Roll - Heave',
                           'Heave Max. - Heave Per. - Vel. Heave',
                           'Pitch - Roll - Inclinação - Vel. Heave',
                           'Full Plot']

        self.plt_group_vars = {}
        r = None
        for i, f in enumerate(self.group_list):
            self.plt_group_vars[f] = tki.IntVar(value=0)
            cb = tki.Checkbutton(self._plt_group,
                                 variable=self.plt_group_vars[f], text=f, **kw)
            if i % 4 == 0:
                r = (r if isinstance(r, int) else -1) + 1
            cb.grid(row=i % 4, column=r, sticky='w')

        self._bt_cb(self._plt_group, 'Limpar', self.plt_group_vars, 0, 1, 3, 0)
        self._bt_cb(self._plt_group, 'Todos', self.plt_group_vars, 1, 1, 3, 50)
        # ============================================================ #
        # Frame de Mensagens #
        # ============================================================ #
        self._msgfrm = tki.LabelFrame(self._mainfrm, bg=BGCOLORAPL,
            bd=2, relief='groove', fg='white', text=' Mensagens ')
        self._msgfrm.pack(fill='both', padx=6)

        # Variável mutante da mensagem
        self._msg = tki.StringVar()
        self._msg.set("Escolha Banco de dados da UCD:\n")
        # Opções de mensagem
        self._qrymsgbox = tki.Label(self._msgfrm, fg='yellow', bg=BGCOLORAPL,
                                    font=('Verdana', '12', 'bold'), bd=1,
                                    textvariable=self._msg, justify='center')
        self._qrymsgbox.grid(column=1, row=2, padx=1, pady=1, sticky='n')

        # ============================================================ #
        # Frame de Execução #
        # ============================================================ #
        self._modfrm = tki.LabelFrame(self._mainfrm, bg=BGCOLORAPL,
            bd=2, relief='groove', fg='white', text=' Execução ')
        self._modfrm.pack(padx=6, anchor='e')

        self._bt_close = tki.Button(self._modfrm, text="Fechar Figuras",
                                    command=lambda: plt.close('all'))
        self._bt_close.grid(padx=6, sticky='w')

        self._bt_plot = tki.Button(self._modfrm, text="Plotar",
                                   state='disabled',
                                   command=lambda: self.plt_bt(True))
        self._bt_plot.grid(column=1, row=0, sticky='e', ipadx=50)

        # ============================================================ #
        # Frame de Qualificação #
        # ============================================================ #
        self._qcfrm = tki.LabelFrame(self._mainfrm, bg=BGCOLORAPL,
            bd=2, relief='groove', fg='white', text=' Qualificação ')
        self._qcfrm.pack(fill='x', padx=6, pady=4, side='bottom')

        # Subframe de QC #
        # ============================================================ #
        self._qc_data = tki.Frame(self._qcfrm, bg=BGCOLORAPL,
                                  bd=1, relief='groove')
        self._qc_data.pack(padx=1, anchor="w", fill='x', expand=1)

        self._get_qc_data = tki.Button(self._qc_data, text="Analizar",
                                       state='disabled',
                                       command=lambda: self.plt_bt())
        self._get_qc_data.grid(padx=6, sticky='w')

        # Variável mutante da mensagem
        self._msg_qc = tki.StringVar()
        # Opções de mensagem
        self._msg_boxqc = tki.Label(self._qc_data, bd=1, fg='yellow',
                                    bg=BGCOLORAPL, justify='center',
                                    font=('Verdana', '12', 'bold'),
                                    textvariable=self._msg_qc)
        self._msg_boxqc.grid(column=1, columnspan=6, row=0,
                             padx=1, pady=1, sticky='n')

        Vars = ['Série Esperada: ', 'PITCH', 'Série Recebida: ', 'ROLL',
                'Série Valida: ', 'HEAVE']
        idx = None
        lst = ['_QC_data', 'PITCH_describe', 'ROLL_describe', 'HEAVE_describe']
        [setattr(self, k, []) for k in lst]
        for i in range(1, 7, 2):
            for j in range(1, 3):
                idx = (idx if isinstance(idx, int) else -1) + 1
                tki.Label(self._qc_data, bg=BGCOLORAPL, text=Vars[idx],
                          fg='white', justify='left').grid(column=i, row=j)
            v = tki.StringVar()
            ent = tki.Label(self._qc_data, fg='yellow', bg=BGCOLORAPL,
                            width=16, font=('Verdana', '12', 'bold'),
                            textvariable=v, justify='center')
            ent.grid(row=1, column=i + 1, sticky='w')
            self._QC_data.append(ent)
            self._QC_data.append(v)
            describe = lst.pop(1)
            for k in range(3, 10):
                v = tki.StringVar()
                tki.Label(self._qc_data, fg='yellow', width=10, textvariable=v,
                          font=('Verdana', '10'), justify='center',
                          bg=BGCOLORAPL).grid(row=k, column=i, sticky='w')
                getattr(self, describe).append(v)

        self._tip_msg = tki.StringVar()
        self._QC_data[4].bind("<Enter>", self._tip_enter)
        self._QC_data[4].bind("<Leave>", self._tip_close)

        Vars = ['Média: ', 'Desvio: ', 'Min: ', '25%', '50%', '75%', 'Max: ']
        for i, j in enumerate(Vars, start=3):
            tki.Label(self._qc_data, bg=BGCOLORAPL, fg='white', justify='left',
                      text=j).grid(column=0, row=i)

        # Subframe de Exportação de dados #
        # ============================================================ #
        self._qc_exp = tki.LabelFrame(self._qcfrm, bg=BGCOLORAPL,
            fg='white', bd=1, relief='groove', text=' Exportação ')
        self._qc_exp.pack(padx=1, side='top', fill='both')

        self._qc_data = tki.Frame(self._qc_exp, bg=BGCOLORAPL,
                                  bd=1, relief='groove')
        self._qc_data.pack(padx=1, side="top", anchor="w", fill='x', expand=1)

        self.exp_vars = {}
        r = None
        for i, f in enumerate(self.dict_var):
            self.exp_vars[f] = tki.IntVar(value=0)
            cb = tki.Checkbutton(self._qc_data, text=f, selectcolor=BGCOLORAPL,
                bg=BGCOLORAPL, fg='white', variable=self.exp_vars[f])
            if i % 5 == 0:
                r = (r if isinstance(r, int) else -1) + 1
            cb.grid(row=r, column=i % 5, sticky='w')

        self._bt_cb(self._qc_data, 'Limpar', self.exp_vars, 0, 4, 2, 0)
        self._bt_cb(self._qc_data, 'Todos', self.exp_vars, 1, 4, 2, 50)

        self.exp_all = tki.Button(self._qc_data, state='disabled',
                                  text="Exportar Série recebida",
                                  command=lambda: self.export_data('all'))
        self.exp_all.grid(column=6, row=0, sticky='w', padx=80)

        self.exp_valid = tki.Button(self._qc_data, state='disabled',
                                    text="Exportar Série válida",
                                    command=lambda: self.export_data('valid'))
        self.exp_valid.grid(column=6, row=2, sticky='w', padx=80)

        self._mainfrm.pack(fill='both', side='top')
        self._mainfrm.bind('<Double-Button-3>', self._qc_mode)
        self._root.bind("Q", self._qc_mode_)
        self._mainfrm.bind('<Double-Button-1>', self._normal_mode)

        [self._set_state(x, 'disable') for x in self._pltfrm.winfo_children()]

        # Pré-inicialização da aplicação com consulta ao BD.
        self.list_ucd()
        self.plot = False
        self._root.update()
        # Carregamento de agrupamentos de UCDs definidos pela aplicação.

    def _qc_mode(self, event):
        self._root.geometry("850x600")

    def _qc_mode_(self, event):
        self._root.geometry("850x700")

    def _normal_mode(self, event):
        self._root.geometry("850x350")

    def _destroyWindow(self):
        root.quit()
        root.destroy()

    def _set_state(self, childList, state):
        [ch.configure(state=state) for ch in childList.winfo_children()]

    def _cb_B(self, list_cb, boolean):
        [value.set(boolean) for key, value in list_cb.items()]

    def _bt_cb(self, bt, text, loc, lig, col, row, padx):
        tki.Button(bt, text=text,
            command=lambda: self._cb_B(loc, lig)).grid(column=col,
                row=row, sticky='w', padx=padx)

    def open_dir(self):
        self._bd_var.set(askdirectory())
        self.list_ucd()
        self._clear_dates()

    def list_ucd(self):
        self._lista = glob.glob(path.join(self._bd_var.get(), '*.db'))
        self._ucd_chosen.set('')
        self._ucd_chosen['values']=[path.basename(i)[:-3] for i in self._lista]

    def _toplevel(self, name):
        x = self._root.winfo_x()
        y = self._root.winfo_y()
        self.top = tki.Toplevel()
        self.top.title(name)
        self.top.geometry("%dx%d+%d+%d" % (250, 20, x + 100, y + 120))
        progress = ttk.Progressbar(self.top, orient="horizontal",
                                   length=250, mode="indeterminate")
        progress.pack()
        progress.start()
        self.top.attributes("-topmost", True, "-toolwindow", True)

    def _tip_enter(self, event=None):
        x = y = 0
        x, y, cx, cy = self._QC_data[4].bbox("insert")
        x += self._QC_data[4].winfo_rootx() + 25
        y += self._QC_data[4].winfo_rooty() + 20
        self.tw = tki.Toplevel(self._QC_data[4])
        # Leaves only the label and removes the app window
        self.tw.wm_overrideredirect(True)
        self.tw.wm_geometry("+%d+%d" % (x, y))
        tki.Label(self.tw, text=self._tip_msg.get(), justify='left',
                  bg='yellow', font=('Verdana', '8', 'normal')).pack(ipadx=1)

    def _tip_close(self, event=None):
        if self.tw:
            self.tw.destroy()

    def _clear_dates(self):
        self.data = None
        [i.set('') for i in self._lst_dt]
        describe = ['PITCH_describe', 'ROLL_describe', 'HEAVE_describe']
        [getattr(self, i)[j].set('') for j in range(7) for i in describe]
        self._msg_qc.set('')
        self._tip_msg.set('')
        self._QC_data[1].set('')
        self._QC_data[3].set('')
        self._QC_data[5].set('')
        [self._set_state(x, 'disable') for x in self._pltfrm.winfo_children()]
        self._bt_plot.config(state='disabled')
        self._get_qc_data.config(state='disabled')
        self._root.update_idletasks()

    def get_dt(self, str_date, fmt, err_msg=None, loc=None):
        try:
            return datetime.strptime(str_date, fmt)
        except Exception:
            if err_msg:
                self.msg_temp(err_msg)
            if loc:
                loc.config(fg='red')
            return None

    def bd_meta(self, eventObject):
        y = getattr(self, 'top', None)
        if (y is not None) and (y.winfo_exists()):
            self.top.destroy()
        self._toplevel('Conectando {}.'.format(self._ucd.get()))
        self._clear_dates()
        y = getattr(self, '_last_ini', None)
        if y is not None:
            delattr(self, '_last_ini')
        t = threading.Thread(target=self.conn_bd_interval)
        t.daemon = True
        t.start()
        self.top.after(500, self.check_if_running, t, self.top)

    def moddatevar(self, date, dtdays, loc=None, lim=None):
        """ Aumentar/diminuir data do período de consulta. """
        var = self.get_dt(date.get(), self.fmt, 'Corrigir Data', loc)
        if var:
            var = var + timedelta(dtdays)
        else:
            return
        if dtdays < 0:
            if var < self.get_dt(lim.get(), self.fmt):
                self.msg_temp('{:{f}} é menor que data liberada.\n'.format(
                    var, f=self.fmt))
                return
        else:
            if var > self.get_dt(lim.get(), self.fmt):
                self.msg_temp('{:{f}} é maior que data liberada.\n'.format(
                    var, f=self.fmt))
                return
        date.set(var.strftime(self.fmt))
        loc.config(fg='black')

    def msg_temp(self, msg):
        old = self._msg.get()
        self._qrymsgbox.config(fg='purple')
        self._msg.set(msg)
        self._root.update()
        sleep(1.5)
        self._qrymsgbox.config(fg='yellow')
        self._msg.set(old)

    def conn_bd_interval(self):
        bd = self._lista[self._ucd_chosen.current()]
        try:
            conn = sqlite3.connect(
            bd, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
            curs = conn.cursor()
            ucd = self._ucd_chosen.get()
            curs.execute(('SELECT MIN(DT_AQUISICAO), MAX(DT_AQUISICAO)'
                          ' FROM TB_IMPORTACOES'))
            dates = curs.fetchall()
            if self._ucd.get() != ucd:
                return
            ini = self.get_dt(dates[0][0], self.fmt_bd)
            fin = self.get_dt(dates[0][1], self.fmt_bd)
            dti = fin - timedelta(days=5)
            self._rot_ucd.configure(bg=rgb2hex((0.392, 0.584, 0.929)), fg='white')
            self._lst_dt[0].set(ini.strftime(self.fmt))
            self._lst_dt[1].set(fin.strftime(self.fmt))
            (self._lst_dt[2].set(ini.strftime(self.fmt)) if dti < ini
             else self._lst_dt[2].set(dti.strftime(self.fmt)))
            self._lst_dt[3].set(fin.strftime(self.fmt))
            [self._set_state(x, 'normal') for x in self._pltfrm.winfo_children()]
            self._bt_plot.config(state='normal')
            self._get_qc_data.config(state='normal')
            self._msg.set("Escolha parâmetros para Plots.\n")
        except Exception as err:
            self._msg.set("Falha na conexão do Banco de Dados.\n")

    def plt_bt(self, plot=False):
        self.plot = plot
        if self._check_date():
            return
        self._msg_qc.set('{0} entre: {{({1}) e ({2})}}'.format(
            self._ucd.get(), self._lst_dt[2].get(), self._lst_dt[3].get()))
        y = getattr(self, '_last_ini', None)
        if y is not None:
            if (self._last_ini <= self._lst_dt[2].get()) and (self._last_fin >= self._lst_dt[3].get()):
                self._bt_plot.after(1000, self.check_if_data, self._bt_plot)
                return
        self._msg.set(('Data inicial: {}\n'
                       'Data final: {}').format(self._lst_dt[2].get(),
                                                self._lst_dt[3].get()))
        self._last_ini = self._lst_dt[2].get()
        self._last_fin = self._lst_dt[3].get()
        self._toplevel('Requisitando dados de {}.'.format(self._ucd.get()))
        t2 = threading.Thread(target=self.get_data)
        t2.daemon = True
        t2.start()
        self.top.after(200, self.check_if_running, t2, self.top)
        self._bt_plot.after(1000, self.check_if_data, self._bt_plot)

    def get_data(self):
        self.data = None
        ini = self.get_dt(self._lst_dt[2].get(), self.fmt)
        fin = self.get_dt(self._lst_dt[3].get(), self.fmt)
        bd = self._lista[self._ucd_chosen.current()]
        conn = sqlite3.connect(
            bd, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
        tb_ = ''.join(x for x in self._ucd_chosen.get() if x.isalnum())
        idx = 'TB_UCD_{}.DT_AQUISICAO'.format(tb_)
        sel = 'SELECT * FROM TB_UCD_{}'.format(tb_)
        fil = "WHERE {0}>='{1}' AND {0}<='{2}'".format(idx, ini, fin)
        qyr_order = 'ORDER BY {};'.format(idx)
        full_qry = '{} {} {}'.format(sel, fil, qyr_order)
        self.data = pd.read_sql_query(full_qry, conn, index_col='DT_AQUISICAO',
                        parse_dates={'DT_AQUISICAO': '%Y-%m-%d %H:%M:%S'})

    def _fitting(self):
        df = self.data[~self.data.index.duplicated()]
        if df.empty:
            return
        ini = self.get_dt(self._lst_dt[2].get(), self.fmt)
        fin = self.get_dt(self._lst_dt[3].get(), self.fmt)
        self.fit_data = df[ini: fin]
        index2 = pd.date_range(ini, fin, freq='s')
        qcdata = self.fit_data.reindex(index2)
        valid = qcdata.drop_duplicates()
        self._QC_data[1].set('{:.0f}'.format(index2.size))
        self._QC_data[3].set('{:.0f}'.format(self.fit_data.shape[0]))
        self._QC_data[5].set('{:.0f}'.format(valid.shape[0]))
        self._tip_msg.set(('{:.2f}% da série esperada\n'
            '{:.2f}% da série recebida').format(
                100 * valid.shape[0] / index2.size,
                100 * valid.shape[0] / self.fit_data.shape[0]))
        if index2.size != self.fit_data.shape[0]:
            self._QC_data[2].config(fg='purple')
        else:
            self._QC_data[2].config(fg='yellow')
        self.valid = valid
        self.exp_valid.configure(state='normal')
        self.exp_all.configure(state='normal')
        self.describe_serie()

    def describe_serie(self):
        desc = self.valid.describe()
        describe = ['PITCH_describe', 'ROLL_describe', 'HEAVE_describe']
        Vars = ['mean', 'std', 'min', '25%', '50%', '75%', 'max']
        for i in describe:
            for j, k in enumerate(Vars):
                val = '{:.2f}'.format(getattr(desc, i.split('_')[0])[k])
                getattr(self, i)[j].set(val)

    def _check_date(self):
        ini = self.get_dt(self._lst_dt[2].get(), self.fmt,
                          'Corrigir data inicial.', self._idatent)
        L_i = self.get_dt(self._lst_dt[0].get(), self.fmt)
        fin = self.get_dt(self._lst_dt[3].get(), self.fmt,
                          'Corrigir data final.', self._fdatent)
        L_f = self.get_dt(self._lst_dt[1].get(), self.fmt)
        if None in (ini, fin):
            return True
        elif ini > fin:
            self._idatent.config(fg='red')
            self._fdatent.config(fg='red')
            self.msg_temp('Data inicial maior que final.')
            return True
        elif ini == fin:
            self._idatent.config(fg='red')
            self._fdatent.config(fg='red')
            self.msg_temp('Datas idênticas.')
            return True
        if ini < L_i:
            self._lst_dt[2].set(L_i.strftime(self.fmt))
        if fin > L_f:
            self._lst_dt[3].set(L_f.strftime(self.fmt))
        self._root.update_idletasks()

    def check_if_data(self, window):
        """Check if the data is returned."""
        y = getattr(self, 'data', None)
        if y is not None:
            if y.empty:
                self._msg.set('Dados inválidos.\n')
                return
            self._msg.set('Dados armazenados em memória.\n')
            self._fitting()
            self._root.update_idletasks()
            if self.plot:
                self._msg.set('Plots das variáveis.\n')
                self._root.update_idletasks()
                self.plt_ind_var()
                self.plt_group()
                self.plot = False
        else:
            window.after(1000, self.check_if_data, window)

    def check_if_running(self, thread, window):
        """Check if the function is finished."""
        if thread.is_alive():
            window.after(200, self.check_if_running, thread, window)
        else:
            window.destroy()

    def plt_ind_var(self):
        df = self.fit_data[::int(self.step.get())].copy()
        for key, value in self.plt_ind_vars.items():
            if value.get() > 0:
                fig = plt.figure(facecolor=(1.0, 1.0, 1.0), figsize=(12, 8))
                fig.show()
                fig.canvas.draw()
                ax = fig.add_subplot(1, 1, 1)
                if df.empty:
                    msg = 'Plot request returned an empty object'
                    kw = {'family': 'serif', 'style': 'italic',
                          'ha': 'center', 'wrap': True}
                    ax.text(0.5, 0.5, msg, **kw)
                else:
                    ax.cla()
                    ax.plot(df.index, df[self.dict_var[key]])
                    ax.set_title('{} @ {}'.format(key, self._ucd.get()))
                    self._set_plot(ax)

    def plt_group(self):
        dict_group = {'Pitch - Roll - Heave': 'plot_PRH',
                      'Heave Max. - Heave Per. - Vel. Heave': 'plot_HV',
                      'Pitch - Roll - Inclinação - Vel. Heave': 'plot_PRIV',
                      'Full Plot': 'plot_full_qc'}
        self.dict_fig = OrderedDict(
            [("axPT", [(0, "PITCH", degree), (1, "PITCH_DM"), (1, "PITCH_UM"),
                       (2, "LIM", "CATEGORIA_AERONAVE", 3, 4)]),
             ("axRL", [(0, "ROLL", degree), (1, "ROLL_PM"), (1, "ROLL_SM"),
                       (2, "LIM", "CATEGORIA_AERONAVE", 3, 4)]),
             ("axIN", [(0, "INCL", degree), (1, "INCL_M"),
                       (2, "LIM", "CATEGORIA_AERONAVE", -3.5, -4.5)]),
             ("axHV", [(0, "HEAVE", 'm'),
                       (2, "LIM", "DIA_NOITE", 5, 4)]),
             ("axHV_M", [(0, "HEAVE_M", 'm')]),
             ("axHV_P", [(0, "HEAVE_PER", 's')]),
             ("axVArf", [(0, "HEAVE_VEL_M", 'm/s'),
                         (2, "LIM", "DIA_NOITE", -1.3, -1)])])
        data = self.fit_data
        for key, value in self.plt_group_vars.items():
            if value.get() > 0:
                getattr(self, dict_group[key])(data)

    def plot_PRH(self, data):
        """ Plot of variables PITCH, ROLL and HEAVE."""
        dic = self.dict_fig.copy()
        dic.pop('axIN')
        dic.pop('axHV_M')
        dic.pop('axHV_P')
        dic.pop('axVArf')
        fig, (self.axPT, self.axRL, self.axHV) = plt.subplots(
            nrows=3, facecolor=(1.0, 1.0, 1.0), figsize=(12, 8), sharex=True)
        fig.show()
        fig.canvas.draw()
        self.axPT.set_title('Atitudes @ {}'.format(self._ucd.get()))
        if data.empty:
            msg = 'Plot request returned an empty object'
            kw = {'family': 'serif', 'style': 'italic',
                  'ha': 'center', 'wrap': True}
            [getattr(self, i).text(0.5, 0.5, msg, **kw) for i in dic.keys()]
        else:
            for i in dic:
                self.plot_sub(data, i, dic[i])

    def plot_HV(self, data):
        """ Plot of variables HEAVE MAX., HEAVE T. and HEAVE VEL."""
        dic = self.dict_fig.copy()
        dic.pop('axPT')
        dic.pop('axRL')
        dic.pop('axIN')
        dic.pop('axHV')
        fig, (self.axHV_M, self.axHV_P, self.axVArf) = plt.subplots(
            nrows=3, facecolor=(1.0, 1.0, 1.0), figsize=(12, 8), sharex=True)
        fig.show()
        fig.canvas.draw()
        self.axHV_M.set_title('HEAVE @ {}'.format(self._ucd.get()))
        if data.empty:
            msg = 'Plot request returned an empty object'
            kw = {'family': 'serif', 'style': 'italic',
                  'ha': 'center', 'wrap': True}
            [getattr(self, i).text(0.5, 0.5, msg, **kw) for i in dic.keys()]
        else:
            for i in dic:
                self.plot_sub(data, i, dic[i])

    def plot_PRIV(self, data):
        """ Plot of variables PITCH, ROLL, INCLINATION and HEAVE."""
        dic = self.dict_fig.copy()
        dic.pop('axHV')
        dic.pop('axHV_M')
        dic.pop('axHV_P')
        fig, (self.axPT, self.axRL, self.axIN, self.axVArf) = plt.subplots(
            nrows=4, facecolor=(1.0, 1.0, 1.0), figsize=(12, 8), sharex=True)
        fig.show()
        fig.canvas.draw()
        self.axPT.set_title('Atitudes @ {}'.format(self._ucd.get()))
        if data.empty:
            msg = 'Plot request returned an empty object'
            kw = {'family': 'serif', 'style': 'italic',
                  'ha': 'center', 'wrap': True}
            [getattr(self, i).text(0.5, 0.5, msg, **kw) for i in dic.keys()]
        else:
            for i in dic:
                self.plot_sub(data, i, dic[i])

    def plot_full_qc(self, data):
        """ Plot of variables determinants to close helideck."""
        dic = self.dict_fig.copy()
        dic.pop('axHV')
        dic.pop('axHV_M')
        dic.pop('axHV_P')
        fig, (self.axPT, self.axRL, self.axIN, self.axVArf) = plt.subplots(
            nrows=4, facecolor=(1.0, 1.0, 1.0), figsize=(12, 8), sharex=True)
        fig.show()
        fig.canvas.draw()
        self.axPT.set_title('Atitudes @ {}'.format(self._ucd.get()))
        if data.empty:
            msg = 'Plot request returned an empty object'
            kw = {'family': 'serif', 'style': 'italic',
                  'ha': 'center', 'wrap': True}
            [getattr(self, i).text(0.5, 0.5, msg, **kw) for i in dic.keys()]
        else:
            for i in dic:
                self.plot_sub(data, i, dic[i])
            step = int(self.step.get())
            df = data['POUSO_PERMITIDO'][::step]
            mask = df == 0
            self.axVArf.plot(df[mask], 'ro')

    def plot_sub(self, data, axes_name, dict_key):
        """ Plot key of dict (inside key type 0, 1, 2)."""
        ax = getattr(self, axes_name)
        kw = {'facecolor': 'green', 'alpha': 0.2}
        df = data[::int(self.step.get())].copy()
        for i in dict_key:
            if i[0] == 1:
                ax.plot(df.index, df[i[1]], ':', color='coral')
            elif i[1] == 'LIM':
                val = asarray([i[4] if j else i[3] for j in data[i[2]]])
                ax.fill_between(data.index,
                                -(abs(val) + val) / 2, abs(val), **kw)
            elif i[0] == 0:
                ax.plot(df.index, df[i[1]], '.-')
                ax.set_ylabel(u'{} [{}]'.format(i[1], i[2]))
        self._set_plot(ax)

    def _set_plot(self, ax):
        ax.autoscale(enable=True, axis='x', tight=True)
        ax.get_yaxis().set_major_formatter(FuncFormatter(lambda x,
                                                         p: '%1.2f' % x))
        ax.fmt_xdata = DateFormatter('%d/%m/%y %H:%M:%S')
        ax.xaxis.set_minor_locator(AutoMinorLocator())
        dt_fmt = '%d/%m\n%H:%M:%S' if diff(
            ax.xaxis.get_data_interval()) < 1 else '%d/%m/%y\n%H:%M'
        ax.xaxis.set_major_formatter(DateFormatter(dt_fmt))
        [l.set_rotation(45) for l in ax.xaxis.get_majorticklabels()]

    def export_data(self, serie='all'):
        keys = [key for key, value in self.exp_vars.items() if value.get() > 0]
        var = []
        for i in keys:
            var.append(self.dict_var[i])
        if serie == 'all':
            output = self.fit_data[var].copy()
            fim = '_raw.csv'
        else:
            output = self.valid[var].copy()
            fim = '_valid.csv'
        name = '{}_{:{fmt}}_{:{fmt}}{}'.format(self._ucd.get(),
            output.index.min(), output.index.max(), fim, fmt='%Y%m%d%H%M%S')
        output.to_csv(path.join(getcwd(), name))
        return self.msg_temp('Dados exportados.\n')

def main():
    """Função para rodar interface usuário."""
    root = tki.Tk()
    root.title("HMS plot for Quality Control" +
               " - OCEANOP - version 0.1")
    root.geometry("850x350+250+250")
    root.resizable(width=False, height=False)
    HMS_gui(root)
    root.mainloop()


if __name__ == "__main__":
    main()
