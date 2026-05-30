"""
dashboard.py — Interface graphique K-Means MapReduce — Capteurs IoT
Groupe 2 — Master IFRI Big Data
"""
import multiprocessing
multiprocessing.freeze_support()

import tkinter as tk
from tkinter import ttk
import subprocess
import threading
import sys
import os
import json
import csv

# ── Palette ───────────────────────────────────────────────────────────
BG        = "#080812"
CARD      = "#0e0e1c"
PANEL     = "#0b0b18"
SIDEBAR   = "#0d0d1e"
BTN       = "#141428"
BTN_HVR   = "#1e1e3a"
ACCENT    = "#6d28d9"
ACCENT2   = "#8b5cf6"
GREEN     = "#10b981"
GREEN2    = "#34d399"
YELLOW    = "#f59e0b"
RED       = "#ef4444"
CYAN      = "#06b6d4"
PINK      = "#ec4899"
ORANGE    = "#f97316"
TEXT      = "#f1f5f9"
MUTED     = "#475569"
BORDER    = "#1e1e3a"
SEP       = "#1a1a30"
RESULT_BG = "#0a0a18"

CLUSTER_COLORS  = ["#6d28d9", "#06b6d4", "#10b981", "#f59e0b", "#ec4899"]
REGION_COLORS   = {
    "server_nord":   "#06b6d4",
    "server_centre": "#8b5cf6",
    "server_sud":    "#10b981",
    "server_ouest":  "#f59e0b",
    "server_est":    "#ec4899",
}

F_MONO  = ("Courier New", 10)
F_BTN   = ("Segoe UI", 10)
F_SMALL = ("Segoe UI", 9)
F_TINY  = ("Segoe UI", 8)

PROJECT_DIR    = os.path.dirname(os.path.abspath(__file__))
SPINNER_FRAMES = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"]
PAGE_SIZE      = 100

DATA_FILES = [
    ("server_nord.csv",   "Serveur Nord   (300 capteurs)"),
    ("server_centre.csv", "Serveur Centre (300 capteurs)"),
    ("server_sud.csv",    "Serveur Sud    (300 capteurs)"),
    ("server_ouest.csv",  "Serveur Ouest  (300 capteurs)"),
    ("server_est.csv",    "Serveur Est    (300 capteurs)"),
    ("all_sensors.csv",   "Tous les capteurs (1 500)"),
    ("large_sensors.csv", "Stress test (50 000 capteurs)"),
]

CSV_COLS = ["sensor_id", "region", "latitude", "longitude",
            "temperature", "humidity", "vibration", "network_traffic"]
COL_HDRS = ["ID", "Région", "Lat", "Lon", "Temp °C", "Hum %", "Vibr.", "Réseau"]
COL_W    = [50, 110, 70, 70, 80, 70, 70, 90]


def _interpret_cluster(centroid):
    temp, hum, vib, net = centroid
    tags = []
    if temp >= 32:     tags.append("temp. élevée")
    elif temp <= 24:   tags.append("temp. fraîche")
    else:              tags.append("temp. modérée")
    if hum >= 70:      tags.append("forte humidité")
    elif hum <= 48:    tags.append("air sec")
    if vib >= 2.0:     tags.append("vibrations intenses")
    elif vib <= 0.8:   tags.append("vibrations faibles")
    if net >= 420:     tags.append("trafic réseau dense")
    elif net <= 160:   tags.append("trafic réseau faible")

    if temp >= 32 and hum >= 70:
        profil = "Zone chaude & humide — type forêt / zone équatoriale"
    elif vib >= 2.0 and net >= 420:
        profil = "Zone industrielle / urbaine active"
    elif temp <= 24 and net >= 420:
        profil = "Centre de données ou zone urbaine froide"
    elif vib >= 1.5:
        profil = "Site industriel — machines en fonctionnement"
    else:
        profil = "Zone suburbaine équilibrée — conditions modérées"
    return profil, ", ".join(tags)


def _color_tag(line):
    l = line.lower()
    if any(k in l for k in ("convergence", "terminé", "✓", "succès")): return "green"
    if any(k in l for k in ("timeout", "erreur", "error", "✗")):        return "red"
    if any(k in l for k in ("itération", "iteration", "vague", "wave")): return "cyan"
    if any(k in l for k in ("===", "───", "---", "╔", "╚", "║", "═")):  return "muted"
    if any(k in l for k in ("mapreduce", "k-means", "classique",
                             "benchmark", "streaming", "distribué")):    return "yellow"
    if any(k in l for k in ("cluster", "centroïde", "centroid")):        return "bold"
    return "white"


def _darken(hex_color, f=0.55):
    r, g, b = int(hex_color[1:3],16), int(hex_color[3:5],16), int(hex_color[5:7],16)
    return f"#{int(r*f):02x}{int(g*f):02x}{int(b*f):02x}"


# ─────────────────────────────────────────────────────────────────────
class Dashboard(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("K-Means MapReduce  ·  Capteurs IoT  ·  Groupe 2 IFRI")
        self.geometry("1360x920")
        self.minsize(1000, 700)
        self.configure(bg=BG)
        self._proc      = None
        self._running   = False
        self._spin_idx  = 0
        self._spin_job  = None
        # données browser
        self._all_rows  = []
        self._filtered  = []
        self._page      = 0
        self._build()

    # ── Construction ─────────────────────────────────────────────────
    def _build(self):
        self._build_header()
        body = tk.Frame(self, bg=BG)
        body.pack(fill=tk.BOTH, expand=True)
        body.columnconfigure(1, weight=1)
        body.rowconfigure(0, weight=1)
        self._build_sidebar(body)
        self._build_main(body)
        self._build_statusbar()

    # ── Header ───────────────────────────────────────────────────────
    def _build_header(self):
        hdr = tk.Frame(self, bg=PANEL, height=56)
        hdr.pack(fill=tk.X)
        hdr.pack_propagate(False)
        tk.Frame(hdr, bg=ACCENT, width=4).pack(side=tk.LEFT, fill=tk.Y)
        tk.Label(hdr, text="◈  K-MEANS MAPREDUCE",
                 font=("Segoe UI", 14, "bold"), bg=PANEL, fg=TEXT
                 ).pack(side=tk.LEFT, padx=14, pady=10)
        tk.Label(hdr, text="Capteurs IoT · 5 Serveurs Régionaux",
                 font=F_SMALL, bg=PANEL, fg=MUTED).pack(side=tk.LEFT)
        right = tk.Frame(hdr, bg=PANEL)
        right.pack(side=tk.RIGHT, padx=18)
        tk.Label(right, text="Groupe 2",
                 font=("Segoe UI", 10, "bold"), bg=PANEL, fg=ACCENT2
                 ).pack(anchor="e", pady=(12, 0))
        tk.Label(right, text="Master 1 GL · IFRI · Big Data",
                 font=F_TINY, bg=PANEL, fg=MUTED).pack(anchor="e")

    # ── Sidebar ──────────────────────────────────────────────────────
    def _build_sidebar(self, parent):
        side = tk.Frame(parent, bg=SIDEBAR, width=238)
        side.grid(row=0, column=0, sticky="nsew")
        side.pack_propagate(False)

        self._slbl(side, "  PARAMÈTRES", top=14)
        pf = tk.Frame(side, bg=SIDEBAR)
        pf.pack(fill=tk.X, padx=12, pady=4)
        self.var_k       = self._prow(pf, "K (clusters)",     "3")
        self.var_iter    = self._prow(pf, "Max itérations",   "10")
        self.var_waves   = self._prow(pf, "Vagues streaming", "6")
        self.var_delay   = self._prow(pf, "Délai vague (s)",  "1.0")
        self.var_sensors = self._prow(pf, "Capteurs / vague", "100")

        self._slbl(side, "  EXÉCUTER", top=14)
        self.btns = {}
        for key, color, icon, lbl, cmd in [
            ("gen",    GREEN2,  "⬡", "Générer les données",    self._run_gen),
            ("class",  CYAN,    "▶", "K-Means Classique",      self._run_classic),
            ("mr",     ACCENT2, "⚡", "MapReduce Chunk",        self._run_mr),
            ("dist",   ACCENT,  "◉", "Multi-Serveurs (5 srv)", self._run_dist),
            ("stream", YELLOW,  "≋", "Streaming continu",      self._run_stream),
            ("bench",  PINK,    "≡", "Benchmark complet",      self._run_bench),
            ("simul",  ORANGE,  "⧗", "Simulation Timeout",     self._run_simulation),
        ]:
            frm = tk.Frame(side, bg=SIDEBAR)
            frm.pack(fill=tk.X, padx=12, pady=2)
            tk.Frame(frm, bg=color, width=3).pack(side=tk.LEFT, fill=tk.Y)
            b = tk.Button(frm, text=f"  {icon}  {lbl}", font=F_BTN,
                          bg=BTN, fg=TEXT, activebackground=BTN_HVR,
                          activeforeground=TEXT, bd=0, pady=8, anchor="w",
                          cursor="hand2", relief="flat", command=cmd)
            b.pack(side=tk.LEFT, fill=tk.X, expand=True)
            b.bind("<Enter>", lambda e, w=b, c=color: w.configure(bg=BTN_HVR, fg=c))
            b.bind("<Leave>", lambda e, w=b: w.configure(bg=BTN, fg=TEXT))
            self.btns[key] = b

        self._slbl(side, "  RÉSULTATS", top=14)
        self.canvas_chart = tk.Canvas(side, bg=CARD, height=120,
                                      bd=0, highlightthickness=1,
                                      highlightbackground=BORDER)
        self.canvas_chart.pack(fill=tk.X, padx=12, pady=(4, 2))
        self._draw_empty_chart()
        self.chart_info = tk.Label(side, text="", font=F_TINY,
                                   bg=SIDEBAR, fg=MUTED, justify=tk.LEFT, anchor="w")
        self.chart_info.pack(fill=tk.X, padx=14, pady=(2, 6))

        tk.Frame(side, bg=SIDEBAR).pack(fill=tk.BOTH, expand=True)
        stop_f = tk.Frame(side, bg=SIDEBAR)
        stop_f.pack(fill=tk.X, padx=12, pady=(0, 10))
        self.btn_stop = tk.Button(stop_f, text="⏹  Arrêter l'exécution",
                                  font=F_BTN, bg="#180808", fg=RED,
                                  activebackground=RED, activeforeground="white",
                                  bd=0, pady=8, cursor="hand2", relief="flat",
                                  command=self._stop, state=tk.DISABLED)
        self.btn_stop.pack(fill=tk.X)
        self.btn_stop.bind("<Enter>", lambda e: self.btn_stop.configure(bg=RED, fg="white"))
        self.btn_stop.bind("<Leave>", lambda e: self.btn_stop.configure(bg="#180808", fg=RED))

    # ── Panneau principal ─────────────────────────────────────────────
    def _build_main(self, parent):
        main = tk.Frame(parent, bg=BG)
        main.grid(row=0, column=1, sticky="nsew", padx=(1, 0))
        main.rowconfigure(0, weight=1)
        main.columnconfigure(0, weight=1)

        # Style du notebook
        style = ttk.Style()
        style.theme_use("clam")
        style.configure("Dark.TNotebook",
                        background=BG, borderwidth=0, tabmargins=0)
        style.configure("Dark.TNotebook.Tab",
                        background=BTN, foreground=MUTED,
                        padding=[18, 7], font=("Segoe UI", 9, "bold"),
                        borderwidth=0)
        style.map("Dark.TNotebook.Tab",
                  background=[("selected", CARD)],
                  foreground=[("selected", TEXT)])
        style.configure("D.Vertical.TScrollbar",
                        background=CARD, troughcolor=BG,
                        arrowcolor=MUTED, bordercolor=BG)
        style.configure("D.Horizontal.TScrollbar",
                        background=CARD, troughcolor=BG,
                        arrowcolor=MUTED, bordercolor=BG)

        self.nb = ttk.Notebook(main, style="Dark.TNotebook")
        self.nb.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)

        self._build_terminal_tab()
        self._build_data_tab()
        self._build_result_tab()

    # ── Onglet Terminal ───────────────────────────────────────────────
    def _build_terminal_tab(self):
        wrap = tk.Frame(self.nb, bg=BG)
        wrap.rowconfigure(1, weight=1)
        wrap.columnconfigure(0, weight=1)

        bar = tk.Frame(wrap, bg=CARD, height=32)
        bar.grid(row=0, column=0, columnspan=2, sticky="ew")
        bar.pack_propagate(False)
        dots = tk.Frame(bar, bg=CARD)
        dots.pack(side=tk.LEFT, padx=12, fill=tk.Y)
        for col in [RED, YELLOW, GREEN2]:
            c = tk.Canvas(dots, width=12, height=12, bg=CARD, bd=0, highlightthickness=0)
            c.pack(side=tk.LEFT, padx=2, pady=10)
            c.create_oval(1, 1, 11, 11, fill=col, outline="")
        tk.Label(bar, text="bash  —  K-Means MapReduce IoT",
                 font=("Segoe UI", 9), bg=CARD, fg=MUTED).pack(side=tk.LEFT, padx=4)
        tk.Button(bar, text="⌫  effacer", font=F_TINY, bg=CARD, fg=MUTED,
                  bd=0, cursor="hand2", relief="flat",
                  activebackground=CARD, activeforeground=TEXT,
                  command=self._clear).pack(side=tk.RIGHT, padx=12)

        self.term = tk.Text(wrap, bg=CARD, fg=TEXT, font=F_MONO,
                            wrap=tk.NONE, bd=0, highlightthickness=0,
                            insertbackground=TEXT, state=tk.DISABLED,
                            selectbackground=BTN, spacing1=2, spacing3=2,
                            padx=14, pady=10)
        self.term.grid(row=1, column=0, sticky="nsew")
        vsb = ttk.Scrollbar(wrap, orient=tk.VERTICAL,
                            style="D.Vertical.TScrollbar", command=self.term.yview)
        hsb = ttk.Scrollbar(wrap, orient=tk.HORIZONTAL,
                            style="D.Horizontal.TScrollbar", command=self.term.xview)
        vsb.grid(row=1, column=1, sticky="ns")
        hsb.grid(row=2, column=0, sticky="ew")
        self.term.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)
        for tag, fg, kw in [
            ("green",  GREEN2,  {}), ("red",    RED,     {}),
            ("yellow", YELLOW,  {}), ("cyan",   CYAN,    {}),
            ("muted",  MUTED,   {}), ("white",  TEXT,    {}),
            ("bold",   TEXT,    {"font": (F_MONO[0], F_MONO[1], "bold")}),
            ("header", ACCENT2, {"font": (F_MONO[0], F_MONO[1], "bold")}),
        ]:
            self.term.tag_configure(tag, foreground=fg, **kw)

        self.nb.add(wrap, text="  ▶  Terminal  ")
        self._print_welcome()

    # ── Onglet Données ────────────────────────────────────────────────
    def _build_data_tab(self):
        frame = tk.Frame(self.nb, bg=BG)
        frame.rowconfigure(2, weight=1)
        frame.columnconfigure(0, weight=1)

        # ── Barre de contrôles
        ctrl = tk.Frame(frame, bg=CARD, height=46)
        ctrl.grid(row=0, column=0, sticky="ew")
        ctrl.pack_propagate(False)
        tk.Frame(ctrl, bg=CYAN, width=3).pack(side=tk.LEFT, fill=tk.Y)
        tk.Label(ctrl, text="  Fichier :", font=F_SMALL, bg=CARD, fg=MUTED
                 ).pack(side=tk.LEFT, padx=(10, 4))

        self.var_file = tk.StringVar(value=DATA_FILES[5][0])
        file_menu = ttk.Combobox(ctrl, textvariable=self.var_file,
                                 values=[f[0] for f in DATA_FILES],
                                 state="readonly", width=22,
                                 font=("Courier New", 9))
        file_menu.pack(side=tk.LEFT, pady=10)
        file_menu.bind("<<ComboboxSelected>>", lambda e: self._load_csv())

        tk.Button(ctrl, text="  ↺  Charger", font=F_SMALL,
                  bg=BTN, fg=CYAN, bd=0, padx=10, pady=5,
                  cursor="hand2", relief="flat",
                  activebackground=BTN_HVR, activeforeground=CYAN,
                  command=self._load_csv).pack(side=tk.LEFT, padx=6)

        tk.Label(ctrl, text="  Filtrer région :", font=F_SMALL,
                 bg=CARD, fg=MUTED).pack(side=tk.LEFT, padx=(16, 4))
        self.var_filter = tk.StringVar(value="Toutes")
        self.filter_cb = ttk.Combobox(ctrl, textvariable=self.var_filter,
                                      values=["Toutes"], state="readonly",
                                      width=16, font=("Courier New", 9))
        self.filter_cb.pack(side=tk.LEFT, pady=10)
        self.filter_cb.bind("<<ComboboxSelected>>", lambda e: self._apply_filter())

        # Stats à droite
        self.data_stats = tk.Label(ctrl, text="", font=F_TINY,
                                   bg=CARD, fg=MUTED, anchor="e")
        self.data_stats.pack(side=tk.RIGHT, padx=14)

        # ── Barre de stats agrégées
        self.stats_bar = tk.Frame(frame, bg=PANEL, height=34)
        self.stats_bar.grid(row=1, column=0, sticky="ew")
        self.stats_bar.pack_propagate(False)
        self.stat_labels = {}
        for key in ["Capteurs", "Temp moy.", "Hum. moy.", "Vibr. moy.", "Réseau moy."]:
            f = tk.Frame(self.stats_bar, bg=PANEL)
            f.pack(side=tk.LEFT, padx=18, fill=tk.Y)
            tk.Label(f, text=key, font=F_TINY, bg=PANEL, fg=MUTED
                     ).pack(anchor="w", pady=(4, 0))
            lbl = tk.Label(f, text="—", font=("Segoe UI", 9, "bold"),
                           bg=PANEL, fg=CYAN)
            lbl.pack(anchor="w")
            self.stat_labels[key] = lbl

        # ── Treeview
        tree_frame = tk.Frame(frame, bg=BG)
        tree_frame.grid(row=2, column=0, sticky="nsew")
        tree_frame.rowconfigure(0, weight=1)
        tree_frame.columnconfigure(0, weight=1)

        style = ttk.Style()
        style.configure("Data.Treeview",
                        background=CARD, foreground=TEXT,
                        fieldbackground=CARD, borderwidth=0,
                        font=("Courier New", 9), rowheight=24)
        style.configure("Data.Treeview.Heading",
                        background=BTN, foreground=MUTED,
                        font=("Segoe UI", 8, "bold"), relief="flat",
                        borderwidth=0)
        style.map("Data.Treeview",
                  background=[("selected", ACCENT)],
                  foreground=[("selected", TEXT)])
        style.map("Data.Treeview.Heading",
                  background=[("active", BTN_HVR)])

        self.tree = ttk.Treeview(tree_frame, columns=CSV_COLS,
                                 show="headings", style="Data.Treeview",
                                 selectmode="browse")
        for col, hdr, w in zip(CSV_COLS, COL_HDRS, COL_W):
            self.tree.heading(col, text=hdr,
                              command=lambda c=col: self._sort_col(c))
            self.tree.column(col, width=w, anchor="center", stretch=False)

        tv_vsb = ttk.Scrollbar(tree_frame, orient=tk.VERTICAL,
                               style="D.Vertical.TScrollbar",
                               command=self.tree.yview)
        tv_hsb = ttk.Scrollbar(tree_frame, orient=tk.HORIZONTAL,
                               style="D.Horizontal.TScrollbar",
                               command=self.tree.xview)
        tv_vsb.grid(row=0, column=1, sticky="ns")
        tv_hsb.grid(row=1, column=0, sticky="ew")
        self.tree.grid(row=0, column=0, sticky="nsew")
        self.tree.configure(yscrollcommand=tv_vsb.set,
                            xscrollcommand=tv_hsb.set)

        # Alternance de couleur de lignes
        self.tree.tag_configure("odd",  background=CARD)
        self.tree.tag_configure("even", background="#0c0c1a")
        for region, color in REGION_COLORS.items():
            self.tree.tag_configure(region, foreground=color)

        # ── Barre de pagination
        pag = tk.Frame(frame, bg=PANEL, height=34)
        pag.grid(row=3, column=0, sticky="ew")
        pag.pack_propagate(False)
        self.btn_prev = tk.Button(pag, text="← Préc.", font=F_SMALL,
                                  bg=BTN, fg=MUTED, bd=0, padx=12, pady=6,
                                  cursor="hand2", relief="flat",
                                  activebackground=BTN_HVR,
                                  command=self._prev_page)
        self.btn_prev.pack(side=tk.LEFT, padx=(10, 4), pady=4)
        self.page_lbl = tk.Label(pag, text="Page 1 / 1", font=F_SMALL,
                                 bg=PANEL, fg=MUTED)
        self.page_lbl.pack(side=tk.LEFT, padx=8)
        self.btn_next = tk.Button(pag, text="Suiv. →", font=F_SMALL,
                                  bg=BTN, fg=MUTED, bd=0, padx=12, pady=6,
                                  cursor="hand2", relief="flat",
                                  activebackground=BTN_HVR,
                                  command=self._next_page)
        self.btn_next.pack(side=tk.LEFT, padx=4)
        self.rows_lbl = tk.Label(pag, text="", font=F_TINY, bg=PANEL, fg=MUTED)
        self.rows_lbl.pack(side=tk.RIGHT, padx=14)

        self.nb.add(frame, text="  ⬡  Données  ")

    # ── Onglet Résultats ──────────────────────────────────────────────
    def _build_result_tab(self):
        frame = tk.Frame(self.nb, bg=RESULT_BG)
        frame.rowconfigure(1, weight=1)
        frame.columnconfigure(0, weight=1)

        rbar = tk.Frame(frame, bg=CARD, height=36)
        rbar.grid(row=0, column=0, columnspan=2, sticky="ew")
        rbar.pack_propagate(False)
        tk.Frame(rbar, bg=GREEN2, width=3).pack(side=tk.LEFT, fill=tk.Y)
        tk.Label(rbar, text="  Résultats sauvegardés  ·  Interprétation",
                 font=("Segoe UI", 9, "bold"), bg=CARD, fg=GREEN2
                 ).pack(side=tk.LEFT, padx=8)

        nav = tk.Frame(rbar, bg=CARD)
        nav.pack(side=tk.RIGHT, padx=8)
        self.res_btns = {}
        for key, lbl, color in [
            ("classic",    "Classique",  CYAN),
            ("mapreduce",  "MapReduce",  ACCENT2),
            ("streaming",  "Streaming",  YELLOW),
            ("benchmark",  "Benchmark",  PINK),
            ("simulation", "Simulation", ORANGE),
        ]:
            b = tk.Button(nav, text=lbl, font=("Segoe UI", 8),
                          bg=BTN, fg=MUTED, bd=0, padx=10, pady=5,
                          cursor="hand2", relief="flat",
                          command=lambda k=key: self._show_result_file(k))
            b.pack(side=tk.LEFT, padx=2)
            b.bind("<Enter>", lambda e, w=b, c=color: w.configure(fg=c))
            b.bind("<Leave>", lambda e, w=b: w.configure(fg=MUTED))
            self.res_btns[key] = b

        self.res_text = tk.Text(frame, bg=RESULT_BG, fg=TEXT,
                                font=("Courier New", 9),
                                wrap=tk.NONE, bd=0, highlightthickness=0,
                                state=tk.DISABLED, padx=14, pady=8,
                                spacing1=1, spacing3=1, selectbackground=BTN)
        self.res_text.grid(row=1, column=0, sticky="nsew")
        rv = ttk.Scrollbar(frame, orient=tk.VERTICAL,
                           style="D.Vertical.TScrollbar",
                           command=self.res_text.yview)
        rh = ttk.Scrollbar(frame, orient=tk.HORIZONTAL,
                           style="D.Horizontal.TScrollbar",
                           command=self.res_text.xview)
        rv.grid(row=1, column=1, sticky="ns")
        rh.grid(row=2, column=0, sticky="ew")
        self.res_text.configure(yscrollcommand=rv.set, xscrollcommand=rh.set)

        for tag, fg, kw in [
            ("h1",     ACCENT2, {"font": ("Courier New", 9, "bold")}),
            ("h2",     GREEN2,  {"font": ("Segoe UI",    9, "bold")}),
            ("val",    CYAN,    {}),
            ("label",  TEXT,    {"font": ("Courier New", 9, "bold")}),
            ("muted",  MUTED,   {}), ("green",  GREEN2,  {}),
            ("yellow", YELLOW,  {}), ("pink",   PINK,    {}),
            ("red",    RED,     {}), ("white",  TEXT,    {}),
        ]:
            self.res_text.tag_configure(tag, foreground=fg, **kw)

        self._rwrite("\n  Lancez un algorithme ou cliquez sur un bouton ci-dessus.\n", "muted")
        self.nb.add(frame, text="  ◈  Résultats  ")

    # ── Helpers sidebar ───────────────────────────────────────────────
    def _slbl(self, parent, title, top=10):
        tk.Frame(parent, bg=SEP, height=1).pack(fill=tk.X, pady=(top, 0))
        tk.Label(parent, text=title, font=("Segoe UI", 8, "bold"),
                 bg=SIDEBAR, fg=ACCENT2).pack(anchor="w", padx=12, pady=(4, 2))

    def _prow(self, parent, label, default):
        row = tk.Frame(parent, bg=SIDEBAR)
        row.pack(fill=tk.X, pady=2)
        tk.Label(row, text=label, font=("Segoe UI", 8), bg=SIDEBAR,
                 fg=MUTED, width=18, anchor="w").pack(side=tk.LEFT)
        var = tk.StringVar(value=default)
        e = tk.Entry(row, textvariable=var, font=("Courier New", 9),
                     bg=BTN, fg=TEXT, insertbackground=TEXT,
                     bd=0, highlightthickness=1, highlightbackground=BORDER, width=6)
        e.pack(side=tk.LEFT, ipady=2)
        e.bind("<FocusIn>",  lambda ev, w=e: w.configure(highlightbackground=ACCENT2))
        e.bind("<FocusOut>", lambda ev, w=e: w.configure(highlightbackground=BORDER))
        return var

    # ── Données browser ───────────────────────────────────────────────
    def _load_csv(self):
        fname = self.var_file.get()
        path  = os.path.join(PROJECT_DIR, "data", fname)
        if not os.path.exists(path):
            self.data_stats.configure(
                text="Fichier introuvable — lancez ⬡ Générer les données", fg=RED)
            return

        self._all_rows = []
        with open(path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                self._all_rows.append(row)

        # Régions disponibles
        regions = sorted({r.get("region", "") for r in self._all_rows})
        self.filter_cb.configure(values=["Toutes"] + regions)
        self.var_filter.set("Toutes")

        self._apply_filter()

    def _apply_filter(self):
        region = self.var_filter.get()
        if region == "Toutes":
            self._filtered = self._all_rows[:]
        else:
            self._filtered = [r for r in self._all_rows
                              if r.get("region") == region]
        self._page = 0
        self._render_page()
        self._update_stats()

    def _render_page(self):
        self.tree.delete(*self.tree.get_children())
        total = len(self._filtered)
        n_pages = max(1, (total + PAGE_SIZE - 1) // PAGE_SIZE)
        start = self._page * PAGE_SIZE
        end   = min(start + PAGE_SIZE, total)
        page_rows = self._filtered[start:end]

        for idx, row in enumerate(page_rows):
            region = row.get("region", "")
            vals = (
                row.get("sensor_id", ""),
                region,
                row.get("latitude", ""),
                row.get("longitude", ""),
                row.get("temperature", ""),
                row.get("humidity", ""),
                row.get("vibration", ""),
                row.get("network_traffic", ""),
            )
            tags = (region,) if region in REGION_COLORS else ()
            tags = tags + (("odd",) if idx % 2 else ("even",))
            self.tree.insert("", tk.END, values=vals, tags=tags)

        self.page_lbl.configure(
            text=f"Page {self._page+1} / {n_pages}")
        self.rows_lbl.configure(
            text=f"Lignes {start+1}–{end} sur {total} capteurs")
        self.data_stats.configure(
            text=f"{total} capteurs  ·  {len(self.var_file.get())} ", fg=MUTED)
        self.btn_prev.configure(state=tk.NORMAL if self._page > 0 else tk.DISABLED)
        self.btn_next.configure(state=tk.NORMAL if self._page < n_pages-1 else tk.DISABLED)

    def _update_stats(self):
        rows = self._filtered
        if not rows:
            for lbl in self.stat_labels.values():
                lbl.configure(text="—")
            return
        n = len(rows)
        def avg(key):
            try:
                return round(sum(float(r[key]) for r in rows) / n, 2)
            except Exception:
                return "—"
        self.stat_labels["Capteurs"].configure(text=str(n), fg=TEXT)
        self.stat_labels["Temp moy."].configure(text=f"{avg('temperature')} °C", fg=YELLOW)
        self.stat_labels["Hum. moy."].configure(text=f"{avg('humidity')} %", fg=CYAN)
        self.stat_labels["Vibr. moy."].configure(text=f"{avg('vibration')} m/s²", fg=PINK)
        self.stat_labels["Réseau moy."].configure(text=f"{avg('network_traffic')} kbps", fg=GREEN2)

    def _sort_col(self, col):
        try:
            self._filtered.sort(
                key=lambda r: float(r.get(col, 0)) if col not in ("region",) else r.get(col, ""))
        except Exception:
            self._filtered.sort(key=lambda r: r.get(col, ""))
        self._page = 0
        self._render_page()

    def _prev_page(self):
        if self._page > 0:
            self._page -= 1
            self._render_page()

    def _next_page(self):
        total   = len(self._filtered)
        n_pages = max(1, (total + PAGE_SIZE - 1) // PAGE_SIZE)
        if self._page < n_pages - 1:
            self._page += 1
            self._render_page()

    # ── Graphique barres ──────────────────────────────────────────────
    def _draw_empty_chart(self):
        self.canvas_chart.delete("all")
        w = self.canvas_chart.winfo_reqwidth() or 214
        self.canvas_chart.create_text(w//2, 60, text="Aucun résultat",
            font=("Segoe UI", 9), fill=MUTED, justify=tk.CENTER)

    def _draw_chart(self, cluster_counts, algo="", duration=""):
        self.canvas_chart.delete("all")
        if not cluster_counts: self._draw_empty_chart(); return
        self.canvas_chart.update_idletasks()
        w, h  = self.canvas_chart.winfo_width() or 214, 120
        n     = len(cluster_counts)
        total = sum(cluster_counts.values())
        max_v = max(cluster_counts.values())
        pl, pr, pt, pb = 10, 8, 18, 24
        ch = h - pt - pb
        cw = w - pl - pr
        gap, bw = 5, max(6, (cw - 5*(n+1)) // n)
        for i, (cid, cnt) in enumerate(cluster_counts.items()):
            col = CLUSTER_COLORS[int(cid) % len(CLUSTER_COLORS)]
            bh  = max(4, round(cnt / max_v * ch))
            x0  = pl + gap + i*(bw+gap)
            x1, y1, y0 = x0+bw, h-pb, h-pb-bh
            self.canvas_chart.create_rectangle(x0, y0, x1, y1, fill=col, outline="")
            self.canvas_chart.create_rectangle(x0, y0+bh*2//3, x1, y1,
                                               fill=_darken(col), outline="")
            self.canvas_chart.create_text((x0+x1)//2, y0-4, text=str(cnt),
                font=("Segoe UI", 7, "bold"), fill=col)
            self.canvas_chart.create_text((x0+x1)//2, h-pb+10,
                text=f"C{cid}", font=("Segoe UI", 8), fill=MUTED)
        self.canvas_chart.create_text(w//2, 9, text=f"{algo}  ·  {total} pts",
            font=("Segoe UI", 8, "bold"), fill=MUTED)
        if duration:
            self.canvas_chart.create_text(w-pr, 9, text=f"{duration}s",
                font=("Segoe UI", 7), fill=ACCENT2, anchor="e")

    # ── Status bar ────────────────────────────────────────────────────
    def _build_statusbar(self):
        bar = tk.Frame(self, bg=PANEL, height=26)
        bar.pack(fill=tk.X, side=tk.BOTTOM)
        bar.pack_propagate(False)
        tk.Frame(bar, bg=ACCENT, width=4).pack(side=tk.LEFT, fill=tk.Y)
        self.spin_lbl = tk.Label(bar, text="", font=("Segoe UI", 10),
                                 bg=PANEL, fg=ACCENT2, width=2)
        self.spin_lbl.pack(side=tk.LEFT, padx=(8, 2))
        self.status_var = tk.StringVar(value="Prêt")
        tk.Label(bar, textvariable=self.status_var, font=F_SMALL,
                 bg=PANEL, fg=MUTED).pack(side=tk.LEFT, padx=4)
        tk.Label(bar, text="IFRI · Big Data · TP4", font=F_TINY,
                 bg=PANEL, fg=MUTED).pack(side=tk.RIGHT, padx=12)
        tk.Label(bar, text="Python " + sys.version.split()[0], font=F_TINY,
                 bg=PANEL, fg=MUTED).pack(side=tk.RIGHT, padx=8)

    # ── Welcome ───────────────────────────────────────────────────────
    def _print_welcome(self):
        self._write("  ┌─────────────────────────────────────────────────────┐\n", "muted")
        self._write("  │  ", "muted"); self._write("K-MEANS MAPREDUCE", "header")
        self._write("  —  Capteurs IoT Distribués          │\n", "muted")
        self._write("  │  TP4 Big Data · Master 1 GL · Groupe 2 · IFRI       │\n", "muted")
        self._write("  └─────────────────────────────────────────────────────┘\n\n", "muted")
        for txt, tag in [
            ("  Problème  ", "cyan"), ("Regrouper 1 500 capteurs répartis sur 5 serveurs\n", "white"),
            ("             ", "white"), ("régionaux en K clusters homogènes.\n\n", "white"),
            ("  Features   ", "cyan"), ("température · humidité · vibration · réseau\n\n", "white"),
            ("  Algos      ", "cyan"), ("Classique · MapReduce · Streaming · Benchmark\n\n", "white"),
        ]:
            self._write(txt, tag)
        self._write("  → Cliquez sur ", "muted")
        self._write("⬡ Générer les données", "green")
        self._write(" puis explorez les onglets.\n\n", "muted")

    # ── Écriture terminal ─────────────────────────────────────────────
    def _write(self, text, tag="white"):
        self.term.configure(state=tk.NORMAL)
        self.term.insert(tk.END, text, tag)
        self.term.see(tk.END)
        self.term.configure(state=tk.DISABLED)

    def _writeln(self, line):
        self._write(line, _color_tag(line))

    def _clear(self):
        self.term.configure(state=tk.NORMAL)
        self.term.delete("1.0", tk.END)
        self.term.configure(state=tk.DISABLED)

    # ── Écriture résultats ────────────────────────────────────────────
    def _rwrite(self, text, tag="white"):
        self.res_text.configure(state=tk.NORMAL)
        self.res_text.insert(tk.END, text, tag)
        self.res_text.configure(state=tk.DISABLED)

    def _rclear(self):
        self.res_text.configure(state=tk.NORMAL)
        self.res_text.delete("1.0", tk.END)
        self.res_text.configure(state=tk.DISABLED)

    # ── Résultats ─────────────────────────────────────────────────────
    def _show_result_file(self, key):
        paths = {
            "classic":    "results/classic_result.json",
            "mapreduce":  "results/mapreduce_result.json",
            "streaming":  "results/streaming_result.json",
            "benchmark":  "results/benchmark.json",
            "simulation": "results/simulation_result.json",
        }
        path = os.path.join(PROJECT_DIR, paths.get(key, ""))
        if not os.path.exists(path):
            self._rclear()
            self._rwrite(f"\n  Fichier introuvable : {paths.get(key)}\n", "red")
            self._rwrite("  → Lancez d'abord l'algorithme correspondant.\n", "muted")
            self.nb.select(2)
            return
        try:
            with open(path) as f:
                data = json.load(f)
        except Exception as e:
            self._rclear()
            self._rwrite(f"\n  Erreur de lecture : {e}\n", "red")
            return
        self._rclear()
        if key == "benchmark" and isinstance(data, list):
            self._render_benchmark(data)
        elif key == "simulation" and isinstance(data, dict) and "results" in data:
            self._render_simulation(data)
        else:
            self._render_result(data, key)
        self.nb.select(2)

    def _render_result(self, data, key):
        algo      = data.get("algorithm", key)
        n_pts     = data.get("n_points", 0)
        iters     = data.get("iterations", "—")
        duration  = data.get("duration_sec", "—")
        centroids = data.get("centroids", [])
        raw_c     = data.get("cluster_counts", {})
        sse       = data.get("sse", None)
        cc        = {str(k): v for k, v in raw_c.items()}
        total     = sum(cc.values()) or n_pts

        self._rwrite(f"\n  ╔{'═'*62}╗\n", "muted")
        self._rwrite(f"  ║  ", "muted")
        self._rwrite(f"RÉSULTATS — {algo.upper():<51}", "h1")
        self._rwrite(f"║\n", "muted")
        self._rwrite(f"  ╚{'═'*62}╝\n\n", "muted")

        self._rwrite("  Métriques globales\n", "h2")
        self._rwrite("  " + "─"*48 + "\n", "muted")
        for lbl, val in [("Points traités", f"{total} capteurs"),
                         ("Itérations", str(iters)),
                         ("Durée totale", f"{duration}s"),
                         ("Inertie (SSE)", f"{sse:,.2f}" if sse else None)]:
            if val is None: continue
            self._rwrite(f"  {lbl:<22}", "muted")
            self._rwrite(f"{val}\n", "val")
        self._rwrite("\n", "white")

        self._rwrite("  Centroïdes des clusters\n", "h2")
        self._rwrite("  " + "─"*62 + "\n", "muted")
        self._rwrite(f"  {'Cluster':<10}{'Temp°C':>9}{'Hum%':>8}"
                     f"{'Vibr':>9}{'Réseau':>10}{'Points':>9}\n", "label")
        self._rwrite("  " + "─"*62 + "\n", "muted")
        for i, centroid in enumerate(centroids):
            cnt = cc.get(str(i), 0)
            pct = round(cnt/total*100, 1) if total else 0
            t, h, v, n = [round(x, 2) for x in centroid]
            self._rwrite(f"  Cluster {i:<4}", "label")
            self._rwrite(f"{t:>8.2f}{h:>8.2f}{v:>9.3f}{n:>10.2f}", "val")
            self._rwrite(f"  {cnt:>5} ({pct}%)\n", "muted")
        self._rwrite("  " + "─"*62 + "\n\n", "muted")

        self._rwrite("  Interprétation automatique des clusters\n", "h2")
        self._rwrite("  " + "─"*62 + "\n", "muted")
        tags_list = ["h1", "val", "green", "yellow", "pink"]
        for i, centroid in enumerate(centroids):
            profil, tags = _interpret_cluster(centroid)
            self._rwrite(f"\n  Cluster {i}", "label")
            self._rwrite(f"  →  {profil}\n", tags_list[i % 5])
            self._rwrite(f"           {tags}\n", "muted")
        self._rwrite("\n  " + "─"*62 + "\n\n", "muted")

        if len(centroids) == 3:
            self._rwrite("  Conclusion\n", "h2")
            temps = [c[0] for c in centroids]
            nets  = [c[3] for c in centroids]
            hot_i = temps.index(max(temps))
            urb_i = nets.index(max(nets))
            sub_i = next((j for j in range(3) if j != hot_i and j != urb_i), 0)
            self._rwrite(f"  Les {total} capteurs se répartissent en 3 profils distincts.\n", "white")
            self._rwrite(f"  Cluster {hot_i} → zones chaudes/humides   ", "white")
            self._rwrite(f"({cc.get(str(hot_i),0)} capteurs)\n", "green")
            self._rwrite(f"  Cluster {urb_i} → fort trafic numérique   ", "white")
            self._rwrite(f"({cc.get(str(urb_i),0)} capteurs)\n", "val")
            self._rwrite(f"  Cluster {sub_i} → environnements modérés  ", "white")
            self._rwrite(f"({cc.get(str(sub_i),0)} capteurs)\n\n", "yellow")

        self._draw_chart(cc, algo, str(duration))
        self.chart_info.configure(
            text=f"{algo}  ·  {iters} itér.  ·  {duration}s  ·  {total} pts")

    def _render_benchmark(self, data):
        self._rwrite(f"\n  ╔{'═'*62}╗\n", "muted")
        self._rwrite("  ║  ", "muted")
        self._rwrite("BENCHMARK — CLASSIQUE vs MAPREDUCE" + " "*28, "h1")
        self._rwrite("║\n", "muted")
        self._rwrite(f"  ╚{'═'*62}╝\n\n", "muted")
        self._rwrite(f"  {'Dataset':<30}{'Classique':>14}{'MapReduce':>14}{'Rapport':>10}\n", "label")
        self._rwrite("  " + "─"*62 + "\n", "muted")
        for r in data:
            name = r.get("dataset", "?")[:28]
            ct   = r.get("classic_time", "—")
            mt   = r.get("mr_time", "—")
            to_c = r.get("classic_timeout", False)
            ct_s = f">{ct}" if to_c else f"{ct}s"
            mt_s = f"{mt}s" if not r.get("mr_timeout") else "ERR"
            try:
                ratio = float(str(ct)) / float(str(mt))
                ratio_s  = f"{ratio:.2f}x ↑" if ratio > 1 else f"{1/ratio:.2f}x ↓"
                ratio_tag = "green" if ratio > 1 else "yellow"
            except Exception:
                ratio_s, ratio_tag = "—", "muted"
            self._rwrite(f"  {name:<30}", "white")
            self._rwrite(f"{ct_s:>14}", "yellow" if to_c else "val")
            self._rwrite(f"{mt_s:>14}", "val")
            self._rwrite(f"{ratio_s:>10}\n", ratio_tag)
        self._rwrite("  " + "─"*62 + "\n\n", "muted")
        self._rwrite("  Interprétation\n", "h2")
        self._rwrite("  " + "─"*62 + "\n\n", "muted")
        self._rwrite("  Petit dataset (1 500 pts)  ", "white")
        self._rwrite("overhead de création des processus\n", "yellow")
        self._rwrite("  → MapReduce légèrement plus lent qu'en séquentiel.\n\n", "muted")
        self._rwrite("  Grand dataset (50 000 pts)  ", "white")
        self._rwrite("MapReduce ~1.8× plus rapide\n", "green")
        self._rwrite("  → Traitement parallélisé sur plusieurs workers CPU.\n\n", "muted")
        self._rwrite("  En production réelle (données sur réseau), le gain\n"
                     "  est encore plus important car les workers tournent\n"
                     "  localement sur chaque serveur régional.\n\n", "muted")

    def _render_simulation(self, data):
        timeout_s  = data.get("timeout_s", 12)
        k          = data.get("k", 3)
        n_servers  = data.get("n_servers", 5)
        cpu_count  = data.get("cpu_count", "?")
        results    = data.get("results", [])

        self._rwrite(f"\n  ╔{'═'*62}╗\n", "muted")
        self._rwrite("  ║  ", "muted")
        self._rwrite(f"SIMULATION — CLASSIQUE (TIMEOUT) vs MAPREDUCE DISTRIBUÉ  ", "h1")
        self._rwrite("║\n", "muted")
        self._rwrite(f"  ╚{'═'*62}╝\n\n", "muted")

        # ── Paramètres ────────────────────────────────────────────────
        self._rwrite("  Paramètres de la simulation\n", "h2")
        self._rwrite("  " + "─"*48 + "\n", "muted")
        for lbl, val in [
            ("Machine",          f"{cpu_count} CPUs logiques"),
            ("Timeout classique", f"{timeout_s}s  — au-delà l'algo est abandonné"),
            ("Clusters K",        str(k)),
            ("Serveurs régionaux", str(n_servers)),
            ("Features",          "temp · humidité · vibration · trafic réseau"),
        ]:
            self._rwrite(f"  {lbl:<22}", "muted")
            self._rwrite(f"{val}\n", "val")
        self._rwrite("\n", "white")

        # ── Tableau comparatif ────────────────────────────────────────
        self._rwrite("  Résultats par palier de données\n", "h2")
        self._rwrite("  " + "─"*66 + "\n", "muted")
        self._rwrite(
            f"  {'Capteurs':>12}  {'Classique':>18}  {'MapReduce':>12}  {'Verdict'}\n",
            "label")
        self._rwrite("  " + "─"*66 + "\n", "muted")

        for r in results:
            n        = r.get("n", 0)
            mr_s     = r.get("mr_s", 0)
            classic_s= r.get("classic_s", timeout_s)
            timed_out= r.get("timed_out", False)

            mr_txt = f"{mr_s*1000:.0f} ms" if mr_s < 1 else f"{mr_s:.2f}s"

            if timed_out:
                cl_txt   = f">{timeout_s}s  TIMEOUT"
                cl_tag   = "red"
                speedup  = classic_s / mr_s if mr_s > 0 else 0
                verdict  = f"MR seul peut terminer ({speedup:.1f}× plus rapide)"
                vrd_tag  = "green"
            else:
                cl_txt   = f"{classic_s*1000:.0f} ms" if classic_s < 1 else f"{classic_s:.2f}s"
                cl_tag   = "yellow"
                if classic_s > 0 and mr_s > 0:
                    ratio = classic_s / mr_s
                    if ratio > 1.05:
                        verdict = f"MR {ratio:.1f}× plus rapide"
                        vrd_tag = "green"
                    elif mr_s / classic_s > 1.05:
                        overhead = mr_s / classic_s
                        verdict  = f"Overhead MR ×{overhead:.1f}  (normal à petite échelle)"
                        vrd_tag  = "yellow"
                    else:
                        verdict, vrd_tag = "Performances équivalentes", "val"
                else:
                    verdict, vrd_tag = "—", "muted"

            self._rwrite(f"  {n:>12,}  ", "white")
            self._rwrite(f"{cl_txt:>18}", cl_tag)
            self._rwrite(f"  {mr_txt:>12}  ", "val")
            self._rwrite(f"{verdict}\n", vrd_tag)

        self._rwrite("  " + "─"*66 + "\n\n", "muted")

        # ── Interprétation ────────────────────────────────────────────
        self._rwrite("  Interprétation\n", "h2")
        self._rwrite("  " + "─"*62 + "\n\n", "muted")

        timeouts = [r for r in results if r.get("timed_out")]
        small    = [r for r in results if not r.get("timed_out") and
                    r.get("mr_s", 0) > r.get("classic_s", 0)]
        wins     = [r for r in results if not r.get("timed_out") and
                    r.get("classic_s", 0) > r.get("mr_s", 0) * 1.05]

        if small:
            n_ex = small[0]["n"]
            self._rwrite(f"  Petite échelle ({n_ex:,} pts)  ", "white")
            self._rwrite("overhead de démarrage des processus\n", "yellow")
            self._rwrite("  → Le classique séquentiel est plus rapide quand les\n", "muted")
            self._rwrite("    données tiennent facilement en mémoire locale.\n\n", "muted")

        if wins:
            n_ex = wins[-1]["n"]
            r_ex = wins[-1]
            ratio = r_ex["classic_s"] / r_ex["mr_s"] if r_ex["mr_s"] > 0 else 0
            self._rwrite(f"  Grande échelle ({n_ex:,} pts)  ", "white")
            self._rwrite(f"MapReduce {ratio:.1f}× plus rapide\n", "green")
            self._rwrite("  → Traitement parallélisé sur les " + str(n_servers) +
                         " serveurs régionaux.\n\n", "muted")

        if timeouts:
            n_ex = timeouts[0]["n"]
            mr_ex = timeouts[0]["mr_s"]
            self._rwrite(f"  Très grand dataset ({n_ex:,} pts)  ", "white")
            self._rwrite(f"Classique KO — MapReduce OK en {mr_ex:.2f}s\n", "red")
            self._rwrite(f"  → L'algo classique dépasse le timeout de {timeout_s}s et\n", "muted")
            self._rwrite("    est abandonné. Le MapReduce distribué termine normalement\n", "muted")
            self._rwrite("    car chaque serveur ne traite qu'une fraction des données.\n\n", "muted")

        self._rwrite("  En production IoT réelle, le gain est encore supérieur :\n", "muted")
        self._rwrite("  chaque serveur régional traite ses capteurs LOCALEMENT\n", "muted")
        self._rwrite("  et n'envoie que des sommes partielles au coordinateur.\n\n", "muted")
        self._rwrite("  " + "─"*62 + "\n", "muted")

        # Mise à jour du mini-graphique avec les temps MapReduce
        mr_counts = {str(i): int(r["mr_s"] * 1000)
                     for i, r in enumerate(results) if not r.get("timed_out")}
        if mr_counts:
            self._draw_chart(mr_counts, "Simulation MR (ms)", "")
            self.chart_info.configure(
                text=f"Simulation  ·  timeout {timeout_s}s  ·  {len(results)} paliers")

    # ── Spinner ───────────────────────────────────────────────────────
    def _start_spinner(self):
        self._spin_idx = 0
        self._tick_spinner()

    def _tick_spinner(self):
        if not self._running:
            self.spin_lbl.configure(text=""); return
        self.spin_lbl.configure(text=SPINNER_FRAMES[self._spin_idx % len(SPINNER_FRAMES)])
        self._spin_idx += 1
        self._spin_job = self.after(80, self._tick_spinner)

    def _stop_spinner(self):
        if self._spin_job:
            self.after_cancel(self._spin_job)
        self.spin_lbl.configure(text="")

    # ── État running ──────────────────────────────────────────────────
    def _set_running(self, running, active_key=None):
        self._running = running
        s = tk.DISABLED if running else tk.NORMAL
        for key, b in self.btns.items():
            b.configure(state=s,
                        bg=BTN_HVR if (running and key == active_key) else BTN)
        self.btn_stop.configure(state=tk.NORMAL if running else tk.DISABLED)
        if running: self._start_spinner()
        else:       self._stop_spinner()

    def _stop(self):
        if self._proc and self._proc.poll() is None:
            self._proc.terminate()
            self._write("\n  ✗ Arrêté par l'utilisateur\n", "red")
        self._set_running(False)
        self.status_var.set("Arrêté")

    # ── Lancement subprocess ──────────────────────────────────────────
    def _launch(self, cmd, title, result_key=None, active_key=None):
        if self._running: return
        self._set_running(True, active_key)
        self.nb.select(0)
        self._write(f"\n  ╔{'═'*52}╗\n", "muted")
        self._write(f"  ║  ▶  ", "muted")
        self._write(f"{title:<48}", "yellow")
        self._write(f"  ║\n", "muted")
        self._write(f"  ╚{'═'*52}╝\n\n", "muted")
        self.status_var.set(title)

        def worker():
            try:
                self._proc = subprocess.Popen(
                    cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                    text=True, bufsize=1, cwd=PROJECT_DIR)
                for line in self._proc.stdout:
                    self.after(0, self._writeln, line)
                self._proc.wait()
                self.after(0, self._on_done, self._proc.returncode, result_key)
            except Exception as e:
                self.after(0, self._write, f"\n  Erreur : {e}\n", "red")
                self.after(0, self._set_running, False)

        threading.Thread(target=worker, daemon=True).start()

    def _on_done(self, rc, result_key):
        if rc == 0:
            self._write("\n  ✓ ", "green")
            self._write("Terminé avec succès\n\n", "white")
            self.status_var.set("Terminé  ✓")
            if result_key:
                self._show_result_file(result_key)
                # Recharge aussi les données si on vient de générer
                if result_key is None and self._all_rows:
                    self._load_csv()
        else:
            self._write(f"\n  ✗ Erreur (code {rc})\n\n", "red")
            self.status_var.set(f"Erreur — code {rc}")
        self._set_running(False)

    # ── Commandes ────────────────────────────────────────────────────
    def _run_gen(self):
        self._launch([sys.executable, "generate_data.py"],
                     "Génération des données IoT simulées", active_key="gen")

    def _run_classic(self):
        k = self.var_k.get()
        self._launch([sys.executable, "kmeans_classic.py",
                      "--input", "data/all_sensors.csv", "--k", k],
                     f"K-Means Classique  (K={k}  ·  1 500 capteurs)",
                     "classic", "class")

    def _run_mr(self):
        k, it = self.var_k.get(), self.var_iter.get()
        self._launch([sys.executable, "kmeans_mapreduce.py",
                      "data/all_sensors.csv", "--k", k, "--max-iter", it],
                     f"MapReduce Chunk  (K={k}  ·  max_iter={it})",
                     "mapreduce", "mr")

    def _run_dist(self):
        k, it = self.var_k.get(), self.var_iter.get()
        self._launch([sys.executable, "kmeans_mapreduce.py",
                      "--servers",
                      "data/server_nord.csv",   "data/server_centre.csv",
                      "data/server_sud.csv",    "data/server_ouest.csv",
                      "data/server_est.csv",
                      "--k", k, "--max-iter", it],
                     f"Multi-Serveurs  (K={k}  ·  5 serveurs régionaux)",
                     "mapreduce", "dist")

    def _run_stream(self):
        k, w = self.var_k.get(), self.var_waves.get()
        d, s = self.var_delay.get(), self.var_sensors.get()
        self._launch([sys.executable, "kmeans_streaming.py",
                      "--k", k, "--waves", w, "--sensors-per-wave", s, "--delay", d],
                     f"Streaming  (K={k}  ·  {w} vagues  ·  délai {d}s)",
                     "streaming", "stream")

    def _run_bench(self):
        self._launch([sys.executable, "benchmark.py"],
                     "Benchmark complet — Classique vs MapReduce",
                     "benchmark", "bench")

    def _run_simulation(self):
        self._launch([sys.executable, "simulation_comparison.py", "--dashboard"],
                     "Simulation Timeout — Classique vs MapReduce distribué",
                     "simulation", "simul")


if __name__ == "__main__":
    app = Dashboard()
    app.mainloop()
