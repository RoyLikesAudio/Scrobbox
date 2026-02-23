#!/usr/bin/env python3
"""
SCROBBOX  ·  Rockbox companion & multi-platform scrobbler
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Features:
  • Scrobble .scrobbler.log → Last.fm · Libre.fm · ListenBrainz
  • Statistics page with album art, top artists/tracks
  • Last.fm deep stats — heatmap calendar, listening trends, milestones
  • Rockbox DB rebuilder — detects new music files on device
  • Rockbox config.cfg editor — full key/value editor with descriptions
  • Submission history with search/pagination
  • Appearance customiser — dark/light, accent presets, full color override

Supports: Rockbox .scrobbler.log (all TZ modes), etc.
"""

import sys, json, hashlib, sqlite3, os, csv, platform, struct, time
import io
import subprocess, shutil, copy
import threading as _threading
import re
from pathlib import Path
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from collections import Counter, defaultdict
from typing import Optional

import requests

try:
    from PyQt6.QtWebEngineWidgets import QWebEngineView
    from PyQt6.QtWebEngineCore import QWebEngineProfile
    _HAS_WEBENGINE = True
except ImportError:
    _HAS_WEBENGINE = False

from PyQt6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QPushButton, QLabel, QLineEdit, QMessageBox, QProgressBar,
    QCheckBox, QFileDialog, QGroupBox, QGridLayout, QDialog,
    QTabWidget, QSpinBox, QComboBox, QListWidget, QFrame,
    QScrollArea, QTableWidget, QTableWidgetItem, QHeaderView,
    QStackedWidget, QSizePolicy, QAbstractItemView, QColorDialog,
    QGraphicsDropShadowEffect, QGraphicsOpacityEffect, QTextEdit, QSlider, QListWidgetItem,
    QSplitter, QToolTip, QTreeWidget, QTreeWidgetItem,
    QPlainTextEdit, QInputDialog,
)
from PyQt6.QtCore import (
    Qt, QUrl, QThread, pyqtSignal, QTimer, QPropertyAnimation,
    QEasingCurve, QSize, QRect, QPoint, QRectF, QProcess,
)
from PyQt6.QtGui import (
    QColor, QFont, QDesktopServices, QPainter, QPen, QBrush,
    QPalette, QIcon, QPixmap, QLinearGradient, QImage, QFontMetrics,
    QPainterPath, QRadialGradient, QKeySequence, QShortcut,
)

from PyQt6 import sip

def open_url(url):
    """Open URL/file bypassing Qt/KDE portal entirely."""
    import subprocess, os
    env = os.environ.copy()
    env.pop('LD_LIBRARY_PATH', None)
    env.pop('LD_PRELOAD', None)
    if isinstance(url, QUrl):
        url_str = url.toString()
    else:
        url_str = str(url)
    for cmd in (['xdg-open', url_str], ['gio', 'open', url_str],
                ['kde-open5', url_str], ['firefox', url_str],
                ['chromium', url_str]):
        try:
            subprocess.Popen(cmd, env=env,
                           stdout=subprocess.DEVNULL,
                           stderr=subprocess.DEVNULL)
            return
        except FileNotFoundError:
            continue
    # All launchers failed — surface this rather than silently doing nothing.
    import sys as _sys_mod
    print(f"[Scrobbox] WARNING: could not open URL — no suitable launcher found: {url_str}",
          file=_sys_mod.stderr)

# ─────────────────────────────────────────────────────────────
#  PATHS
# ─────────────────────────────────────────────────────────────

APP_NAME    = "Scrobbox"
APP_VERSION = "0.3.0"   # bump this with each release
GITHUB_REPO = "RoyLikesAudio/Scrobbox"
_sys = platform.system()

if _sys == "Darwin":
    CONFIG_DIR = Path.home() / "Library" / "Application Support" / APP_NAME
else:
    CONFIG_DIR = Path.home() / ".config" / APP_NAME.lower()

CONFIG_DIR.mkdir(parents=True, exist_ok=True)
CONF_FILE   = CONFIG_DIR / "config.json"
DB_FILE     = CONFIG_DIR / "scrobbles.db"
SESSION_DIR          = CONFIG_DIR / "sessions"
SESSION_DIR.mkdir(exist_ok=True)
RSYNC_PROFILES_FILE  = CONFIG_DIR / "rsync_profiles.json"


# ─────────────────────────────────────────────────────────────
#  PLATFORM CONSTANTS
# ─────────────────────────────────────────────────────────────

P_LASTFM       = "Last.fm"
P_LIBREFM      = "Libre.fm"
P_LISTENBRAINZ = "ListenBrainz"
ALL_PLATFORMS  = [P_LASTFM, P_LIBREFM, P_LISTENBRAINZ]

LASTFM_API  = "https://ws.audioscrobbler.com/2.0/"
LASTFM_AUTH = "https://www.last.fm/api/auth/"
LIBREFM_API = "https://libre.fm/2.0/"
LIBREFM_AUTH= "https://libre.fm/api/auth/"
LBZ_API     = "https://api.listenbrainz.org/1/"

AUDIO_EXTS = {".mp3", ".flac", ".m4a", ".aac", ".ogg", ".opus", ".wv", ".ape", ".mpc"}


# ─────────────────────────────────────────────────────────────
#  DESIGN TOKENS
# ─────────────────────────────────────────────────────────────

DARK = {
    "bg0":      "#111318",
    "bg1":      "#0d0f13",
    "bg2":      "#191c22",
    "bg3":      "#1e2128",
    "bg4":      "#252930",
    "accent":   "#c8861a",
    "accent2":  "#dfa030",
    "accentlo": "#c8861a18",
    "success":  "#3a9955",
    "danger":   "#c04040",
    "warning":  "#b88018",
    "txt0":     "#e2ddd6",
    "txt1":     "#b8b2aa",
    "txt2":     "#8a857f",
    "border":   "#252830",
    "bordhi":   "#c8861a50",
    "shadow":   "#00000060",
}
_current_theme = DARK.copy()

def tok(key: str) -> str:
    return _current_theme.get(key, "#ff0000")


def build_stylesheet(t: dict) -> str:
    # Glass-dark aesthetic — structural colors are hardcoded rgba, only accent uses theme tokens
    _a  = t["accent"]
    _a2 = t["accent2"]
    _alo= t["accentlo"]
    _bhi= t["bordhi"]
    _suc= t["success"]
    _dan= t["danger"]
    _war= t["warning"]
    return f"""
* {{ font-family: 'Inter', 'SF Pro Text', 'Segoe UI Variable', 'Segoe UI', 'Helvetica Neue', sans-serif; }}
QMainWindow, QDialog {{ background: #0d0f13; }}
QWidget {{ background: #111318; color: #e2ddd6; font-size: 13px;
          selection-background-color: {_a}; selection-color: #0a0a0a; }}

QWidget#sidebar {{ background: rgba(5,7,11,0.95); border-right: 1px solid rgba(255,255,255,0.06); }}
QWidget#card {{ background: rgba(255,255,255,0.05); border: 1px solid rgba(255,255,255,0.08); border-radius: 8px; }}
QWidget#panel {{ background: rgba(255,255,255,0.04); border: 1px solid rgba(255,255,255,0.07); border-radius: 8px; }}
QWidget#page_hdr {{ background: rgba(5,7,11,0.65); border-bottom: 1px solid rgba(255,255,255,0.07); }}
QWidget#card_top {{ background: rgba(5,7,11,0.65); border-bottom: 1px solid rgba(255,255,255,0.07); }}
QWidget#inset {{ background: rgba(255,255,255,0.04); border: 1px solid rgba(255,255,255,0.06); border-radius: 4px; }}

QWidget#card QLabel {{ background: transparent; color: #e2ddd6; }}
QWidget#card QLabel#muted {{ color: rgba(255,255,255,0.35); }}
QWidget#card QLabel#secondary {{ color: rgba(255,255,255,0.55); }}
QWidget#card QLabel#subheading {{ color: #e2ddd6; font-weight: 600; }}
QWidget#inset QLabel {{ background: transparent; color: #e2ddd6; }}
QWidget#queue_hdr {{ background: rgba(255,255,255,0.04); border-bottom: 1px solid rgba(255,255,255,0.07); }}
QWidget#queue_hdr QLabel {{ background: transparent; color: #e2ddd6; }}
QWidget#queue_acts {{ background: rgba(255,255,255,0.04); border-top: 1px solid rgba(255,255,255,0.07); }}
QWidget#queue_acts QLabel {{ background: transparent; color: #e2ddd6; }}

QLabel {{ color: #e2ddd6; background: transparent; }}
QLabel#muted      {{ color: rgba(255,255,255,0.35); font-size: 11px; }}
QLabel#secondary  {{ color: rgba(255,255,255,0.55); font-size: 13px; }}
QLabel#heading    {{ color: #fff; font-size: 20px; font-weight: 600; letter-spacing: -0.3px; }}
QLabel#mono       {{ font-family: 'Cascadia Code','SF Mono','Consolas',monospace; font-size: 12px; color: rgba(255,255,255,0.55); background: transparent; }}
QLabel#subheading {{ color: #e2ddd6; font-size: 13px; font-weight: 600; }}
QLabel#success    {{ color: {_suc}; }}
QLabel#danger     {{ color: {_dan}; }}
QLabel#warning    {{ color: {_war}; }}
QLabel#statnum    {{ color: {_a}; font-size: 26px; font-weight: 600; letter-spacing: -0.5px; }}
QLabel#statlabel  {{ color: rgba(255,255,255,0.35); font-size: 10px; font-weight: 600; letter-spacing: 1.2px; }}
QLabel#sectiontitle {{ color: rgba(255,255,255,0.30); font-size: 10px; font-weight: 700; letter-spacing: 2px; background: transparent; }}
QLabel#biglabel   {{ color: #fff; font-size: 32px; font-weight: 700; letter-spacing: -1px; }}

QLineEdit {{ background: rgba(255,255,255,0.07); border: 1px solid rgba(255,255,255,0.12); border-radius: 5px;
             padding: 8px 11px; color: #e2ddd6; font-size: 13px; }}
QLineEdit:focus  {{ border-color: {_a}; background: rgba(255,255,255,0.10); }}
QLineEdit:disabled {{ color: rgba(255,255,255,0.25); }}
QTextEdit {{ background: rgba(255,255,255,0.07); border: 1px solid rgba(255,255,255,0.12); border-radius: 5px;
             padding: 8px; color: #e2ddd6; font-size: 13px; }}
QTextEdit:focus {{ border-color: {_a}; }}
QPlainTextEdit {{ background: rgba(0,0,0,0.30); border: 1px solid rgba(255,255,255,0.10); border-radius: 5px;
                  padding: 8px; color: rgba(255,255,255,0.80); font-size: 13px; }}
QPlainTextEdit:focus {{ border-color: {_a}; }}

QSpinBox, QComboBox {{ background: rgba(255,255,255,0.07); border: 1px solid rgba(255,255,255,0.12); border-radius: 4px;
                       padding: 6px 10px; color: #e2ddd6; min-height: 30px; font-size: 13px; }}
QSpinBox:focus, QComboBox:focus {{ border-color: {_a}; }}
QComboBox::drop-down {{ border: none; width: 22px; }}
QComboBox QAbstractItemView {{ background: #1e2128; border: 1px solid rgba(255,255,255,0.12);
    selection-background-color: {_a}; selection-color: #0a0a0a; outline: none; padding: 2px; }}

QPushButton {{ background: rgba(255,255,255,0.07); color: #e2ddd6; border: 1px solid rgba(255,255,255,0.12);
               border-radius: 5px; padding: 0 14px; font-weight: 500; font-size: 13px;
               min-height: 32px; max-height: 44px; }}
QPushButton:hover    {{ background: rgba(255,255,255,0.13); border-color: rgba(255,255,255,0.25); }}
QPushButton:pressed  {{ background: rgba(255,255,255,0.04); }}
QPushButton:disabled {{ color: rgba(255,255,255,0.25); background: rgba(255,255,255,0.03); border-color: rgba(255,255,255,0.07); }}
QPushButton#primary {{ background: {_a}; color: #0a0a0a; border: none; font-weight: 600;
                        font-size: 13px; border-radius: 6px; padding: 0 18px;
                        min-height: 32px; max-height: 44px; }}
QPushButton#primary:hover    {{ background: {_a2}; }}
QPushButton#primary:pressed  {{ background: {_a}; }}
QPushButton#primary:disabled {{ background: rgba(200,134,26,0.20); color: rgba(10,10,10,0.4); }}
QPushButton#run {{ background: {_suc}; color: #ffffff; border: none; font-weight: 600;
                   font-size: 13px; border-radius: 5px; padding: 0 22px;
                   min-height: 34px; max-height: 44px; }}
QPushButton#run:hover    {{ background: {_suc}; border: 1px solid rgba(255,255,255,0.22); }}
QPushButton#run:disabled {{ background: rgba(255,255,255,0.08); color: rgba(255,255,255,0.25); }}
QPushButton#ghost {{ background: transparent; border: 1px solid rgba(255,255,255,0.14); color: rgba(255,255,255,0.75);
                     font-size: 12px; padding: 0 12px; border-radius: 5px; font-weight: 500;
                     min-height: 28px; max-height: 44px; }}
QPushButton#ghost:hover    {{ border-color: {_a}; color: {_a}; background: {_alo}; }}
QPushButton#ghost:checked  {{ background: {_alo}; border-color: {_a}; color: {_a}; }}
QPushButton#ghost:disabled {{ color: rgba(255,255,255,0.20); border-color: rgba(255,255,255,0.07); }}
QPushButton#danger {{ background: rgba(192,64,64,0.15); border: 1px solid rgba(192,64,64,0.35); color: rgba(210,110,110,0.90);
                       font-size: 12px; border-radius: 5px; padding: 0 12px; font-weight: 500;
                       min-height: 28px; max-height: 44px; }}
QPushButton#danger:hover {{ background: rgba(192,64,64,0.28); border-color: rgba(192,64,64,0.70); }}
QPushButton#cancel {{ background: rgba(192,64,64,0.15); border: 1px solid rgba(192,64,64,0.35);
    color: rgba(210,110,110,0.90); border-radius: 5px; font-size: 12px; padding: 0 10px; font-weight: 500;
    min-height: 28px; max-height: 44px; }}
QPushButton#cancel:hover {{ background: rgba(192,64,64,0.28); border-color: rgba(192,64,64,0.70); }}
QPushButton#navbtn {{ background: transparent; border: none; border-radius: 5px; color: rgba(255,255,255,0.65);
                       text-align: left; padding: 0 12px; font-size: 13px;
                       min-height: 34px; max-height: 44px; }}
QPushButton#navbtn:hover {{ background: rgba(255,255,255,0.06); color: #e2ddd6; }}
QPushButton#navbtn_active {{ background: {_alo}; border: none;
    border-left: 3px solid {_a}; border-radius: 0px;
    border-top-right-radius: 5px; border-bottom-right-radius: 5px;
    color: {_a}; text-align: left; padding: 0 12px; padding-left: 9px;
    font-size: 13px; font-weight: 600; min-height: 34px; max-height: 44px; }}
QPushButton#tabbtn {{ background: transparent; border: none; min-width: 60px;
    border-bottom: 2px solid transparent; color: rgba(255,255,255,0.45);
    font-size: 13px; padding: 0 14px; border-radius: 0; font-weight: 500; }}
QPushButton#tabbtn:checked {{ border-bottom-color: {_a}; color: {_a}; font-weight: 600; }}
QPushButton#tabbtn:hover:!checked {{ color: #e2ddd6; background: rgba(255,255,255,0.05); }}
QPushButton#toggle {{ background: rgba(255,255,255,0.07); border: 1px solid rgba(255,255,255,0.12);
    border-radius: 5px; color: #e2ddd6; font-size: 12px; padding: 0 12px; font-weight: 500;
    min-height: 28px; max-height: 44px; }}
QPushButton#toggle:checked {{ background: {_alo}; border-color: {_a}; color: {_a}; font-weight: 600; }}
QPushButton#toggle:hover:!checked {{ border-color: rgba(255,255,255,0.25); }}
QPushButton#icon_btn {{ background: transparent; border: 1px solid rgba(255,255,255,0.12);
    border-radius: 5px; color: #e2ddd6; font-size: 12px; padding: 0 10px; font-weight: 500;
    min-height: 28px; max-height: 44px; }}
QPushButton#icon_btn:hover {{ border-color: {_a}; color: {_a}; background: {_alo}; }}

QLabel#sectiontitle {{ color: rgba(255,255,255,0.30); font-size: 9px; font-weight: 700; letter-spacing: 2px; background: transparent; }}

QWidget#queue_hdr QPushButton {{ background: transparent; border: 1px solid rgba(255,255,255,0.12);
    color: #e2ddd6; border-radius: 4px; font-size: 11px; padding: 0 8px;
    min-height: 22px; max-height: 26px; font-weight: 500; }}
QWidget#queue_hdr QPushButton:hover {{ border-color: {_a}; color: {_a}; background: {_alo}; }}
QWidget#queue_hdr QPushButton#danger {{ background: rgba(192,64,64,0.15); border-color: rgba(192,64,64,0.35); color: rgba(210,110,110,0.90); }}
QWidget#queue_hdr QPushButton#danger:hover {{ background: rgba(192,64,64,0.28); border-color: rgba(192,64,64,0.70); }}

QWidget#alb_row {{ background: rgba(255,255,255,0.05); border: 1px solid rgba(255,255,255,0.08); border-radius: 8px; }}
QWidget#alb_row QLabel {{ background: transparent; border: none; color: #e2ddd6; }}
QWidget#alb_row QPushButton {{ background: transparent; border: 1px solid rgba(255,255,255,0.12);
    color: #e2ddd6; border-radius: 5px; font-size: 12px; padding: 0 12px;
    min-height: 30px; max-height: 34px; font-weight: 500; }}
QWidget#alb_row QPushButton:hover {{ border-color: {_a}; color: {_a}; background: {_alo}; }}
QWidget#alb_row QPushButton#dl_btn {{ background: {_a}; color: #0a0a0a; border: none;
    font-weight: 600; border-radius: 5px; padding: 0 14px;
    min-height: 30px; max-height: 34px; }}
QWidget#alb_row QPushButton#dl_btn:hover {{ background: {_a2}; }}

QTableWidget {{ background: rgba(10,12,16,0.40); alternate-background-color: rgba(255,255,255,0.022);
                gridline-color: transparent; border: none; outline: none; font-size: 13px; border-radius: 8px; }}
QTableWidget::item {{ padding: 3px 12px; border-bottom: 1px solid rgba(255,255,255,0.04); color: #e2ddd6; }}
QTableWidget::item:selected {{ background: {_alo}; color: #e2ddd6; border: none; }}
QTableWidget::item:hover {{ background: transparent; border: none; }}
QHeaderView {{ border: none; background: transparent; }}
QHeaderView::section {{ background: rgba(255,255,255,0.03); color: rgba(255,255,255,0.30); padding: 6px 12px;
    border: none; border-bottom: 1px solid rgba(255,255,255,0.06);
    font-size: 10px; font-weight: 700; letter-spacing: 1.2px; }}
QHeaderView::section:last-child {{ border-right: none; }}

QScrollBar:vertical {{ background: transparent; width: 6px; margin: 0; }}
QScrollBar::handle:vertical {{ background: rgba(255,255,255,0.15); border-radius: 3px; min-height: 24px; }}
QScrollBar::handle:vertical:hover {{ background: rgba(255,255,255,0.30); }}
QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical {{ height: 0; }}
QScrollBar:horizontal {{ background: transparent; height: 6px; }}
QScrollBar::handle:horizontal {{ background: rgba(255,255,255,0.15); border-radius: 3px; }}
QScrollBar::add-line:horizontal, QScrollBar::sub-line:horizontal {{ width: 0; }}

QProgressBar {{ background: rgba(255,255,255,0.08); border: none; border-radius: 3px;
                height: 6px; text-align: center; color: transparent; }}
QProgressBar::chunk {{ background: {_a}; border-radius: 3px; }}

QCheckBox {{ color: #e2ddd6; spacing: 8px; font-size: 13px; background: transparent; }}
QCheckBox::indicator {{ width: 15px; height: 15px; border-radius: 4px;
    border: 1px solid rgba(255,255,255,0.20); background: rgba(255,255,255,0.06); }}
QCheckBox::indicator:checked {{ background: {_a}; border-color: {_a}; }}
QCheckBox::indicator:hover {{ border-color: rgba(255,255,255,0.20); }}
QCheckBox:disabled {{ color: rgba(255,255,255,0.25); }}

QListWidget {{ background: rgba(10,12,16,0.40); border: none; outline: none; border-radius: 6px; }}
QListWidget::item {{ padding: 6px 12px; border: none; color: #e2ddd6; }}
QListWidget::item:selected {{ background: {_alo}; color: {_a}; border: none; }}
QListWidget::item:hover {{ background: rgba(255,255,255,0.05); color: #e2ddd6; }}

QToolTip {{ background: #1e2128; color: #e2ddd6; border: 1px solid rgba(255,255,255,0.12);
            padding: 5px 8px; border-radius: 5px; font-size: 12px; }}

QTabWidget::pane {{ border: 1px solid rgba(255,255,255,0.10); border-radius: 6px;
                    background: rgba(255,255,255,0.04); padding-top: 8px; color: #e2ddd6; }}
QTabBar::tab {{ background: rgba(255,255,255,0.05); border: 1px solid rgba(255,255,255,0.10);
                padding: 6px; color: rgba(255,255,255,0.55); font-size: 12px; }}
QTabBar::tab:selected {{ background: rgba(255,255,255,0.10); color: #e2ddd6; }}

QSplitter::handle {{ background: rgba(255,255,255,0.07); }}
QTreeWidget {{ background: rgba(10,12,16,0.40); border: none; outline: none; border-radius: 6px; color: #e2ddd6; }}
QTreeWidget::item {{ padding: 3px; }}
QTreeWidget::item:selected {{ background: {_alo}; color: {_a}; }}
QTreeWidget::item:hover {{ background: rgba(255,255,255,0.05); }}
QTreeWidget QHeaderView::section {{ background: rgba(255,255,255,0.03); color: rgba(255,255,255,0.35);
    padding: 4px 8px; border: none; border-bottom: 1px solid rgba(255,255,255,0.06);
    font-size: 10px; font-weight: 700; letter-spacing: 1.0px; }}
QMenu {{ background: #1e2128; border: 1px solid rgba(255,255,255,0.12); border-radius: 6px; padding: 4px; }}
QMenu::item {{ padding: 6px 18px; color: #e2ddd6; border-radius: 4px; }}
QMenu::item:selected {{ background: {_alo}; color: {_a}; }}
QMenu::separator {{ height: 1px; background: rgba(255,255,255,0.08); margin: 3px 8px; }}
QScrollArea {{ background: transparent; border: none; }}
QScrollArea > QWidget > QWidget {{ background: transparent; }}
"""


# ─────────────────────────────────────────────────────────────
#  DATABASE
# ─────────────────────────────────────────────────────────────

_db      = sqlite3.connect(str(DB_FILE), check_same_thread=False)
_db_lock = _threading.Lock()

with _db_lock:
    _cur = _db.cursor()
    _cur.executescript("""
    CREATE TABLE IF NOT EXISTS scrobbled (
        artist       TEXT NOT NULL,
        album        TEXT NOT NULL,
        title        TEXT NOT NULL,
        ts           INTEGER NOT NULL,
        platform     TEXT NOT NULL DEFAULT 'lastfm',
        submitted_at INTEGER DEFAULT (strftime('%s','now')),
        PRIMARY KEY (artist, album, title, ts, platform)
    );
    CREATE INDEX IF NOT EXISTS idx_scrobbled_plat_ts ON scrobbled (platform, ts);
    CREATE INDEX IF NOT EXISTS idx_scrobbled_submitted ON scrobbled (submitted_at DESC);
""")

    _existing_cols = {row[1] for row in _cur.execute("PRAGMA table_info(scrobbled)")}
    if "platform"     not in _existing_cols:
        _cur.execute("ALTER TABLE scrobbled ADD COLUMN platform TEXT NOT NULL DEFAULT 'lastfm'")
    if "submitted_at" not in _existing_cols:
        _cur.execute("ALTER TABLE scrobbled ADD COLUMN submitted_at INTEGER")
    _db.commit()


def _pk(plat: str) -> str:
    return plat.lower().replace(".", "").replace(" ", "")


def db_is_done(artist, album, title, ts, plat):
    with _db_lock:
        cur = _db.cursor()
        cur.execute(
            "SELECT 1 FROM scrobbled WHERE artist=? AND album=? AND title=? AND ts=? AND platform=?",
            (artist, album, title, ts, _pk(plat)),
        )
        return cur.fetchone() is not None


def db_batch_done(tracks: list, plat: str) -> set:
    """Single query — returns set of (artist,album,title,ts) already scrobbled."""
    if not tracks:
        return set()
    pk = _pk(plat)
    with _db_lock:
        cur = _db.cursor()
        ts_min = min(t.timestamp for t in tracks)
        ts_max = max(t.timestamp for t in tracks)
        cur.execute(
            "SELECT artist, album, title, ts FROM scrobbled WHERE platform=? AND ts BETWEEN ? AND ?",
            (pk, ts_min, ts_max),
        )
        return set(cur.fetchall())


def db_mark_done(artist, album, title, ts, plat):
    with _db_lock:
        cur = _db.cursor()
        cur.execute(
            "INSERT OR IGNORE INTO scrobbled VALUES (?,?,?,?,?,strftime('%s','now'))",
            (artist, album, title, ts, _pk(plat)),
        )
        _db.commit()


def db_history(limit=5000):
    with _db_lock:
        cur = _db.cursor()
        cur.execute(
            "SELECT artist, album, title, ts, platform, submitted_at FROM scrobbled "
            "ORDER BY submitted_at DESC LIMIT ?",
            (limit,),
        )
        return cur.fetchall()


def db_delete(artist: str, album: str, title: str, ts: int, plat: str) -> bool:
    """Remove a single scrobble record. Returns True if a row was deleted."""
    with _db_lock:
        cur = _db.cursor()
        cur.execute(
            "DELETE FROM scrobbled WHERE artist=? AND album=? AND title=? AND ts=? AND platform=?",
            (artist, album, title, ts, _pk(plat)),
        )
        _db.commit()
        return cur.rowcount > 0


def db_stats_for_lfm(plat=P_LASTFM) -> dict:
    """Aggregate stats from local DB for the Last.fm stats page."""
    pk = _pk(plat)
    with _db_lock:
        cur = _db.cursor()
        cur.execute("SELECT COUNT(*) FROM scrobbled WHERE platform=?", (pk,))
        total = cur.fetchone()[0]

        cur.execute(
            "SELECT COUNT(DISTINCT artist) FROM scrobbled WHERE platform=?",
            (pk,),
        )
        n_artists = cur.fetchone()[0]

        cur.execute(
            "SELECT COUNT(DISTINCT album) FROM scrobbled WHERE platform=?",
            (pk,),
        )
        n_albums = cur.fetchone()[0]

        cur.execute(
            "SELECT artist, COUNT(*) as c FROM scrobbled WHERE platform=? GROUP BY artist ORDER BY c DESC LIMIT 20",
            (pk,),
        )
        top_artists = cur.fetchall()

        cur.execute(
            "SELECT album, artist, COUNT(*) as c FROM scrobbled WHERE platform=? GROUP BY album, artist ORDER BY c DESC LIMIT 20",
            (pk,),
        )
        top_albums = cur.fetchall()

        cur.execute(
            "SELECT title, artist, COUNT(*) as c FROM scrobbled WHERE platform=? GROUP BY title, artist ORDER BY c DESC LIMIT 20",
            (pk,),
        )
        top_tracks = cur.fetchall()

        # Daily play counts for the heatmap (last 53 weeks = ~1 year)
        cutoff = int(time.time()) - 53 * 7 * 86400
        cur.execute(
            "SELECT date(ts,'unixepoch') as d, COUNT(*) FROM scrobbled "
            "WHERE platform=? AND ts >= ? GROUP BY d",
            (pk, cutoff),
        )
        daily = dict(cur.fetchall())

        # Monthly counts for trend chart (last 24 months)
        cutoff24 = int(time.time()) - 24 * 30 * 86400
        cur.execute(
            "SELECT strftime('%Y-%m', ts,'unixepoch') as m, COUNT(*) FROM scrobbled "
            "WHERE platform=? AND ts >= ? GROUP BY m ORDER BY m",
            (pk, cutoff24),
        )
        monthly = cur.fetchall()

        # First and most recent scrobble
        cur.execute("SELECT MIN(ts), MAX(ts) FROM scrobbled WHERE platform=?", (pk,))
        row = cur.fetchone()
        first_ts, last_ts = row if row else (None, None)

    return {
        "total": total,
        "n_artists": n_artists,
        "n_albums": n_albums,
        "top_artists": top_artists,
        "top_albums": top_albums,
        "top_tracks": top_tracks,
        "daily": daily,
        "monthly": monthly,
        "first_ts": first_ts,
        "last_ts": last_ts,
    }


# ─────────────────────────────────────────────────────────────
#  DATA MODEL
# ─────────────────────────────────────────────────────────────

@dataclass
class Track:
    artist:    str
    album:     str
    title:     str
    tracknum:  int
    length:    int
    rating:    str       # "L" = listened, "S" = skipped
    timestamp: int       # TRUE UTC unix epoch
    mbid:      Optional[str]
    enabled:   bool = True

    @property
    def listened(self): return self.rating == "L"
    @property
    def skipped(self):  return self.rating == "S"
    @property
    def utc_ts(self) -> int:    return self.timestamp
    @property
    def utc_dt(self) -> datetime:
        return datetime.fromtimestamp(self.timestamp, tz=timezone.utc).replace(tzinfo=None)
    @property
    def local_dt(self) -> datetime:
        return datetime.fromtimestamp(self.timestamp)
    @property
    def local_tz_name(self) -> str:
        try:
            aware = datetime.fromtimestamp(self.timestamp).astimezone()
            name = aware.strftime("%Z")
            if name and not name[0].isdigit():
                return name
            return aware.strftime("UTC%z")[:8]
        except Exception:
            return "local"
    @property
    def duration_str(self) -> str:
        if self.length <= 0: return "—"
        return f"{self.length // 60}:{self.length % 60:02d}"


# ─────────────────────────────────────────────────────────────
#  CONFIG
# ─────────────────────────────────────────────────────────────

def load_conf() -> dict:
    try:
        data = json.loads(CONF_FILE.read_text()) if CONF_FILE.exists() else {}
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}

def save_conf(conf: dict):
    # Write to a temp file first, then rename — atomic on all major OSes.
    # Prevents corrupted config if the process crashes mid-write.
    tmp = CONF_FILE.with_suffix(".tmp")
    try:
        tmp.write_text(json.dumps(conf, indent=2), encoding="utf-8")
        tmp.replace(CONF_FILE)
    except Exception as e:
        import sys as _sys_mod
        print(f"[Scrobbox] WARNING: failed to save config: {e}", file=_sys_mod.stderr)
        try:
            tmp.unlink(missing_ok=True)
        except Exception:
            pass

def session_path(plat: str) -> Path:
    return SESSION_DIR / f"{_pk(plat)}_session.txt"

def load_session(plat: str) -> Optional[str]:
    p = session_path(plat)
    return p.read_text().strip() or None if p.exists() else None

def save_session(plat: str, key: str):
    session_path(plat).write_text(key)

def clear_session(plat: str):
    session_path(plat).unlink(missing_ok=True)


def load_rsync_profiles() -> list:
    try:
        return json.loads(RSYNC_PROFILES_FILE.read_text()) if RSYNC_PROFILES_FILE.exists() else []
    except Exception:
        return []

def save_rsync_profiles(profiles: list):
    RSYNC_PROFILES_FILE.write_text(json.dumps(profiles, indent=2))


# ─────────────────────────────────────────────────────────────
#  API HELPERS
# ─────────────────────────────────────────────────────────────

def api_sig(params: dict, secret: str) -> str:
    # Last.fm spec: exclude 'format' and 'callback' from signature computation
    EXCLUDE = {"format", "callback"}
    raw = "".join(k + str(params[k]) for k in sorted(params) if k not in EXCLUDE) + secret
    return hashlib.md5(raw.encode()).hexdigest()

def lfm_call(params: dict, api_url: str, timeout=20) -> dict:
    """POST to Last.fm/Libre.fm API with simple retry on rate-limit and transient errors."""
    RETRIES = 3
    for attempt in range(RETRIES):
        r = requests.post(api_url, data=params, timeout=timeout)
        if r.status_code == 429 or r.status_code >= 500:
            if attempt < RETRIES - 1:
                time.sleep(2 ** attempt)  # 1s, 2s backoff
                continue
        r.raise_for_status()
        return r.json()
    r.raise_for_status()
    return r.json()


# ─────────────────────────────────────────────────────────────
#  LOG PARSING
# ─────────────────────────────────────────────────────────────

_BOM = "\ufeff"

def _strip_bom(s: str) -> str:
    return s.lstrip(_BOM)

def _local_utc_offset_seconds() -> int:
    if time.daylight and time.localtime().tm_isdst:
        return -time.altzone
    return -time.timezone

def parse_log(path: Path) -> list[Track]:
    """Parse .scrobbler.log → list of Track (timestamp always true UTC epoch).

    Returns (tracks, skipped_future) where skipped_future is the count of entries
    dropped because their timestamp is more than 1 hour in the future (likely a
    device clock error).
    """
    tracks         = []
    skipped_future = 0
    # Default: no header → treat timestamps as local time (same as #TZ/UNKNOWN).
    # Rockbox writes local time when the timezone is unknown, so this is the
    # safest fallback. The future-check is skipped in this case because we're
    # already making a best-effort guess and a wrong system clock shouldn't
    # silently discard the entire log.
    tz_offset      = _local_utc_offset_seconds()
    has_tz_header  = False   # True once we've seen a #TZ/ line
    try:
        with open(path, encoding="utf-8-sig", errors="ignore") as f:
            for raw_line in f:
                line     = raw_line.rstrip("\n\r")
                stripped = line.lstrip(_BOM)
                if not stripped:
                    continue
                if stripped.startswith("#TZ/"):
                    has_tz_header = True
                    tz_str = stripped[4:].strip().upper()
                    if tz_str == "UTC":
                        tz_offset = 0
                    elif tz_str == "UNKNOWN":
                        tz_offset = _local_utc_offset_seconds()
                    else:
                        try:
                            sign = -1 if tz_str.startswith("-") else 1
                            d    = tz_str.lstrip("+-")
                            h, m = int(d[:2]), int(d[2:4]) if len(d) >= 4 else 0
                            tz_offset = sign * (h * 3600 + m * 60)
                        except Exception:
                            tz_offset = 0
                    continue
                if stripped.startswith("#"):
                    continue
                line = _strip_bom(line)
                p    = line.split("\t")
                if len(p) < 7:
                    continue
                p = [_strip_bom(x) for x in p]
                try:
                    ts_raw = int(p[6].strip())
                    ts_utc = ts_raw - tz_offset
                    # Only apply the future-timestamp guard when a #TZ/ header was
                    # present — without one we assume the raw values are already UTC
                    # and a wrong system clock shouldn't silently discard the whole log.
                    if has_tz_header and ts_utc > int(time.time()) + 3600:
                        skipped_future += 1
                        continue
                    tracks.append(Track(
                        artist   = p[0].strip(),
                        album    = p[1].strip(),
                        title    = p[2].strip(),
                        tracknum = int(p[3]) if p[3].strip().lstrip("-").isdigit() else 0,
                        length   = int(p[4]) if p[4].strip().lstrip("-").isdigit() else 0,
                        rating   = p[5].strip().upper(),
                        timestamp= ts_utc,
                        mbid     = p[7].strip() if len(p) > 7 and p[7].strip() else None,
                    ))
                except (ValueError, IndexError):
                    continue
    except OSError:
        pass
    return sorted(tracks, key=lambda t: t.timestamp), skipped_future


def _safe_exists(p: Path) -> bool:
    try:
        return p.exists()
    except (PermissionError, OSError):
        return False


def _iter_dir(p: Path):
    """Iterate a directory, silently skipping permission errors."""
    try:
        yield from p.iterdir()
    except (PermissionError, OSError):
        pass


def _mount_roots() -> list[Path]:
    """Return all candidate mounted volume roots for the current OS."""
    roots: list[Path] = []
    _s = platform.system()
    if _s == "Linux":
        user = Path.home().name
        uid  = os.getuid()
        for base in [
            Path(f"/run/media/{user}"),
            Path(f"/media/{user}"),
            Path("/media"),
            Path("/mnt"),
            Path(f"/run/user/{uid}/gvfs"),
        ]:
            if _safe_exists(base):
                for entry in _iter_dir(base):
                    if entry.is_dir():
                        roots.append(entry)
                        # gvfs mounts can nest (e.g. gvfs/mtp:host=.../Internal Storage)
                        for sub in _iter_dir(entry):
                            if sub.is_dir():
                                roots.append(sub)
    elif _s == "Darwin":
        for entry in _iter_dir(Path("/Volumes")):
            if entry.is_dir():
                roots.append(entry)
    return roots


def find_rockbox_devices() -> list[Path]:
    """
    Return device root paths that contain a .rockbox directory.
    Works regardless of device name (iPod, stupid, my player, etc.).
    """
    return [r for r in _mount_roots() if _safe_exists(r / ".rockbox")]


def find_logs() -> list[Path]:
    """
    Return paths to .scrobbler.log on connected Rockbox devices.
    Prefers .rockbox-bearing mounts; falls back to legacy log scan.
    """
    logs: list[Path] = []

    # Primary: only look on mounts that actually have .rockbox
    # Note: .scrobbler.log is always in the device root (or Music/ subdir),
    # never inside .rockbox/ itself
    for root in find_rockbox_devices():
        for candidate in [
            root / ".scrobbler.log",
            root / "Music" / ".scrobbler.log",
        ]:
            if _safe_exists(candidate):
                logs.append(candidate)

    if logs:
        return logs

    # Fallback: non-Rockbox players that just produce a .scrobbler.log
    cands: list[Path] = []
    for root in _mount_roots():
        cands += [root / ".scrobbler.log", root / "Music" / ".scrobbler.log"]
    return [p for p in cands if _safe_exists(p)]


def detect_sessions(tracks: list[Track], gap_min: int = 20) -> list[list[Track]]:
    sessions, cur = [], []
    for t in tracks:
        if not t.listened:
            continue
        if not cur or t.timestamp - cur[-1].timestamp > gap_min * 60:
            if cur:
                sessions.append(cur)
            cur = [t]
        else:
            cur.append(t)
    if cur:
        sessions.append(cur)
    return sessions


# ─────────────────────────────────────────────────────────────
#  ROCKBOX CONFIG KNOWLEDGE BASE
# ─────────────────────────────────────────────────────────────

# All known config.cfg keys with type, allowed values, description
# Types: int, bool, enum, str
ROCKBOX_CONFIG_SCHEMA = {
    # ── Sound ────────────────────────────────────────────────
    "volume":           ("int",  (-74, 6),    "Volume level in dB"),
    "bass":             ("int",  (-6, 9),     "Bass EQ adjustment in dB"),
    "treble":           ("int",  (-6, 9),     "Treble EQ adjustment in dB"),
    "balance":          ("int",  (-100, 100), "Stereo balance (negative=left, positive=right)"),
    "channels":         ("enum", ["stereo","mono","custom","mono left","mono right","karaoke"], "Stereo channel mode"),
    "stereo width":     ("int",  (0, 255),    "Stereo width (100 = normal)"),
    "volume limit":     ("int",  (0, 100),    "Maximum volume limit (0 = no limit)"),
    "replaygain":       ("bool", None,        "Enable ReplayGain volume normalisation"),
    "replaygain type":  ("enum", ["track gain","album gain","track gain if shuffling","off"], "ReplayGain mode"),
    "replaygain noclip":("bool", None,        "Prevent clipping when ReplayGain is active"),
    "replaygain preamp":("int",  (-120, 120), "ReplayGain pre-amplification in 0.1 dB steps"),
    "crossfeed":        ("bool", None,        "Enable crossfeed (reduces stereo separation)"),
    "crossfeed direct gain":  ("int", (0, 60), "Crossfeed direct signal gain (0.1 dB)"),
    "crossfeed cross gain":   ("int", (0, 60), "Crossfeed cross signal gain (0.1 dB)"),
    "crossfeed hf attenuation":("int",(0,60), "Crossfeed HF attenuation (0.1 dB)"),
    "eq enabled":       ("bool", None,        "Enable the parametric equaliser"),
    "bass cutoff":      ("int",  (0, 24000),  "Bass shelf cutoff frequency (Hz)"),
    "treble cutoff":    ("int",  (0, 24000),  "Treble shelf cutoff frequency (Hz)"),
    "dithering enabled":("bool", None,        "Enable audio dithering"),

    # ── Playback ─────────────────────────────────────────────
    "shuffle":          ("bool", None,        "Enable shuffle playback"),
    "repeat":           ("enum", ["off","all","one","shuffle","ab"], "Repeat mode"),
    "play selected first":("bool", None,      "Start playback at selected track in playlist"),
    "fade on stop":     ("bool", None,        "Fade out audio when stopping/pausing"),
    "fade duration":    ("int",  (1, 10),     "Fade in/out duration in seconds"),
    "crossfade":        ("enum", ["off","auto track change","man. track skip","shuffle","shuffle and man. track skip","always"], "Crossfade between tracks"),
    "crossfade fade in delay":  ("int",(0,7), "Crossfade: fade-in delay (seconds)"),
    "crossfade fade out delay": ("int",(0,7), "Crossfade: fade-out delay (seconds)"),
    "crossfade fade in duration":("int",(0,15),"Crossfade: fade-in duration (seconds)"),
    "crossfade fade out duration":("int",(0,15),"Crossfade: fade-out duration (seconds)"),
    "crossfade fade out mixmode":("enum",["crossfade","mix"],"Crossfade fade-out mix mode"),
    "party mode":       ("bool", None,        "Party mode — prevents accidental playback changes"),
    "skip length":      ("enum", ["track","2s","3s","5s","7s","10s","15s","20s","30s","1min","90s","3min","5min","10min"], "Skip length for fwd/rew buttons"),
    "prevent skipping": ("bool", None,        "Prevent track skipping during playback"),
    "rewind before resume":("int",(0,60),     "Rewind N seconds before resuming playback"),
    "autobind next folder":("bool", None,     "Automatically advance to next folder"),

    # ── Display ──────────────────────────────────────────────
    "backlight":        ("int",  (-1, 90),    "Backlight timeout in seconds (-1=always on, 0=always off)"),
    "backlight on button press":("bool", None,"Turn on backlight on any button press"),
    "caption backlight":("bool", None,        "Backlight on when track changes"),
    "backlight fade in":("int",  (0, 2000),   "Backlight fade-in time (ms)"),
    "backlight fade out":("int", (0, 2000),   "Backlight fade-out time (ms)"),
    "brightness":       ("int",  (0, 20),     "Screen brightness level"),
    "scroll speed":     ("int",  (1, 20),     "Scrolling text speed"),
    "scroll delay":     ("int",  (0, 2500),   "Delay before scrolling starts (ms)"),
    "scroll step size": ("int",  (1, 20),     "Pixels per scroll step"),
    "bidir limit":      ("int",  (0, 200),    "Bidirectional scroll threshold (% of screen)"),
    "statusbar":        ("enum", ["top","bottom","off","custom"], "Status bar position"),
    "scrollbar":        ("enum", ["off","left","right"], "Scrollbar position"),
    "scrollbar width":  ("int",  (3, 10),     "Scrollbar width in pixels"),
    "show icons":       ("bool", None,        "Show file type icons in browser"),
    "peak meter release":("int", (1, 200),    "Peak meter release speed"),
    "peak meter hold":  ("int",  (0, 250),    "Peak meter hold time (1/100 seconds)"),
    "peak meter clip hold":("int",(0,850),    "Peak meter clip indicator hold time (seconds)"),
    "font":             ("str",  None,        "Path to .fnt font file in /.rockbox/fonts/"),
    "glyphs to cache":  ("int",  (50, 65535), "Number of font glyphs to cache in RAM"),

    # ── Sleep / Power ─────────────────────────────────────────
    "sleep timer":      ("int",  (0, 300),    "Auto-sleep after N minutes of idle (0=off)"),
    "idle poweroff":    ("int",  (0, 60),     "Power off after N minutes idle (0=off)"),
    "disk poweroff":    ("bool", None,        "Power off hard disk when idle"),
    "max files in dir": ("int",  (50, 10000), "Maximum files to show in file browser"),
    "max files in playlist":("int",(1000,32000),"Maximum playlist size"),

    # ── Navigation / UI ──────────────────────────────────────
    "sort files":       ("enum", ["alpha","oldest first","newest first","by type"], "File browser sort order"),
    "sort dirs":        ("enum", ["alpha","oldest first","newest first"], "Directory sort order"),
    "show files":       ("enum", ["all","supported","music","playlists"], "Which files to show in browser"),
    "follow playlist":  ("bool", None,        "File browser follows currently playing track"),
    "start screen":     ("enum", ["previous","root","files","database","wps","menu","bookmarks","plugins"], "Screen shown on startup"),
    "language":         ("str",  None,        "Path to .lng language file in /.rockbox/langs/"),
    "talk menu":        ("bool", None,        "Voice the menus (requires voice file)"),
    "talk dir":         ("enum", ["off","number","spell","clip"], "How to announce directory names"),
    "talk file":        ("enum", ["off","number","spell","time","clip"], "How to announce file names"),
    "talk file clip":   ("bool", None,        "Use .talk clip files for announcements"),

    # ── Bookmarks ─────────────────────────────────────────────
    "bookmark on stop": ("enum", ["off","on","ask","ask - no recent bookmarks","on - no recent bookmarks"], "Create bookmark when stopping"),
    "load last bookmark":("enum",["off","on","ask"], "Load last bookmark when opening a file"),
    "show bookmarks":   ("enum", ["off","on","unique only"], "Whether to offer bookmark list on file open"),

    # ── Database ──────────────────────────────────────────────
    "auto update":      ("bool", None,        "Auto-update database on each boot"),
    "gather runtime data":("bool", None,      "Track play counts and ratings in database"),
    "database directory":("str", None,        "Directory to store database files (default: /.rockbox/)"),
    "runtimedb":        ("bool", None,        "Enable runtime database (required for gather runtime data)"),

    # ── Recording ─────────────────────────────────────────────
    "rec frequency":    ("enum", ["8","11.025","12","16","22.05","24","32","44.1","48","64","88.2","96"], "Recording sample rate (kHz)"),
    "rec channels":     ("enum", ["stereo","mono"], "Recording channels"),
    "rec source":       ("enum", ["mic","line","spdif","fmradio"], "Recording input source"),
    "rec quality":      ("int",  (0, 7),      "MP3 recording quality (0=lowest, 7=highest)"),
    "rec mono mode":    ("enum", ["mix","left","right"], "Mono recording mix mode"),

    # ── Theme / WPS ───────────────────────────────────────────
    "wps":              ("str",  None,        "Path to .wps While Playing Screen theme"),
    "rwps":             ("str",  None,        "Path to .rwps Remote WPS theme"),
    "sbs":              ("str",  None,        "Path to .sbs Status Bar Skin theme"),
    "rsbs":             ("str",  None,        "Path to .rsbs Remote SBS theme"),
    "fms":              ("str",  None,        "Path to .fms FM Radio Screen theme"),
    "backdrop":         ("str",  None,        "Path to backdrop image file"),
    "ui viewport":      ("str",  None,        "UI viewport definition (x,y,w,h,...)"),
    "line selector type":("enum",["pointer","bar (inverse)","bar (color)","bar (gradient)","gradient"], "List selection indicator style"),
    "line selector color":("str", None,       "Selection bar color (RRGGBB hex)"),
    "line selector gradient start":("str",None,"Gradient bar start color (RRGGBB)"),
    "line selector gradient end":  ("str",None,"Gradient bar end color (RRGGBB)"),
    "fg color":         ("str",  None,        "Foreground text color (RRGGBB hex)"),
    "bg color":         ("str",  None,        "Background color (RRGGBB hex)"),
}

# Group them for the UI
ROCKBOX_CONFIG_GROUPS = {
    "🔊 Sound":     ["volume","bass","treble","balance","channels","stereo width","volume limit",
                     "replaygain","replaygain type","replaygain noclip","replaygain preamp",
                     "crossfeed","dithering enabled","eq enabled","bass cutoff","treble cutoff"],
    "▶ Playback":  ["shuffle","repeat","play selected first","fade on stop","fade duration",
                    "crossfade","skip length","prevent skipping","rewind before resume",
                    "party mode","autobind next folder"],
    "🖥 Display":  ["backlight","brightness","scroll speed","scroll delay","scroll step size",
                    "bidir limit","statusbar","scrollbar","scrollbar width","show icons",
                    "font","glyphs to cache","peak meter release","peak meter hold",
                    "backlight on button press","caption backlight","backlight fade in","backlight fade out"],
    "⚡ Power":    ["sleep timer","idle poweroff","disk poweroff","max files in dir","max files in playlist"],
    "🗂 Browser":  ["sort files","sort dirs","show files","follow playlist","start screen","language",
                    "talk menu","talk dir","talk file","talk file clip"],
    "📖 Bookmarks":["bookmark on stop","load last bookmark","show bookmarks"],
    "🗄 Database": ["auto update","gather runtime data","runtimedb","database directory"],
    "🎙 Recording":["rec frequency","rec channels","rec source","rec quality","rec mono mode"],
    "🎨 Theme":    ["wps","sbs","fms","backdrop","ui viewport","line selector type",
                    "line selector color","fg color","bg color","font"],
}


# ─────────────────────────────────────────────────────────────
#  WORKER (scrobble submission)
# ─────────────────────────────────────────────────────────────

class Worker(QThread):
    progress   = pyqtSignal(int, int)
    track_done = pyqtSignal(object, bool, str)
    finished   = pyqtSignal(int, int)

    def __init__(self, tracks, platform, conf, dry_run):
        super().__init__()
        self.tracks   = tracks
        self.platform = platform
        self.conf     = conf
        self.dry_run  = dry_run
        self._ok = self._fail = 0

    def run(self):
        total = len(self.tracks)
        if self.dry_run:
            for i, t in enumerate(self.tracks, 1):
                self.track_done.emit(t, True, "")
                self._ok += 1
                self.progress.emit(i, total)
            self.finished.emit(self._ok, self._fail)
            return
        if self.platform == P_LISTENBRAINZ:
            self._submit_lbz(total)
        else:
            self._submit_lfm_style(total)
        self.finished.emit(self._ok, self._fail)

    def _submit_lfm_style(self, total):
        api_url = LIBREFM_API if self.platform == P_LIBREFM else LASTFM_API
        if self.platform == P_LIBREFM:
            key    = self.conf.get("librefm_key", self.conf.get("api_key", ""))
            secret = self.conf.get("librefm_secret", self.conf.get("api_secret", ""))
        else:
            key    = self.conf.get("api_key", "")
            secret = self.conf.get("api_secret", "")
        session = load_session(self.platform)
        # Last.fm / Libre.fm support batches of up to 50 scrobbles per call
        BATCH = 50
        done_count = 0
        for batch_start in range(0, len(self.tracks), BATCH):
            if self.isInterruptionRequested():
                return
            chunk = self.tracks[batch_start:batch_start + BATCH]
            params = {"method": "track.scrobble", "api_key": key, "sk": session}
            for idx, t in enumerate(chunk):
                params[f"artist[{idx}]"]    = t.artist
                params[f"track[{idx}]"]     = t.title
                params[f"album[{idx}]"]     = t.album
                params[f"timestamp[{idx}]"] = t.utc_ts
                if t.length > 0:
                    params[f"duration[{idx}]"] = t.length
                if t.mbid:
                    params[f"mbid[{idx}]"] = t.mbid
            try:
                params["api_sig"] = api_sig(params, secret)
                params["format"]  = "json"
                resp = lfm_call(params, api_url)
                if "error" in resp:
                    raise Exception(resp.get("message", "API error"))
                # Mark all in batch as done
                for t in chunk:
                    db_mark_done(t.artist, t.album, t.title, t.timestamp, self.platform)
                    self.track_done.emit(t, True, "")
                    self._ok += 1
            except Exception as e:
                # On batch failure fall back to one-by-one so partial success is possible
                for t in chunk:
                    try:
                        p1 = {"method": "track.scrobble", "api_key": key, "sk": session,
                              "artist": t.artist, "track": t.title, "album": t.album,
                              "timestamp": t.utc_ts}
                        if t.length > 0:
                            p1["duration"] = t.length
                        if t.mbid:
                            p1["mbid"] = t.mbid
                        p1["api_sig"] = api_sig(p1, secret)
                        p1["format"]  = "json"
                        r1 = lfm_call(p1, api_url)
                        if "error" in r1:
                            raise Exception(r1.get("message", "API error"))
                        db_mark_done(t.artist, t.album, t.title, t.timestamp, self.platform)
                        self.track_done.emit(t, True, "")
                        self._ok += 1
                    except Exception as e2:
                        self.track_done.emit(t, False, str(e2))
                        self._fail += 1
            done_count += len(chunk)
            self.progress.emit(done_count, total)

    def _submit_lbz(self, total):
        token      = self.conf.get("lbz_token", "")
        chunk_size = 100
        done = 0
        for i in range(0, len(self.tracks), chunk_size):
            if self.isInterruptionRequested():
                return
            chunk   = self.tracks[i:i+chunk_size]
            payload = {
                "listen_type": "import",
                "payload": [{
                    "listened_at": t.utc_ts,
                    "track_metadata": {
                        "artist_name": t.artist, "track_name": t.title, "release_name": t.album,
                        "additional_info": {
                            "tracknumber": t.tracknum, "duration": t.length,
                            **({"recording_mbid": t.mbid} if t.mbid else {}),
                        }
                    }
                } for t in chunk]
            }
            try:
                r = requests.post(LBZ_API + "submit-listens", json=payload,
                    headers={"Authorization": f"Token {token}", "Content-Type": "application/json"},
                    timeout=30)
                if r.status_code == 200:
                    for t in chunk:
                        db_mark_done(t.artist, t.album, t.title, t.timestamp, self.platform)
                        self.track_done.emit(t, True, "")
                        self._ok += 1
                else:
                    err = f"HTTP {r.status_code}: {r.text[:120]}"
                    for t in chunk:
                        self.track_done.emit(t, False, err)
                        self._fail += 1
            except Exception as e:
                for t in chunk:
                    self.track_done.emit(t, False, str(e))
                    self._fail += 1
            done += len(chunk)
            self.progress.emit(done, total)


# ─────────────────────────────────────────────────────────────
# ─────────────────────────────────────────────────────────────
# ─────────────────────────────────────────────────────────────
#  ROCKBOX DATABASE BUILDER
#  Uses the official Rockbox database tool compiled from source.
#  Writes real .tcd files directly — no on-device scan needed.
# ─────────────────────────────────────────────────────────────

# Rockbox target number used when compiling the database tool.
# Any valid target works — the DB tool binary is host-native (x86_64/arm64)
# and the target only affects minor compile-time constants.
# We use 22 (iPod Video 5G) as a stable, well-tested reference target.
_RBX_TOOL_TARGET = "22"

# Where we cache the compiled binary
_RBX_TOOL_PATH = CONFIG_DIR / "rockbox_dbtool"

# Where we cache the Rockbox source checkout
_RBX_SRC_PATH  = CONFIG_DIR / "rockbox_src"




def _find_bundled_dbtool() -> Optional[Path]:
    """
    When running as an AppImage or PyInstaller bundle, the pre-compiled
    database tool binary is bundled alongside the app.
    Returns the path if found, else None.
    """
    # PyInstaller / cx_Freeze bundle
    _base = Path(getattr(sys, "_MEIPASS", ""))
    if _base and (_base / "rockbox_dbtool").exists():
        return _base / "rockbox_dbtool"
    # AppImage: binary next to the squashfs root
    _appdir = os.environ.get("APPDIR", "")
    if _appdir and (Path(_appdir) / "usr" / "bin" / "rockbox_dbtool").exists():
        return Path(_appdir) / "usr" / "bin" / "rockbox_dbtool"
    # Same directory as the script (dev convenience)
    _script_dir = Path(__file__).parent
    if (_script_dir / "rockbox_dbtool").exists():
        return _script_dir / "rockbox_dbtool"
    return None


class RockboxDbWorker(QThread):
    """
    Builds a real Rockbox tagcache database on the PC by:

      1. Cloning the Rockbox source (once, cached in ~/.config/scrobbox/rockbox_src)
      2. Compiling the official Rockbox database tool for the host platform
         (once, cached at ~/.config/scrobbox/rockbox_dbtool)
      3. Running the compiled binary against the device / local library root,
         which walks the filesystem and writes real .tcd files to /.rockbox/

    The compiled binary is host-native (Linux x86_64 or arm64) — it does NOT
    run on the device. It simply reads files from the mounted path and writes
    the database. No Docker, no on-device scan.

    For AppImage distribution: bundle a pre-compiled rockbox_dbtool binary
    inside the AppImage. The worker detects it automatically.

    Requires: git, gcc, make  (standard on any Linux system)
    """
    progress     = pyqtSignal(str)
    count_update = pyqtSignal(int, int)
    finished     = pyqtSignal(dict)

    MODE_UPDATE     = "update"
    MODE_INITIALIZE = "initialize"

    def __init__(self, device_root: Path, mode: str = MODE_UPDATE,
                 library_root: Optional[Path] = None):
        super().__init__()
        self.device_root     = device_root
        self.mode            = mode
        self.library_root    = library_root
        self._pause_event    = _threading.Event()
        self._pause_event.set()
        self._proc: Optional[subprocess.Popen] = None  # currently running subprocess

    # ── internal helpers ──────────────────────────────────────

    def _emit(self, msg: str):
        self.progress.emit(msg)

    def _check(self):
        self._pause_event.wait()
        if self.isInterruptionRequested():
            raise InterruptedError("Cancelled")

    def _run_cmd(self, cmd: list, cwd=None, env=None, capture=False) -> subprocess.CompletedProcess:
        """Run a command, streaming output to the log. Raises on non-zero exit."""
        import subprocess as _sp
        # Always run with a UTF-8 locale so the tool handles multibyte filenames
        # (special chars, accented letters, unicode quotes, CJK, etc.) correctly.
        # Without this, the C runtime's mbstowcs() fails on non-ASCII and writes
        # null bytes into the .tcd paths, corrupting them.
        _utf8_env = os.environ.copy()
        _utf8_env["LANG"]     = "C.UTF-8"
        _utf8_env["LC_ALL"]   = "C.UTF-8"
        _utf8_env["LC_CTYPE"] = "C.UTF-8"
        if env:
            _utf8_env.update(env)
        env = _utf8_env
        self._emit(f"  $ {' '.join(str(c) for c in cmd)}")
        if capture:
            r = _sp.run(cmd, cwd=cwd, env=env,
                        stdout=_sp.PIPE, stderr=_sp.STDOUT, text=True,
                        encoding="utf-8", errors="replace")
            if r.stdout:
                for line in r.stdout.splitlines():
                    self._emit(f"    {line}")
            return r
        # streaming
        proc = _sp.Popen(cmd, cwd=cwd, env=env,
                         stdout=_sp.PIPE, stderr=_sp.STDOUT, text=True,
                         encoding="utf-8", errors="replace",
                         bufsize=1)
        self._proc = proc
        try:
            for line in proc.stdout:
                self._check()
                line = line.rstrip()
                if line:
                    self._emit(f"    {line}")
            proc.wait()
        finally:
            self._proc = None
        if proc.returncode != 0:
            # Negative exit code means killed by signal (e.g. SIGKILL from cancel).
            # Treat as cancellation so the clean InterruptedError path fires.
            if proc.returncode < 0 or self.isInterruptionRequested():
                raise InterruptedError("Cancelled")
            raise RuntimeError(f"Command failed (exit {proc.returncode}): {' '.join(str(c) for c in cmd)}")
        return proc

    # ── step 1: ensure source ─────────────────────────────────

    def _ensure_source(self) -> Path:
        """Clone the Rockbox source if not already cached. Returns path to source root.

        We never auto-update an existing checkout — the source is only needed
        to compile the database tool once.  If the user wants a fresh compile
        they can click 'Force Recompile', which deletes the cached binary and
        triggers a fresh clone/compile.
        """
        src = _RBX_SRC_PATH
        if src.exists() and (src / "tools" / "configure").exists():
            self._emit(f"✓ Rockbox source found at {src} (using cached)")
            return src

        self._emit("Cloning Rockbox source (shallow, ~60 MB, one-time)…")
        src.parent.mkdir(parents=True, exist_ok=True)
        self._run_cmd([
            "git", "clone",
            "--depth=1",
            "--filter=blob:none",
            "--no-checkout",
            "https://github.com/Rockbox/rockbox.git",
            str(src),
        ])
        # Sparse checkout — we only need tools/ and apps/ and firmware/
        self._run_cmd(["git", "sparse-checkout", "init", "--cone"], cwd=src)
        self._run_cmd(["git", "sparse-checkout", "set",
                       "tools", "apps", "firmware", "lib", "uisimulator"], cwd=src)
        self._run_cmd(["git", "checkout", "master"], cwd=src)
        self._emit(f"✓ Source cloned to {src}")
        return src

    # ── step 2: compile ───────────────────────────────────────

    def _patch_source_for_utf8(self, src: Path):
        """
        Patch tagcache.c to call setlocale(LC_ALL, "") at startup.
        Without this the C runtime uses the "C" locale which cannot handle
        multibyte UTF-8 — special chars in filenames get corrupted to null
        bytes in the .tcd paths. setlocale(LC_ALL, "") uses the system
        locale (UTF-8 on all modern Linux) fixing this completely.
        """
        tagcache_c = src / "apps" / "tagcache.c"
        if not tagcache_c.exists():
            self._emit("  Warning: could not find apps/tagcache.c to patch")
            return

        text = tagcache_c.read_text(encoding='utf-8', errors='replace')

        # Add locale.h include if not already present
        if '#include <locale.h>' not in text:
            idx = text.find('#include ')
            if idx != -1:
                eol = text.index('\n', idx)
                text = text[:eol+1] + '#include <locale.h>\n' + text[eol+1:]

        # Insert setlocale(LC_ALL, "") as first statement in main()
        if 'setlocale(LC_ALL' not in text:
            import re as _re
            def _ins(m):
                return m.group(0) + '\n    setlocale(LC_ALL, "");'
            text = _re.sub(
                r'int\s+main\s*\([^)]*\)\s*\{',
                _ins,
                text, count=1
            )

        tagcache_c.write_text(text, encoding='utf-8')
        self._emit("  Patched tagcache.c for UTF-8 locale support.")


    def _compile_tool(self, src: Path) -> Path:
        """Configure and compile the database tool. Returns path to binary."""
        import tempfile as _tf
        tool_out = _RBX_TOOL_PATH

        self._emit("Compiling Rockbox database tool…")
        self._emit(f"  Source: {src}")
        self._emit(f"  Target: {tool_out}")

        # Patch the source for UTF-8 support before compiling
        self._patch_source_for_utf8(src)

        build_dir = Path(_tf.mkdtemp(prefix="scrobbox_rbxbuild_"))
        try:
            # Configure for database tool (--type=D), host-native binary
            self._run_cmd([
                str(src / "tools" / "configure"),
                f"--target={_RBX_TOOL_TARGET}",
                "--type=D",
            ], cwd=build_dir)

            self._check()

            # Compile
            cpu_count = str(max(1, os.cpu_count() or 1))
            self._run_cmd(["make", f"-j{cpu_count}", "clean"], cwd=build_dir)
            self._run_cmd(["make", f"-j{cpu_count}"], cwd=build_dir)

            # Find the compiled binary (named database.<targetname>)
            candidates = list(build_dir.glob("database.*"))
            if not candidates:
                raise RuntimeError(
                    "Compilation succeeded but no database.* binary found in build dir.\n"
                    f"Build dir contents: {list(build_dir.iterdir())}"
                )
            binary = candidates[0]
            shutil.copy2(str(binary), str(tool_out))
            tool_out.chmod(tool_out.stat().st_mode | 0o111)  # ensure executable
            self._emit(f"✓ Compiled: {binary.name}  →  {tool_out}")
        finally:
            shutil.rmtree(str(build_dir), ignore_errors=True)

        return tool_out



    def _run_tool(self, tool: Path, scan_root: Path, rbdir: Path):
        """
        Run the compiled database tool.

        The tool must be run with the DEVICE ROOT as cwd — it records paths
        relative to cwd, which must match what Rockbox sees on the device.

        Local library mode:
          The library folder must mirror the music folder structure on the
          device. We figure out where music lives on the device relative to
          the device root (e.g. /Music), then create a fake device root where:
            <fake_root>/
              .rockbox/              (empty — tool writes .tcd files here)
              <music_rel>/           (symlink → library_root)
          This makes the tool record paths like /Music/Artist/song.mp3 which
          is exactly what Rockbox expects on the device.
        """
        import tempfile as _tf

        if self.library_root:
            self._emit(f"Mode: local library  {self.library_root}")
            self._emit(f"Output: {rbdir}")

            # Work out where the music folder sits relative to the device root.
            # e.g. device_root=/media/roy/IPOD, music on device at /media/roy/IPOD/Music
            # → music_rel = "Music"
            # We find this by looking at what subdirectory on the device contains
            # audio files, excluding .rockbox. If we can't determine it, fall back
            # to putting music directly under fake_root (rel = "").
            device_root = rbdir.parent
            music_rel = ""
            _AUDIO = {
                ".mp3", ".flac", ".ogg", ".m4a", ".aac",
                ".wv", ".ape", ".wav", ".opus", ".mpc",
                ".aiff", ".alac", ".wma", ".mod", ".spc"
            }
            try:
                # Shallow two-level scan — just enough to find the music
                # subfolder without traversing the entire device tree.
                for item in device_root.iterdir():
                    if item.name.startswith(".") or not item.is_dir():
                        continue
                    # Level 1: audio file directly inside this dir
                    for child in item.iterdir():
                        if child.suffix.lower() in _AUDIO:
                            music_rel = item.name
                            break
                    if music_rel:
                        break
                    # Level 2: one subdir deeper (Artist/Album/file.mp3)
                    for child in item.iterdir():
                        if not child.is_dir():
                            continue
                        for grandchild in child.iterdir():
                            if grandchild.suffix.lower() in _AUDIO:
                                music_rel = item.name
                                break
                        if music_rel:
                            break
                    if music_rel:
                        break
            except Exception:
                pass

            self._emit(
                f"  Device music folder: /{music_rel if music_rel else '(root)'}"
            )

            fake_root = Path(_tf.mkdtemp(prefix="scrobbox_fakedev_"))
            try:
                (fake_root / ".rockbox").mkdir()

                if music_rel:
                    # Symlink library_root as the music subfolder
                    # e.g. fake_root/Music -> /home/roy/Music
                    # Use os.fsencode-safe paths so Unicode folder names
                    # in the library path itself don't cause issues
                    os.symlink(os.fsencode(str(self.library_root)),
                               os.fsencode(str(fake_root / music_rel)))
                else:
                    # Music is at device root level — symlink each item
                    for item in self.library_root.iterdir():
                        if item.name != ".rockbox":
                            os.symlink(os.fsencode(str(item)),
                                       os.fsencode(str(fake_root / item.name)))

                self._emit("Running database tool against local library…")
                self._run_cmd([str(tool)], cwd=str(fake_root))

                tcd_files = list((fake_root / ".rockbox").glob("database*.tcd"))
                if not tcd_files:
                    raise RuntimeError(
                        "Database tool ran but produced no .tcd files.\n"
                        "Check that your music directory contains supported audio files."
                    )
                # Fix FAT32 path issues in temp dir BEFORE copying to device.
                # Pass the real device root so _fix_tcd_paths resolves path
                # components against the actual FAT32 filesystem (not the fake
                # temp dir which still has the original local names).
                self._fix_tcd_paths(fake_root / ".rockbox", real_device_root=device_root)
                # Verify fix worked
                _tcd4 = fake_root / ".rockbox" / "database_4.tcd"
                if _tcd4.exists():
                    _raw = _tcd4.read_bytes()
                    if b"Start Here." in _raw:
                        self._emit("  WARNING: trailing dot still present after fix!")
                    else:
                        self._emit("  Verified: trailing dot removed from temp file")
                self._emit(f"Copying {len(tcd_files)} .tcd file(s) to device…")
                for tcd in tcd_files:
                    shutil.copy2(str(tcd), str(rbdir / tcd.name))
                    self._emit(f"  Wrote /.rockbox/{tcd.name}")
                # Verify copy worked
                _tcd4dev = rbdir / "database_4.tcd"
                if _tcd4dev.exists():
                    _raw2 = _tcd4dev.read_bytes()
                    if b"Start Here." in _raw2:
                        self._emit("  WARNING: trailing dot still present on device after copy!")
                    else:
                        self._emit("  Verified: device file has no trailing dot")
            finally:
                shutil.rmtree(str(fake_root), ignore_errors=True)
        else:
            # Device mode: run tool directly against device root
            self._emit(f"Mode: device  {scan_root}")
            self._emit("Running database tool directly on device…")
            self._run_cmd([str(tool)], cwd=str(scan_root))
            self._fix_tcd_paths(rbdir)

    def _sanitize_library_for_fat32(self, library_root: Path):
        """
        Walk library_root and rename any file or folder whose name would be
        mangled by FAT32 on the iPod, so local names match device names exactly.

        Fixes:
          - FAT32-illegal chars (\\ : * ? " < > |) → replaced with -
          - Trailing dots and trailing spaces on name stems → stripped

        Deepest paths first so parent renames don't break child paths.
        Collisions resolved with _2, _3 … suffix.
        """
        self._emit(f"── Sanitizing local library for FAT32: {library_root}")

        to_rename: list[Path] = []
        try:
            for fp in sorted(library_root.rglob("*"),
                             key=lambda p: len(p.parts), reverse=True):
                if _needs_sanitize(fp.name) or fp.name != fp.name.rstrip('. '):
                    to_rename.append(fp)
        except Exception as e:
            self._emit(f"  ⚠ Scan error: {e}"); return

        if not to_rename:
            self._emit("  ✓ No FAT32-unsafe names found — library already clean"); return

        self._emit(f"  Found {len(to_rename):,} item(s) to rename…")
        renamed = 0; errors = 0

        for fp in to_rename:
            self._check()
            if not fp.exists():
                continue  # parent already renamed
            # Build clean name: replace illegal chars then strip trailing dots/spaces
            clean = _sanitize_fat32_name(fp.name)
            stem, _, ext = clean.rpartition('.')
            if not stem:
                clean = clean.rstrip('. ') or fp.name
            elif not ext or not ext.strip():
                clean = stem.rstrip('. ') or fp.name
            else:
                clean = f"{stem.rstrip('. ')}.{ext}"
            if clean == fp.name:
                continue
            new_path = fp.parent / clean
            if new_path.exists() and new_path != fp:
                base = new_path.stem; ext = new_path.suffix; n = 2
                while new_path.exists():
                    new_path = fp.parent / f"{base}_{n}{ext}"; n += 1
            try:
                fp.rename(new_path)
                self._emit(f"  ✎ {fp.name!r}  →  {new_path.name!r}")
                renamed += 1
            except Exception as e:
                self._emit(f"  ⚠ Could not rename {fp.name!r}: {e}"); errors += 1

        if errors:
            self._emit(f"  ⚠ Renamed {renamed:,}, {errors} failed — check permissions")
        else:
            self._emit(f"  ✓ Renamed {renamed:,} item(s) — library is FAT32-clean")

    def _fix_tcd_paths(self, rbdir: Path, real_device_root: Optional[Path] = None):
        """
        Post-process database_4.tcd to fix paths that don't match what's actually
        on the FAT32 device. Walks the real device filesystem and matches each
        path component by case-insensitive comparison, using the actual name on disk.
        Handles trailing dots, trailing spaces, and any other character differences
        caused by FAT32 sanitization during sync.

        In local-library mode, pass real_device_root so we resolve against the
        actual FAT32 device instead of the fake temp dir (which still has the
        original local names with dots, special chars, etc.).

        IMPORTANT: modifies raw bytearray directly — do NOT use slices as they are copies.
        """
        tcd = rbdir / "database_4.tcd"
        if not tcd.exists():
            return
        # In local mode, rbdir.parent is the fake temp dir — use the real device
        # root so we walk the actual FAT32 filesystem for name resolution.
        device_root = real_device_root if real_device_root is not None else rbdir.parent

        def find_actual_name(parent: Path, wanted: str) -> str:
            """Find the actual name on disk matching wanted (case-insensitive, fuzzy).
            Handles FAT32 sanitization: trailing dots/spaces stripped, illegal chars
            replaced with '-', as rsync's FAT32 sanitize step does."""
            try:
                entries = os.listdir(str(parent))
            except Exception:
                return wanted
            # Exact match first
            if wanted in entries:
                return wanted
            # Case-insensitive match
            wanted_lower = wanted.lower()
            for e in entries:
                if e.lower() == wanted_lower:
                    return e
            # Strip trailing dots/spaces (FAT32 drops these) and try again
            wanted_stripped = wanted.rstrip('. ')
            if wanted_stripped != wanted:
                if wanted_stripped in entries:
                    return wanted_stripped
                for e in entries:
                    if e.lower() == wanted_stripped.lower():
                        return e
            # Try FAT32 illegal-char substitution (: ? < > | * " \ replaced with -)
            # This mirrors what the rsync sanitize step does on the source.
            import re as _re
            _fat32_illegal = re.compile(r'[\\/:*?"<>|]')
            wanted_sanitized = _fat32_illegal.sub('-', wanted_stripped if wanted_stripped != wanted else wanted)
            if wanted_sanitized != wanted:
                if wanted_sanitized in entries:
                    return wanted_sanitized
                for e in entries:
                    if e.lower() == wanted_sanitized.lower():
                        return e
            # No match found — return original
            return wanted

        try:
            raw   = bytearray(tcd.read_bytes())
            magic, datasize, entry_count = struct.unpack_from('<III', raw, 0)
            if entry_count == 0 or entry_count > 10_000_000:
                self._emit(f"  Warning: database_4.tcd looks invalid (entries={entry_count}) — skipping")
                return
            HDR    = 12
            fixed  = 0
            offset = HDR
            for _ in range(entry_count):
                str_start = offset + 4
                null_pos  = raw.index(0, str_start)
                old_bytes = bytes(raw[str_start:null_pos])
                try:    old_path = old_bytes.decode('utf-8')
                except: old_path = old_bytes.decode('latin-1')

                # Walk the path components and resolve each against actual device
                parts     = old_path.split('/')
                new_parts = []
                current   = device_root
                for i, part in enumerate(parts):
                    is_last = (i == len(parts) - 1)
                    if part == '':
                        new_parts.append(part)
                        continue
                    # Resolve every component (directories AND filename) against
                    # the real device — handles trailing dots/spaces and FAT32
                    # illegal-char substitution for both folder names and files.
                    actual = find_actual_name(current, part)
                    new_parts.append(actual)
                    if not is_last:
                        current = current / actual
                new_path = '/'.join(new_parts)

                if new_path != old_path:
                    new_b = new_path.encode('utf-8')
                    if len(new_b) <= len(old_bytes):
                        pad = len(old_bytes) - len(new_b)
                        raw[str_start : str_start + len(old_bytes)] = new_b + b'\x00' * pad
                        fixed += 1

                entry_len = ((null_pos - str_start + 1) + 3) & ~3
                offset    = str_start + entry_len

            if fixed:
                self._emit(f"  Fixed {fixed} path(s) to match device filesystem")
                tcd.write_bytes(raw)
            else:
                self._emit("  All paths already match device filesystem")
        except Exception as e:
            self._emit(f"  Warning: path fix failed: {e}")

    # ── main run ──────────────────────────────────────────────────────────

    def run(self):
        root  = self.device_root
        rbdir = root / ".rockbox"

        if not rbdir.exists():
            self.finished.emit({"error": f"No .rockbox directory found at {root}"}); return

        scan_root = self.library_root if self.library_root else root

        written      = []
        write_errors = []

        try:
            # ── Check for prerequisites ───────────────────────
            for tool in ("git", "gcc", "make"):
                if not shutil.which(tool):
                    self.finished.emit({"error":
                        f"'{tool}' is not installed.\n"
                        f"Install it with your package manager, e.g.:\n"
                        f"  sudo apt install build-essential git"}); return

            # ── Find or compile the database tool ─────────────
            tool_path = _find_bundled_dbtool()

            if tool_path:
                self._emit(f"✓ Using bundled database tool: {tool_path}")
            elif _RBX_TOOL_PATH.exists():
                tool_path = _RBX_TOOL_PATH
                self._emit(f"✓ Using cached database tool: {tool_path}")
            else:
                self._check()
                src = self._ensure_source()
                self._check()
                tool_path = self._compile_tool(src)

            self._check()

            # ── Build Fresh: wipe existing .tcd files first ───
            if self.mode == self.MODE_INITIALIZE:
                old_tcd = list(rbdir.glob("database*.tcd"))
                if old_tcd:
                    self._emit(f"Removing {len(old_tcd)} existing .tcd file(s)…")
                    for f in old_tcd:
                        try:
                            f.unlink()
                        except Exception as e:
                            self._emit(f"  Warning: could not remove {f.name}: {e}")

            self._check()

            # ── Run the tool against the full library root ────
            # For both modes the tool is run against the complete library.
            # In Update mode the existing .tcd files are left in .rockbox/ so
            # tagcache's own incremental logic picks them up and only processes
            # new/changed entries, writing updated .tcd files in place.
            self._run_tool(tool_path, scan_root, rbdir)

            self._check()

            # ── Collect results ───────────────────────────────
            written = [f.name for f in sorted(rbdir.glob("database*.tcd"))]
            if not written:
                self.finished.emit({"error":
                    "Database tool ran but no .tcd files found on device.\n"
                    "Ensure the device root is correct and contains audio files."}); return

        except InterruptedError:
            if self._proc:
                try: self._proc.kill()
                except Exception: pass
            self.finished.emit({"error": "Cancelled."}); return
        except Exception as e:
            import traceback
            self.finished.emit({"error": f"{e}\n\n{traceback.format_exc()}"}); return

        self.finished.emit({
            "total":        len(written),
            "written":      written,
            "write_errors": write_errors,
            "tag_errors":   0,
            "mode":         self.mode,
            "stats_loaded": 0,
            "new_files":    0,
            "changed_files":0,
            "removed_files":0,
        })


# ─────────────────────────────────────────────────────────────
#  LAST.FM HISTORY FETCHER (for deep stats)
# ─────────────────────────────────────────────────────────────

class LfmWebLoginFetcher(QThread):
    """
    Authenticates with Last.fm or Libre.fm using auth.getMobileSession
    (official API method for desktop apps). Then pulls full scrobble
    history via user.getRecentTracks (200 tracks/page).

    Requires API key + secret (set up in Platforms page).
    """
    progress  = pyqtSignal(str)
    finished  = pyqtSignal(int, str)   # (count, error_or_empty)
    login_ok  = pyqtSignal(str)        # emits username on success

    def __init__(self, username: str, password: str, pages: int = 10,
                 api_key: str = "", api_secret: str = "",
                 platform: str = "Last.fm", api_url: str = ""):
        super().__init__()
        self.username   = username
        self.password   = password
        self.pages      = pages
        self.api_key    = api_key
        self.api_secret = api_secret
        self.platform   = platform
        self.api_url    = api_url or LASTFM_API

    def run(self):
        if not self.api_key or not self.api_secret:
            self.finished.emit(0,
                f"No API credentials found for {self.platform}.\n"
                f"Add your API key and secret in Platforms → {self.platform}, "
                "connect your account, then try again.")
            return
        self._run_api()

    def _run_api(self):
        self.progress.emit(f"Authenticating with {self.platform}…")

        # auth.getMobileSession: uses MD5(username.lower() + MD5(password))
        pw_md5    = hashlib.md5(self.password.encode("utf-8")).hexdigest()
        auth_tok  = hashlib.md5((self.username.lower() + pw_md5).encode("utf-8")).hexdigest()

        params = {
            "method":    "auth.getMobileSession",
            "username":  self.username,
            "authToken": auth_tok,
            "api_key":   self.api_key,
        }
        params["api_sig"] = api_sig(params, self.api_secret)
        params["format"]  = "json"

        try:
            r = requests.post(self.api_url, data=params, timeout=20)
            r.raise_for_status()
            resp = r.json()
        except Exception as e:
            self.finished.emit(0, f"Network error during auth: {e}"); return

        if "error" in resp:
            code = resp.get("error", 0)
            msg  = resp.get("message", "Unknown error")
            if code in (4, 6):
                msg = "Incorrect username or password."
            elif code == 26:
                msg = "API key suspended — get a new one at last.fm/api."
            elif code == 10:
                msg = "Invalid API key. Check the key in Platforms."
            elif code == 14:
                msg = "Unauthorized token — try connecting in Platforms first."
            self.finished.emit(0, f"Login failed ({self.platform}): {msg}"); return

        sk = resp.get("session", {}).get("key", "")
        if not sk:
            self.finished.emit(0, "Login failed: no session key returned."); return

        actual_user = resp.get("session", {}).get("name", self.username)
        self.login_ok.emit(actual_user)
        self.progress.emit(f"Logged in as {actual_user} ✓  Fetching history…")

        # Fetch recent tracks page by page
        inserted = 0
        plat_key = P_LIBREFM if "libre" in self.api_url.lower() else P_LASTFM
        try:
            for page in range(1, self.pages + 1):
                self.progress.emit(f"Fetching page {page}/{self.pages} ({inserted:,} tracks so far)…")
                p2 = {
                    "method":  "user.getRecentTracks",
                    "user":    actual_user,
                    "api_key": self.api_key,
                    "page":    page,
                    "limit":   200,
                    "format":  "json",
                }
                try:
                    r2 = requests.get(self.api_url, params=p2, timeout=30)
                    r2.raise_for_status()
                    data = r2.json()
                except Exception as e:
                    self.progress.emit(f"Page {page} failed: {e} — skipping")
                    continue

                if "error" in data:
                    self.progress.emit(f"API error on page {page}: {data.get('message')} — stopping")
                    break

                tracks = data.get("recenttracks", {}).get("track", [])
                if isinstance(tracks, dict):
                    tracks = [tracks]  # single-result edge case
                if not tracks:
                    break

                for t in tracks:
                    # Skip now-playing (no timestamp)
                    date_info = t.get("date", {})
                    if not date_info:
                        continue
                    uts = date_info.get("uts", "")
                    if not uts:
                        continue
                    try:
                        ts     = int(uts)
                        artist = t.get("artist", {}).get("#text", "").strip()
                        album  = t.get("album",  {}).get("#text", "").strip()
                        title  = t.get("name", "").strip()
                        if artist and title and ts > 0:
                            db_mark_done(artist, album, title, ts, plat_key)
                            inserted += 1
                    except (ValueError, TypeError):
                        continue

                # Check total pages from response
                attr = data.get("recenttracks", {}).get("@attr", {})
                total_pages = int(attr.get("totalPages", 1))
                self.progress.emit(f"Page {page}/{min(self.pages, total_pages)} — {inserted:,} imported")
                if page >= total_pages:
                    break

            self.finished.emit(inserted, "")
        except Exception as e:
            self.finished.emit(inserted, str(e))


# Keep old name as alias for any remaining references
LfmHistoryFetcher = LfmWebLoginFetcher


class LbzHistoryFetcher(QThread):
    """
    Fetches listen history from ListenBrainz using the public API.
    Requires username + user token.
    """
    progress = pyqtSignal(str)
    finished = pyqtSignal(int, str)

    def __init__(self, username: str, token: str, pages: int = 10):
        super().__init__()
        self.username = username
        self.token    = token
        self.pages    = pages

    def run(self):
        inserted = 0
        max_ts   = None  # for pagination (LBZ uses max_ts to get older listens)
        headers  = {"Authorization": f"Token {self.token}"}

        try:
            for page in range(1, self.pages + 1):
                self.progress.emit(f"Fetching page {page}/{self.pages} ({inserted:,} imported)…")
                params = {"count": 100}
                if max_ts is not None:
                    params["max_ts"] = max_ts

                try:
                    r = requests.get(
                        f"{LBZ_API}user/{self.username}/listens",
                        headers=headers, params=params, timeout=30,
                    )
                    if r.status_code == 401:
                        self.finished.emit(0, "Invalid token — check Platforms → ListenBrainz."); return
                    if r.status_code == 404:
                        self.finished.emit(0, f"User '{self.username}' not found on ListenBrainz."); return
                    r.raise_for_status()
                    data = r.json()
                except Exception as e:
                    self.progress.emit(f"Page {page} error: {e}"); break

                listens = data.get("payload", {}).get("listens", [])
                if not listens:
                    break

                for listen in listens:
                    try:
                        ts   = listen.get("listened_at", 0)
                        meta = listen.get("track_metadata", {})
                        artist = meta.get("artist_name", "").strip()
                        title  = meta.get("track_name", "").strip()
                        album  = meta.get("release_name", "").strip()
                        if artist and title and ts > 0:
                            db_mark_done(artist, album, title, ts, P_LISTENBRAINZ)
                            inserted += 1
                        # Track oldest ts for next page
                        if max_ts is None or ts < max_ts:
                            max_ts = ts
                    except Exception:
                        continue

                self.progress.emit(f"Page {page} — {inserted:,} listens imported")
                if len(listens) < 100:
                    break  # reached the end

            self.finished.emit(inserted, "")
        except Exception as e:
            self.finished.emit(inserted, str(e))


# ─────────────────────────────────────────────────────────────
#  ALBUM ART ENGINE
# ─────────────────────────────────────────────────────────────

def _make_pixmap(raw: bytes, size: int = 120) -> Optional[QPixmap]:
    """Must be called on main thread."""
    if not raw:
        return None
    img = QImage()
    if not img.loadFromData(raw):
        return None
    px = QPixmap.fromImage(img)
    if px.isNull():
        return None
    return px.scaled(size, size, Qt.AspectRatioMode.KeepAspectRatio,
                     Qt.TransformationMode.SmoothTransformation)


def _dominant_color(raw: bytes) -> Optional[QColor]:
    """Average mid-tone color from the image, saturation-boosted for tinting."""
    if not raw:
        return None
    try:
        img = QImage()
        if not img.loadFromData(raw):
            return None
        small = img.scaled(16, 16, Qt.AspectRatioMode.IgnoreAspectRatio,
                           Qt.TransformationMode.SmoothTransformation)
        r_sum = g_sum = b_sum = n = 0
        for y in range(small.height()):
            for x in range(small.width()):
                c = QColor(small.pixel(x, y))
                if c.lightness() < 20 or c.lightness() > 235:
                    continue
                r_sum += c.red(); g_sum += c.green(); b_sum += c.blue(); n += 1
        if n == 0:
            return None
        col = QColor(r_sum // n, g_sum // n, b_sum // n)
        h, s, v, a = col.getHsv()
        col.setHsv(h, min(255, int(s * 1.8 + 80)), min(255, int(v * 0.85)), a)
        return col
    except Exception:
        return None


def _blur_pixmap(px: QPixmap, radius: int = 32) -> QPixmap:
    """
    Multi-pass box blur approximation — smoother than single-pass at extreme radii.
    Three successive downscale+upscale passes with decreasing factors avoids
    the blocky pixelation that a single massive downsample produces.
    """
    if px.isNull():
        return px
    w, h = px.width(), px.height()
    factor = max(2, radius // 4)
    result = px
    # Three passes: each halves the detail progressively
    for divisor in (factor, max(2, factor // 2), max(2, factor // 3)):
        small = result.scaled(max(1, w // divisor), max(1, h // divisor),
                              Qt.AspectRatioMode.IgnoreAspectRatio,
                              Qt.TransformationMode.SmoothTransformation)
        result = small.scaled(w, h,
                              Qt.AspectRatioMode.IgnoreAspectRatio,
                              Qt.TransformationMode.SmoothTransformation)
    return result


class PageBackground(QWidget):
    """
    Full-page background layer: blurred album art + dark overlay.
    Sits behind all content via self.lower(). Mouse-transparent.
    """
    def __init__(self, parent=None):
        super().__init__(parent)
        self._raw:   Optional[bytes]   = None
        self._px:    Optional[QPixmap] = None
        self._color: Optional[QColor]  = None
        self.setAttribute(Qt.WidgetAttribute.WA_TransparentForMouseEvents, True)
        self.lower()

    def set_art(self, raw: bytes, color: Optional[QColor] = None):
        self._raw   = raw
        self._color = color
        self._px    = None
        self.update()

    def clear(self):
        self._raw = self._px = self._color = None
        self.update()

    def resizeEvent(self, event):
        self._px = None
        super().resizeEvent(event)

    def paintEvent(self, event):
        painter = QPainter(self)
        t = _current_theme
        painter.fillRect(self.rect(), QColor(t["bg0"]))
        if self._raw:
            if self._px is None:
                img = QImage()
                if img.loadFromData(self._raw):
                    src = QPixmap.fromImage(img)
                    if not src.isNull():
                        filled = src.scaled(
                            self.width(), self.height(),
                            Qt.AspectRatioMode.KeepAspectRatioByExpanding,
                            Qt.TransformationMode.SmoothTransformation)
                        cx = (filled.width()  - self.width())  // 2
                        cy = (filled.height() - self.height()) // 2
                        if cx > 0 or cy > 0:
                            filled = filled.copy(max(0,cx), max(0,cy),
                                                 self.width(), self.height())
                        self._px = _blur_pixmap(filled, radius=80)
            if self._px and not self._px.isNull():
                painter.drawPixmap(0, 0, self._px)
        # Heavy dark scrim — keeps text readable across entire page
        grad = QLinearGradient(0, 0, 0, self.height())
        grad.setColorAt(0.0, QColor(0, 0, 0, 195))
        grad.setColorAt(0.5, QColor(0, 0, 0, 170))
        grad.setColorAt(1.0, QColor(0, 0, 0, 205))
        painter.fillRect(self.rect(), QBrush(grad))
        if self._color and self._raw:
            tint = QColor(self._color); tint.setAlpha(20)
            painter.fillRect(self.rect(), tint)
        painter.end()


# Keep name alias so any leftover references don't crash
HeroBanner = PageBackground



def _id3_art_bytes(data: bytes) -> Optional[bytes]:
    try:
        if data[:3] != b"ID3":
            return None
        version  = data[3]
        sb       = data[6:10]
        tag_size = ((sb[0]&0x7f)<<21|(sb[1]&0x7f)<<14|(sb[2]&0x7f)<<7|(sb[3]&0x7f))
        tag = data[10:10+tag_size]
        i = 0
        while i < len(tag) - 10:
            if version >= 3:
                fid  = tag[i:i+4].decode("latin-1","ignore")
                fsz  = int.from_bytes(tag[i+4:i+8],"big")
                body = tag[i+10:i+10+fsz]
                i   += 10+fsz
            else:
                fid  = tag[i:i+3].decode("latin-1","ignore")
                fsz  = int.from_bytes(tag[i+3:i+6],"big")
                body = tag[i+6:i+6+fsz]
                i   += 6+fsz
            if fsz <= 0:
                break
            if fid in ("APIC","PIC") and body:
                enc  = body[0]; rest = body[1:]
                null = rest.find(b"\x00")
                if null == -1: continue
                rest = rest[null+1+1:]
                if enc in (1,2):
                    j = 0
                    while j < len(rest)-1:
                        if rest[j]==0 and rest[j+1]==0:
                            rest = rest[j+2:]; break
                        j += 1
                else:
                    n2 = rest.find(b"\x00")
                    rest = rest[n2+1:] if n2!=-1 else rest
                if rest: return bytes(rest)
    except Exception:
        pass
    return None


def _flac_art_bytes(data: bytes) -> Optional[bytes]:
    try:
        if data[:4] != b"fLaC":
            return None
        pos = 4
        while pos < len(data)-4:
            h     = data[pos:pos+4]
            btype = h[0] & 0x7F; is_last = h[0] & 0x80
            bsize = int.from_bytes(h[1:4],"big")
            pos  += 4
            bdata = data[pos:pos+bsize]; pos += bsize
            if btype == 6:
                idx  = 4
                mlen = int.from_bytes(bdata[idx:idx+4],"big"); idx+=4+mlen
                dlen = int.from_bytes(bdata[idx:idx+4],"big"); idx+=4+dlen
                idx += 16
                ilen = int.from_bytes(bdata[idx:idx+4],"big"); idx+=4
                return bytes(bdata[idx:idx+ilen])
            if is_last: break
    except Exception:
        pass
    return None


def _m4a_art_bytes(data: bytes) -> Optional[bytes]:
    try:
        idx = data.find(b"covr")
        if idx == -1: return None
        chunk = data[idx:idx+256]
        di    = chunk.find(b"data")
        if di == -1: return None
        img_start = idx + di + 16
        atom_size = int.from_bytes(data[idx+di-4:idx+di], "big")
        img_end   = idx + di - 4 + atom_size
        raw = data[img_start:img_end]
        return bytes(raw) if raw else None
    except Exception:
        pass
    return None


def _ogg_opus_art_bytes(data: bytes) -> Optional[bytes]:
    """Extract embedded cover from Ogg Vorbis / Opus streams (METADATA_BLOCK_PICTURE in Vorbis comments)."""
    import base64
    try:
        # Search for the METADATA_BLOCK_PICTURE key (case-insensitive) in the raw page data
        marker = b"METADATA_BLOCK_PICTURE="
        idx = data.upper().find(marker.upper())
        if idx == -1:
            return None
        # The value runs until a null byte or next \x01 page boundary or end
        start = idx + len(marker)
        end = start
        while end < len(data) and data[end] not in (0, 1, 0xFF):
            end += 1
        b64 = data[start:end]
        # Strip any trailing zero-bytes / whitespace
        b64 = b64.rstrip(b"\x00\r\n ")
        raw = base64.b64decode(b64 + b"==")  # pad to avoid padding errors
        # METADATA_BLOCK_PICTURE structure: 4 bytes type, then lengths + data
        pos = 0
        _type = int.from_bytes(raw[pos:pos+4], "big"); pos += 4
        mime_len = int.from_bytes(raw[pos:pos+4], "big"); pos += 4 + mime_len
        desc_len = int.from_bytes(raw[pos:pos+4], "big"); pos += 4 + desc_len
        pos += 16  # width, height, color_depth, color_count
        data_len = int.from_bytes(raw[pos:pos+4], "big"); pos += 4
        img = raw[pos:pos+data_len]
        return bytes(img) if img else None
    except Exception:
        return None


def _apev2_art_bytes(data: bytes) -> Optional[bytes]:
    """Extract cover from APEv2 tags (used by WavPack, Monkey's Audio, MPC)."""
    try:
        # APEv2 preamble can appear at start or end of file; search both
        preamble = b"APETAGEX"
        idx = data.rfind(preamble)   # prefer footer (end of file)
        if idx == -1:
            idx = data.find(preamble)
        if idx == -1:
            return None
        # Header: 8 preamble + 4 version + 4 tag_size + 4 item_count + 4 flags + 8 reserved
        version  = int.from_bytes(data[idx+8:idx+12],  "little")
        tag_size = int.from_bytes(data[idx+12:idx+16], "little")
        n_items  = int.from_bytes(data[idx+16:idx+20], "little")
        # Items start right after the 32-byte header
        pos = idx + 32
        for _ in range(n_items):
            if pos + 8 > len(data):
                break
            item_size  = int.from_bytes(data[pos:pos+4],   "little")
            item_flags = int.from_bytes(data[pos+4:pos+8], "little")
            pos += 8
            # Key ends at null byte
            key_end = data.find(b"\x00", pos)
            if key_end == -1:
                break
            key = data[pos:key_end].decode("ascii", errors="ignore").lower()
            pos = key_end + 1
            value = data[pos:pos+item_size]
            pos += item_size
            if key in ("cover art (front)", "cover art (back)", "cover art (other)"):
                # Value is null-terminated filename followed by binary image data
                null = value.find(b"\x00")
                img = value[null+1:] if null != -1 else value
                return bytes(img) if img else None
    except Exception:
        pass
    return None


def _extract_art_bytes(path: Path) -> Optional[bytes]:
    ext = path.suffix.lower()
    if ext not in AUDIO_EXTS:
        return None
    try:
        # Read up to 1 MB — FLAC PICTURE blocks can sit well past 256 KB
        with open(path, "rb") as f:
            header = f.read(1_048_576)
        if ext == ".mp3":
            return _id3_art_bytes(header)
        elif ext == ".flac":
            return _flac_art_bytes(header)
        elif ext in (".m4a", ".aac", ".mp4"):
            return _m4a_art_bytes(header)
        elif ext in (".ogg", ".opus"):
            return _ogg_opus_art_bytes(header)
        elif ext in (".wv", ".ape", ".mpc"):
            # APEv2 footer is at the end of the file — read the last 64 KB too
            try:
                fsize = path.stat().st_size
                if fsize > 1_048_576:
                    with open(path, "rb") as f:
                        f.seek(max(0, fsize - 65536))
                        tail = f.read()
                    combined = header + tail
                else:
                    combined = header
                return _apev2_art_bytes(combined)
            except OSError:
                return _apev2_art_bytes(header)
    except (OSError, PermissionError):
        pass
    return None


class ArtFetcher(QThread):
    """
    Fetches album art for a single artist/album.
    Strategy (fastest first, stops at first hit):
      1. Direct path guess: device_root/Artist/Album/cover.jpg (and variants)
      2. Embedded tag in first audio file found in that folder
      3. Last.fm API (only if api_key provided)
    Never walks the whole device — stays in the expected folder only.
    """
    result  = pyqtSignal(str, str, bytes)  # artist, album, raw bytes (may be empty)
    # Only cache successful (non-empty) results so transient failures are retried.
    # Bounded to avoid unbounded memory growth.
    _cache: dict = {}
    _CACHE_MAX = 256

    def __init__(self, artist: str, album: str, log_paths: list, tracks: list, api_key: str = ""):
        super().__init__()
        self.artist    = artist
        self.album     = album
        self.log_paths = log_paths
        self.api_key   = api_key
        # Only keep tracks for this artist/album
        self.tracks    = [t for t in tracks
                          if t.artist.lower() == artist.lower()
                          and t.album.lower()  == album.lower()]

    def run(self):
        key = (self.artist.lower(), self.album.lower())
        if key in ArtFetcher._cache:
            self.result.emit(self.artist, self.album, ArtFetcher._cache[key])
            return
        raw = self._try_folder_art() or self._try_embedded() or self._try_lastfm() or b""
        # Only cache hits; failures are retried next time so stale empty results don't stick.
        if raw:
            if len(ArtFetcher._cache) >= ArtFetcher._CACHE_MAX:
                ArtFetcher._cache.pop(next(iter(ArtFetcher._cache)))
            ArtFetcher._cache[key] = raw
        self.result.emit(self.artist, self.album, raw)

    def _device_root(self) -> Optional[Path]:
        for lp in self.log_paths:
            if lp.exists():
                p = lp.parent
                # Step up past Music/ subdir if needed
                if p.name.lower() in ("music", ".rockbox"):
                    p = p.parent
                return p
        return None

    def _candidate_dirs(self) -> list[Path]:
        """
        Return a short list of the most likely directories to find art in,
        without walking the full device. Tries direct name matches first.
        """
        root = self._device_root()
        if not root:
            return []

        al  = self.artist.lower()
        alb = self.album.lower()
        candidates: list[Path] = []

        # 1. Try exact / partial name match one level deep
        try:
            for d in root.iterdir():
                if not d.is_dir(): continue
                dl = d.name.lower()
                if al in dl or dl in al:
                    # Inside artist folder, look for album subfolder
                    try:
                        matched_sub = False
                        for sub in d.iterdir():
                            if not sub.is_dir(): continue
                            sl = sub.name.lower()
                            if alb in sl or sl in alb:
                                candidates.insert(0, sub)   # best match first
                                matched_sub = True
                        if not matched_sub:
                            candidates.append(d)            # fallback: artist dir itself
                    except (OSError, PermissionError):
                        candidates.append(d)
        except (OSError, PermissionError):
            pass

        # 2. Append root itself as last resort (flat library layout)
        if root not in candidates:
            candidates.append(root)

        return candidates[:6]   # never check more than 6 dirs

    def _try_folder_art(self) -> Optional[bytes]:
        art_names = ("cover", "folder", "album", "front", "artwork", "AlbumArt")
        art_exts  = (".jpg", ".jpeg", ".png", ".bmp", ".webp")
        for dirpath in self._candidate_dirs():
            for name in art_names:
                for ext in art_exts:
                    p = dirpath / (name + ext)
                    if p.exists():
                        try:
                            return p.read_bytes()
                        except OSError:
                            pass
        return None

    def _try_embedded(self) -> Optional[bytes]:
        for dirpath in self._candidate_dirs():
            try:
                # Try up to 5 audio files per directory — the first one found
                # might not have embedded art even if others in the same album do.
                tried = 0
                for f in sorted(dirpath.iterdir()):
                    if not (f.is_file() and f.suffix.lower() in AUDIO_EXTS):
                        continue
                    raw = _extract_art_bytes(f)
                    if raw:
                        return raw
                    tried += 1
                    if tried >= 5:
                        break
            except (OSError, PermissionError):
                pass
        return None

    def _try_lastfm(self) -> Optional[bytes]:
        if not self.api_key:
            return None
        try:
            r = requests.get(LASTFM_API, params={
                "method": "album.getInfo", "artist": self.artist,
                "album": self.album, "api_key": self.api_key, "format": "json",
            }, timeout=6)
            images = r.json().get("album", {}).get("image", [])
            url = ""
            for size in ("extralarge", "large", "medium"):
                for img in images:
                    if img.get("size") == size and img.get("#text"):
                        url = img["#text"]; break
                if url: break
            if not url:
                return None
            return requests.get(url, timeout=6).content
        except Exception:
            pass
        return None


# ─────────────────────────────────────────────────────────────
#  REUSABLE WIDGETS
# ─────────────────────────────────────────────────────────────

def Spacer(h=True):
    s = QWidget()
    s.setSizePolicy(
        QSizePolicy.Policy.Expanding if h  else QSizePolicy.Policy.Minimum,
        QSizePolicy.Policy.Expanding if not h else QSizePolicy.Policy.Minimum,
    )
    return s

def HDivider():
    f = QFrame(); f.setFrameShape(QFrame.Shape.HLine); f.setFixedHeight(1); return f

def VDivider():
    f = QFrame(); f.setFrameShape(QFrame.Shape.VLine); f.setFixedWidth(1); return f

def SectionLabel(text: str) -> QLabel:
    lbl = QLabel(text.upper()); lbl.setObjectName("sectiontitle"); return lbl


class StatusDot(QLabel):
    def __init__(self, color="#555"):
        super().__init__("●")
        self.set_color(color)
    def set_color(self, color: str):
        self.setStyleSheet(f"color: {color}; background: transparent; font-size: 11px;")


class StatCard(QWidget):
    def __init__(self, label: str, icon: str = ""):
        super().__init__()
        self.setObjectName("card")
        self.setMinimumWidth(110)
        vb = QVBoxLayout(self)
        vb.setContentsMargins(20, 18, 20, 18)
        vb.setSpacing(6)
        if icon:
            ic = QLabel(icon)
            ic.setStyleSheet(f"color: {_current_theme['txt2']}; font-size: 16px; background: transparent;")
            vb.addWidget(ic)
        self._num = QLabel("—"); self._num.setObjectName("statnum")
        self._lbl = QLabel(label.upper()); self._lbl.setObjectName("statlabel")
        vb.addWidget(self._num)
        vb.addWidget(self._lbl)

    def set_value(self, v: str):
        self._num.setText(v)


class NavButton(QPushButton):
    def __init__(self, text: str, icon_text: str = ""):
        display = f"{icon_text}  {text}" if icon_text else text
        super().__init__(display)
        self.setObjectName("navbtn")

    def set_active(self, v: bool):
        self.setObjectName("navbtn_active" if v else "navbtn")
        self.style().unpolish(self); self.style().polish(self)


class Banner(QLabel):
    def __init__(self, text="", severity="info"):
        super().__init__(text)
        self.setWordWrap(True)
        self.setMinimumHeight(36)
        self.set(text, severity)

    def set(self, text: str, severity: str = "info"):
        t = _current_theme
        colors = {
            "success": (t["success"], t["success"]+"18"),
            "danger":  (t["danger"],  t["danger"]+"12"),
            "warning": (t["warning"], t["warning"]+"14"),
            "info":    (t["accent"],  t["accentlo"]),
            "muted":   (t["txt2"],    t["bg3"]),
        }
        fg, bg = colors.get(severity, colors["muted"])
        self.setText(text)
        self.setStyleSheet(
            f"border: 1px solid {fg}44; border-radius: 8px; padding: 9px 13px; "
            f"background: {bg}; color: rgba(255,255,255,0.55); font-size: 12px;"
        )
        self.setVisible(bool(text))


class PlatformBadge(QLabel):
    COLORS = {P_LASTFM: "#d4222a", P_LIBREFM: "#4d9e4d", P_LISTENBRAINZ: "#eb743b"}
    def __init__(self, platform: str):
        super().__init__(platform)
        c = self.COLORS.get(platform, "#888")
        self.setStyleSheet(
            f"color: {c}; background: {c}15; border: 1px solid {c}44; "
            f"border-radius: 5px; padding: 3px 10px; font-size: 12px; font-weight: 700;"
        )


class ColorSwatch(QPushButton):
    color_changed = pyqtSignal(str)
    def __init__(self, color: str):
        super().__init__()
        self._color = color
        self.setFixedSize(36, 28)
        self._update_style()
        self.clicked.connect(self._pick)

    def _update_style(self):
        self.setStyleSheet(
            f"background: {self._color}; border: 2px solid rgba(255,255,255,0.12); border-radius: 6px;"
        )
    def _pick(self):
        dlg = QColorDialog(QColor(self._color), self)
        if dlg.exec() == QColorDialog.DialogCode.Accepted:
            self._color = dlg.currentColor().name()
            self._update_style(); self.color_changed.emit(self._color)
    def set_color(self, color: str):
        self._color = color; self._update_style()
    def color(self) -> str:
        return self._color


class ScalableArtLabel(QWidget):
    """
    Album art widget that scales the pixmap to fill its actual size.
    Unlike AlbumArtLabel which is fixed-size, this expands with the layout.
    Has the same fade-in animation and placeholder as AlbumArtLabel.
    """
    def __init__(self, parent=None):
        super().__init__(parent)
        self._raw:  Optional[bytes]   = None
        self._img:  Optional[QImage]  = None   # decoded once, scaled in paintEvent
        self._opacity  = QGraphicsOpacityEffect(self)
        self.setGraphicsEffect(self._opacity)
        self._opacity.setOpacity(1.0)
        self._anim = QPropertyAnimation(self._opacity, b"opacity", self)
        self._anim.setDuration(280)
        self._anim.setStartValue(0.0); self._anim.setEndValue(1.0)
        self._anim.setEasingCurve(QEasingCurve.Type.OutCubic)
        self.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)
        self.setMinimumSize(80, 80)

    def hasHeightForWidth(self) -> bool:
        return False

    def set_art(self, raw: bytes):
        if not raw:
            self._raw = self._img = None
            self.update(); return
        img = QImage()
        if img.loadFromData(raw):
            self._raw = raw
            self._img = img
            self._anim.stop(); self._anim.start()
        else:
            self._raw = self._img = None
        self.update()

    def _show_placeholder(self):
        self._raw = self._img = None
        self.update()

    def paintEvent(self, event):
        painter = QPainter(self)
        painter.setRenderHint(QPainter.RenderHint.SmoothPixmapTransform)
        t = _current_theme
        painter.fillRect(self.rect(), QColor(t["bg3"]))
        if self._img:
            iw, ih = self._img.width(), self._img.height()
            ww, wh = self.width(), self.height()
            # Fill + center-crop: no empty space, no distortion
            scale = max(ww / iw, wh / ih)
            sw, sh = int(iw * scale), int(ih * scale)
            sx = (sw - ww) // 2
            sy = (sh - wh) // 2
            scaled = self._img.scaled(sw, sh,
                                      Qt.AspectRatioMode.IgnoreAspectRatio,
                                      Qt.TransformationMode.SmoothTransformation)
            painter.drawImage(0, 0, scaled, sx, sy, ww, wh)
        else:
            painter.setPen(QColor(t["txt2"]))
            f = painter.font(); f.setPointSize(20); painter.setFont(f)
            painter.drawText(self.rect(), Qt.AlignmentFlag.AlignCenter, "♪")
        painter.end()


class AlbumArtLabel(QLabel):
    SIZE = 120

    def __init__(self, size: int = 120):
        super().__init__()
        self.SIZE = size
        self.setFixedSize(size, size)
        self.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self._opacity  = QGraphicsOpacityEffect(self)
        self.setGraphicsEffect(self._opacity)
        self._opacity.setOpacity(1.0)
        self._anim = QPropertyAnimation(self._opacity, b"opacity", self)
        self._anim.setDuration(280)
        self._anim.setStartValue(0.0)
        self._anim.setEndValue(1.0)
        self._anim.setEasingCurve(QEasingCurve.Type.OutCubic)
        self._show_placeholder()

    def _show_placeholder(self):
        t = _current_theme
        self.setStyleSheet(
            f"background: rgba(255,255,255,0.08); border: 1px solid rgba(255,255,255,0.09); "
            f"border-radius: 4px; color: rgba(255,255,255,0.35); font-size: 22px;"
        )
        self.setText("♪"); self.setPixmap(QPixmap())

    def set_art(self, raw: bytes):
        px = _make_pixmap(raw, self.SIZE) if raw else None
        if px:
            self.setText("")
            self.setStyleSheet("background: transparent; border: none; border-radius: 4px;")
            self.setPixmap(px)
            self._anim.stop()
            self._anim.start()
        else:
            self._show_placeholder()


# ─────────────────────────────────────────────────────────────
#  HEATMAP WIDGET  (GitHub-style contribution calendar)
# ─────────────────────────────────────────────────────────────

class HeatmapWidget(QWidget):
    """Renders a 53-week × 7-day heatmap of play counts."""

    def __init__(self, parent=None):
        super().__init__(parent)
        self._daily: dict[str, int] = {}   # "YYYY-MM-DD" -> count
        self._max = 1
        self._cells: list[tuple] = []      # (QRect, date_str, count) for tooltip
        self.setMinimumHeight(120)
        self.setMouseTracking(True)

    def set_data(self, daily: dict[str, int]):
        self._daily = daily
        self._max   = max(daily.values(), default=1)
        self._cells = []
        self.update()

    def mouseMoveEvent(self, event):
        pos = event.pos()
        for rect, ds, count in self._cells:
            if rect.contains(pos):
                try:
                    label = datetime.strptime(ds, "%Y-%m-%d").strftime("%d %b %Y")
                except Exception:
                    label = ds
                tip = f"{label}  ·  {count} play{'s' if count != 1 else ''}" if count else f"{label}  ·  no plays"
                QToolTip.showText(event.globalPosition().toPoint(), tip, self)
                return
        QToolTip.hideText()

    def paintEvent(self, event):
        painter = QPainter(self)
        painter.setRenderHint(QPainter.RenderHint.Antialiasing)

        t       = _current_theme
        bg_col  = QColor(t["bg3"])
        hi_col  = QColor(t["accent"])
        txt_col = QColor(t["txt2"])
        painter.fillRect(self.rect(), QColor(t["bg2"]))

        W, H    = self.width(), self.height()
        cell    = min((W - 60) // 53, (H - 30) // 7)
        cell    = max(cell, 8)
        gap     = 2
        off_x   = 40
        off_y   = 20

        painter.setPen(txt_col)
        f = painter.font(); f.setPointSize(9); painter.setFont(f)

        today   = datetime.now().date()
        start   = today - timedelta(weeks=52)
        start   = start - timedelta(days=start.weekday())

        day_labels = ["M","","W","","F","","S"]
        for di, dl in enumerate(day_labels):
            if dl:
                painter.drawText(off_x - 22, off_y + di*(cell+gap) + cell, dl)

        cells = []
        last_month = ""
        col = 0
        cur = start
        while cur <= today:
            week_start = cur
            for row in range(7):
                d = week_start + timedelta(days=row)
                if d > today:
                    break
                ds    = d.strftime("%Y-%m-%d")
                count = self._daily.get(ds, 0)
                x = off_x + col * (cell + gap)
                y = off_y + row * (cell + gap)

                if count == 0:
                    c = bg_col
                else:
                    ratio = min(count / self._max, 1.0)
                    c = QColor(
                        int(bg_col.red()   + (hi_col.red()   - bg_col.red())   * ratio),
                        int(bg_col.green() + (hi_col.green() - bg_col.green()) * ratio),
                        int(bg_col.blue()  + (hi_col.blue()  - bg_col.blue())  * ratio),
                    )
                painter.setBrush(c)
                painter.setPen(Qt.PenStyle.NoPen)
                cw = cell - gap
                painter.drawRoundedRect(x, y, cw, cw, 2, 2)
                cells.append((QRect(x, y, cw, cw), ds, count))

                if row == 0:
                    month_str = d.strftime("%b")
                    if month_str != last_month:
                        painter.setPen(txt_col)
                        painter.drawText(x, off_y - 5, month_str)
                        last_month = month_str
                        painter.setPen(Qt.PenStyle.NoPen)

            cur += timedelta(weeks=1)
            col += 1

        self._cells = cells
        painter.end()


# ─────────────────────────────────────────────────────────────
#  BAR CHART WIDGET  (monthly trend)
# ─────────────────────────────────────────────────────────────

class BarChartWidget(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        self._data: list[tuple[str,int]] = []
        self.setMinimumHeight(160)

    def set_data(self, monthly: list[tuple[str,int]]):
        self._data = monthly[-24:]
        self.update()

    def paintEvent(self, event):
        painter = QPainter(self)
        painter.setRenderHint(QPainter.RenderHint.Antialiasing)
        t = _current_theme
        painter.fillRect(self.rect(), QColor(t["bg2"]))

        if not self._data:
            painter.setPen(QColor(t["txt2"]))
            painter.drawText(self.rect(), Qt.AlignmentFlag.AlignCenter, "No data")
            painter.end(); return

        W, H    = self.width(), self.height()
        pad_l, pad_r, pad_t, pad_b = 44, 12, 12, 28
        plot_w  = W - pad_l - pad_r
        plot_h  = H - pad_t - pad_b
        n       = len(self._data)
        mx      = max(v for _, v in self._data) or 1
        bar_w   = max(plot_w // n - 3, 4)
        step    = plot_w / n

        accent  = QColor(t["accent"])
        accent2 = QColor(t["accent2"])
        txt_col = QColor(t["txt2"])

        painter.setPen(txt_col)
        f = painter.font(); f.setPointSize(9); painter.setFont(f)

        for i, (label, val) in enumerate(self._data):
            bh   = int(plot_h * val / mx)
            x    = pad_l + int(i * step)
            y    = pad_t + plot_h - bh
            grad = QLinearGradient(x, y, x, y + bh)
            grad.setColorAt(0, accent2); grad.setColorAt(1, accent)
            painter.setBrush(QBrush(grad))
            painter.setPen(Qt.PenStyle.NoPen)
            painter.drawRoundedRect(x, y, bar_w, bh, 2, 2)

            # X label every 3 months
            if i % 3 == 0 and len(label) >= 7:
                painter.setPen(txt_col)
                painter.drawText(x - 6, H - 4, label[2:])

        # Y axis gridlines
        for frac in (0.25, 0.5, 0.75, 1.0):
            y   = pad_t + int(plot_h * (1 - frac))
            val = int(mx * frac)
            painter.setPen(QPen(QColor(t["border"]), 1, Qt.PenStyle.DotLine))
            painter.drawLine(pad_l, y, W - pad_r, y)
            painter.setPen(txt_col)
            painter.drawText(0, y + 4, pad_l - 4, 20, Qt.AlignmentFlag.AlignRight, str(val))

        painter.end()


# ─────────────────────────────────────────────────────────────
#  PAGE: SCROBBLE
# ─────────────────────────────────────────────────────────────

class LogLoader(QThread):
    """Parse a .scrobbler.log off the main thread."""
    done  = pyqtSignal(str, list, int)  # path as str, tracks, skipped_future_count
    error = pyqtSignal(str, str)        # path as str, message

    def __init__(self, path: Path):
        super().__init__()
        self.path = path

    def run(self):
        try:
            tracks, skipped_future = parse_log(self.path)
            self.done.emit(str(self.path), tracks, skipped_future)
        except Exception as e:
            self.error.emit(str(self.path), str(e))

class ScrobblePage(QWidget):
    status_changed = pyqtSignal()
    art_ready = pyqtSignal(object)  # emitted when bg art is fetched (bytes) or cleared (None)

    def __init__(self, conf_ref: list, get_platform_fn, parent=None):
        super().__init__(parent)
        self.conf_ref        = conf_ref
        self.get_platform    = get_platform_fn
        self.tracks: list[Track]    = []
        self.log_paths: list[Path]  = []
        self._worker: Optional[Worker] = None
        self._log_loaders: list[LogLoader] = []
        self._submitted_set: set    = set()
        self._build()

    def conf(self) -> dict:
        return self.conf_ref[0]

    def _build(self):
        t = _current_theme
        root = QVBoxLayout(self)
        root.setContentsMargins(0, 0, 0, 0)
        root.setSpacing(0)

        # ── Blurred background art ────────────────────────────────────
        self._bg = PageBackground(self)
        self._bg.setGeometry(self.rect())

        # ── Glass header bar ──────────────────────────────────────────
        hdr = QWidget()
        hdr.setFixedHeight(52)
        hdr.setStyleSheet("background: rgba(5,7,11,0.65); border-bottom: 1px solid rgba(255,255,255,0.07);")
        hdr_hl = QHBoxLayout(hdr)
        hdr_hl.setContentsMargins(28, 0, 20, 0); hdr_hl.setSpacing(10)

        title = QLabel("Scrobble")
        title.setStyleSheet("color:#fff; font-size:18px; font-weight:700; letter-spacing:-0.3px; background:transparent;")
        hdr_hl.addWidget(title)

        sub = QLabel("Load a .scrobbler.log and submit to your scrobbling platform.")
        sub.setStyleSheet("color:rgba(255,255,255,0.35); font-size:12px; background:transparent;")
        hdr_hl.addWidget(sub); hdr_hl.addStretch()

        _opt_ss = ("QCheckBox { color:rgba(255,255,255,0.55); font-size:12px; background:transparent; spacing:5px; }"
                   "QCheckBox::indicator { width:14px; height:14px; border-radius:3px; border:1px solid rgba(255,255,255,0.20); background:rgba(255,255,255,0.06); }"
                   f"QCheckBox::indicator:checked {{ background:{tok('accent')}; border-color:{tok('accent')}; }}")
        self._skip_confirm_cb = QCheckBox("Skip confirm")
        self._skip_confirm_cb.setToolTip("Submit without asking for confirmation each time")
        self._skip_confirm_cb.setStyleSheet(_opt_ss)
        self._dry_cb = QCheckBox("Dry run")
        self._dry_cb.setToolTip("Dry run: marks tracks as previewed only — nothing is actually sent to any platform")
        self._dry_cb.stateChanged.connect(self._refresh_queue_label)
        self._dry_cb.setStyleSheet(_opt_ss)
        hdr_hl.addWidget(self._skip_confirm_cb); hdr_hl.addSpacing(4); hdr_hl.addWidget(self._dry_cb)
        root.addWidget(hdr)

        # ── Log source card ───────────────────────────────────────────
        body = QWidget(); body.setStyleSheet("background:transparent;")
        body_vb = QVBoxLayout(body); body_vb.setContentsMargins(28, 18, 28, 0); body_vb.setSpacing(14)
        root.addWidget(body)

        log_card = QWidget()
        log_card.setObjectName("scrobble_log_card")
        log_card.setStyleSheet(
            "QWidget#scrobble_log_card { background:rgba(10,12,16,0.45); border-radius:10px; border:none; }"
            "QWidget#scrobble_log_card QLabel { background:transparent; border:none; border-radius:0; }"
            "QWidget#scrobble_log_card QCheckBox { background:transparent; border:none; border-radius:0; }")
        lc = QVBoxLayout(log_card); lc.setContentsMargins(18, 14, 18, 14); lc.setSpacing(10)

        log_top = QHBoxLayout()
        log_icon = QLabel("◈")
        log_icon.setStyleSheet(f"color:{tok('accent')}; font-size:14px; background:transparent; border:none;")
        log_title = QLabel("Log File")
        log_title.setStyleSheet("color:#fff; font-weight:700; font-size:13px; background:transparent; border:none;")
        log_top.addWidget(log_icon); log_top.addWidget(log_title); log_top.addStretch()

        _glass_btn = ("QPushButton { background:rgba(255,255,255,0.07); color:rgba(255,255,255,0.75); "
                      "border:1px solid rgba(255,255,255,0.12); border-radius:5px; font-size:12px; "
                      "padding:0 12px; min-height:28px; max-height:28px; font-weight:500; }"
                      "QPushButton:hover { background:rgba(255,255,255,0.13); border-color:rgba(255,255,255,0.25); color:#fff; }")
        _danger_btn = ("QPushButton { background:rgba(192,64,64,0.15); color:rgba(210,100,100,0.85); "
                       "border:1px solid rgba(192,64,64,0.35); border-radius:5px; font-size:12px; "
                       "padding:0 12px; min-height:28px; max-height:28px; font-weight:500; }"
                       "QPushButton:hover { background:rgba(192,64,64,0.28); border-color:rgba(192,64,64,0.7); }")
        self._auto_btn   = QPushButton("⟳  Auto-detect")
        self._browse_btn = QPushButton("Browse…")
        self._clear_btn  = QPushButton("Clear")
        self._auto_btn.setStyleSheet(_glass_btn)
        self._browse_btn.setStyleSheet(_glass_btn)
        self._clear_btn.setStyleSheet(_danger_btn)
        self._auto_btn.clicked.connect(self._auto_detect)
        self._browse_btn.clicked.connect(self._browse_log)
        self._clear_btn.clicked.connect(self._clear_logs)
        for b in [self._auto_btn, self._browse_btn, self._clear_btn]:
            log_top.addWidget(b)
        lc.addLayout(log_top)

        self._log_label = QLabel("No log loaded  ·  Connect your device then Auto-detect, or browse manually.")
        self._log_label.setStyleSheet("color:rgba(255,255,255,0.38); font-size:12px; background:transparent; border:none;")
        self._log_label.setWordWrap(True)
        lc.addWidget(self._log_label)

        self._archive_cb = QCheckBox("Archive log after submitting (renames to .scrobbler_YYYYMMDD.log)")
        self._archive_cb.setStyleSheet(
            "QCheckBox { color:rgba(255,255,255,0.45); font-size:11px; background:transparent; spacing:5px; }"
            "QCheckBox::indicator { width:13px; height:13px; border-radius:3px; border:1px solid rgba(255,255,255,0.18); background:rgba(255,255,255,0.05); }"
            f"QCheckBox::indicator:checked {{ background:{tok('accent')}; border-color:{tok('accent')}; }}")
        lc.addWidget(self._archive_cb)
        body_vb.addWidget(log_card)

        # ── Queue toolbar ─────────────────────────────────────────────
        tbl_bar = QWidget()
        tbl_bar.setStyleSheet("background:transparent;")
        tbl_hl = QHBoxLayout(tbl_bar); tbl_hl.setContentsMargins(0,0,0,0); tbl_hl.setSpacing(8)
        self._queue_label = QLabel("Queue empty")
        self._queue_label.setStyleSheet("color:rgba(255,255,255,0.45); font-size:12px; background:transparent;")
        tbl_hl.addWidget(self._queue_label); tbl_hl.addStretch()
        self._select_all_btn = QPushButton("Select all"); self._deselect_btn = QPushButton("Deselect all")
        self._select_all_btn.setStyleSheet(_glass_btn); self._deselect_btn.setStyleSheet(_glass_btn)
        self._select_all_btn.clicked.connect(self._select_all)
        self._deselect_btn.clicked.connect(self._deselect_all)
        tbl_hl.addWidget(self._select_all_btn); tbl_hl.addWidget(self._deselect_btn)
        body_vb.addWidget(tbl_bar)

        # ── Track table ───────────────────────────────────────────────
        self._table = QTableWidget(0, 7)
        self._table.setHorizontalHeaderLabels(["✓","ARTIST","TITLE","ALBUM","DUR",
            f"LOCAL TIME ({self._local_tz_label()})","UTC TIME"])
        self._table.setAlternatingRowColors(True)
        self._table.setSelectionMode(QAbstractItemView.SelectionMode.NoSelection)
        self._table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self._table.verticalHeader().setVisible(False)
        self._table.verticalHeader().setDefaultSectionSize(34)
        self._table.setShowGrid(False)
        self._table.setSortingEnabled(True)
        self._table.setStyleSheet(
            "QTableWidget { background:rgba(10,12,16,0.40); alternate-background-color:rgba(255,255,255,0.022);"
            " border:none; border-radius:10px; outline:none; font-size:13px;"
            " selection-background-color: rgba(255,255,255,0.08); selection-color: #fff; }"
            "QTableWidget::item { padding:4px 14px; border-bottom:1px solid rgba(255,255,255,0.035); color:rgba(255,255,255,0.85); }"
            "QTableWidget::item:selected { background:rgba(255,255,255,0.08); color:#fff; border:none; }"
            "QTableWidget::item:hover { background:transparent; border:none; }"
            "QHeaderView { border:none; background:transparent; }"
            "QHeaderView::section { background:rgba(255,255,255,0.03); color:rgba(255,255,255,0.30);"
            " padding:6px 14px; border:none; border-bottom:1px solid rgba(255,255,255,0.06);"
            " font-size:10px; font-weight:700; letter-spacing:1.3px; }"
        )
        self._table.setMouseTracking(False)
        self._table.viewport().setMouseTracking(False)
        self._table.viewport().setAttribute(Qt.WidgetAttribute.WA_Hover, False)
        h = self._table.horizontalHeader()
        h.setSectionResizeMode(0, QHeaderView.ResizeMode.Fixed); self._table.setColumnWidth(0, 40)
        for c in [1,2,3]: h.setSectionResizeMode(c, QHeaderView.ResizeMode.Stretch)
        for c in [4,5,6]: h.setSectionResizeMode(c, QHeaderView.ResizeMode.ResizeToContents)
        self._table.cellClicked.connect(self._on_cell_click)
        body_vb.addWidget(self._table, stretch=1)

        # ── Submit footer ─────────────────────────────────────────────
        foot = QWidget()
        foot.setStyleSheet("background:transparent;")
        foot_hl = QHBoxLayout(foot); foot_hl.setContentsMargins(0, 10, 0, 18); foot_hl.setSpacing(14)

        left_col = QVBoxLayout(); left_col.setSpacing(5)
        self._status_label = QLabel("")
        self._status_label.setStyleSheet("color:rgba(255,255,255,0.50); font-size:12px; background:transparent;")
        self._progress = QProgressBar()
        self._progress.setVisible(False)
        self._progress.setFixedHeight(4)
        self._progress.setStyleSheet(
            "QProgressBar { background:rgba(255,255,255,0.08); border:none; border-radius:2px; }"
            f"QProgressBar::chunk {{ background:{tok('accent')}; border-radius:2px; }}")
        left_col.addStretch(); left_col.addWidget(self._status_label); left_col.addWidget(self._progress)
        foot_hl.addLayout(left_col, stretch=1)

        self._submit_btn = QPushButton("Submit")
        self._submit_btn.setFixedHeight(42)
        self._submit_btn.setMinimumWidth(170)
        self._submit_btn.setStyleSheet(
            f"QPushButton {{ background:{tok('accent')}; color:#0a0a0a; border:none; border-radius:8px;"
            " font-weight:700; font-size:14px; padding:0 22px; }"
            f"QPushButton:hover {{ background:{tok('accent2')}; }}"
            "QPushButton:pressed { background:#b87516; }"
            f"QPushButton:disabled {{ background:{tok('accent')}40; color:rgba(10,10,10,0.4); }}")
        self._submit_btn.setToolTip("Submit selected tracks  (Ctrl+Return)")
        self._submit_btn.clicked.connect(self._submit)
        _sc = QShortcut(QKeySequence("Ctrl+Return"), self)
        _sc.activated.connect(self._submit)
        foot_hl.addWidget(self._submit_btn)
        body_vb.addWidget(foot)

    def resizeEvent(self, event):
        super().resizeEvent(event)
        if hasattr(self, '_bg'):
            self._bg.setGeometry(self.rect())

    def _local_tz_label(self) -> str:
        try:   return datetime.now().astimezone().strftime("%Z") or "local"
        except Exception: return "local"

    def _auto_detect(self):
        found = find_logs()
        if not found:
            QMessageBox.information(self, "Nothing found",
                "No .scrobbler.log found.\n\nMake sure your player is connected and mounted.")
            return
        self._clear_logs()
        for p in found: self._load_log(p)

    def _browse_log(self):
        path, _ = QFileDialog.getOpenFileName(self, "Open log file", str(Path.home()),
            "Log files (*.log);;All files (*)")
        if path: self._load_log(Path(path))

    def _clear_logs(self):
        self.log_paths.clear(); self.tracks.clear()
        self._table.setRowCount(0)
        self._log_label.setText("No log loaded.")
        ArtFetcher._cache.clear()   # device changed — stale art gone
        self._bg.clear()
        self.art_ready.emit(None)   # tell other pages to clear their bg too
        self._refresh_queue_label()

    def _load_log(self, path: Path):
        if path in self.log_paths:
            return
        self._log_label.setText(f"Loading {path.name}…")
        self._auto_btn.setEnabled(False)
        self._browse_btn.setEnabled(False)
        loader = LogLoader(path)
        loader.done.connect(self._on_log_loaded)
        loader.error.connect(self._on_log_error)
        self._log_loaders.append(loader)
        loader.start()

    def _on_log_loaded(self, path_str: str, tracks: list, skipped_future: int):
        self._auto_btn.setEnabled(True)
        self._browse_btn.setEnabled(True)
        path = Path(path_str)
        if not tracks:
            msg = "No valid tracks found in:\n" + str(path)
            if skipped_future:
                msg += (f"\n\n{skipped_future} entr{'y' if skipped_future == 1 else 'ies'} "
                        "had timestamps more than 1 hour in the future and were skipped.\n"
                        "This usually means the device clock is set incorrectly.")
            QMessageBox.warning(self, "Empty log", msg)
            return
        self.log_paths.append(path)
        self.tracks.extend(tracks)
        self.tracks.sort(key=lambda t: t.timestamp)
        self._rebuild_table()
        self._fetch_bg_art()
        paths_txt = "  ·  ".join(p.name for p in self.log_paths)
        total   = sum(1 for t in self.tracks if t.listened)
        skipped = sum(1 for t in self.tracks if t.skipped)
        self._log_label.setText(f"{paths_txt}  ·  {total} listened, {skipped} skipped")
        self._table.setHorizontalHeaderItem(5,
            QTableWidgetItem(f"LOCAL TIME ({self._local_tz_label()})"))
        if skipped_future:
            QMessageBox.warning(
                self, "Future-dated tracks skipped",
                f"{skipped_future} track{'s' if skipped_future != 1 else ''} in {path.name} "
                f"had timestamps more than 1 hour in the future and were skipped.\n\n"
                "This usually means your device's clock is set incorrectly. "
                "Fix the clock and re-scrobble the log if tracks are missing."
            )

    def _fetch_bg_art(self):
        """Fetch art for the top album in the loaded log and set it as the blurred background."""
        lt = [t for t in self.tracks if t.listened]
        if not lt:
            return
        from collections import Counter
        top = Counter((t.artist, t.album) for t in lt).most_common(1)
        if not top:
            return
        (artist, album), _ = top[0]
        key = (artist.lower(), album.lower())
        # Check cache first
        if key in ArtFetcher._cache:
            raw = ArtFetcher._cache[key]
            if raw:
                self._bg.set_art(raw, _dominant_color(raw))
                self.art_ready.emit(raw)
            return
        log_paths = self.get_log_paths() if hasattr(self, 'get_log_paths') else list(self.log_paths)
        api_key   = self.conf_ref[0].get("api_key", "")
        f = ArtFetcher(artist, album, log_paths, lt, api_key=api_key)
        def _on_result(art, alb, raw):
            if raw:
                QTimer.singleShot(0, lambda: (
                    self._bg.set_art(raw, _dominant_color(raw)),
                    self.art_ready.emit(raw)
                ))
        f.result.connect(_on_result)
        self._log_loaders.append(f)   # keep reference alive
        f.start()

    def _on_log_error(self, path_str: str, msg: str):
        self._auto_btn.setEnabled(True)
        self._browse_btn.setEnabled(True)
        self._log_label.setText("Load failed.")
        QMessageBox.critical(self, "Load error", f"Failed to load {Path(path_str).name}:\n{msg}")

    def _rebuild_table(self):
        plat = self.get_platform()
        rows = [t for t in self.tracks if t.listened]
        done_set = db_batch_done(rows, plat)
        self._submitted_set = done_set
        submitted = {id(t): (t.artist, t.album, t.title, t.timestamp) in done_set for t in rows}
        for t in rows:
            if submitted[id(t)]: t.enabled = False

        self._table.setSortingEnabled(False)
        self._table.setUpdatesEnabled(False)
        self._table.setRowCount(len(rows))
        muted   = QColor(_current_theme["txt2"])
        success = QColor(_current_theme["success"])

        for row, t in enumerate(rows):
            done = submitted[id(t)]
            cb = QCheckBox()
            cb.setChecked(t.enabled and not done)
            cb.setStyleSheet(
                "QCheckBox { background:transparent; margin-left:9px; }"
                "QCheckBox::indicator { width:14px; height:14px; border-radius:3px;"
                " border:1px solid rgba(255,255,255,0.22); background:rgba(255,255,255,0.06); }"
                f"QCheckBox::indicator:checked {{ background:{tok('accent')}; border-color:{tok('accent')}; }}"
                f"QCheckBox::indicator:checked:hover {{ background:{tok('accent')}; border-color:{tok('accent')}; }}"
                "QCheckBox::indicator:hover { border-color:rgba(255,255,255,0.22); background:rgba(255,255,255,0.06); }")
            cb.stateChanged.connect(lambda st, tr=t, c=cb: self._on_cb_changed(tr, c))
            self._table.setCellWidget(row, 0, cb)

            local_str = t.local_dt.strftime("%d %b  %H:%M")
            utc_str   = t.utc_dt.strftime("%d %b  %H:%M")

            for col, val in enumerate([t.artist, t.title, t.album, t.duration_str, local_str, utc_str], 1):
                item = QTableWidgetItem(val)
                if done:
                    item.setForeground(muted)
                    # ✓ badge on the artist column
                    if col == 1:
                        item = QTableWidgetItem(f"✓  {val}")
                        item.setForeground(success)
                    item.setToolTip(f"{val}  ✓ already scrobbled to {plat}")
                else:
                    item.setToolTip(val)
                self._table.setItem(row, col, item)

        self._table.setUpdatesEnabled(True)
        self._table.setSortingEnabled(True)
        self._refresh_queue_label()

    def _on_cb_changed(self, track: Track, cb: QCheckBox):
        track.enabled = cb.isChecked(); self._refresh_queue_label()

    def _on_cell_click(self, row, col):
        if col != 0:
            w = self._table.cellWidget(row, 0)
            if w: w.setChecked(not w.isChecked())

    def _select_all(self):
        for i in range(self._table.rowCount()):
            w = self._table.cellWidget(i, 0)
            if w: w.setChecked(True)

    def _deselect_all(self):
        for i in range(self._table.rowCount()):
            w = self._table.cellWidget(i, 0)
            if w: w.setChecked(False)

    def _refresh_queue_label(self):
        plat     = self.get_platform()
        total    = sum(1 for t in self.tracks if t.listened)
        selected = sum(1 for t in self.tracks if t.listened and t.enabled)
        done_set = getattr(self, "_submitted_set", set())
        already  = sum(1 for t in self.tracks
                       if t.listened and (t.artist, t.album, t.title, t.timestamp) in done_set)
        if not self.tracks:
            self._queue_label.setText("Queue empty")
            self._submit_btn.setText("Submit")
        else:
            dry = "  ·  DRY RUN" if self._dry_cb.isChecked() else ""
            self._queue_label.setText(
                f"{total} tracks  ·  {selected} selected  ·  {already} already scrobbled{dry}")
            if self._dry_cb.isChecked():
                self._submit_btn.setText(f"Preview {selected} tracks")
            else:
                self._submit_btn.setText(f"Submit {selected} to {plat}" if selected else "Submit")

    def _submit(self):
        plat = self.get_platform()
        dry  = self._dry_cb.isChecked()
        if not dry:
            if plat in (P_LASTFM, P_LIBREFM):
                if not load_session(plat):
                    QMessageBox.warning(self, "Not logged in", f"Log in to {plat} first.")
                    return
            elif plat == P_LISTENBRAINZ:
                if not self.conf().get("lbz_token"):
                    QMessageBox.warning(self, "No token", "Add your ListenBrainz token first.")
                    return
        candidates = [t for t in self.tracks if t.listened and t.enabled]
        done_set   = db_batch_done(candidates, plat)
        to_submit  = [t for t in candidates if (t.artist, t.album, t.title, t.timestamp) not in done_set]
        if not to_submit:
            QMessageBox.information(self, "Nothing to submit", "All selected tracks already scrobbled.")
            return
        if not self._skip_confirm_cb.isChecked():
            mode = "DRY RUN" if dry else f"send to {plat}"
            ans  = QMessageBox.question(self, "Confirm", f"Submit {len(to_submit)} track(s)? ({mode})",
                QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No)
            if ans != QMessageBox.StandardButton.Yes: return

        self._progress.setVisible(True)
        self._progress.setMaximum(len(to_submit))
        self._progress.setValue(0)
        self._submit_btn.setEnabled(False)
        self._status_label.setText(f"Submitting 0 / {len(to_submit)}…")
        self._ok_count = self._fail_count = 0
        self._worker = Worker(to_submit, plat, self.conf(), dry)
        self._worker.progress.connect(self._on_progress)
        self._worker.track_done.connect(self._on_track_done)
        self._worker.finished.connect(self._on_finished)
        self._worker.start()

    def _on_progress(self, cur, total):
        self._progress.setValue(cur)
        self._status_label.setText(f"Submitting {cur} / {total}…")

    def _on_track_done(self, track, ok, msg):
        if ok: self._ok_count += 1
        else:  self._fail_count += 1

    def _on_finished(self, ok, fail):
        self._submit_btn.setEnabled(True)
        self._progress.setVisible(False)
        dry = self._dry_cb.isChecked()
        if self._archive_cb.isChecked() and not dry:
            today = datetime.now().strftime("%Y%m%d")
            for lp in self.log_paths:
                if lp.exists():
                    try:
                        dest = lp.with_name(f".scrobbler_{today}.log")
                        n = 1
                        while dest.exists():
                            dest = lp.with_name(f".scrobbler_{today}_{n}.log"); n+=1
                        lp.rename(dest)
                    except Exception: pass
        if fail:
            self._status_label.setText(f"Done: {ok} submitted · {fail} failed")
        else:
            verb = "previewed" if dry else "scrobbled"
            self._status_label.setText(f"✓  {ok} tracks {verb} successfully")
        if not dry: self._rebuild_table()
        self.status_changed.emit()

    def refresh_for_platform(self):
        if self.tracks: self._rebuild_table()



# ─────────────────────────────────────────────────────────────
#  PAGE: STATISTICS  (local log stats + album art)
# ─────────────────────────────────────────────────────────────

class StatsPage(QWidget):
    def __init__(self, get_tracks_fn, get_log_paths_fn, conf_ref: list, parent=None):
        super().__init__(parent)
        self.get_tracks    = get_tracks_fn
        self.get_log_paths = get_log_paths_fn
        self.conf_ref      = conf_ref
        self._fetchers: list[ArtFetcher] = []
        self._art_queue: list[ArtFetcher] = []
        self._art_labels: dict[tuple, AlbumArtLabel] = {}
        self._build()

    def _lfm_key(self) -> str:
        return self.conf_ref[0].get("api_key", "")

    def _build(self):
        root = QVBoxLayout(self)
        root.setContentsMargins(0, 0, 0, 0)
        root.setSpacing(0)

        # ── Full-page blurred background ──────────────────────────────
        # PageBackground is a WA_TransparentForMouseEvents child that paints
        # behind everything. We keep a reference to resize it with the page.
        self._bg = PageBackground(self)
        self._bg.setGeometry(self.rect())

        # ── Compact header bar (title + controls) ────────────────────
        hdr = QWidget()
        hdr.setStyleSheet("background: transparent;")
        hdr.setFixedHeight(52)
        hdr_hl = QHBoxLayout(hdr)
        hdr_hl.setContentsMargins(28, 0, 20, 0); hdr_hl.setSpacing(10)

        title = QLabel("Statistics")
        title.setStyleSheet("color:#fff; font-size:18px; font-weight:700; "
                            "letter-spacing:-0.3px; background:transparent;")
        hdr_hl.addWidget(title); hdr_hl.addStretch()

        gap_lbl = QLabel("Session gap")
        gap_lbl.setStyleSheet("color:rgba(255,255,255,0.55); font-size:12px; background:transparent;")
        self._gap_spin = QSpinBox()
        self._gap_spin.setRange(1, 180); self._gap_spin.setValue(20)
        self._gap_spin.setSuffix(" min"); self._gap_spin.setFixedWidth(88)
        self._gap_spin.valueChanged.connect(self.refresh)
        _glass = ("background:rgba(0,0,0,0.40); color:#fff; "
                  "border:1px solid rgba(255,255,255,0.18); border-radius:4px; padding:4px 6px;")
        self._gap_spin.setStyleSheet(f"QSpinBox {{ {_glass} }} "
                                     "QSpinBox::up-button, QSpinBox::down-button { width:16px; }")
        _btn_ss = (f"QPushButton {{ {_glass} font-size:12px; padding:5px 14px; }}"
                   " QPushButton:hover { background:rgba(255,255,255,0.14); }")
        refresh_btn = QPushButton("Refresh"); refresh_btn.setStyleSheet(_btn_ss)
        refresh_btn.clicked.connect(self.refresh)
        exp_btn = QPushButton("Export..."); exp_btn.setStyleSheet(_btn_ss)
        exp_btn.clicked.connect(self._export)

        hdr_hl.addWidget(gap_lbl); hdr_hl.addWidget(self._gap_spin)
        hdr_hl.addSpacing(4); hdr_hl.addWidget(refresh_btn); hdr_hl.addWidget(exp_btn)
        root.addWidget(hdr)

        # ── Stat cards row ────────────────────────────────────────────
        cards_wrap = QWidget(); cards_wrap.setStyleSheet("background:transparent;")
        cards_hl = QHBoxLayout(cards_wrap)
        cards_hl.setContentsMargins(28, 0, 28, 0); cards_hl.setSpacing(10)
        self._card_total    = self._make_stat_card("TOTAL",    "TRACKS")
        self._card_listened = self._make_stat_card("LISTENED", "TRACKS")
        self._card_skipped  = self._make_stat_card("SKIPPED",  "TRACKS")
        self._card_sessions = self._make_stat_card("SESSIONS", "")
        self._card_time     = self._make_stat_card("PLAY",     "TIME")
        for c in [self._card_total, self._card_listened, self._card_skipped,
                  self._card_sessions, self._card_time]:
            cards_hl.addWidget(c, stretch=1)
        root.addWidget(cards_wrap)

        # ── Scrollable content ────────────────────────────────────────
        scroll = QScrollArea(); scroll.setWidgetResizable(True)
        scroll.setFrameShape(QFrame.Shape.NoFrame)
        scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)
        scroll.setStyleSheet("QScrollArea { background: transparent; }"
                             "QScrollArea > QWidget > QWidget { background: transparent; }")
        body = QWidget(); body.setStyleSheet("background: transparent;")
        body_vb = QVBoxLayout(body)
        body_vb.setContentsMargins(28, 16, 28, 28); body_vb.setSpacing(20)
        scroll.setWidget(body)
        root.addWidget(scroll, stretch=1)

        # ── Top Albums ────────────────────────────────────────────────
        sec_lbl = QLabel("TOP ALBUMS"); sec_lbl.setObjectName("sectiontitle")
        body_vb.addWidget(sec_lbl)

        # Fixed-size flashcards in a left-aligned HBox (max 5)
        self._albums_inner  = QWidget(); self._albums_inner.setStyleSheet("background:transparent;")
        self._albums_layout = QHBoxLayout(self._albums_inner)
        self._albums_layout.setSpacing(10); self._albums_layout.setContentsMargins(0,0,0,0)
        self._albums_layout.setAlignment(Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignTop)
        body_vb.addWidget(self._albums_inner)

        # ── Top Artists + Tracks ──────────────────────────────────────
        bottom_row = QHBoxLayout(); bottom_row.setSpacing(14)
        self._top_artists_w = self._make_ranked_list("TOP ARTISTS")
        self._top_tracks_w  = self._make_ranked_list("TOP TRACKS")
        bottom_row.addWidget(self._top_artists_w, stretch=1)
        bottom_row.addWidget(self._top_tracks_w,  stretch=1)
        body_vb.addLayout(bottom_row)

    def resizeEvent(self, event):
        super().resizeEvent(event)
        self._bg.setGeometry(self.rect())

    def _make_stat_card(self, line1: str, line2: str) -> QWidget:
        """Compact glass-style stat card. No emoji, no per-card color."""
        t = _current_theme
        w = QWidget()
        w.setMinimumHeight(72)
        w.setMaximumHeight(90)
        w.setStyleSheet(
            "QWidget { background: rgba(10,12,16,0.55);"
            " border: none;"
            " border-radius: 7px; }")
        vb = QVBoxLayout(w); vb.setContentsMargins(14, 10, 14, 10); vb.setSpacing(1)
        num = QLabel("—")
        num.setStyleSheet(f"color:{t['accent']}; font-size:22px; font-weight:700;"
                          " letter-spacing:-0.5px; background:transparent;")
        lbl_text = f"{line1} {line2}".strip()
        lbl = QLabel(lbl_text)
        lbl.setStyleSheet("color:rgba(255,255,255,0.45); font-size:9px; font-weight:600;"
                          " letter-spacing:1.1px; background:transparent;")
        vb.addStretch(); vb.addWidget(num); vb.addWidget(lbl)
        w._num = num
        return w

    def _make_ranked_list(self, title: str) -> QWidget:
        w = QWidget()
        w.setMinimumHeight(280)
        w.setStyleSheet("QWidget { background:rgba(10,12,16,0.55);"
                        " border:none; border-radius:8px; }")
        vb = QVBoxLayout(w); vb.setContentsMargins(0, 0, 0, 0); vb.setSpacing(0)
        hdr = QWidget()
        hdr.setStyleSheet("background:rgba(255,255,255,0.05); border-top-left-radius:8px;"
                          " border-top-right-radius:8px;")
        hdr.setFixedHeight(38)
        hdr_hl = QHBoxLayout(hdr); hdr_hl.setContentsMargins(16, 0, 16, 0)
        lbl = QLabel(title)
        lbl.setStyleSheet("color:rgba(255,255,255,0.40); font-size:10px; font-weight:600;"
                          " letter-spacing:1.5px; background:transparent;")
        hdr_hl.addWidget(lbl)
        vb.addWidget(hdr)
        lst = QListWidget()
        lst.setSelectionMode(QAbstractItemView.SelectionMode.NoSelection)
        lst.setFrameShape(QFrame.Shape.NoFrame)
        lst.setStyleSheet("QListWidget { background:transparent; border:none;"
                          " border-bottom-left-radius:8px; border-bottom-right-radius:8px; }"
                          "QListWidget::item { padding:0px; border:none; }"
                          "QListWidget::item:hover { background:rgba(255,255,255,0.05); }")
        vb.addWidget(lst, stretch=1)
        w._lst = lst
        return w

    def _make_list(self, title: str) -> QWidget:
        return self._make_ranked_list(title)

    def _clear_albums(self):
        while self._albums_layout.count():
            item = self._albums_layout.takeAt(0)
            if item.widget(): item.widget().deleteLater()
        self._art_labels.clear()
        self._bg.clear()

    def _make_album_card(self, artist: str, album: str, count: int, rank: int) -> tuple:
        """Square art card: art is always 1:1, compact info strip below."""
        t = _current_theme

        # Fixed flashcard: 160px wide art square + 62px info strip
        CARD_W = 160
        ART_H  = 160
        INFO_H = 62

        card = QWidget()
        card.setFixedSize(CARD_W, ART_H + INFO_H)
        card.setStyleSheet(
            "QWidget { background:rgba(10,12,16,0.60); border-radius:8px; border:none; }"
            "QWidget:hover { background:rgba(255,255,255,0.07); }")
        vb = QVBoxLayout(card)
        vb.setContentsMargins(0, 0, 0, 0); vb.setSpacing(0)

        # Art: fixed square
        art = ScalableArtLabel()
        art.setFixedSize(CARD_W, ART_H)
        art.setSizePolicy(QSizePolicy.Policy.Fixed, QSizePolicy.Policy.Fixed)
        art._card = card
        vb.addWidget(art)

        # Info strip — fixed height directly below the square art
        info = QWidget()
        info.setFixedSize(CARD_W, INFO_H)
        info.setStyleSheet("background:transparent;")
        info_vb = QVBoxLayout(info)
        info_vb.setContentsMargins(10, 6, 10, 8); info_vb.setSpacing(1)

        meta_row = QHBoxLayout(); meta_row.setSpacing(4)
        rank_lbl = QLabel(f"#{rank}")
        rank_lbl.setStyleSheet(
            f"color:{t['accent']}; font-size:10px; font-weight:700; background:transparent;")
        cnt_lbl = QLabel(f"{count} play{'s' if count != 1 else ''}")
        cnt_lbl.setStyleSheet(
            "color:rgba(255,255,255,0.40); font-size:10px; background:transparent;")
        meta_row.addWidget(rank_lbl); meta_row.addStretch(); meta_row.addWidget(cnt_lbl)
        info_vb.addLayout(meta_row)

        albl = QLabel(album); albl.setWordWrap(False)
        fm = QFontMetrics(albl.font())
        albl.setText(fm.elidedText(album, Qt.TextElideMode.ElideRight, CARD_W - 20))
        albl.setStyleSheet(
            "font-weight:700; font-size:12px; color:#fff; background:transparent;")
        arlbl = QLabel(artist); arlbl.setWordWrap(False)
        arlbl.setText(fm.elidedText(artist, Qt.TextElideMode.ElideRight, CARD_W - 20))
        arlbl.setStyleSheet(
            "font-size:11px; color:rgba(255,255,255,0.50); background:transparent;")
        info_vb.addWidget(albl); info_vb.addWidget(arlbl)
        vb.addWidget(info)
        return card, art

    def _add_album_card(self, artist: str, album: str, count: int, index: int = 0):
        key = (artist.lower(), album.lower())
        card, art = self._make_album_card(artist, album, count, rank=index + 1)
        self._albums_layout.addWidget(card)
        self._art_labels[key] = art

    _ART_CONCURRENCY = 2   # low enough to avoid disk I/O storms

    def _fetch_art_for(self, artist: str, album: str, all_tracks: list, is_top: bool = False):
        key = (artist.lower(), album.lower())
        if is_top:
            self._top_key = key
        if key in ArtFetcher._cache:
            raw = ArtFetcher._cache[key]
            lbl = self._art_labels.get(key)
            if lbl:
                lbl.set_art(raw)
            if is_top and raw:
                self._bg.set_art(raw, _dominant_color(raw))
            return
        f = ArtFetcher(artist, album, self.get_log_paths(), all_tracks, api_key=self._lfm_key())
        f.result.connect(self._on_art_result)
        f.finished.connect(self._on_fetcher_done)
        self._fetchers.append(f); self._art_queue.append(f)
        self._drain_art_queue()

    def _drain_art_queue(self):
        running = sum(1 for f in self._fetchers if f.isRunning())
        while self._art_queue and running < self._ART_CONCURRENCY:
            self._art_queue.pop(0).start(); running += 1

    def _on_fetcher_done(self):
        # Remove finished fetchers so they can be GC'd cleanly
        self._fetchers = [f for f in self._fetchers if not f.isFinished()]
        self._drain_art_queue()

    def _on_art_result(self, artist: str, album: str, raw: bytes):
        key = (artist.lower(), album.lower())
        def _apply():
            lbl = self._art_labels.get(key)
            if lbl:
                lbl.set_art(raw)
            # Update page background with art from whichever is stored as is_top
            if key == getattr(self, "_top_key", None) and raw:
                self._bg.set_art(raw, _dominant_color(raw))
        QTimer.singleShot(0, _apply)

    def refresh(self, tracks=None):
        # Stop any in-progress fetchers (but keep the cache across refreshes)
        for f in self._fetchers:
            if f.isRunning():
                f.quit()
                f.wait(200)   # give it 200ms to exit cleanly
        self._fetchers.clear()
        self._art_queue.clear()

        tracks   = tracks or self.get_tracks()
        total    = len(tracks)
        listened = sum(1 for t in tracks if t.listened)
        skipped  = sum(1 for t in tracks if t.skipped)
        sessions = len(detect_sessions(tracks, self._gap_spin.value())) if tracks else 0
        secs     = sum(t.length for t in tracks if t.listened and t.length > 0)

        self._card_total._num.setText(str(total)    if total else "—")
        self._card_listened._num.setText(str(listened) if total else "—")
        self._card_skipped._num.setText(str(skipped)   if total else "—")
        self._card_sessions._num.setText(str(sessions)  if sessions else "—")
        if secs >= 3600:   self._card_time._num.setText(f"{secs//3600}h {(secs%3600)//60}m")
        elif secs > 0:     self._card_time._num.setText(f"{secs//60}m")
        else:              self._card_time._num.setText("—")

        lt = [t for t in tracks if t.listened]
        self._clear_albums()
        top_albums = Counter((t.artist, t.album) for t in lt).most_common(10)
        # Max 5 fixed-size flashcards, left-aligned, no stretching
        for i, ((artist, album), cnt) in enumerate(top_albums[:5]):
            self._add_album_card(artist, album, cnt, index=i)
            self._fetch_art_for(artist, album, lt, is_top=(i == 0))

        t = _current_theme

        def _fill_ranked(widget, items):
            lst = widget._lst; lst.clear()
            if not items:
                return
            max_cnt = items[0][1] if items else 1
            for rank, (label, cnt) in enumerate(items):
                item_w = QWidget(); item_w.setStyleSheet("background: transparent;")
                hl = QHBoxLayout(item_w); hl.setContentsMargins(14, 5, 14, 5); hl.setSpacing(10)

                rank_lbl = QLabel(f"{rank+1:2d}"); rank_lbl.setFixedWidth(22)
                rank_lbl.setStyleSheet("color:rgba(255,255,255,0.35); font-size:11px;"
                                       " font-weight:600; background:transparent;")
                hl.addWidget(rank_lbl)

                bar_col = QVBoxLayout(); bar_col.setSpacing(2)
                name_lbl = QLabel(label)
                name_lbl.setStyleSheet("color:rgba(255,255,255,0.85); font-size:12px; background:transparent;")
                name_lbl.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Preferred)

                bar_bg = QWidget(); bar_bg.setFixedHeight(3)
                bar_bg.setStyleSheet("background:rgba(255,255,255,0.10); border-radius:2px;")
                bar_fill = QWidget(bar_bg); bar_fill.setFixedHeight(3)
                bar_fill.setStyleSheet(f"background:{t['accent']}; border-radius:2px;")
                fill_pct = cnt / max_cnt if max_cnt > 0 else 0
                bar_fill._pct = fill_pct
                def _resize_bar(e, bg=bar_bg, fill=bar_fill):
                    fill.setFixedWidth(max(2, int(bg.width() * fill._pct)))
                    # must return None for sip
                bar_bg.resizeEvent = _resize_bar

                bar_col.addWidget(name_lbl); bar_col.addWidget(bar_bg)
                hl.addLayout(bar_col, stretch=1)

                cnt_lbl = QLabel(str(cnt)); cnt_lbl.setFixedWidth(36)
                cnt_lbl.setAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
                cnt_lbl.setStyleSheet(f"color:{t['accent']}; font-size:11px;"
                                      " font-weight:600; background:transparent;")
                hl.addWidget(cnt_lbl)

                li = QListWidgetItem(lst)
                li.setSizeHint(QSize(0, 44))
                lst.addItem(li)
                lst.setItemWidget(li, item_w)

        top_artists = Counter(t.artist for t in lt).most_common(15)
        top_tracks  = Counter(f"{t.artist} — {t.title}" for t in lt).most_common(15)
        _fill_ranked(self._top_artists_w, top_artists)
        _fill_ranked(self._top_tracks_w,  top_tracks)

    def _export(self):
        tracks = self.get_tracks()
        if not tracks:
            QMessageBox.warning(self, "No data", "Load a log file first."); return
        path, _ = QFileDialog.getSaveFileName(self, "Export", str(Path.home()/"scrobbox_export"),
            "CSV (*.csv);;JSON (*.json);;Text (*.txt)")
        if not path: return
        try:
            if path.endswith(".csv"):
                with open(path, "w", newline="", encoding="utf-8") as f:
                    w = csv.writer(f)
                    w.writerow(["Artist","Album","Title","Rating","Duration(s)","Local Time","UTC Time","MBID"])
                    for t in tracks:
                        w.writerow([t.artist, t.album, t.title, t.rating, t.length,
                            t.local_dt.strftime("%Y-%m-%d %H:%M:%S")+f" {t.local_tz_name}",
                            t.utc_dt.strftime("%Y-%m-%d %H:%M:%S")+" UTC", t.mbid or ""])
            elif path.endswith(".json"):
                data = [{"artist":t.artist,"album":t.album,"title":t.title,"rating":t.rating,
                    "length_sec":t.length,"utc_timestamp":t.utc_ts,"mbid":t.mbid} for t in tracks]
                Path(path).write_text(json.dumps(data, indent=2, ensure_ascii=False))
            else:
                listened = [t for t in tracks if t.listened]
                secs = sum(t.length for t in listened if t.length > 0)
                lines = [f"Scrobbox Export  {datetime.now():%Y-%m-%d %H:%M}",
                    f"Tracks: {len(tracks)}  Listened: {len(listened)}",
                    f"Total time: {secs//3600}h {(secs%3600)//60}m", "", "── Top Artists ──",
                ] + [f"  {n:4d}  {a}" for a, n in Counter(t.artist for t in listened).most_common(20)]
                Path(path).write_text("\n".join(lines), encoding="utf-8")
            QMessageBox.information(self, "Exported", f"Saved to:\n{path}")
        except Exception as e:
            QMessageBox.critical(self, "Export failed", str(e))



# ─────────────────────────────────────────────────────────────
#  PAGE: HISTORY
# ─────────────────────────────────────────────────────────────

class HistoryLoader(QThread):
    done = pyqtSignal(list)
    def __init__(self, limit): super().__init__(); self.limit = limit
    def run(self): self.done.emit(db_history(self.limit))


class HistoryPage(QWidget):
    PAGE_SIZE = 200

    def __init__(self, parent=None):
        super().__init__(parent)
        self._all_rows:  list = []
        self._filtered:  list = []
        self._page = 0
        self._loader: Optional[HistoryLoader] = None
        self._build()

    def _build(self):
        root = QVBoxLayout(self)
        root.setContentsMargins(0, 0, 0, 0)
        root.setSpacing(0)

        # ── Blurred background art ────────────────────────────────────
        self._bg = PageBackground(self)
        self._bg.setGeometry(self.rect())

        # ── Glass header ──────────────────────────────────────────────
        hdr = QWidget()
        hdr.setFixedHeight(52)
        hdr.setStyleSheet("background:rgba(5,7,11,0.65); border-bottom:1px solid rgba(255,255,255,0.07);")
        hdr_hl = QHBoxLayout(hdr)
        hdr_hl.setContentsMargins(28, 0, 20, 0); hdr_hl.setSpacing(12)

        title = QLabel("History")
        title.setStyleSheet("color:#fff; font-size:18px; font-weight:700; letter-spacing:-0.3px; background:transparent;")
        hdr_hl.addWidget(title); hdr_hl.addStretch()

        self._search = QLineEdit()
        self._search.setPlaceholderText("Search artists, titles, albums…")
        self._search.setFixedWidth(240)
        self._search.setFixedHeight(30)
        self._search.setStyleSheet(
            "QLineEdit { background:rgba(255,255,255,0.07); border:1px solid rgba(255,255,255,0.12);"
            " border-radius:6px; padding:0 10px; color:#fff; font-size:12px; }"
            "QLineEdit:focus { border-color:rgba(200,134,26,0.7); background:rgba(255,255,255,0.10); }")
        self._search.textChanged.connect(self._apply_filter)

        _glass_btn = ("QPushButton { background:rgba(255,255,255,0.07); color:rgba(255,255,255,0.75);"
                      " border:1px solid rgba(255,255,255,0.12); border-radius:5px; font-size:12px;"
                      " padding:0 12px; min-height:28px; max-height:28px; font-weight:500; }"
                      "QPushButton:hover { background:rgba(255,255,255,0.13); color:#fff; border-color:rgba(255,255,255,0.25); }")
        _danger_btn = ("QPushButton { background:rgba(192,64,64,0.15); color:rgba(210,100,100,0.85);"
                       " border:1px solid rgba(192,64,64,0.35); border-radius:5px; font-size:12px;"
                       " padding:0 12px; min-height:28px; max-height:28px; font-weight:500; }"
                       "QPushButton:hover { background:rgba(192,64,64,0.28); border-color:rgba(192,64,64,0.7); }")

        refresh_btn = QPushButton("↻  Refresh")
        refresh_btn.setStyleSheet(_glass_btn)
        refresh_btn.clicked.connect(self.refresh)
        hdr_hl.addWidget(self._search); hdr_hl.addWidget(refresh_btn)
        root.addWidget(hdr)

        # ── Body ──────────────────────────────────────────────────────
        body = QWidget()
        body.setObjectName("hist_body")
        body.setStyleSheet("QWidget#hist_body { background:transparent; border:none; }")
        body_vb = QVBoxLayout(body); body_vb.setContentsMargins(28, 16, 28, 0); body_vb.setSpacing(10)
        root.addWidget(body, stretch=1)

        # Table
        self._tbl = QTableWidget(0, 6)
        self._tbl.setHorizontalHeaderLabels(["ARTIST","TITLE","ALBUM","PLATFORM","LISTENED","SUBMITTED"])
        self._tbl.setAlternatingRowColors(True)
        self._tbl.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self._tbl.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self._tbl.verticalHeader().setVisible(False)
        self._tbl.verticalHeader().setDefaultSectionSize(34)
        self._tbl.setShowGrid(False); self._tbl.setSortingEnabled(True)
        self._tbl.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        self._tbl.customContextMenuRequested.connect(self._show_context_menu)
        self._tbl.setStyleSheet(
            "QTableWidget { background:rgba(10,12,16,0.40); alternate-background-color:rgba(255,255,255,0.022);"
            " border:none; border-radius:10px; outline:none; font-size:13px; }"
            "QTableWidget::item { padding:4px 14px; border-bottom:1px solid rgba(255,255,255,0.035); color:rgba(255,255,255,0.85); }"
            "QTableWidget::item:selected { background:rgba(200,134,26,0.15); color:#fff; border:none; }"
            "QTableWidget::item:hover { background:transparent; border:none; }"
            "QHeaderView { border:none; background:transparent; }"
            "QHeaderView::section { background:rgba(255,255,255,0.03); color:rgba(255,255,255,0.30);"
            " padding:6px 14px; border:none; border-bottom:1px solid rgba(255,255,255,0.06);"
            " font-size:10px; font-weight:700; letter-spacing:1.3px; }"
        )
        h = self._tbl.horizontalHeader()
        for c in [0,1,2]: h.setSectionResizeMode(c, QHeaderView.ResizeMode.Stretch)
        for c in [3,4,5]: h.setSectionResizeMode(c, QHeaderView.ResizeMode.ResizeToContents)
        body_vb.addWidget(self._tbl, stretch=1)

        self._loading_lbl = QLabel("Loading…")
        self._loading_lbl.setStyleSheet("color:rgba(255,255,255,0.35); font-size:13px; background:transparent;")
        self._loading_lbl.setAlignment(Qt.AlignmentFlag.AlignCenter); self._loading_lbl.setVisible(False)
        body_vb.addWidget(self._loading_lbl)

        # ── Footer toolbar ────────────────────────────────────────────
        foot = QWidget(); foot.setStyleSheet("background:transparent;")
        foot_hl = QHBoxLayout(foot); foot_hl.setContentsMargins(0, 4, 0, 18); foot_hl.setSpacing(8)

        self._count_lbl = QLabel("")
        self._count_lbl.setStyleSheet("color:rgba(255,255,255,0.35); font-size:12px; background:transparent;")
        foot_hl.addWidget(self._count_lbl); foot_hl.addStretch()

        self._save_history_chk = QCheckBox("Save history")
        self._save_history_chk.setToolTip("When unchecked, scrobbles won't be saved to local history")
        save_hist = load_conf().get("save_history", True)
        self._save_history_chk.setChecked(save_hist)
        self._save_history_chk.toggled.connect(self._on_save_history_toggled)
        self._save_history_chk.setStyleSheet(
            "QCheckBox { color:rgba(255,255,255,0.45); font-size:12px; background:transparent; spacing:5px; }"
            "QCheckBox::indicator { width:13px; height:13px; border-radius:3px; border:1px solid rgba(255,255,255,0.18); background:rgba(255,255,255,0.05); }"
            f"QCheckBox::indicator:checked {{ background:{tok('accent')}; border-color:{tok('accent')}; }}")
        foot_hl.addWidget(self._save_history_chk); foot_hl.addSpacing(8)

        self._select_all_btn = QPushButton("Select All")
        self._select_all_btn.setToolTip("Select all visible rows")
        self._select_all_btn.setStyleSheet(_glass_btn)
        self._select_all_btn.clicked.connect(self._tbl.selectAll)
        foot_hl.addWidget(self._select_all_btn)

        self._del_btn = QPushButton("Delete Selected")
        self._del_btn.setToolTip("Remove selected scrobble(s) from local history")
        self._del_btn.setStyleSheet(_danger_btn)
        self._del_btn.clicked.connect(self._delete_selected)

        self._del_all_btn = QPushButton("Delete All")
        self._del_all_btn.setToolTip("Remove ALL scrobbles from local history")
        self._del_all_btn.setStyleSheet(_danger_btn)
        self._del_all_btn.clicked.connect(self._delete_all)
        foot_hl.addWidget(self._del_btn); foot_hl.addWidget(self._del_all_btn)

        foot_hl.addSpacing(8)
        _pag_ss = ("QPushButton { background:rgba(255,255,255,0.06); color:rgba(255,255,255,0.60);"
                   " border:1px solid rgba(255,255,255,0.10); border-radius:5px; font-size:12px;"
                   " padding:0 14px; min-height:28px; max-height:28px; }"
                   "QPushButton:hover:!disabled { background:rgba(255,255,255,0.12); color:#fff; }"
                   "QPushButton:disabled { color:rgba(255,255,255,0.20); border-color:rgba(255,255,255,0.06); }")
        self._prev_btn = QPushButton("‹  Prev")
        self._next_btn = QPushButton("Next  ›")
        self._prev_btn.setStyleSheet(_pag_ss); self._next_btn.setStyleSheet(_pag_ss)
        self._prev_btn.clicked.connect(self._prev_page)
        self._next_btn.clicked.connect(self._next_page)
        foot_hl.addWidget(self._prev_btn); foot_hl.addWidget(self._next_btn)
        body_vb.addWidget(foot)

    def resizeEvent(self, event):
        super().resizeEvent(event)
        if hasattr(self, '_bg'):
            self._bg.setGeometry(self.rect())

    def set_bg_art(self, raw):
        """Called externally (e.g. from ScrobblePage) to share/clear blurred background art."""
        if not hasattr(self, '_bg'): return
        if raw:
            self._bg.set_art(raw, _dominant_color(raw))
        else:
            self._bg.clear()

    def _on_save_history_toggled(self, checked: bool):
        c = load_conf(); c["save_history"] = checked; save_conf(c)

    def _delete_all(self):
        total = len(self._all_rows)
        if not total:
            return
        ans = QMessageBox.question(self, "Delete All",
            f"Remove ALL {total} scrobble{'s' if total != 1 else ''} from local history?\n"
            "This won't affect Last.fm — only the local record.",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No)
        if ans != QMessageBox.StandardButton.Yes:
            return
        try:
            con = sqlite3.connect(DB_FILE)
            con.execute("DELETE FROM scrobbled")
            con.commit(); con.close()
        except Exception as e:
            QMessageBox.critical(self, "Error", str(e))
            return
        self.refresh()

    def refresh(self):
        self._loading_lbl.setVisible(True); self._tbl.setVisible(False)
        self._count_lbl.setText("Loading…")
        if self._loader and self._loader.isRunning(): self._loader.quit()
        self._loader = HistoryLoader(limit=5000)
        self._loader.done.connect(self._on_loaded); self._loader.start()

    def _on_loaded(self, rows: list):
        self._loading_lbl.setVisible(False); self._tbl.setVisible(True)
        self._all_rows = rows; self._page = 0; self._apply_filter()

    def _apply_filter(self):
        q = self._search.text().strip().lower()
        self._filtered = ([r for r in self._all_rows if any(q in str(f).lower() for f in r[:3])]
                          if q else self._all_rows)
        self._page = 0; self._render_page()

    # Platform → (label, bg color, text color)
    _PLAT_BADGE = {
        "Last.fm":       ("#d51007", "#fff"),
        "Libre.fm":      ("#2ecc71", "#0a0a0a"),
        "ListenBrainz":  ("#eb743b", "#0a0a0a"),
    }

    def _render_page(self):
        rows  = self._filtered; total = len(rows)
        start = self._page * self.PAGE_SIZE; end = min(start + self.PAGE_SIZE, total)
        page_rows = rows[start:end]
        self._page_rows = page_rows   # keep for delete lookup
        self._tbl.setSortingEnabled(False); self._tbl.setUpdatesEnabled(False)
        self._tbl.setRowCount(len(page_rows))
        self._tbl.verticalHeader().setDefaultSectionSize(34)
        for i, (artist, album, title, ts, plat, sub_at) in enumerate(page_rows):
            try:    local_str = datetime.fromtimestamp(ts).strftime("%d %b %Y  %H:%M")
            except Exception: local_str = str(ts)
            try:    sub_str = (datetime.fromtimestamp(sub_at).strftime("%d %b %Y  %H:%M") if sub_at else "—")
            except Exception: sub_str = "—"
            for col, val in enumerate([artist, title, album, plat, local_str, sub_str]):
                item = QTableWidgetItem(val); item.setToolTip(val)
                if col == 3:  # platform — colored badge via foreground
                    bg, fg = self._PLAT_BADGE.get(val, ("#555", "#fff"))
                    item.setForeground(QColor(fg))
                    item.setBackground(QColor(bg + "33"))  # semi-transparent bg tint
                self._tbl.setItem(i, col, item)
        self._tbl.setUpdatesEnabled(True); self._tbl.setSortingEnabled(True)
        pages = max(1, (total + self.PAGE_SIZE - 1) // self.PAGE_SIZE)
        self._prev_btn.setEnabled(self._page > 0)
        self._next_btn.setEnabled(self._page < pages - 1)
        page_info = f"page {self._page+1} / {pages}" if pages > 1 else ""
        self._count_lbl.setText(
            f"{total} scrobbles  ·  {start+1}–{end}  {page_info}".strip(" ·")
            if total else "No history yet")

    def _show_context_menu(self, pos):
        row = self._tbl.rowAt(pos.y())
        if row < 0: return
        from PyQt6.QtWidgets import QMenu
        menu = QMenu(self)
        act = menu.addAction("🗑  Delete this scrobble")
        act.triggered.connect(lambda: self._delete_rows([row]))
        menu.exec(self._tbl.viewport().mapToGlobal(pos))

    def _delete_selected(self):
        rows = sorted({idx.row() for idx in self._tbl.selectedIndexes()})
        if not rows: return
        n = len(rows)
        ans = QMessageBox.question(self, "Delete",
            f"Remove {n} scrobble{'s' if n > 1 else ''} from local history?\n"
            "This won't affect Last.fm — only the local record.",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No)
        if ans != QMessageBox.StandardButton.Yes: return
        self._delete_rows(rows)

    def _delete_rows(self, row_indices: list):
        page_rows = getattr(self, "_page_rows", [])
        deleted = 0
        for ri in row_indices:
            if ri >= len(page_rows): continue
            artist, album, title, ts, plat, _ = page_rows[ri]
            if db_delete(artist, album, title, ts, plat):
                deleted += 1
        if deleted:
            self.refresh()   # reload from DB

    def _prev_page(self):
        if self._page > 0: self._page -= 1; self._render_page()

    def _next_page(self):
        pages = max(1, (len(self._filtered) + self.PAGE_SIZE - 1) // self.PAGE_SIZE)
        if self._page < pages - 1: self._page += 1; self._render_page()


# ─────────────────────────────────────────────────────────────
#  PAGE: ROCKBOX TOOLS
# ─────────────────────────────────────────────────────────────

class _EjectWorker(QThread):
    done = pyqtSignal(str)

    def __init__(self, root_str: str):
        super().__init__()
        self.root_str = root_str

    def run(self):
        # Strip AppImage-injected library paths so system tools (udisksctl,
        # findmnt, umount, diskutil) load their own libs and not ours.
        env = os.environ.copy()
        env.pop("LD_LIBRARY_PATH", None)
        env.pop("LD_PRELOAD", None)
        try:
            if platform.system() == "Linux":
                if shutil.which("udisksctl"):
                    # findmnt to get block device, then unmount it
                    fm = subprocess.run(
                        ["findmnt", "-n", "-o", "SOURCE", self.root_str],
                        capture_output=True, text=True, timeout=5, env=env,
                    )
                    blk = fm.stdout.strip()
                    if not blk:
                        self.done.emit("⚠  Could not find block device — try umount manually."); return
                    r = subprocess.run(
                        ["udisksctl", "unmount", "-b", blk],
                        capture_output=True, text=True, timeout=15, env=env,
                    )
                    msg = r.stdout.strip() or r.stderr.strip()
                else:
                    r = subprocess.run(
                        ["umount", self.root_str],
                        capture_output=True, text=True, timeout=15, env=env,
                    )
                    msg = r.stdout.strip() or r.stderr.strip() or f"Unmounted {self.root_str}"
            elif platform.system() == "Darwin":
                r = subprocess.run(
                    ["diskutil", "eject", self.root_str],
                    capture_output=True, text=True, timeout=15, env=env,
                )
                msg = r.stdout.strip() or r.stderr.strip()
            else:
                self.done.emit("Use your OS to safely eject the device."); return
            self.done.emit(f"⏏  {msg}" if msg else "⏏  Device ejected — safe to unplug.")
        except subprocess.TimeoutExpired:
            self.done.emit("⚠  Eject timed out.")
        except Exception as e:
            self.done.emit(f"⚠  Eject failed: {e}")


class RockboxToolsPage(QWidget):
    """
    Two-panel Rockbox toolbox:
      Left panel  → Database Rebuilder (detects new music, triggers Rockbox db update)
      Right panel → config.cfg Editor  (full key/value editor with schema docs)
    """

    def __init__(self, conf_ref: list, parent=None):
        super().__init__(parent)
        self.conf_ref = conf_ref
        self._db_worker: Optional[RockboxDbWorker] = None
        self._cfg_path: Optional[Path] = None
        self._cfg_data: dict[str, str] = {}     # key -> raw string value
        self._cfg_widgets: dict[str, QWidget] = {}
        self._build()

    def conf(self) -> dict:
        return self.conf_ref[0]

    def _build(self):
        outer = QVBoxLayout(self)
        outer.setContentsMargins(0, 0, 0, 0)
        outer.setSpacing(0)

        # Glass header
        hdr = QWidget(); hdr.setFixedHeight(52)
        hdr.setStyleSheet("background:rgba(5,7,11,0.65); border-bottom:1px solid rgba(255,255,255,0.07);")
        hdr_hl = QHBoxLayout(hdr); hdr_hl.setContentsMargins(28, 0, 20, 0); hdr_hl.setSpacing(10)
        hdr_title = QLabel("Rockbox Tools")
        hdr_title.setStyleSheet("color:#fff; font-size:18px; font-weight:700; letter-spacing:-0.3px; background:transparent;")
        hdr_sub = QLabel("Database rebuilder, config.cfg editor and tagnavi.config editor.")
        hdr_sub.setStyleSheet("color:rgba(255,255,255,0.35); font-size:12px; background:transparent;")
        hdr_hl.addWidget(hdr_title); hdr_hl.addWidget(hdr_sub); hdr_hl.addStretch()
        outer.addWidget(hdr)

        scroll = QScrollArea(); scroll.setWidgetResizable(True)
        scroll.setFrameShape(QFrame.Shape.NoFrame)
        inner = QWidget()
        root  = QVBoxLayout(inner)
        root.setContentsMargins(28, 20, 28, 24)
        root.setSpacing(20)

        # ── Device selector ───────────────────────────────────
        dev_card = QWidget(); dev_card.setObjectName("card")
        dc = QHBoxLayout(dev_card); dc.setContentsMargins(18,14,18,14); dc.setSpacing(12)
        dev_lbl = QLabel("Device root:"); dev_lbl.setObjectName("secondary"); dev_lbl.setFixedWidth(90)
        self._dev_path = QLineEdit(); self._dev_path.setPlaceholderText("/media/user/iPod  or  E:\\")
        saved_dev = self.conf().get("device_root", "")
        self._dev_path.setText(saved_dev)
        detect_btn = QPushButton("Auto-detect"); detect_btn.setObjectName("ghost")
        browse_btn = QPushButton("Browse…");     browse_btn.setObjectName("ghost")
        detect_btn.clicked.connect(self._detect_device)
        browse_btn.clicked.connect(self._browse_device)
        dc.addWidget(dev_lbl); dc.addWidget(self._dev_path, stretch=1)
        dc.addWidget(detect_btn); dc.addWidget(browse_btn)
        root.addWidget(dev_card)

        # ── Tab widget: DB rebuilder | Config editor | Tagnavi ──
        tabs = QTabWidget()
        tabs.setDocumentMode(True)
        tabs.setStyleSheet("""
            QTabWidget::pane {{ border: 1px solid {_current_theme['border']}; border-radius: 6px;
                                background: {_current_theme['bg0']}; }}
            QTabBar::tab {{ background: {_current_theme['bg2']}; color: {_current_theme['txt1']};
                            border: 1px solid {_current_theme['border']};
                            border-bottom: none; border-radius: 4px 4px 0 0;
                            padding: 7px 18px; font-size: 13px; }}
            QTabBar::tab:selected {{ background: {_current_theme['bg0']}; color: {_current_theme['accent']};
                                      border-bottom: 2px solid {_current_theme['accent']}; font-weight: 600; }}
            QTabBar::tab:hover {{ background: {_current_theme['bg3']}; }}
        """)

        tabs.addTab(self._build_db_panel(),    "⚙ Database Rebuilder")
        tabs.addTab(self._build_cfg_panel(),   "📄 config.cfg Editor")
        tabs.addTab(self._build_tagnavi_panel(),"🏷 tagnavi.config")
        root.addWidget(tabs, stretch=1)

        root.addStretch()
        scroll.setWidget(inner)
        outer.addWidget(scroll)

    # ─── DB Rebuilder Panel ───────────────────────────────────

    def _build_db_panel(self) -> QWidget:
        w = QWidget(); w.setObjectName("card")
        vb = QVBoxLayout(w); vb.setContentsMargins(18,16,18,16); vb.setSpacing(12)

        hdr = QHBoxLayout()
        title = QLabel("Database Rebuilder"); title.setStyleSheet("font-weight:700;font-size:14px;")
        hdr.addWidget(title); hdr.addStretch()
        self._db_dot = StatusDot(_current_theme["txt2"]); hdr.addWidget(self._db_dot)
        vb.addLayout(hdr)

        how = QLabel(
            "Builds a real Rockbox tagcache database on your PC using the official "
            "Rockbox database tool compiled from source — no on-device scan needed.\n\n"
            "Requires:  git  gcc  make  (standard on Linux)"
        )
        how.setObjectName("secondary"); how.setWordWrap(True)
        vb.addWidget(how)

        vb.addWidget(HDivider())

        # ── Music source selector ─────────────────────────────────
        src_lbl = QLabel("Music source"); src_lbl.setStyleSheet("font-weight:600;")
        vb.addWidget(src_lbl)

        src_row = QHBoxLayout(); src_row.setSpacing(8)
        self._db_src_device = QPushButton("📱  From device")
        self._db_src_local  = QPushButton("💻  From local folder")
        for btn in (self._db_src_device, self._db_src_local):
            btn.setCheckable(True); btn.setObjectName("ghost")
            btn.setFixedHeight(32)
        self._db_src_device.setChecked(True)
        self._db_src_device.setToolTip(
            "Scan audio files directly from the mounted device.\n"
            "Works for any path structure.")
        self._db_src_local.setToolTip(
            "Scan a local music library folder instead of the device.\n"
            "Much faster — reads from local SSD, writes database to device.")
        self._db_src_device.clicked.connect(lambda: self._set_db_source("device"))
        self._db_src_local.clicked.connect(lambda:  self._set_db_source("local"))
        src_row.addWidget(self._db_src_device)
        src_row.addWidget(self._db_src_local)
        src_row.addStretch()
        vb.addLayout(src_row)

        # ── Device subfolder path (optional, shown when "From device" selected) ──
        self._db_dev_sub_widget = QWidget()
        dev_sub_row = QHBoxLayout(self._db_dev_sub_widget)
        dev_sub_row.setContentsMargins(0, 0, 0, 0); dev_sub_row.setSpacing(8)
        dev_sub_lbl = QLabel("Music folder:"); dev_sub_lbl.setObjectName("secondary"); dev_sub_lbl.setFixedWidth(100)
        self._db_dev_sub_path = QLineEdit()
        self._db_dev_sub_path.setPlaceholderText("(whole device)  e.g. /run/media/roy/IPOD/Music")
        saved_dev_sub = self.conf().get("db_device_sub", "")
        self._db_dev_sub_path.setText(saved_dev_sub)
        dev_sub_browse = QPushButton("Browse…"); dev_sub_browse.setObjectName("ghost")
        dev_sub_browse.clicked.connect(self._browse_device_sub)
        dev_sub_row.addWidget(dev_sub_lbl)
        dev_sub_row.addWidget(self._db_dev_sub_path, stretch=1)
        dev_sub_row.addWidget(dev_sub_browse)
        self._db_dev_sub_widget.setVisible(True)
        vb.addWidget(self._db_dev_sub_widget)

        # ── Local library path ────────────────────────────────────
        self._db_lib_widget = QWidget()
        lib_row = QHBoxLayout(self._db_lib_widget)
        lib_row.setContentsMargins(0, 0, 0, 0); lib_row.setSpacing(8)
        lib_lbl = QLabel("Library folder:"); lib_lbl.setObjectName("secondary"); lib_lbl.setFixedWidth(100)
        self._db_lib_path = QLineEdit()
        self._db_lib_path.setPlaceholderText("/home/you/Music")
        saved_lib = self.conf().get("db_library_root", "")
        self._db_lib_path.setText(saved_lib)
        lib_browse = QPushButton("Browse…"); lib_browse.setObjectName("ghost")
        lib_browse.clicked.connect(self._browse_library)
        self._db_sanitize_btn = QPushButton("🧹  Sanitize Names"); self._db_sanitize_btn.setObjectName("ghost")
        self._db_sanitize_btn.setToolTip(
            "Rename files and folders in the local library to be FAT32-safe.\n"
            "Replaces : * ? \" < > | \\ with - and strips trailing dots/spaces.\n"
            "Run this before building the database so local names match the iPod.")
        self._db_sanitize_btn.clicked.connect(self._start_db_sanitize)
        lib_row.addWidget(lib_lbl)
        lib_row.addWidget(self._db_lib_path, stretch=1)
        lib_row.addWidget(lib_browse)
        lib_row.addWidget(self._db_sanitize_btn)
        self._db_lib_widget.setVisible(False)
        vb.addWidget(self._db_lib_widget)

        vb.addWidget(HDivider())

        self._db_log = QTextEdit()
        self._db_log.setReadOnly(True)
        self._db_log.setMinimumHeight(200)
        self._db_log.setPlaceholderText("Build output will appear here…")
        self._db_log.setStyleSheet(
            f"background: {_current_theme['bg3']}; font-family: 'Cascadia Code','SF Mono','Consolas',monospace;"
            f"font-size: 11px; color: {_current_theme['txt1']}; border: none; border-radius: 4px;"
        )
        vb.addWidget(self._db_log, stretch=1)

        self._db_progress = QProgressBar(); self._db_progress.setRange(0, 0)
        self._db_progress.setVisible(False); vb.addWidget(self._db_progress)

        btn_row = QHBoxLayout()
        self._db_build_btn = QPushButton("🗄  Build Database"); self._db_build_btn.setObjectName("primary")
        self._db_build_btn.setToolTip(
            "Build the Rockbox tagcache database from your music library.\n"
            "Existing .tcd files are replaced with a fresh build.")
        self._db_build_btn.clicked.connect(self._start_db_build)

        self._db_eject_btn = QPushButton("⏏  Eject"); self._db_eject_btn.setObjectName("ghost")
        self._db_eject_btn.setToolTip("Safely unmount the device")
        self._db_eject_btn.clicked.connect(self._eject_device)

        self._db_cancel_btn = QPushButton("⏹  Cancel"); self._db_cancel_btn.setObjectName("ghost")
        self._db_cancel_btn.clicked.connect(self._cancel_db_worker)
        self._db_cancel_btn.setVisible(False)

        self._db_pause_btn = QPushButton("⏸  Pause"); self._db_pause_btn.setObjectName("ghost")
        self._db_pause_btn.clicked.connect(self._toggle_db_pause)
        self._db_pause_btn.setVisible(False)
        self._db_paused = False

        btn_row.addWidget(self._db_build_btn)
        btn_row.addWidget(self._db_pause_btn)
        btn_row.addWidget(self._db_cancel_btn)
        btn_row.addStretch()
        btn_row.addWidget(self._db_eject_btn)
        vb.addLayout(btn_row)
        return w

    def _set_db_source(self, mode: str):
        is_local = (mode == "local")
        self._db_src_device.setChecked(not is_local)
        self._db_src_local.setChecked(is_local)
        self._db_dev_sub_widget.setVisible(not is_local)
        self._db_lib_widget.setVisible(is_local)
        accent = _current_theme["accent"]
        active_style  = f"background:{accent}22; color:{accent}; border:1px solid {accent}60; border-radius:4px;"
        default_style = ""
        self._db_src_local.setStyleSheet(active_style  if is_local  else default_style)
        self._db_src_device.setStyleSheet(active_style if not is_local else default_style)

    def _browse_library(self):
        path = QFileDialog.getExistingDirectory(self, "Select music library folder",
            self._db_lib_path.text() or str(Path.home()))
        if path:
            self._db_lib_path.setText(path)
            c = self.conf(); c["db_library_root"] = path; save_conf(c)

    def _browse_device_sub(self):
        root_str = self._dev_path.text().strip()
        start = root_str or str(Path.home())
        path = QFileDialog.getExistingDirectory(self, "Select music folder on device", start)
        if path:
            self._db_dev_sub_path.setText(path)
            c = self.conf(); c["db_device_sub"] = path; save_conf(c)

    def _start_db_sanitize(self):
        lib_str = self._db_lib_path.text().strip()
        if not lib_str:
            QMessageBox.warning(self, "No library", "Enter the path to your local music library folder first."); return
        lib_path = Path(lib_str)
        if not lib_path.exists():
            QMessageBox.warning(self, "Not found", f"Library folder does not exist:\n{lib_path}"); return
        ans = QMessageBox.question(self, "Sanitize Library Names",
            f"This will permanently rename files and folders in:\n{lib_path}\n\n"
            "Characters like : * ? \" < > | \\ will be replaced with -\n"
            "and trailing dots/spaces will be stripped.\n\n"
            "This cannot be undone automatically. Continue?",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.Cancel)
        if ans != QMessageBox.StandardButton.Yes:
            return
        self._db_log.clear()
        self._db_sanitize_btn.setEnabled(False)
        self._db_build_btn.setEnabled(False)
        self._db_progress.setVisible(True)
        self._db_progress.setRange(0, 0)
        # Run in a simple thread — reuse the worker's method via a thin QThread wrapper
        class _SanitizeRun(QThread):
            progress = pyqtSignal(str)
            done     = pyqtSignal()
            def __init__(self, root):
                super().__init__(); self._root = root
            def run(self):
                # Inline the logic here so we don't need the full worker
                import os as _os
                _FAT32 = re.compile(r'[\\/:*?"<>|]')
                def _clean(name):
                    c = _FAT32.sub('-', name)
                    stem, _, ext = c.rpartition('.')
                    if not stem:
                        return c.rstrip('. ') or name
                    if not ext or not ext.strip():
                        return stem.rstrip('. ') or name
                    return f"{stem.rstrip('. ')}.{ext}"
                self.progress.emit(f"── Sanitizing: {self._root}")
                try:
                    items = sorted(self._root.rglob("*"), key=lambda p: len(p.parts), reverse=True)
                except Exception as e:
                    self.progress.emit(f"  ⚠ Scan error: {e}"); self.done.emit(); return
                to_do = [fp for fp in items if _clean(fp.name) != fp.name]
                if not to_do:
                    self.progress.emit("  ✓ Nothing to rename — already clean"); self.done.emit(); return
                self.progress.emit(f"  Found {len(to_do):,} item(s)…")
                renamed = 0; errors = 0
                for fp in to_do:
                    if not fp.exists(): continue
                    clean = _clean(fp.name)
                    new_path = fp.parent / clean
                    if new_path.exists() and new_path != fp:
                        base = new_path.stem; ext = new_path.suffix; n = 2
                        while new_path.exists():
                            new_path = fp.parent / f"{base}_{n}{ext}"; n += 1
                    try:
                        fp.rename(new_path)
                        self.progress.emit(f"  ✎ {fp.name!r}  →  {new_path.name!r}")
                        renamed += 1
                    except Exception as e:
                        self.progress.emit(f"  ⚠ {fp.name!r}: {e}"); errors += 1
                self.progress.emit(f"  ✓ Done — {renamed:,} renamed, {errors} failed")
                self.done.emit()
        self._sanitize_thread = _SanitizeRun(lib_path)
        self._sanitize_thread.progress.connect(lambda msg: self._db_log.append(msg))
        self._sanitize_thread.done.connect(self._on_sanitize_done)
        self._sanitize_thread.start()

    def _on_sanitize_done(self):
        self._db_sanitize_btn.setEnabled(True)
        self._db_build_btn.setEnabled(True)
        self._db_progress.setVisible(False)

    def _start_db_build(self):
        self._run_db_worker()

    def _run_db_worker(self):
        root_str = self._dev_path.text().strip()
        if not root_str:
            QMessageBox.warning(self, "No device", "Set the device root path first."); return
        root = Path(root_str)
        if not root.exists():
            QMessageBox.warning(self, "Not found", f"Path does not exist:\n{root}"); return

        library_root: Optional[Path] = None
        if self._db_src_local.isChecked():
            lib_str = self._db_lib_path.text().strip()
            if not lib_str:
                QMessageBox.warning(self, "No library",
                    "Enter the path to your local music library folder."); return
            library_root = Path(lib_str)
            if not library_root.exists():
                QMessageBox.warning(self, "Not found",
                    f"Library folder does not exist:\n{library_root}"); return
            c = self.conf(); c["db_library_root"] = str(library_root); save_conf(c)
        else:
            # Device mode: optional subfolder — if set, scan only that folder
            dev_sub_str = self._db_dev_sub_path.text().strip()
            if dev_sub_str:
                library_root = Path(dev_sub_str)
                if not library_root.exists():
                    QMessageBox.warning(self, "Not found",
                        f"Music folder does not exist:\n{library_root}"); return
                c = self.conf(); c["db_device_sub"] = dev_sub_str; save_conf(c)

        c = self.conf(); c["device_root"] = str(root); save_conf(c)

        self._db_log.clear()
        self._db_build_btn.setEnabled(False)
        self._db_progress.setVisible(True)
        self._db_progress.setRange(0, 0)
        self._db_cancel_btn.setVisible(True)
        self._db_cancel_btn.setEnabled(True)
        self._db_pause_btn.setVisible(True)
        self._db_pause_btn.setEnabled(True)
        self._db_pause_btn.setText("⏸  Pause")
        self._db_paused = False
        self._db_dot.set_color(_current_theme["warning"])

        self._db_worker = RockboxDbWorker(root, RockboxDbWorker.MODE_INITIALIZE,
                                          library_root=library_root)
        self._db_worker.progress.connect(lambda msg: self._db_log.append(msg))
        self._db_worker.count_update.connect(self._on_db_count)
        self._db_worker.finished.connect(self._on_db_done)
        self._db_worker.start()

    def _on_db_count(self, current: int, total: int):
        if total > 0:
            self._db_progress.setRange(0, total)
            self._db_progress.setValue(current)
            self._db_progress.setFormat(f"{current:,} / {total:,}")
        else:
            SCAN_CEIL = max(current + 200, 500)
            self._db_progress.setRange(0, SCAN_CEIL)
            self._db_progress.setValue(current)
            self._db_progress.setFormat(f"Building… {current:,}")

    def _on_db_done(self, result: dict):
        self._db_build_btn.setEnabled(True)
        self._db_progress.setVisible(False)
        self._db_cancel_btn.setVisible(False)
        self._db_pause_btn.setVisible(False)
        self._db_paused = False

        if "error" in result:
            self._db_log.append(f"\n⚠ Error: {result['error']}")
            self._db_dot.set_color(_current_theme["danger"]); return

        # Already up to date — nothing was rebuilt
        if result.get("up_to_date"):
            self._db_dot.set_color(_current_theme["success"])
            return

        total   = result.get("total", 0)
        written = result.get("written", [])
        errors  = result.get("write_errors", [])
        n_new   = result.get("new_files", 0)
        n_chg   = result.get("changed_files", 0)
        n_rem   = result.get("removed_files", 0)

        if written:
            self._db_log.append(f"\n✓ Database written to device ({len(written)} files):")
            for fn in written:
                self._db_log.append(f"   /.rockbox/{fn}")
            # Show a summary of what changed
            parts = []
            if n_new:     parts.append(f"{n_new:,} new")
            if n_chg:     parts.append(f"{n_chg:,} changed")
            if n_rem:     parts.append(f"{n_rem:,} removed")
            summary = (", ".join(parts) + " file(s)") if parts else "full rebuild"
            self._db_log.append(
                f"\n✓ Done ({summary}). Safely eject your device — the database is ready.\n"
                "  No on-device scan required."
            )
            self._db_dot.set_color(_current_theme["success"])
        if errors:
            self._db_log.append(f"\n⚠ Write errors:")
            for e in errors:
                self._db_log.append(f"   {e}")
            self._db_dot.set_color(_current_theme["warning"])

    def _cancel_db_worker(self):
        if self._db_worker and self._db_worker.isRunning():
            if self._db_paused:
                self._db_worker._pause_event.set()
                self._db_paused = False
                self._db_pause_btn.setText("⏸  Pause")
            self._db_worker.requestInterruption()
            # Also kill any running subprocess immediately
            if self._db_worker._proc:
                try: self._db_worker._proc.kill()
                except Exception: pass
            self._db_log.append("\n⏹ Cancelling…")
            self._db_cancel_btn.setEnabled(False)

    def _toggle_db_pause(self):
        if not self._db_worker or not self._db_worker.isRunning():
            return
        if not self._db_paused:
            self._db_worker._pause_event.clear()
            self._db_paused = True
            self._db_pause_btn.setText("▶  Resume")
            self._db_log.append("⏸ Paused.")
        else:
            self._db_worker._pause_event.set()
            self._db_paused = False
            self._db_pause_btn.setText("⏸  Pause")
            self._db_log.append("▶ Resumed.")


    def _eject_device(self):
        root_str = self._dev_path.text().strip()
        if not root_str:
            QMessageBox.warning(self, "No device", "Set the device root path first."); return
        self._db_eject_btn.setEnabled(False)
        self._db_log.append("⏏  Ejecting…")
        worker = _EjectWorker(root_str)
        worker.done.connect(self._on_eject_done)
        # keep reference so it's not GC'd
        self._eject_worker = worker
        worker.start()

    def _on_eject_done(self, msg: str):
        self._db_eject_btn.setEnabled(True)
        self._db_log.append(msg)

    # ─── Config Editor Panel ──────────────────────────────────

    def _build_cfg_panel(self) -> QWidget:
        w = QWidget(); w.setObjectName("card")
        vb = QVBoxLayout(w); vb.setContentsMargins(18,16,18,16); vb.setSpacing(10)

        hdr = QHBoxLayout()
        title = QLabel("config.cfg Editor"); title.setStyleSheet("font-weight:700;font-size:14px;")
        hdr.addWidget(title); hdr.addStretch()
        auto_cfg_btn = QPushButton("Auto-detect"); auto_cfg_btn.setObjectName("ghost")
        auto_cfg_btn.clicked.connect(self._auto_load_cfg)
        load_btn = QPushButton("Browse…"); load_btn.setObjectName("ghost")
        load_btn.clicked.connect(self._load_cfg)
        save_btn = QPushButton("Save to device"); save_btn.setObjectName("primary")
        save_btn.clicked.connect(self._save_cfg)
        hdr.addWidget(auto_cfg_btn); hdr.addWidget(load_btn); hdr.addWidget(save_btn)
        vb.addLayout(hdr)

        self._cfg_path_lbl = QLabel("No config loaded  ·  Auto-detect or browse to /.rockbox/config.cfg")
        self._cfg_path_lbl.setObjectName("muted")
        vb.addWidget(self._cfg_path_lbl)

        vb.addWidget(HDivider())

        # Search + group filter
        filter_row = QHBoxLayout()
        self._cfg_search = QLineEdit(); self._cfg_search.setPlaceholderText("Search settings…")
        self._cfg_search.textChanged.connect(self._filter_cfg)
        self._cfg_group_combo = QComboBox()
        self._cfg_group_combo.addItem("All groups")
        for g in ROCKBOX_CONFIG_GROUPS: self._cfg_group_combo.addItem(g)
        self._cfg_group_combo.currentTextChanged.connect(self._filter_cfg)
        filter_row.addWidget(self._cfg_search, stretch=1); filter_row.addWidget(self._cfg_group_combo)
        vb.addLayout(filter_row)

        self._cfg_scroll = QScrollArea(); self._cfg_scroll.setWidgetResizable(True)
        self._cfg_scroll.setFrameShape(QFrame.Shape.NoFrame)
        self._cfg_inner = QWidget()
        self._cfg_layout = QVBoxLayout(self._cfg_inner)
        self._cfg_layout.setSpacing(4); self._cfg_layout.setContentsMargins(0,4,4,4)
        self._cfg_layout.addStretch()
        self._cfg_scroll.setWidget(self._cfg_inner)
        vb.addWidget(self._cfg_scroll, stretch=1)

        self._cfg_status = QLabel(""); self._cfg_status.setObjectName("muted")
        vb.addWidget(self._cfg_status)
        return w

    def _auto_load_cfg(self):
        root_str = self._dev_path.text().strip()
        if root_str:
            candidate = Path(root_str) / ".rockbox" / "config.cfg"
            if candidate.exists():
                self._do_load_cfg(candidate); return
        for root in find_rockbox_devices():
            candidate = root / ".rockbox" / "config.cfg"
            if candidate.exists():
                self._do_load_cfg(candidate); return
        QMessageBox.information(self, "Not found",
            "Could not auto-detect config.cfg.\nSet the device root or browse manually.")

    def _load_cfg(self):
        path, _ = QFileDialog.getOpenFileName(self, "Open config.cfg", str(Path.home()),
            "Config files (*.cfg);;All files (*)")
        if path: self._do_load_cfg(Path(path))

    def _do_load_cfg(self, path: Path):
        self._cfg_path = path
        self._cfg_data = {}
        try:
            for line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
                line = line.strip()
                if not line or line.startswith("#"): continue
                if ": " in line:
                    k, _, v = line.partition(": ")
                    self._cfg_data[k.strip().lower()] = v.strip()
                elif "=" in line:
                    k, _, v = line.partition("=")
                    self._cfg_data[k.strip().lower()] = v.strip()
            self._cfg_path_lbl.setText(f"Loaded: {path}")
            self._cfg_status.setText(f"{len(self._cfg_data)} settings loaded")
            self._rebuild_cfg_ui()
        except Exception as e:
            QMessageBox.critical(self, "Load failed", str(e))

    def _rebuild_cfg_ui(self):
        """Rebuild the config editor rows from schema + loaded data."""
        # Clear existing
        while self._cfg_layout.count() > 1:
            item = self._cfg_layout.takeAt(0)
            if item.widget(): item.widget().deleteLater()
        self._cfg_widgets.clear()

        if not self._cfg_path:
            hint = QLabel("Load a config.cfg from your device to edit settings.\n"
                          "Click Auto-detect if your device is connected, or Browse… to find it manually.")
            hint.setObjectName("muted"); hint.setWordWrap(True)
            hint.setAlignment(Qt.AlignmentFlag.AlignCenter)
            hint.setStyleSheet(f"padding: 32px; color: {_current_theme['txt2']};")
            self._cfg_layout.insertWidget(0, hint)
            self._cfg_status.setText("")
            return

        group_sel = self._cfg_group_combo.currentText()
        search_q  = self._cfg_search.text().strip().lower()

        # Collect keys to show
        if group_sel == "All groups":
            groups_to_show = list(ROCKBOX_CONFIG_GROUPS.items())
        else:
            keys = ROCKBOX_CONFIG_GROUPS.get(group_sel, [])
            groups_to_show = [(group_sel, keys)]

        row_count = 0
        for group_name, keys in groups_to_show:
            group_rows = []
            for key in keys:
                schema = ROCKBOX_CONFIG_SCHEMA.get(key)
                if not schema: continue
                if search_q and search_q not in key and search_q not in schema[2].lower():
                    continue
                group_rows.append((key, schema))

            if not group_rows: continue

            # Group header
            grp_lbl = QLabel(group_name)
            grp_lbl.setStyleSheet(
                f"font-weight: 700; font-size: 11px; color: {_current_theme['txt2']}; "
                f"background: {_current_theme['bg3']}; padding: 4px 8px; border-radius: 3px; "
                f"letter-spacing: 0.5px;"
            )
            self._cfg_layout.insertWidget(self._cfg_layout.count()-1, grp_lbl)

            for key, (typ, allowed, desc) in group_rows:
                row = self._make_cfg_row(key, typ, allowed, desc)
                self._cfg_layout.insertWidget(self._cfg_layout.count()-1, row)
                row_count += 1

        self._cfg_status.setText(f"Showing {row_count} settings")

    def _make_cfg_row(self, key: str, typ: str, allowed, desc: str) -> QWidget:
        t    = _current_theme
        row  = QWidget()
        row.setStyleSheet(
            f"QWidget{{background:rgba(255,255,255,0.08);border-radius:4px;}}"
            f"QWidget:hover{{background:rgba(255,255,255,0.12);}}"
        )
        hl   = QHBoxLayout(row); hl.setContentsMargins(10, 6, 10, 6); hl.setSpacing(12)

        info = QVBoxLayout(); info.setSpacing(1)
        key_lbl = QLabel(key)
        key_lbl.setStyleSheet("font-family:'Cascadia Code','SF Mono','Consolas',monospace;"
                               f"font-size:12px;color:rgba(255,255,255,0.87);font-weight:600;background:transparent;")
        desc_lbl = QLabel(desc)
        desc_lbl.setStyleSheet("font-size:11px;color:rgba(255,255,255,0.35);background:transparent;")
        desc_lbl.setWordWrap(True)
        info.addWidget(key_lbl); info.addWidget(desc_lbl)
        hl.addLayout(info, stretch=1)

        # Input widget based on type
        current_val = self._cfg_data.get(key, "")

        if typ == "bool":
            w = QCheckBox()
            w.setChecked(current_val.lower() in ("on","yes","true","1","enabled") if current_val else False)
            w.setStyleSheet("background:transparent;")
            self._cfg_widgets[key] = w
        elif typ == "int" and allowed:
            lo, hi = allowed
            w = QSpinBox()
            w.setRange(lo, hi); w.setFixedWidth(90)
            try: w.setValue(int(current_val))
            except Exception: w.setValue(lo)
            self._cfg_widgets[key] = w
        elif typ == "enum" and allowed:
            w = QComboBox(); w.setFixedWidth(200)
            for opt in allowed: w.addItem(opt)
            if current_val in allowed: w.setCurrentText(current_val)
            self._cfg_widgets[key] = w
        else:  # str
            w = QLineEdit(); w.setFixedWidth(200)
            w.setPlaceholderText("value")
            w.setText(current_val)
            self._cfg_widgets[key] = w

        hl.addWidget(w)

        # Show "current on device" badge if key in loaded data
        if key in self._cfg_data and current_val:
            badge = QLabel(f"  {current_val}  ")
            badge.setStyleSheet(
                f"background:{t['accentlo']};color:{t['accent']};font-size:10px;"
                f"border-radius:3px;font-family:'Cascadia Code','SF Mono','Consolas',monospace;"
            )
            badge.setToolTip(f"Current value on device: {current_val}")
            hl.addWidget(badge)

        return row

    def _filter_cfg(self):
        self._rebuild_cfg_ui()

    def _save_cfg(self):
        if not self._cfg_path:
            QMessageBox.warning(self, "No config loaded", "Load a config.cfg first."); return

        # Collect values from all visible widgets
        for key, widget in self._cfg_widgets.items():
            if isinstance(widget, QCheckBox):
                self._cfg_data[key] = "on" if widget.isChecked() else "off"
            elif isinstance(widget, QSpinBox):
                self._cfg_data[key] = str(widget.value())
            elif isinstance(widget, QComboBox):
                self._cfg_data[key] = widget.currentText()
            elif isinstance(widget, QLineEdit):
                v = widget.text().strip()
                if v: self._cfg_data[key] = v
                elif key in self._cfg_data: del self._cfg_data[key]

        try:
            lines = ["# Rockbox config.cfg — edited by Scrobbox\n"]
            for k, v in sorted(self._cfg_data.items()):
                lines.append(f"{k}: {v}\n")
            self._cfg_path.write_text("".join(lines), encoding="utf-8")
            self._cfg_status.setText(f"✓ Saved {len(self._cfg_data)} settings to {self._cfg_path.name}")
        except Exception as e:
            QMessageBox.critical(self, "Save failed", str(e))

    # ─── Tagnavi Panel ────────────────────────────────────────

    # All valid tag names that can be browsed/displayed
    TAGNAVI_TAGS = [
        "artist", "album", "title", "genre", "year", "tracknum",
        "composer", "comment", "albumartist", "grouping", "discnum",
        "bitrate", "frequency", "length", "filesize",
        "lastplayed", "playcount", "rating", "commitid", "lastoffset",
    ]

    # What each tag browsing type means as a menu target
    TAGNAVI_BROWSE_TYPES = {
        "All Artists":        ("allartists",  "artist",   "%s",             "artist",   "ascending"),
        "All Albums":         ("allalbums",   "album",    "%s - %s",        "artist album", "ascending"),
        "All Tracks":         ("tracks",      "title",    "%s — %s",        "artist title", "ascending"),
        "Albums (drilldown)": ("albums",      "album",    "%s (%04d)",      "album year", "ascending"),
        "Artists (drilldown)":("artists",     "artist",   "%s",             "artist",   "ascending"),
        "Genres":             ("genres",      "genre",    "%s",             "genre",    "ascending"),
        "Years":              ("years",       "year",     "%04d",           "year",     "descending"),
        "Composers":          ("allcomposers","composer", "%s",             "composer", "ascending"),
        "Album Artists":      ("albumartists","albumartist","%s",           "albumartist","ascending"),
        "Tracks (drilldown)": ("tracks",      "title",    "%02d. %s",       "tracknum title","ascending"),
        "Recently Played":    ("tracks",      "title",    "%s — %s",        "artist title","descending"),
        "Highest Rated":      ("tracks",      "title",    "%s — %s",        "rating",   "descending"),
        "Most Played":        ("tracks",      "title",    "%s — %s",        "playcount","descending"),
    }

    def _build_tagnavi_panel(self) -> QWidget:
        """Visual tagnavi.config builder with live preview."""
        self._tagnavi_path: Optional[Path] = None
        # Internal model: list of menu dicts
        self._tagnavi_menus: list = [
            self._tagnavi_default_item("Artist",       "All Artists"),
            self._tagnavi_default_item("Album",        "All Albums"),
            self._tagnavi_default_item("Tracks",       "All Tracks"),
            self._tagnavi_default_item("Genre",        "Genres"),
            self._tagnavi_default_item("Year",         "Years"),
            self._tagnavi_default_item("Recently Played", "Recently Played"),
            self._tagnavi_default_item("Most Played",  "Most Played"),
        ]

        w = QWidget()
        vb = QVBoxLayout(w); vb.setContentsMargins(18, 14, 18, 14); vb.setSpacing(10)

        # ── Header ───────────────────────────────────────────
        hdr = QHBoxLayout()
        title_lbl = QLabel("tagnavi.config Editor"); title_lbl.setStyleSheet("font-weight:700;font-size:14px;")
        hdr.addWidget(title_lbl); hdr.addStretch()
        auto_btn  = QPushButton("Auto-detect"); auto_btn.setObjectName("ghost"); auto_btn.setFixedHeight(28)
        load_btn  = QPushButton("Load…");       load_btn.setObjectName("ghost")
        save_btn  = QPushButton("Save to device"); save_btn.setObjectName("primary")
        auto_btn.clicked.connect(self._tagnavi_auto_load)
        load_btn.clicked.connect(self._tagnavi_load)
        save_btn.clicked.connect(self._tagnavi_save)
        hdr.addWidget(auto_btn); hdr.addWidget(load_btn); hdr.addWidget(save_btn)
        vb.addLayout(hdr)

        self._tagnavi_path_lbl = QLabel("No file loaded  ·  Use Auto-detect or Load to open your tagnavi.config")
        self._tagnavi_path_lbl.setObjectName("muted"); vb.addWidget(self._tagnavi_path_lbl)
        vb.addWidget(HDivider())

        # ── Main splitter: tree | detail+preview ──────────────
        splitter = QSplitter(Qt.Orientation.Horizontal); splitter.setHandleWidth(6)
        splitter.setChildrenCollapsible(False)

        # ─ Left: menu tree ────────────────────────────────────
        left = QWidget(); lv = QVBoxLayout(left); lv.setContentsMargins(0,0,0,0); lv.setSpacing(6)

        tree_hdr = QHBoxLayout()
        tree_hdr_lbl = QLabel("Menu items"); tree_hdr_lbl.setStyleSheet("font-weight:600;font-size:12px;")
        tree_hdr.addWidget(tree_hdr_lbl); tree_hdr.addStretch()

        add_root_btn = QPushButton("+ Add"); add_root_btn.setObjectName("ghost"); add_root_btn.setFixedHeight(26)
        add_child_btn= QPushButton("+ Sub"); add_child_btn.setObjectName("ghost"); add_child_btn.setFixedHeight(26)
        del_btn      = QPushButton("Delete"); del_btn.setObjectName("danger"); del_btn.setFixedHeight(26)
        up_btn       = QPushButton("↑"); up_btn.setObjectName("ghost"); up_btn.setFixedHeight(26); up_btn.setFixedWidth(30)
        dn_btn       = QPushButton("↓"); dn_btn.setObjectName("ghost"); dn_btn.setFixedHeight(26); dn_btn.setFixedWidth(30)
        tree_hdr.addWidget(up_btn); tree_hdr.addWidget(dn_btn)
        tree_hdr.addWidget(add_root_btn); tree_hdr.addWidget(add_child_btn); tree_hdr.addWidget(del_btn)
        lv.addLayout(tree_hdr)

        self._tagnavi_tree = QTreeWidget()
        self._tagnavi_tree.setHeaderHidden(True)
        self._tagnavi_tree.setIndentation(16)
        self._tagnavi_tree.setStyleSheet("""
            QTreeWidget {{ background: {_current_theme['bg2']}; border: 1px solid {_current_theme['border']};
                           border-radius: 4px; outline: none; font-size: 12px; }}
            QTreeWidget::item {{ padding: 4px 6px; border-radius: 3px; color: {_current_theme['txt0']}; }}
            QTreeWidget::item:hover {{ background: {_current_theme['bg3']}; }}
            QTreeWidget::item:selected {{ background: {_current_theme['accentlo']}; color: {_current_theme['accent']}; }}
        """)
        self._tagnavi_tree.currentItemChanged.connect(self._tagnavi_on_select)
        lv.addWidget(self._tagnavi_tree, stretch=1)

        add_root_btn.clicked.connect(self._tagnavi_add_root)
        add_child_btn.clicked.connect(self._tagnavi_add_child)
        del_btn.clicked.connect(self._tagnavi_delete)
        up_btn.clicked.connect(self._tagnavi_move_up)
        dn_btn.clicked.connect(self._tagnavi_move_down)

        splitter.addWidget(left)

        # ─ Right: detail editor + preview ────────────────────
        right = QWidget(); rv = QVBoxLayout(right); rv.setContentsMargins(0,0,0,0); rv.setSpacing(8)

        # Detail card
        detail = QWidget(); detail.setObjectName("card")
        dv = QVBoxLayout(detail); dv.setContentsMargins(14,12,14,12); dv.setSpacing(10)
        detail_title = QLabel("Item properties"); detail_title.setStyleSheet("font-weight:600;font-size:12px;")
        dv.addWidget(detail_title)

        grid = QGridLayout(); grid.setHorizontalSpacing(10); grid.setVerticalSpacing(8)
        grid.setColumnMinimumWidth(0, 85)

        def _lbl(t):
            l = QLabel(t); l.setObjectName("secondary"); return l

        self._tn_label_in = QLineEdit(); self._tn_label_in.setPlaceholderText('e.g. "Artists"')
        self._tn_type_cb  = QComboBox()
        for bt in self.TAGNAVI_BROWSE_TYPES: self._tn_type_cb.addItem(bt)
        self._tn_sort_cb  = QComboBox()
        for tag in self.TAGNAVI_TAGS: self._tn_sort_cb.addItem(tag)
        self._tn_dir_cb   = QComboBox(); self._tn_dir_cb.addItems(["ascending", "descending"])
        self._tn_limit_spin = QSpinBox(); self._tn_limit_spin.setRange(0, 9999)
        self._tn_limit_spin.setSpecialValueText("No limit"); self._tn_limit_spin.setValue(0)
        self._tn_fmt_in   = QLineEdit(); self._tn_fmt_in.setPlaceholderText('e.g. "%s — %s"  (printf-style)')
        self._tn_fmttags_in = QLineEdit(); self._tn_fmttags_in.setPlaceholderText("e.g. artist title")

        grid.addWidget(_lbl("Label"),      0, 0, Qt.AlignmentFlag.AlignRight|Qt.AlignmentFlag.AlignVCenter)
        grid.addWidget(self._tn_label_in,  0, 1)
        grid.addWidget(_lbl("Browse type"),1, 0, Qt.AlignmentFlag.AlignRight|Qt.AlignmentFlag.AlignVCenter)
        grid.addWidget(self._tn_type_cb,   1, 1)
        grid.addWidget(_lbl("Sort by"),    2, 0, Qt.AlignmentFlag.AlignRight|Qt.AlignmentFlag.AlignVCenter)
        sort_row = QHBoxLayout(); sort_row.addWidget(self._tn_sort_cb); sort_row.addWidget(self._tn_dir_cb)
        grid.addLayout(sort_row,           2, 1)
        grid.addWidget(_lbl("Limit"),      3, 0, Qt.AlignmentFlag.AlignRight|Qt.AlignmentFlag.AlignVCenter)
        grid.addWidget(self._tn_limit_spin,3, 1)
        grid.addWidget(_lbl("Format"),     4, 0, Qt.AlignmentFlag.AlignRight|Qt.AlignmentFlag.AlignVCenter)
        grid.addWidget(self._tn_fmt_in,    4, 1)
        grid.addWidget(_lbl("Format tags"), 5, 0, Qt.AlignmentFlag.AlignRight|Qt.AlignmentFlag.AlignVCenter)
        grid.addWidget(self._tn_fmttags_in,5, 1)
        dv.addLayout(grid)

        # Fill defaults button
        fill_btn = QPushButton("Fill defaults from browse type"); fill_btn.setObjectName("ghost")
        fill_btn.setFixedHeight(26); fill_btn.clicked.connect(self._tagnavi_fill_defaults)
        dv.addWidget(fill_btn, alignment=Qt.AlignmentFlag.AlignLeft)

        apply_btn = QPushButton("Apply changes"); apply_btn.setObjectName("primary")
        apply_btn.setFixedHeight(32); apply_btn.clicked.connect(self._tagnavi_apply_item)
        dv.addWidget(apply_btn, alignment=Qt.AlignmentFlag.AlignRight)

        self._tn_no_sel = QLabel("Select an item in the tree to edit its properties.")
        self._tn_no_sel.setObjectName("muted"); self._tn_no_sel.setAlignment(Qt.AlignmentFlag.AlignCenter)

        self._tn_detail_stack = QStackedWidget()
        self._tn_detail_stack.addWidget(self._tn_no_sel)
        self._tn_detail_stack.addWidget(detail)
        rv.addWidget(self._tn_detail_stack)

        # Preview card
        prev_card = QWidget(); prev_card.setObjectName("card")
        pv = QVBoxLayout(prev_card); pv.setContentsMargins(14,10,14,10); pv.setSpacing(6)
        prev_hdr = QHBoxLayout()
        prev_title = QLabel("Generated config preview"); prev_title.setStyleSheet("font-weight:600;font-size:12px;")
        prev_hdr.addWidget(prev_title); prev_hdr.addStretch()
        copy_prev_btn = QPushButton("Copy"); copy_prev_btn.setObjectName("ghost"); copy_prev_btn.setFixedHeight(24)
        copy_prev_btn.clicked.connect(lambda: QApplication.clipboard().setText(self._tagnavi_preview.toPlainText()))
        prev_hdr.addWidget(copy_prev_btn)
        pv.addLayout(prev_hdr)
        self._tagnavi_preview = QTextEdit(); self._tagnavi_preview.setReadOnly(True)
        self._tagnavi_preview.setMaximumHeight(200)
        self._tagnavi_preview.setStyleSheet(
            f"background:{_current_theme['bg3']};font-family:'Cascadia Code','SF Mono','Consolas',monospace;"
            f"font-size:11px;color:{_current_theme['txt1']};border:none;border-radius:4px;"
        )
        pv.addWidget(self._tagnavi_preview)
        rv.addWidget(prev_card)

        self._tagnavi_status = QLabel(""); self._tagnavi_status.setObjectName("muted")
        rv.addWidget(self._tagnavi_status)

        splitter.addWidget(right)
        splitter.setSizes([280, 520])
        vb.addWidget(splitter, stretch=1)

        # Populate tree with default items
        QTimer.singleShot(0, self._tagnavi_rebuild_tree)
        return w

    # ── Tagnavi model helpers ─────────────────────────────────

    def _tagnavi_default_item(self, label="New item", browse_type="All Artists") -> dict:
        bt = self.TAGNAVI_BROWSE_TYPES.get(browse_type, list(self.TAGNAVI_BROWSE_TYPES.values())[0])
        target, fmt_tag, fmt_str, sort_tags, sort_dir = bt
        return {
            "label": label,
            "browse_type": browse_type,
            "target": target,
            "fmt_name": f"fmt_{target}",
            "fmt_str": fmt_str,
            "fmt_tags": sort_tags,
            "sort": sort_tags.split()[0],
            "sort_dir": sort_dir,
            "limit": 0,
            "children": [],
        }

    def _tagnavi_tree_item(self, data: dict) -> "QTreeWidgetItem":
        icon = "▸" if data["children"] else "•"
        lbl  = f"{icon}  {data['label']}  [{data['browse_type']}]"
        item = QTreeWidgetItem([lbl])
        item.setData(0, Qt.ItemDataRole.UserRole, data)
        for child_data in data["children"]:
            item.addChild(self._tagnavi_tree_item(child_data))
        return item

    def _tagnavi_rebuild_tree(self):
        self._tagnavi_tree.blockSignals(True)
        self._tagnavi_tree.clear()
        for menu in self._tagnavi_menus:
            self._tagnavi_tree.addTopLevelItem(self._tagnavi_tree_item(menu))
        self._tagnavi_tree.expandAll()
        self._tagnavi_tree.blockSignals(False)
        self._tagnavi_update_preview()

    def _tagnavi_current_data(self) -> Optional[dict]:
        item = self._tagnavi_tree.currentItem()
        return item.data(0, Qt.ItemDataRole.UserRole) if item else None

    def _tagnavi_find_parent_list(self, target_data: dict) -> Optional[list]:
        """Find the list that contains target_data."""
        def _search(lst, td):
            if td in lst: return lst
            for d in lst:
                r = _search(d["children"], td)
                if r is not None: return r
            return None
        return _search(self._tagnavi_menus, target_data)

    def _tagnavi_on_select(self, cur, _prev):
        if not cur:
            self._tn_detail_stack.setCurrentIndex(0); return
        self._tn_detail_stack.setCurrentIndex(1)
        data = cur.data(0, Qt.ItemDataRole.UserRole)
        self._tn_label_in.setText(data.get("label", ""))
        bt = data.get("browse_type", "All Artists")
        idx = self._tn_type_cb.findText(bt)
        if idx >= 0: self._tn_type_cb.setCurrentIndex(idx)
        sort_tag = data.get("sort", "artist")
        idx = self._tn_sort_cb.findText(sort_tag)
        if idx >= 0: self._tn_sort_cb.setCurrentIndex(idx)
        self._tn_dir_cb.setCurrentText(data.get("sort_dir", "ascending"))
        self._tn_limit_spin.setValue(data.get("limit", 0))
        self._tn_fmt_in.setText(data.get("fmt_str", ""))
        self._tn_fmttags_in.setText(data.get("fmt_tags", ""))

    def _tagnavi_fill_defaults(self):
        bt_name = self._tn_type_cb.currentText()
        bt = self.TAGNAVI_BROWSE_TYPES.get(bt_name)
        if not bt: return
        target, fmt_tag, fmt_str, sort_tags, sort_dir = bt
        self._tn_fmt_in.setText(fmt_str)
        self._tn_fmttags_in.setText(sort_tags)
        idx = self._tn_sort_cb.findText(sort_tags.split()[0])
        if idx >= 0: self._tn_sort_cb.setCurrentIndex(idx)
        self._tn_dir_cb.setCurrentText(sort_dir)

    def _tagnavi_apply_item(self):
        cur = self._tagnavi_tree.currentItem()
        if not cur: return
        data = cur.data(0, Qt.ItemDataRole.UserRole)
        bt_name = self._tn_type_cb.currentText()
        bt = self.TAGNAVI_BROWSE_TYPES.get(bt_name, list(self.TAGNAVI_BROWSE_TYPES.values())[0])
        data["label"]       = self._tn_label_in.text().strip() or "Item"
        data["browse_type"] = bt_name
        data["target"]      = bt[0]
        data["fmt_str"]     = self._tn_fmt_in.text().strip() or bt[2]
        data["fmt_tags"]    = self._tn_fmttags_in.text().strip() or bt[3]
        data["fmt_name"]    = f"fmt_{bt[0]}"
        data["sort"]        = self._tn_sort_cb.currentText()
        data["sort_dir"]    = self._tn_dir_cb.currentText()
        data["limit"]       = self._tn_limit_spin.value()
        cur.setData(0, Qt.ItemDataRole.UserRole, data)
        icon = "▸" if data["children"] else "•"
        cur.setText(0, f"{icon}  {data['label']}  [{data['browse_type']}]")
        self._tagnavi_update_preview()
        self._tagnavi_status.setText(f"Updated: {data['label']}")

    def _tagnavi_add_root(self):
        data = self._tagnavi_default_item("New menu item")
        self._tagnavi_menus.append(data)
        self._tagnavi_rebuild_tree()
        # Select the new item
        last = self._tagnavi_tree.topLevelItem(self._tagnavi_tree.topLevelItemCount()-1)
        if last: self._tagnavi_tree.setCurrentItem(last)

    def _tagnavi_add_child(self):
        cur = self._tagnavi_tree.currentItem()
        if not cur:
            QMessageBox.information(self, "No selection", "Select a parent item first.")
            return
        parent_data = cur.data(0, Qt.ItemDataRole.UserRole)
        child_data  = self._tagnavi_default_item("Sub-item", "Albums (drilldown)")
        parent_data["children"].append(child_data)
        # Rebuild and reselect
        self._tagnavi_rebuild_tree()
        self._tagnavi_status.setText(f"Added sub-item under {parent_data['label']}")

    def _tagnavi_delete(self):
        cur = self._tagnavi_tree.currentItem()
        if not cur: return
        data = cur.data(0, Qt.ItemDataRole.UserRole)
        lst  = self._tagnavi_find_parent_list(data)
        if lst is None: return
        ans  = QMessageBox.question(self, "Delete",
            f"Delete \"{data['label']}\" and all its children?",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No)
        if ans == QMessageBox.StandardButton.Yes:
            lst.remove(data)
            self._tagnavi_rebuild_tree()

    def _tagnavi_move_up(self):
        cur = self._tagnavi_tree.currentItem()
        if not cur: return
        data = cur.data(0, Qt.ItemDataRole.UserRole)
        lst  = self._tagnavi_find_parent_list(data)
        if lst is None: return
        idx  = lst.index(data)
        if idx == 0: return
        lst[idx], lst[idx-1] = lst[idx-1], lst[idx]
        self._tagnavi_rebuild_tree()

    def _tagnavi_move_down(self):
        cur = self._tagnavi_tree.currentItem()
        if not cur: return
        data = cur.data(0, Qt.ItemDataRole.UserRole)
        lst  = self._tagnavi_find_parent_list(data)
        if lst is None: return
        idx  = lst.index(data)
        if idx >= len(lst)-1: return
        lst[idx], lst[idx+1] = lst[idx+1], lst[idx]
        self._tagnavi_rebuild_tree()

    # ── Tagnavi serialisation ─────────────────────────────────

    def _tagnavi_to_text(self) -> str:
        lines = [
            "#! rockbox/tagbrowser/2.0",
            "# Generated by Scrobbox — edit tagnavi_custom.config to persist changes",
            "",
            '%menu_start "main" "Database"',
        ]
        for data in self._tagnavi_menus:
            lines.extend(self._tagnavi_render_item(data, depth=0))
        lines.append("")
        lines.append('%root_menu "main"')
        return "\n".join(lines) + "\n"

    def _tagnavi_render_item(self, data: dict, depth: int) -> list:
        """Render a single menu item to proper Rockbox tagnavi.config chained syntax."""
        indent   = "    " * depth
        lines    = []
        label    = data["label"]
        target   = data["target"]
        fmt_name = data.get("fmt_name", "fmt_title")
        sort_tag = data.get("sort", "title")
        sort_dir = data.get("sort_dir", "ascending")
        limit    = data.get("limit", 0)
        children = data.get("children", [])

        # Build the chained navigation expression
        # e.g.: "Artists" -> canonicalartist -> album -> title = "fmt_title"
        chain = self._build_chain(target, fmt_name)
        limit_str = f" ? playcount > \"-1\"" if limit > 0 else ""

        if children:
            # Has children: render as a sub-menu container
            lines.append(f'{indent}%menu_start "{label}" "{label}"')
            for child in children:
                lines.extend(self._tagnavi_render_item(child, depth + 1))
            lines.append(f'{indent}%menu_end')
        else:
            # Leaf item: render as chained navigation line
            lines.append(f'{indent}"{label}" -> {chain}')
        return lines

    def _build_chain(self, target: str, fmt_name: str = "fmt_title") -> str:
        """Build a Rockbox tagnavi chained navigation expression for a target type."""
        chains = {
            "allartists":   'canonicalartist -> album -> title = "fmt_title"',
            "artists":      'canonicalartist -> album -> title = "fmt_title"',
            "allalbums":    'album -> title = "fmt_title"',
            "albums":       'album -> title = "fmt_title"',
            "tracks":       f'title = "{fmt_name}"',
            "genres":       'genre -> canonicalartist -> album -> title = "fmt_title"',
            "years":        'year ? year > "0" -> canonicalartist -> album -> title = "fmt_title"',
            "allcomposers": 'composer -> album -> title = "fmt_title"',
            "albumartists": 'albumartist -> album -> title = "fmt_title"',
        }
        return chains.get(target, f'title = "{fmt_name}"')

    def _tagnavi_update_preview(self):
        self._tagnavi_preview.setPlainText(self._tagnavi_to_text())

    # ── Tagnavi parse (load existing file into model) ─────────

    def _tagnavi_parse_text(self, text: str) -> list:
        """Parse tagnavi.config into internal model. Handles both %item and %menu_start/%menu_end."""
        menus      = []
        # Stack of (list_to_append_to, indent_level)
        # We use a simple flat approach: track parent stacks by indent
        item_stack = []   # list of dicts in order of nesting

        for line in text.splitlines():
            stripped = line.lstrip()
            if not stripped or stripped.startswith("#"): continue

            indent = len(line) - len(stripped)

            if stripped.startswith("%menu_start"):
                # e.g.: %menu_start "main" "Database"  or  %menu_start "Artists" "Artists"
                m = re.match(r'%menu_start\s+"([^"]+)"', stripped)
                if not m: continue
                label = m.group(1)
                if label == "main":
                    # Top-level container — don't add as a menu item, just continue
                    continue
                bt_key = "All Artists"
                data = {
                    "label": label, "browse_type": bt_key,
                    "target": self.TAGNAVI_BROWSE_TYPES[bt_key][0],
                    "fmt_name": "fmt_allartists", "fmt_str": "%s", "fmt_tags": "artist",
                    "sort": "artist", "sort_dir": "ascending", "limit": 0,
                    "children": [], "_indent": indent,
                }
                while item_stack and item_stack[-1]["_indent"] >= indent:
                    item_stack.pop()
                if item_stack:
                    item_stack[-1]["children"].append(data)
                else:
                    menus.append(data)
                item_stack.append(data)
                continue

            if stripped.startswith("%menu_end"):
                if item_stack:
                    item_stack.pop()
                continue

            if stripped.startswith("%item"):
                m = re.match(r'%item\s+"([^"]+)"\s+->\s+(\S+)', stripped)
                if not m: continue
                label  = m.group(1)
                target = m.group(2)
                bt_key = next((k for k, v in self.TAGNAVI_BROWSE_TYPES.items()
                               if v[0] == target), "All Tracks")
                data = {
                    "label": label, "browse_type": bt_key, "target": target,
                    "fmt_name": f"fmt_{target}", "fmt_str": "%s", "fmt_tags": "title",
                    "sort": "title", "sort_dir": "ascending", "limit": 0,
                    "children": [], "_indent": indent,
                }
                while item_stack and item_stack[-1]["_indent"] >= indent:
                    item_stack.pop()
                if item_stack:
                    item_stack[-1]["children"].append(data)
                else:
                    menus.append(data)
                item_stack.append(data)
                continue

            # Property lines: belong to the deepest item shallower than current indent
            if not item_stack:
                continue
            owner = None
            for it in reversed(item_stack):
                if it["_indent"] < indent:
                    owner = it; break
            if owner is None:
                owner = item_stack[-1]

            if stripped.startswith("%format"):
                parts = stripped.split(None, 3)
                if len(parts) >= 3 and owner is not None:
                    owner["fmt_name"] = parts[1].strip('"')
                    owner["fmt_str"]  = parts[2].strip('"')
                    owner["fmt_tags"] = parts[3].strip() if len(parts) > 3 else "title"
            elif stripped.startswith("%sort"):
                parts = stripped.split()
                if len(parts) >= 2 and owner is not None:
                    owner["sort"]     = parts[1].strip('"')
                    owner["sort_dir"] = parts[2] if len(parts) > 2 else "ascending"
            elif stripped.startswith("%limit"):
                parts = stripped.split()
                if len(parts) >= 2 and owner is not None:
                    try: owner["limit"] = int(parts[1])
                    except Exception: pass

        # Strip internal _indent keys
        def _clean(lst):
            for d in lst:
                d.pop("_indent", None)
                _clean(d["children"])
        _clean(menus)
        return menus

    def _tagnavi_auto_load(self):
        root_str = self._dev_path.text().strip()
        if root_str:
            candidate = Path(root_str) / ".rockbox" / "tagnavi.config"
            if candidate.exists():
                self._tagnavi_do_load(candidate); return
        for root in find_rockbox_devices():
            candidate = root / ".rockbox" / "tagnavi.config"
            if candidate.exists():
                self._tagnavi_do_load(candidate); return
        QMessageBox.information(self, "Not found",
            "Could not auto-detect tagnavi.config.\nSet the device root or browse manually.")

    def _tagnavi_load(self):
        path, _ = QFileDialog.getOpenFileName(
            self, "Open tagnavi.config", str(Path.home()),
            "Config files (tagnavi.config *.config);;All files (*)")
        if path: self._tagnavi_do_load(Path(path))

    def _tagnavi_do_load(self, path: Path):
        try:
            text = path.read_text(encoding="utf-8", errors="ignore")
            self._tagnavi_menus = self._tagnavi_parse_text(text)
            self._tagnavi_path  = path
            self._tagnavi_path_lbl.setText(f"Loaded: {path}")
            self._tagnavi_rebuild_tree()
            n = len(self._tagnavi_menus)
            self._tagnavi_status.setText(f"{n} top-level items loaded from {path.name}")
            # Select first item and explicitly populate detail panel
            first = self._tagnavi_tree.topLevelItem(0)
            if first:
                self._tagnavi_tree.setCurrentItem(first)
                self._tagnavi_on_select(first, None)  # force panel population
        except Exception as e:
            QMessageBox.critical(self, "Load failed", str(e))


    def _tagnavi_save(self):
        path = getattr(self, '_tagnavi_path', None)
        if not self._tagnavi_menus:
            QMessageBox.warning(self, "Empty", "No menu items to save. Add some first."); return
        if not path:
            root_str = self._dev_path.text().strip()
            default_dir = str(Path(root_str) / ".rockbox") if root_str else str(Path.home())
            out, _ = QFileDialog.getSaveFileName(
                self, "Save tagnavi.config", default_dir + "/tagnavi.config",
                "Config files (*.config);;All files (*)")
            if not out: return
            path = Path(out)
        try:
            text = self._tagnavi_to_text()
            path.write_text(text, encoding="utf-8")
            self._tagnavi_path = path
            self._tagnavi_path_lbl.setText(f"Saved: {path}")
            self._tagnavi_status.setText(f"✓ Saved to {path.name}")
        except Exception as e:
            QMessageBox.critical(self, "Save failed", str(e))

    # ─── Device detection helpers ─────────────────────────────

    def _detect_device(self):
        # Look for any mounted volume with a .rockbox directory (name-independent)
        devices = find_rockbox_devices()
        if devices:
            root = devices[0]
            self._dev_path.setText(str(root))
            c = self.conf(); c["device_root"] = str(root); save_conf(c)
            if len(devices) > 1:
                names = ", ".join(d.name for d in devices)
                QMessageBox.information(self, "Multiple devices found",
                    f"Found {len(devices)} Rockbox devices: {names}\n"
                    f"Using: {root}\n\nYou can browse to select a different one.")
            return
        QMessageBox.information(self, "Not found",
            "Could not auto-detect device.\n\nMake sure your iPod is connected and mounted.\n"
            "Any device with a .rockbox folder will be detected automatically.")

    def _browse_device(self):
        path = QFileDialog.getExistingDirectory(self, "Select device root / iPod drive", str(Path.home()))
        if path:
            self._dev_path.setText(path)
            c = self.conf(); c["device_root"] = path; save_conf(c)



# ─────────────────────────────────────────────────────────────
#  EMBEDDED WEB AUTH DIALOG
# ─────────────────────────────────────────────────────────────

class WebAuthDialog(QDialog):
    """
    Opens an actual website inside the app using QWebEngineView (if available),
    or falls back to a system browser with a manual confirm button.

    For Last.fm / Libre.fm: watches the URL; when the auth page is approved
    (URL changes from the auth approval page back to last.fm/home or similar),
    emits auth_approved.

    For ListenBrainz: opens the profile page directly so user can copy their token.
    """
    auth_approved = pyqtSignal()   # emitted when user approves in-browser

    def __init__(self, url: str, platform: str, success_url_fragment: str = "",
                 parent=None):
        super().__init__(parent)
        self.setWindowTitle(f"Connect to {platform}")
        self.resize(920, 680)
        self._success_fragment = success_url_fragment
        self._approved = False

        t = _current_theme
        self.setStyleSheet("background: rgba(5,7,11,0.95); color: rgba(255,255,255,0.85);")

        outer = QVBoxLayout(self)
        outer.setContentsMargins(0, 0, 0, 0)
        outer.setSpacing(0)

        # ── Top bar ──────────────────────────────────────────
        bar = QWidget()
        bar.setStyleSheet("background:rgba(5,7,11,0.65); border-bottom:1px solid rgba(255,255,255,0.07);")
        bar.setFixedHeight(46)
        bl = QHBoxLayout(bar); bl.setContentsMargins(14, 0, 14, 0); bl.setSpacing(10)

        self._url_lbl = QLabel(url)
        self._url_lbl.setStyleSheet("color: rgba(255,255,255,0.35); font-size: 11px; background: transparent;")
        self._url_lbl.setTextInteractionFlags(Qt.TextInteractionFlag.TextSelectableByMouse)
        bl.addWidget(self._url_lbl, stretch=1)

        open_btn = QPushButton("Open in Browser")
        open_btn.setObjectName("ghost")
        open_btn.setFixedHeight(34)
        open_btn.clicked.connect(lambda: open_url(QUrl(url)))
        bl.addWidget(open_btn)

        done_btn = QPushButton("✓  I've Authorized — Done")
        done_btn.setObjectName("primary")
        done_btn.setFixedHeight(30)
        done_btn.clicked.connect(self._on_manual_done)
        bl.addWidget(done_btn)

        outer.addWidget(bar)

        # ── Web view or fallback ──────────────────────────────
        if _HAS_WEBENGINE:
            self._view = QWebEngineView()
            # Use a fresh off-the-record profile so sessions are clean
            self._view.setUrl(QUrl(url))
            self._view.urlChanged.connect(self._on_url_changed)
            self._view.loadFinished.connect(self._check_current_url)
            outer.addWidget(self._view, stretch=1)
        else:
            # No web engine — show instructions + open externally
            fallback = QWidget()
            fl = QVBoxLayout(fallback); fl.setContentsMargins(40, 40, 40, 40); fl.setSpacing(16)
            fl.addStretch()

            icon_lbl = QLabel("🌐"); icon_lbl.setAlignment(Qt.AlignmentFlag.AlignHCenter)
            icon_lbl.setStyleSheet("font-size: 48px; background: transparent;")
            fl.addWidget(icon_lbl)

            info = QLabel(
                f"<b>Embedded browser not available.</b><br><br>"
                f"PyQt6-WebEngine is not installed.<br>"
                f"A browser window has been opened at:<br>"
                f"<code>{url}</code><br><br>"
                f"Authorize Scrobbox there, then click <b>✓ I've Authorized — Done</b> above."
            )
            info.setWordWrap(True)
            info.setAlignment(Qt.AlignmentFlag.AlignHCenter)
            info.setStyleSheet("color: rgba(255,255,255,0.55); font-size: 13px; background: transparent; line-height: 1.6;")
            fl.addWidget(info)
            fl.addStretch()

            # Auto-open in system browser
            open_url(QUrl(url))
            outer.addWidget(fallback, stretch=1)

    def _on_url_changed(self, qurl: QUrl):
        url_str = qurl.toString()
        self._url_lbl.setText(url_str)
        if self._success_fragment and self._success_fragment in url_str:
            if not self._approved:
                self._approved = True
                QTimer.singleShot(800, self._emit_and_close)

    def _check_current_url(self, ok: bool):
        if _HAS_WEBENGINE:
            self._on_url_changed(self._view.url())

    def _on_manual_done(self):
        self._approved = True
        self._emit_and_close()

    def _emit_and_close(self):
        self.auth_approved.emit()
        self.accept()



# ─────────────────────────────────────────────────────────────
#  PAGE: RSYNC
# ─────────────────────────────────────────────────────────────

# Option definitions: (key, flag, label, type, default, tooltip)
RSYNC_OPTIONS = [
    # ── Core transfer behaviour ───────────────────────────────
    ("archive",        "-a",                "Archive mode (-a)",                   "bool", True,
     "Equivalent to -rlptgoD — recursive, preserves symlinks, permissions, times, group, owner, devices"),
    ("recursive",      "-r",                "Recursive (-r)",                      "bool", False,
     "Recurse into directories (included in archive mode)"),
    ("verbose",        "-v",                "Verbose output (-v)",                 "bool", True,
     "Increase verbosity — shows files as they are transferred"),
    ("progress",       "--progress",        "Show transfer progress",              "bool", True,
     "Show progress information as each file transfers"),
    ("compress",       "-z",                "Compress during transfer (-z)",       "bool", False,
     "Compress file data as it is sent — useful for slow or remote links"),
    ("human",          "-h",                "Human-readable sizes (-h)",           "bool", True,
     "Output numbers in human-readable format (K, M, G)"),
    # ── What to preserve ─────────────────────────────────────
    ("times",          "-t",                "Preserve time",                       "bool", True,
     "Preserve modification times (included in archive mode)"),
    ("perms",          "-p",                "Preserve permissions",                "bool", False,
     "Preserve file permissions (included in archive mode)"),
    ("owner",          "-o",                "Preserve owner",                      "bool", False,
     "Preserve owner (super-user only, included in archive mode)"),
    ("group",          "-g",                "Preserve group",                      "bool", False,
     "Preserve group (included in archive mode)"),
    ("links",          "-l",                "Reproduce symlinks (-l)",             "bool", False,
     "Reproduce symbolic links on the destination"),
    ("hardlinks",      "-H",                "Preserve hard links (-H)",            "bool", False,
     "Look for hard-linked files and preserve the linking"),
    # ── Skip / conflict behaviour ─────────────────────────────
    ("ignore_existing","--ignore-existing",  "Ignore existing files",               "bool", False,
     "Skip updating files that already exist on the destination (like Grsync 'Ignore existing')"),
    ("skip_newer",     "-u",                "Skip newer files on dest",            "bool", False,
     "Skip any file that is newer on the destination than the source (same as Grsync 'Skip newer')"),
    ("size_only",      "--size-only",        "Size only (skip checksum/time)",      "bool", False,
     "Skip files that match in size — ignore modification time entirely"),
    ("checksum",       "-c",                "Checksum verification",               "bool", False,
     "Skip files that match by checksum instead of just mod-time & size"),
    # ── Deletion ─────────────────────────────────────────────
    ("delete",         "--delete",           "Delete on destination",               "bool", False,
     "⚠ Delete files on the destination that no longer exist on the source"),
    ("one_fs",         "--one-file-system",  "Do not leave filesystem",             "bool", False,
     "Don't cross filesystem boundaries — stays on same filesystem as the source (same as Grsync option)"),
    # ── Misc ─────────────────────────────────────────────────
    ("dry_run",        "-n",                "Dry run — simulate, no changes",      "bool", False,
     "Perform a trial run with no changes actually made"),
    ("partial",        "--partial",          "Keep partial files",                  "bool", False,
     "Keep partially transferred files — allows resuming interrupted transfers"),
    ("inplace",        "--inplace",          "In-place file update",                "bool", False,
     "Update destination files in-place rather than creating a temp copy"),
    ("fat_compat",     "--modify-window=2",  "FAT/exFAT timestamp tolerance",      "bool", False,
     "Use a 2-second timestamp tolerance — required for FAT/exFAT devices (iPod, SD cards) whose filesystem has 2-second timestamp granularity"),
    ("itemize",        "--itemize-changes",  "Itemize changes",                     "bool", False,
     "Output a change-summary for every updated file"),
    ("stats",          "--stats",            "Print transfer statistics",           "bool", False,
     "Print a verbose set of statistics on the file transfer"),
    # ── Filters / limits ─────────────────────────────────────
    ("exclude",        "--exclude",          "Exclude pattern",                     "str",  "",
     "Exclude files matching this pattern, e.g.  *.tmp  or  .DS_Store"),
    ("exclude_from",   "--exclude-from",     "Exclude-from file",                   "str",  "",
     "Read exclude patterns from this file, one per line"),
    ("bwlimit",        "--bwlimit",          "Bandwidth limit (KB/s, 0=unlimited)", "int",  0,
     "Limit I/O bandwidth; KBytes per second. 0 means no limit."),
    ("ssh_opts",       "-e",                "Remote shell / SSH options",          "str",  "",
     "Specify the remote shell to use, e.g.  ssh -p 2222 -i ~/.ssh/id_rsa"),
    ("extra",          "",                  "Extra flags (freeform)",              "str",  "",
     "Any extra flags to append verbatim to the rsync command"),
    ("sanitize",       "",                  "Sanitize filenames for FAT32",        "bool", False,
     "Before syncing, rename files with FAT32-illegal characters (:?<>|*\"\\) to use - instead. Fixes cover art and other files that fail on iPod/FAT32 destinations."),
]


# ── FAT32 illegal characters: \ / : * ? " < > |
_FAT32_ILLEGAL = re.compile(r'[\\/:*?"<>|]')

def _sanitize_fat32_name(name: str) -> str:
    """Replace FAT32-illegal characters in a filename with -."""
    return _FAT32_ILLEGAL.sub("-", name)

def _needs_sanitize(name: str) -> bool:
    return bool(_FAT32_ILLEGAL.search(name))


class SanitizeWorker(QThread):
    """
    Pre-sync step: walk source, find files/dirs whose names contain FAT32-illegal
    characters and rename them IN PLACE on the source with sanitized names.
    After this runs, the source is permanently clean and the normal rsync runs
    against it — no staging, no temp folders, no --delete surprises.
    """
    progress = pyqtSignal(str)
    finished = pyqtSignal(int)   # exit code

    def __init__(self, src: str, dst: str, rsync_flags: list, parent=None):
        super().__init__(parent)
        self._src   = src.rstrip("/\\")
        self._dst   = dst
        self._flags = rsync_flags   # unused now but kept for API compat

    def run(self):
        src_path = Path(self._src)
        if not src_path.exists():
            self.progress.emit(f"  ✗ Source not found: {self._src}")
            self.finished.emit(11); return

        self.progress.emit("Scanning source for FAT32-illegal filenames…")

        # Collect all entries (files AND dirs) that need renaming.
        # We must rename bottom-up (deepest first) so parent renames don't
        # invalidate child paths.
        to_rename: list[Path] = []
        try:
            for fp in sorted(src_path.rglob("*"), key=lambda p: len(p.parts), reverse=True):
                if _needs_sanitize(fp.name):
                    to_rename.append(fp)
        except Exception as e:
            self.progress.emit(f"  ✗ Scan error: {e}")
            self.finished.emit(11); return

        if not to_rename:
            self.progress.emit("  ✓ No filenames need sanitizing — source is already clean.")
            self.finished.emit(0); return

        self.progress.emit(f"  Found {len(to_rename):,} item(s) with illegal characters — renaming in place…")

        renamed = 0
        errors  = 0
        for fp in to_rename:
            if self.isInterruptionRequested():
                self.finished.emit(20); return
            # fp may no longer exist if a parent dir was already renamed — skip
            if not fp.exists():
                continue
            new_name = _sanitize_fat32_name(fp.name)
            new_path = fp.parent / new_name
            if new_path == fp:
                continue  # nothing changed
            try:
                # If a file with the sanitized name already exists, add a suffix
                if new_path.exists():
                    stem = new_path.stem; suffix = new_path.suffix; n = 1
                    while new_path.exists():
                        new_path = fp.parent / f"{stem}_{n}{suffix}"; n += 1
                fp.rename(new_path)
                self.progress.emit(f"  ✎ {fp.name}  →  {new_path.name}")
                renamed += 1
            except Exception as e:
                self.progress.emit(f"  ⚠ Could not rename {fp.name}: {e}")
                errors += 1

        if errors:
            self.progress.emit(f"  ⚠ Renamed {renamed:,}, {errors} failed — check permissions.")
        else:
            self.progress.emit(f"  ✓ Renamed {renamed:,} item(s) — source is now FAT32-clean.")

        self.finished.emit(0)


class RsyncPage(QWidget):
    """Grsync-style friendly GUI for rsync with saved profiles."""

    def __init__(self, conf_ref, parent=None):
        super().__init__(parent)
        self.conf_ref        = conf_ref
        self._process: Optional[QProcess] = None
        self._profiles: list = load_rsync_profiles()
        self._current_idx: int = -1
        self._option_widgets: dict = {}
        self._last_sync_info: Optional[dict] = self._load_last_sync_info()
        self._build()
        self._populate_profile_list()
        if self._profiles:
            self._profile_list.setCurrentRow(0)

    # ── Build UI ──────────────────────────────────────────────

    def _build(self):
        t = _current_theme
        outer = QVBoxLayout(self)
        outer.setContentsMargins(0, 0, 0, 0)
        outer.setSpacing(0)

        # ── Top bar ───────────────────────────────────────────
        top = QWidget()
        top.setStyleSheet("background:rgba(5,7,11,0.65); border-bottom:1px solid rgba(255,255,255,0.07);")
        top.setFixedHeight(60)
        tb = QHBoxLayout(top); tb.setContentsMargins(24, 0, 24, 0); tb.setSpacing(16)
        title_lbl = QLabel("Rsync"); title_lbl.setObjectName("heading")
        sub_lbl   = QLabel("Friendly GUI for rsync — build sync profiles and run them with one click")
        sub_lbl.setObjectName("secondary")
        tb.addWidget(title_lbl); tb.addWidget(sub_lbl); tb.addStretch()

        rsync_bin = shutil.which("rsync")
        if rsync_bin:
            try:
                ver = subprocess.check_output(["rsync", "--version"], text=True,
                                              stderr=subprocess.DEVNULL).split("\n")[0]
                ver_short = ver.split("version ")[-1].split(" ")[0] if "version" in ver else "found"
            except Exception:
                ver_short = "found"
            av_lbl = QLabel(f"● rsync {ver_short}")
            av_lbl.setStyleSheet(f"color:{t['success']};background:transparent;font-size:12px;")
        else:
            av_lbl = QLabel("⚠  rsync not in PATH")
            av_lbl.setStyleSheet(f"color:{t['warning']};background:transparent;font-size:12px;")
        tb.addWidget(av_lbl)
        outer.addWidget(top)

        # ── Main split ────────────────────────────────────────
        splitter = QSplitter(Qt.Orientation.Horizontal)
        splitter.setHandleWidth(1)
        splitter.setStyleSheet(f"QSplitter::handle{{background:rgba(255,255,255,0.09);}}")

        # Left: profile panel
        left = QWidget()
        left.setFixedWidth(215)
        left.setStyleSheet("background:rgba(5,7,11,0.50); border-right:1px solid rgba(255,255,255,0.07);")
        lv = QVBoxLayout(left); lv.setContentsMargins(10, 14, 10, 12); lv.setSpacing(8)
        lv.addWidget(SectionLabel("Saved Profiles"))
        self._profile_list = QListWidget()
        self._profile_list.setStyleSheet(
            f"QListWidget{{background:transparent;border:none;outline:none;}}"
            f"QListWidget::item{{padding:7px 8px;border-radius:5px;color:rgba(255,255,255,0.55);}}"
            f"QListWidget::item:hover{{background:rgba(255,255,255,0.05);color:rgba(255,255,255,0.87);}}"
            f"QListWidget::item:selected{{background:{t['accentlo']};color:{t['accent']};}}")
        self._profile_list.currentRowChanged.connect(self._on_profile_select)
        lv.addWidget(self._profile_list, stretch=1)
        prof_btns = QHBoxLayout(); prof_btns.setSpacing(6)
        new_btn = QPushButton("+ New"); new_btn.setObjectName("ghost"); new_btn.setFixedHeight(30)
        dup_btn = QPushButton("⧉ Dup"); dup_btn.setObjectName("ghost"); dup_btn.setFixedHeight(30)
        del_btn = QPushButton("✕"); del_btn.setObjectName("danger"); del_btn.setFixedHeight(30); del_btn.setFixedWidth(32)
        new_btn.clicked.connect(self._new_profile)
        dup_btn.clicked.connect(self._dup_profile)
        del_btn.clicked.connect(self._del_profile)
        prof_btns.addWidget(new_btn); prof_btns.addWidget(dup_btn); prof_btns.addStretch(); prof_btns.addWidget(del_btn)
        lv.addLayout(prof_btns)
        splitter.addWidget(left)

        # Right: editor + output
        right = QWidget()
        rv = QVBoxLayout(right); rv.setContentsMargins(0, 0, 0, 0); rv.setSpacing(0)

        # Editor scroll area
        editor_scroll = QScrollArea(); editor_scroll.setWidgetResizable(True)
        editor_scroll.setFrameShape(QFrame.Shape.NoFrame)
        editor_inner = QWidget()
        ev = QVBoxLayout(editor_inner); ev.setContentsMargins(24, 18, 24, 18); ev.setSpacing(18)

        # Profile name row
        name_row = QHBoxLayout(); name_row.setSpacing(10)
        name_ico = QLabel("📋"); name_ico.setStyleSheet("background:transparent;font-size:16px;")
        self._name_edit = QLineEdit(); self._name_edit.setPlaceholderText("Profile name, e.g. Music → NAS backup")
        self._name_edit.setFixedHeight(36)
        save_name_btn = QPushButton("Rename"); save_name_btn.setObjectName("ghost"); save_name_btn.setFixedHeight(36)
        save_name_btn.clicked.connect(self._save_name)
        name_row.addWidget(name_ico); name_row.addWidget(self._name_edit, stretch=1); name_row.addWidget(save_name_btn)
        ev.addLayout(name_row)

        # Source / destination card
        paths_card = QWidget(); paths_card.setObjectName("card")
        pc = QVBoxLayout(paths_card); pc.setContentsMargins(18, 14, 18, 16); pc.setSpacing(10)
        hdr_paths = QHBoxLayout()
        hdr_paths.addWidget(QLabel("⇄")); paths_ttl = QLabel("Source & Destination")
        paths_ttl.setObjectName("subheading"); hdr_paths.addWidget(paths_ttl); hdr_paths.addStretch()
        swap_btn = QPushButton("⇅ Swap"); swap_btn.setObjectName("ghost"); swap_btn.setFixedHeight(28)
        swap_btn.setToolTip("Swap source and destination"); swap_btn.clicked.connect(self._swap_paths)
        hdr_paths.addWidget(swap_btn); pc.addLayout(hdr_paths)
        pc.addWidget(HDivider())
        for attr, icon, label, placeholder in [
            ("_src_edit", "📂", "Source",      "e.g.  /home/user/Music/   or   user@host:/path/to/files/"),
            ("_dst_edit", "🎯", "Destination", "e.g.  /mnt/nas/Backup/   or   user@host:/backup/music/"),
        ]:
            row = QHBoxLayout(); row.setSpacing(10)
            ico_lbl = QLabel(icon); ico_lbl.setStyleSheet("background:transparent;font-size:15px;"); ico_lbl.setFixedWidth(22)
            lbl = QLabel(label); lbl.setObjectName("secondary"); lbl.setFixedWidth(82)
            edit = QLineEdit(); edit.setPlaceholderText(placeholder); edit.setFixedHeight(34)
            edit.textChanged.connect(self._update_cmd_preview)
            setattr(self, attr, edit)
            browse = QPushButton("Browse…"); browse.setObjectName("ghost"); browse.setFixedHeight(34)
            browse.clicked.connect(lambda _, e=edit: self._browse_dir(e))
            row.addWidget(ico_lbl); row.addWidget(lbl); row.addWidget(edit, stretch=1); row.addWidget(browse)
            pc.addLayout(row)
        # Trailing-slash note
        slash_note = QLabel("Tip: trailing / on source copies contents; no trailing / copies the folder itself.")
        slash_note.setObjectName("muted"); pc.addWidget(slash_note)
        ev.addWidget(paths_card)

        # Options card
        opts_card = QWidget(); opts_card.setObjectName("card")
        oc = QVBoxLayout(opts_card); oc.setContentsMargins(18, 14, 18, 16); oc.setSpacing(10)
        opts_hdr = QHBoxLayout()
        opts_ttl = QLabel("Options"); opts_ttl.setObjectName("subheading"); opts_hdr.addWidget(opts_ttl); opts_hdr.addStretch()
        preset_lbl = QLabel("Presets:"); preset_lbl.setObjectName("secondary")
        opts_hdr.addWidget(preset_lbl)
        for pname, pfn in [("Mirror", self._preset_mirror), ("Backup", self._preset_backup),
                           ("iPod / FAT", self._preset_ipod),
                           ("Remote (SSH)", self._preset_ssh), ("Reset", self._preset_reset)]:
            pb = QPushButton(pname); pb.setObjectName("ghost"); pb.setFixedHeight(26)
            pb.clicked.connect(pfn); opts_hdr.addWidget(pb)
        oc.addLayout(opts_hdr); oc.addWidget(HDivider())

        # Split bool options into 2 columns, string/int options full width
        bool_opts  = [(k,f,l,tp,d,tt) for k,f,l,tp,d,tt in RSYNC_OPTIONS if tp == "bool"]
        other_opts = [(k,f,l,tp,d,tt) for k,f,l,tp,d,tt in RSYNC_OPTIONS if tp != "bool"]

        grid = QGridLayout(); grid.setHorizontalSpacing(20); grid.setVerticalSpacing(4)
        for i, (key, flag, label, typ, default, tooltip) in enumerate(bool_opts):
            w = QCheckBox(label); w.setChecked(default); w.setToolTip(f"{flag}  —  {tooltip}")
            w.stateChanged.connect(self._update_cmd_preview)
            self._option_widgets[key] = w
            grid.addWidget(w, i // 2, i % 2)
        oc.addLayout(grid); oc.addSpacing(8); oc.addWidget(HDivider())

        for key, flag, label, typ, default, tooltip in other_opts:
            row = QHBoxLayout(); row.setSpacing(10)
            lbl2 = QLabel(label); lbl2.setObjectName("secondary"); lbl2.setFixedWidth(230); lbl2.setToolTip(tooltip)
            if typ == "int":
                w = QSpinBox(); w.setRange(0, 99999); w.setValue(default); w.setFixedWidth(110)
                if key == "bwlimit": w.setSpecialValueText("Unlimited")
                w.valueChanged.connect(self._update_cmd_preview)
            else:
                w = QLineEdit(); w.setFixedHeight(32)
                w.setPlaceholderText(tooltip[:55] + ("…" if len(tooltip) > 55 else ""))
                if default: w.setText(str(default))
                w.textChanged.connect(self._update_cmd_preview)
            self._option_widgets[key] = w
            row.addWidget(lbl2); row.addWidget(w); row.addStretch()
            oc.addLayout(row)
        ev.addWidget(opts_card)

        # Command preview card
        cmd_card = QWidget(); cmd_card.setObjectName("card")
        cc = QVBoxLayout(cmd_card); cc.setContentsMargins(18, 14, 18, 14); cc.setSpacing(8)
        cmd_hdr = QHBoxLayout(); cmd_ttl = QLabel("Command Preview"); cmd_ttl.setObjectName("subheading")
        cmd_hdr.addWidget(cmd_ttl); cmd_hdr.addStretch()
        copy_cmd_btn = QPushButton("Copy"); copy_cmd_btn.setObjectName("ghost"); copy_cmd_btn.setFixedHeight(26)
        copy_cmd_btn.clicked.connect(self._copy_cmd); cmd_hdr.addWidget(copy_cmd_btn)
        cc.addLayout(cmd_hdr)
        self._cmd_preview = QLabel("rsync …")
        self._cmd_preview.setTextInteractionFlags(Qt.TextInteractionFlag.TextSelectableByMouse)
        self._cmd_preview.setWordWrap(True)
        self._cmd_preview.setStyleSheet(
            f"background:rgba(5,7,11,0.50);border:1px solid rgba(255,255,255,0.09);border-radius:5px;"
            f"padding:10px 14px;font-family:'Cascadia Code','SF Mono','Consolas',monospace;"
            f"font-size:12px;color:rgba(255,255,255,0.55);")
        cc.addWidget(self._cmd_preview)
        ev.addWidget(cmd_card)
        ev.addStretch()

        editor_scroll.setWidget(editor_inner)
        rv.addWidget(editor_scroll, stretch=3)

        # ── Bottom run bar ────────────────────────────────────
        run_bar = QWidget()
        run_bar.setStyleSheet("background:rgba(5,7,11,0.60); border-top:1px solid rgba(255,255,255,0.07);")
        rb = QHBoxLayout(run_bar); rb.setContentsMargins(16, 8, 20, 8); rb.setSpacing(10)
        self._save_btn = QPushButton("💾  Save Profile"); self._save_btn.setObjectName("ghost")
        self._save_btn.setFixedHeight(38); self._save_btn.clicked.connect(self._save_profile)
        rb.addWidget(self._save_btn); rb.addStretch()
        self._dry_run_toggle = QCheckBox("Dry run")
        self._dry_run_toggle.setToolTip("Simulate — no files are actually transferred or deleted")
        self._dry_run_toggle.stateChanged.connect(self._update_cmd_preview)
        rb.addWidget(self._dry_run_toggle)
        rb.addSpacing(10)
        self._run_btn = QPushButton("▶  Run Sync"); self._run_btn.setObjectName("run")
        self._run_btn.setMinimumWidth(130); self._run_btn.setFixedHeight(42)
        self._run_btn.clicked.connect(self._run)
        self._pause_btn = QPushButton("⏸  Pause"); self._pause_btn.setObjectName("ghost")
        self._pause_btn.setFixedHeight(42); self._pause_btn.setMinimumWidth(90)
        self._pause_btn.setEnabled(False); self._pause_btn.setCheckable(True)
        self._pause_btn.clicked.connect(self._toggle_pause_rsync)
        self._stop_btn = QPushButton("■  Stop"); self._stop_btn.setObjectName("danger")
        self._stop_btn.setFixedHeight(42); self._stop_btn.setMinimumWidth(90); self._stop_btn.setEnabled(False)
        self._stop_btn.clicked.connect(self._stop)
        rb.addWidget(self._run_btn); rb.addWidget(self._pause_btn); rb.addWidget(self._stop_btn)

        rv.addWidget(run_bar)

        # ── Terminal output ───────────────────────────────────
        term_hdr = QWidget()
        term_hdr.setStyleSheet("background:rgba(5,7,11,0.60); border-top:1px solid rgba(255,255,255,0.07);")
        th = QHBoxLayout(term_hdr); th.setContentsMargins(16, 5, 14, 5)
        th.addWidget(SectionLabel("Output")); th.addStretch()
        self._status_dot = StatusDot(t["txt2"]); th.addWidget(self._status_dot)
        self._status_lbl = QLabel("Idle"); self._status_lbl.setObjectName("muted"); th.addWidget(self._status_lbl)
        th.addSpacing(12)
        clr_btn = QPushButton("Clear"); clr_btn.setObjectName("ghost"); clr_btn.setFixedHeight(24)
        clr_btn.clicked.connect(lambda: self._output.clear()); th.addWidget(clr_btn)
        rv.addWidget(term_hdr)

        self._output = QPlainTextEdit()
        self._output.setReadOnly(True); self._output.setMinimumHeight(200)
        self._output.setPlaceholderText("rsync output will appear here…")
        self._output.setStyleSheet(
            f"QPlainTextEdit{{background:rgba(0,0,0,0.30);color:rgba(255,255,255,0.87);"
            f"font-family:'Cascadia Code','SF Mono','Consolas',monospace;font-size:12px;"
            f"border:none;border-radius:0px;padding:10px;}}")
        rv.addWidget(self._output, stretch=2)

        splitter.addWidget(right)
        splitter.setSizes([215, 900])
        outer.addWidget(splitter, stretch=1)

    # ── Profile list ──────────────────────────────────────────

    def _populate_profile_list(self):
        self._profile_list.blockSignals(True)
        self._profile_list.clear()
        for p in self._profiles:
            name = p.get("name", "Unnamed")
            last = p.get("last_run")
            if last:
                try:
                    dt = datetime.fromtimestamp(last).strftime("%d %b %H:%M")
                    name = f"{name}  ·  {dt}"
                except Exception:
                    pass
            self._profile_list.addItem(name)
        self._profile_list.blockSignals(False)

    def _on_profile_select(self, idx: int):
        if idx < 0 or idx >= len(self._profiles):
            return
        self._current_idx = idx
        p = self._profiles[idx]
        self._name_edit.setText(p.get("name", ""))
        self._src_edit.setText(p.get("src", ""))
        self._dst_edit.setText(p.get("dst", ""))
        opts = p.get("options", {})
        for key, w in self._option_widgets.items():
            default = next((d for k,_,_,_,d,_ in RSYNC_OPTIONS if k == key), None)
            val = opts.get(key, default)
            if isinstance(w, QCheckBox):   w.setChecked(bool(val))
            elif isinstance(w, QLineEdit): w.setText(str(val) if val else "")
            elif isinstance(w, QSpinBox):  w.setValue(int(val) if val else 0)
        self._update_cmd_preview()

    def _new_profile(self):
        name, ok = QInputDialog.getText(self, "New Profile", "Profile name:")
        if not ok or not name.strip():
            return
        prof = {"name": name.strip(), "src": "", "dst": "", "options": {}}
        self._profiles.append(prof)
        save_rsync_profiles(self._profiles)
        self._populate_profile_list()
        self._profile_list.setCurrentRow(len(self._profiles) - 1)

    def _dup_profile(self):
        idx = self._profile_list.currentRow()
        if idx < 0 or idx >= len(self._profiles):
            return
        prof = copy.deepcopy(self._profiles[idx])
        prof["name"] = prof.get("name", "Profile") + " (copy)"
        self._profiles.append(prof)
        save_rsync_profiles(self._profiles)
        self._populate_profile_list()
        self._profile_list.setCurrentRow(len(self._profiles) - 1)

    def _del_profile(self):
        idx = self._profile_list.currentRow()
        if idx < 0 or idx >= len(self._profiles):
            return
        name = self._profiles[idx].get("name", "this profile")
        ans = QMessageBox.question(self, "Delete Profile", f"Delete '{name}'?",
                                   QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No)
        if ans != QMessageBox.StandardButton.Yes:
            return
        self._profiles.pop(idx)
        save_rsync_profiles(self._profiles)
        self._populate_profile_list()
        new_idx = min(idx, len(self._profiles) - 1)
        if new_idx >= 0:
            self._profile_list.setCurrentRow(new_idx)
        else:
            self._clear_editor()

    def _save_name(self):
        idx = self._profile_list.currentRow()
        if idx < 0 or idx >= len(self._profiles):
            return
        name = self._name_edit.text().strip() or "Unnamed"
        self._profiles[idx]["name"] = name
        self._profile_list.item(idx).setText(name)
        save_rsync_profiles(self._profiles)

    def _save_profile(self):
        idx = self._profile_list.currentRow()
        if idx < 0:
            # No profile selected — create one
            name = self._name_edit.text().strip() or "New Profile"
            self._profiles.append({"name": name, "src": "", "dst": "", "options": {}})
            idx = len(self._profiles) - 1
            self._populate_profile_list()
            self._profile_list.setCurrentRow(idx)
        p = self._profiles[idx]
        p["name"] = self._name_edit.text().strip() or p.get("name", "Unnamed")
        p["src"]  = self._src_edit.text().strip()
        p["dst"]  = self._dst_edit.text().strip()
        p["options"] = self._collect_options()
        save_rsync_profiles(self._profiles)
        self._profile_list.item(idx).setText(p["name"])
        self._output.appendPlainText(f"[Profile '{p['name']}' saved]")

    def _clear_editor(self):
        self._current_idx = -1
        self._name_edit.clear(); self._src_edit.clear(); self._dst_edit.clear()

    # ── Option helpers ────────────────────────────────────────

    def _collect_options(self) -> dict:
        opts = {}
        for key, w in self._option_widgets.items():
            if isinstance(w, QCheckBox):   opts[key] = w.isChecked()
            elif isinstance(w, QLineEdit): opts[key] = w.text().strip()
            elif isinstance(w, QSpinBox):  opts[key] = w.value()
        return opts

    def _set_bool(self, key: str, val: bool):
        w = self._option_widgets.get(key)
        if isinstance(w, QCheckBox): w.setChecked(val)

    def _preset_mirror(self):
        """Mirror source to dest exactly — deletes extraneous files."""
        self._preset_reset()
        for k in ("archive", "verbose", "progress", "delete", "human", "times"):
            self._set_bool(k, True)
        self._update_cmd_preview()

    def _preset_backup(self):
        """Safe backup — never deletes, keeps partial."""
        self._preset_reset()
        for k in ("archive", "verbose", "progress", "partial", "human", "times"):
            self._set_bool(k, True)
        self._update_cmd_preview()

    def _preset_ipod(self):
        """Sync music to iPod/FAT device — fast skip of existing files via size-only + FAT32 timestamp tolerance."""
        self._preset_reset()
        for k in ("archive", "verbose", "progress", "delete", "human",
                  "ignore_existing", "size_only", "fat_compat"):
            self._set_bool(k, True)
        # 2-second window handles FAT32's coarse timestamp resolution
        w = self._option_widgets.get("fat_compat")
        if isinstance(w, QCheckBox):
            w.setChecked(True)
        self._update_cmd_preview()

    def _preset_ssh(self):
        """Remote backup over SSH."""
        self._preset_reset()
        for k in ("archive", "verbose", "compress", "progress", "partial", "human"):
            self._set_bool(k, True)
        w = self._option_widgets.get("ssh_opts")
        if isinstance(w, QLineEdit) and not w.text().strip():
            w.setText("ssh -o StrictHostKeyChecking=no")
        self._update_cmd_preview()

    def _preset_reset(self):
        for key, _, _, typ, default, _ in RSYNC_OPTIONS:
            w = self._option_widgets.get(key)
            if w is None: continue
            if isinstance(w, QCheckBox):   w.setChecked(bool(default))
            elif isinstance(w, QLineEdit): w.setText(str(default) if default else "")
            elif isinstance(w, QSpinBox):  w.setValue(int(default) if default else 0)
        self._update_cmd_preview()

    # ── Path browsing ─────────────────────────────────────────

    def _browse_dir(self, edit: QLineEdit):
        current = edit.text().strip().rstrip("/")
        start   = current if current and Path(current).exists() else str(Path.home())
        path    = QFileDialog.getExistingDirectory(self, "Select folder", start)
        if path:
            edit.setText(path + "/")   # trailing slash = sync contents

    def _swap_paths(self):
        s = self._src_edit.text(); d = self._dst_edit.text()
        self._src_edit.setText(d); self._dst_edit.setText(s)

    # ── Command building ──────────────────────────────────────

    def _build_cmd(self, dry_run_override: bool = False) -> list:
        flags = []
        for key, flag, _, typ, _, _ in RSYNC_OPTIONS:
            if typ != "bool" or not flag: continue
            w = self._option_widgets.get(key)
            if not isinstance(w, QCheckBox): continue
            checked = w.isChecked()
            if key == "dry_run": checked = checked or dry_run_override
            if checked: flags.append(flag)

        # String / int opts
        for key, flag, _, typ, _, _ in RSYNC_OPTIONS:
            if typ == "bool": continue
            w = self._option_widgets.get(key)
            if key == "extra":
                val = w.text().strip() if isinstance(w, QLineEdit) else ""
                if val: flags.extend(val.split())
                continue
            if key == "ssh_opts":
                val = w.text().strip() if isinstance(w, QLineEdit) else ""
                if val: flags += ["-e", val]
                continue
            if key == "bwlimit":
                val = w.value() if isinstance(w, QSpinBox) else 0
                if val > 0: flags.append(f"--bwlimit={val}")
                continue
            if isinstance(w, QLineEdit):
                val = w.text().strip()
                if val and flag: flags += [flag, val]
            elif isinstance(w, QSpinBox):
                val = w.value()
                if val and flag: flags += [flag, str(val)]

        src = self._src_edit.text().strip()
        dst = self._dst_edit.text().strip()
        return ["rsync"] + flags + ([src, dst] if src or dst else ["<source>", "<destination>"])


    # ── Revert support ─────────────────────────────────────────

    def _last_sync_path(self) -> Path:
        # keep per-user config, not in project folder
        return CONFIG_DIR / "rsync_last_sync.json"

    def _load_last_sync_info(self) -> Optional[dict]:
        try:
            p = self._last_sync_path()
            if p.exists():
                return json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            pass
        return None

    def _save_last_sync_info(self, info: dict):
        try:
            self._last_sync_path().write_text(json.dumps(info, indent=2), encoding="utf-8")
            self._last_sync_info = info
        except Exception:
            pass
        try:
            if hasattr(self, "_revert_btn"):
                self._revert_btn.setEnabled(bool(self._last_sync_info))
        except Exception:
            pass

    def _make_backup_dir(self) -> Path:
        base = CONFIG_DIR / "rsync_backups"
        base.mkdir(parents=True, exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d-%H%M%S")
        d = base / ts
        d.mkdir(parents=True, exist_ok=True)
        return d

    def _update_cmd_preview(self):
        try:
            dry = self._dry_run_toggle.isChecked() if hasattr(self, "_dry_run_toggle") else False
            cmd = self._build_cmd(dry_run_override=dry)
            self._cmd_preview.setText(" \\\n  ".join(cmd))
        except Exception as e:
            self._cmd_preview.setText(f"(error: {e})")

    def _copy_cmd(self):
        try:
            dry = self._dry_run_toggle.isChecked()
            cmd = self._build_cmd(dry_run_override=dry)
            QApplication.clipboard().setText(" ".join(cmd))
            self._output.appendPlainText("[Command copied to clipboard]")
        except Exception: pass

    # ── Run / Stop ────────────────────────────────────────────

    def _run(self):
        if not shutil.which("rsync"):
            QMessageBox.critical(self, "rsync not found",
                "rsync is not installed or not in PATH.\n\n"
                "Install it with your package manager:\n"
                "  Linux:  sudo apt install rsync\n"
                "  macOS:  brew install rsync")
            return

        src = self._src_edit.text().strip()
        dst = self._dst_edit.text().strip()
        if not src or not dst:
            QMessageBox.warning(self, "Missing paths",
                "Set both source and destination before running."); return

        dry = self._dry_run_toggle.isChecked()
        cmd = self._build_cmd(dry_run_override=dry)

        # Warn about --delete
        delete_checked = isinstance(self._option_widgets.get("delete"), QCheckBox) and \
                         self._option_widgets["delete"].isChecked()
        if delete_checked and not dry:
            ans = QMessageBox.warning(self, "Destructive operation",
                f"--delete is enabled.\n\nFiles at:\n  {dst}\nthat don't exist at source will be PERMANENTLY DELETED.\n\n"
                "Continue?", QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.Cancel)
            if ans != QMessageBox.StandardButton.Yes: return

        self._output.clear()
        self._output.appendPlainText("$ " + " ".join(cmd))
        if dry: self._output.appendPlainText("[DRY RUN — no changes will be made]\n")
        else:   self._output.appendPlainText("")

        self._run_btn.setEnabled(False); self._stop_btn.setEnabled(True)
        self._pause_btn.setEnabled(True); self._pause_btn.setChecked(False)
        self._pause_btn.setText("⏸  Pause")
        self._status_dot.set_color(_current_theme["warning"]); self._status_lbl.setText("Running…")

        # ── Sanitize step (runs before main rsync if enabled) ────
        sanitize_on = isinstance(self._option_widgets.get("sanitize"), QCheckBox) and                       self._option_widgets["sanitize"].isChecked()
        if sanitize_on and not dry:
            self._output.appendPlainText("── Sanitize filenames step ──")
            # Build flags without src/dst for the sanitize worker
            flags_only = [f for f in cmd[1:] if f not in (src, dst)]
            self._sanitize_worker = SanitizeWorker(src, dst, flags_only, parent=self)
            self._sanitize_worker.progress.connect(
                lambda msg: (self._output.appendPlainText(msg), self._output.ensureCursorVisible()))
            self._sanitize_worker.finished.connect(self._on_sanitize_done)
            self._sanitize_worker.start()
            return   # _on_sanitize_done will kick off the main rsync

        self._start_main_rsync(cmd)

    def _on_sanitize_done(self, exit_code: int):
        if exit_code not in (0, 23, 24):
            self._output.appendPlainText(f"\n✗ Sanitize step failed (exit {exit_code}) — aborting sync.")
            self._on_finished(exit_code, None)
            return
        self._output.appendPlainText("── Main rsync ──")
        src = self._src_edit.text().strip()
        dst = self._dst_edit.text().strip()
        dry = self._dry_run_toggle.isChecked()
        cmd = self._build_cmd(dry_run_override=dry)
        self._start_main_rsync(cmd)

    def _start_main_rsync(self, cmd):
        self._process = QProcess(self)
        self._process.setProcessChannelMode(QProcess.ProcessChannelMode.MergedChannels)
        self._process.readyReadStandardOutput.connect(self._on_output)
        self._process.finished.connect(self._on_finished)
        self._process.start(cmd[0], cmd[1:])

        if not self._process.waitForStarted(3000):
            self._output.appendPlainText("Error: failed to start rsync process.")
            self._run_btn.setEnabled(True); self._stop_btn.setEnabled(False)
            self._status_dot.set_color(_current_theme["danger"]); self._status_lbl.setText("Failed to start")

    def _on_output(self):
        raw  = self._process.readAllStandardOutput().data()
        text = raw.decode("utf-8", errors="replace")
        # Filter out carriage-return progress lines for cleaner display
        lines = text.split("\n")
        clean = []
        for line in lines:
            # rsync uses \r to overwrite progress — keep last segment
            if "\r" in line:
                line = line.split("\r")[-1]
            if line.strip():
                clean.append(line)
        if clean:
            self._output.moveCursor(self._output.textCursor().MoveOperation.End)
            self._output.insertPlainText("\n".join(clean) + "\n")
            self._output.ensureCursorVisible()

    def _stop(self):
        if self._process and self._process.state() != QProcess.ProcessState.NotRunning:
            self._process.kill()
            self._output.appendPlainText("\n[Stopped by user]")
            self._status_dot.set_color(_current_theme["txt2"]); self._status_lbl.setText("Stopped")
        self._pause_btn.setEnabled(False); self._pause_btn.setChecked(False)
        self._pause_btn.setText("⏸  Pause")

    def _toggle_pause_rsync(self):
        """Send SIGSTOP/SIGCONT to the rsync process to pause/resume it."""
        import signal as _signal
        if not self._process or self._process.state() == QProcess.ProcessState.NotRunning:
            self._pause_btn.setChecked(False)
            return
        pid = self._process.processId()
        if pid <= 0:
            return
        if self._pause_btn.isChecked():
            # Pause
            try:
                os.kill(pid, _signal.SIGSTOP)
                self._pause_btn.setText("▶  Resume")
                self._status_dot.set_color(_current_theme["warning"])
                self._status_lbl.setText("Paused")
                self._output.appendPlainText("\n[Paused]")
            except Exception:
                self._pause_btn.setChecked(False)
        else:
            # Resume
            try:
                os.kill(pid, _signal.SIGCONT)
                self._pause_btn.setText("⏸  Pause")
                self._status_dot.set_color(_current_theme["warning"])
                self._status_lbl.setText("Running…")
                self._output.appendPlainText("[Resumed]")
            except Exception:
                pass

    def _on_finished(self, exit_code: int, _exit_status):
        self._run_btn.setEnabled(True); self._stop_btn.setEnabled(False)
        self._pause_btn.setEnabled(False); self._pause_btn.setChecked(False)
        self._pause_btn.setText("⏸  Pause")
        t = _current_theme
        msg = {
            0:  ("✓  Completed successfully.",     t["success"]),
            1:  ("✗  Syntax or usage error.",       t["danger"]),
            2:  ("✗  Protocol incompatibility.",    t["danger"]),
            3:  ("✗  Errors selecting I/O files.",  t["danger"]),
            10: ("✗  Error in socket I/O.",         t["danger"]),
            11: ("✗  Error in file I/O.",           t["danger"]),
            20: ("⚠  Stopped by user signal.",      t["warning"]),
            23: ("⚠  Partial transfer — some files skipped.", t["warning"]),
            24: ("⚠  Partial transfer — source files vanished.", t["warning"]),
            25: ("⚠  The --max-delete limit stopped deletions.", t["warning"]),
        }.get(exit_code, (f"✗  rsync exited with code {exit_code}.", t["danger"]))
        self._output.appendPlainText(f"\n{msg[0]}")
        self._status_dot.set_color(msg[1])
        self._status_lbl.setText("Done" if exit_code == 0 else f"Exit {exit_code}")
        # Auto-save profile after a successful real run
        if exit_code == 0 and not self._dry_run_toggle.isChecked():
            idx = self._profile_list.currentRow()
            if 0 <= idx < len(self._profiles):
                self._profiles[idx]["last_run"] = int(time.time())
            self._save_profile()
            self._populate_profile_list()
            self._profile_list.setCurrentRow(idx)


# ─────────────────────────────────────────────────────────────
#  PAGE: PLATFORMS
# ─────────────────────────────────────────────────────────────

class PlatformsPage(QWidget):
    auth_changed = pyqtSignal()

    def __init__(self, conf_ref: list, parent=None):
        super().__init__(parent)
        self.conf_ref = conf_ref
        self._tokens: dict[str, Optional[str]] = {P_LASTFM: None, P_LIBREFM: None}
        self._build()

    def conf(self) -> dict: return self.conf_ref[0]

    def _build(self):
        outer = QVBoxLayout(self); outer.setContentsMargins(0,0,0,0)
        scroll = QScrollArea(); scroll.setWidgetResizable(True)
        scroll.setFrameShape(QFrame.Shape.NoFrame)
        inner = QWidget()
        root  = QVBoxLayout(inner); root.setContentsMargins(28,24,28,20); root.setSpacing(20)
        title = QLabel("Platforms"); title.setObjectName("heading"); root.addWidget(title)
        sub = QLabel("Connect your scrobbling accounts and submit to each platform independently.")
        sub.setObjectName("secondary"); root.addWidget(sub)
        root.addWidget(self._make_lfm_card(P_LASTFM,  "Last.fm",  LASTFM_API,  LASTFM_AUTH,
                                           "https://www.last.fm/api/account/create"))
        root.addWidget(self._make_lfm_card(P_LIBREFM, "Libre.fm", LIBREFM_API, LIBREFM_AUTH,
                                           "https://libre.fm/join.php"))
        root.addWidget(self._make_lbz_card())
        root.addStretch()
        scroll.setWidget(inner); outer.addWidget(scroll)

    def _make_lfm_card(self, plat, display, api_url, auth_url, new_url) -> QWidget:
        card = QWidget(); card.setObjectName("card")
        vb = QVBoxLayout(card); vb.setContentsMargins(18,14,18,16); vb.setSpacing(12)
        hdr = QHBoxLayout(); badge = PlatformBadge(plat); hdr.addWidget(badge); hdr.addStretch()
        dot = StatusDot(); status_lbl = QLabel("Not connected"); status_lbl.setObjectName("secondary")
        hdr.addWidget(dot); hdr.addWidget(status_lbl); vb.addLayout(hdr)
        grid = QGridLayout(); grid.setHorizontalSpacing(12); grid.setVerticalSpacing(8)
        grid.setColumnMinimumWidth(0, 110)
        key_lbl = QLabel("API Key"); key_lbl.setObjectName("secondary")
        key_in = QLineEdit(self.conf().get(f"{_pk(plat)}_key", ""))
        key_in.setPlaceholderText("32-character hex key from last.fm/api")
        grid.addWidget(key_lbl, 0, 0, Qt.AlignmentFlag.AlignRight|Qt.AlignmentFlag.AlignVCenter)
        grid.addWidget(key_in, 0, 1)
        sec_lbl = QLabel("Shared Secret"); sec_lbl.setObjectName("secondary")
        sec_in = QLineEdit(self.conf().get(f"{_pk(plat)}_secret", ""))
        sec_in.setPlaceholderText("Shared secret"); sec_in.setEchoMode(QLineEdit.EchoMode.Password)
        grid.addWidget(sec_lbl, 1, 0, Qt.AlignmentFlag.AlignRight|Qt.AlignmentFlag.AlignVCenter)
        grid.addWidget(sec_in, 1, 1); vb.addLayout(grid)
        new_link = QLabel(f'<a href="{new_url}" style="color:{_current_theme["accent"]};font-size:12px;">'
                          f'No account? Register here →</a>')
        new_link.setOpenExternalLinks(False)
        new_link.linkActivated.connect(lambda url: open_url(QUrl(url)))
        vb.addWidget(new_link)
        banner = Banner(); vb.addWidget(banner)

        btn_row = QHBoxLayout(); btn_row.setSpacing(8)
        connect_btn = QPushButton(f"Connect to {display}  →"); connect_btn.setObjectName("primary")
        connect_btn.setMinimumWidth(180)
        logout_btn = QPushButton("Disconnect"); logout_btn.setObjectName("danger")
        btn_row.addWidget(connect_btn); btn_row.addStretch(); btn_row.addWidget(logout_btn)
        vb.addLayout(btn_row)

        # Status note for api key tip
        api_note = QLabel("Enter your API key & secret above, then click Connect.")
        api_note.setObjectName("muted"); api_note.setWordWrap(True)
        vb.addWidget(api_note)

        def refresh_ui():
            sess = load_session(plat)
            if sess:
                dot.set_color(_current_theme["success"]); status_lbl.setText("Connected")
                status_lbl.setStyleSheet(f"color:{_current_theme['success']};font-size:13px;")
                banner.set(f"Connected to {display}. You're all set to scrobble.", "success")
                connect_btn.setEnabled(False); logout_btn.setEnabled(True)
                api_note.setVisible(False)
            else:
                dot.set_color(_current_theme["txt2"]); status_lbl.setText("Not connected")
                status_lbl.setStyleSheet(f"color:{_current_theme['txt2']};font-size:13px;")
                banner.set("Enter your API credentials, then click Connect to authenticate.", "muted")
                connect_btn.setEnabled(True); logout_btn.setEnabled(False)
                api_note.setVisible(True)

        def save_creds():
            c = self.conf()
            c[f"{_pk(plat)}_key"]    = key_in.text().strip()
            c[f"{_pk(plat)}_secret"] = sec_in.text().strip()
            if plat == P_LASTFM:
                c["api_key"] = key_in.text().strip(); c["api_secret"] = sec_in.text().strip()
            save_conf(c)

        def do_connect():
            save_creds()
            k = key_in.text().strip(); s = sec_in.text().strip()
            if not k or not s:
                QMessageBox.warning(self, "Missing credentials",
                    "Enter your API key and shared secret first.\n\n"
                    f"Get them at: {'last.fm/api/account/create' if plat == P_LASTFM else 'libre.fm/api/account/create'}")
                return
            # Step 1: get a token
            banner.set(f"Getting auth token from {display}…", "info")
            try:
                resp = lfm_call({"method":"auth.getToken","api_key":k,"format":"json"}, api_url)
                if "error" in resp:
                    raise Exception(resp.get("message", "Could not get token"))
                token = resp["token"]
                self._tokens[plat] = token
            except Exception as e:
                banner.set(f"Error: {e}", "danger")
                QMessageBox.critical(self, "Auth error", str(e))
                return

            # Step 2: open the auth URL in an embedded browser
            full_auth_url = f"{auth_url}?api_key={k}&token={token}"
            banner.set(f"Opening {display} in embedded browser — log in and click Authorise.", "warning")

            # Success detection: after user approves, last.fm redirects to last.fm/user/<name>
            # or just navigates away from the auth page
            success_frag = "last.fm/user/" if plat == P_LASTFM else "libre.fm/user/"
            dlg = WebAuthDialog(full_auth_url, display, success_url_fragment=success_frag, parent=self)

            def on_approved():
                # Step 3: exchange token for session key
                banner.set(f"Authorised! Completing login…", "info")
                params = {"method":"auth.getSession","api_key":k,"token":token}
                params["api_sig"] = api_sig(params, s); params["format"] = "json"
                try:
                    r = lfm_call(params, api_url)
                    if "error" in r:
                        code = r.get("error", 0); msg = r.get("message", "Error")
                        if code == 14:
                            msg = (f"Not authorized yet — make sure you clicked 'Authorise' on {display}.\n\n"
                                   "Try connecting again.")
                        raise Exception(msg)
                    save_session(plat, r["session"]["key"])
                    self._tokens[plat] = None
                    refresh_ui(); self.auth_changed.emit()
                    banner.set(f"✓ Successfully connected to {display}!", "success")
                except Exception as e:
                    banner.set(f"Login failed: {e}", "danger")
                    QMessageBox.critical(self, "Login error", str(e))

            dlg.auth_approved.connect(on_approved)
            result = dlg.exec()
            # If dialog closed without auto-auth (fallback browser), trigger manually
            if result == QDialog.DialogCode.Accepted and not load_session(plat):
                on_approved()

        def logout():
            clear_session(plat); refresh_ui(); self.auth_changed.emit()

        connect_btn.clicked.connect(do_connect)
        logout_btn.clicked.connect(logout)
        refresh_ui()
        return card

    def _make_lbz_card(self) -> QWidget:
        card = QWidget(); card.setObjectName("card")
        vb = QVBoxLayout(card); vb.setContentsMargins(18,14,18,16); vb.setSpacing(12)
        hdr = QHBoxLayout(); badge = PlatformBadge(P_LISTENBRAINZ); hdr.addWidget(badge); hdr.addStretch()
        self._lbz_dot = StatusDot(); self._lbz_status = QLabel("Not connected")
        self._lbz_status.setObjectName("secondary"); hdr.addWidget(self._lbz_dot); hdr.addWidget(self._lbz_status)
        vb.addLayout(hdr)

        # Open profile page button
        open_site_btn = QPushButton("Open ListenBrainz Profile Page  →")
        open_site_btn.setObjectName("primary"); open_site_btn.setFixedHeight(36)
        open_site_btn.clicked.connect(self._open_lbz_site)
        vb.addWidget(open_site_btn)

        note_lbl = QLabel(
            "Log into ListenBrainz in the window above, then copy your User Token "
            "from your profile page and paste it below."
        )
        note_lbl.setObjectName("muted"); note_lbl.setWordWrap(True)
        vb.addWidget(note_lbl)

        token_row = QHBoxLayout()
        tok_lbl = QLabel("User Token"); tok_lbl.setObjectName("secondary"); tok_lbl.setFixedWidth(110)
        self._lbz_tok_in = QLineEdit(self.conf().get("lbz_token", ""))
        self._lbz_tok_in.setPlaceholderText("Paste your token from listenbrainz.org/profile/")
        self._lbz_tok_in.setEchoMode(QLineEdit.EchoMode.Password)
        save_btn = QPushButton("Save Token"); save_btn.setObjectName("primary"); save_btn.setFixedWidth(100)
        save_btn.clicked.connect(self._save_lbz)
        token_row.addWidget(tok_lbl); token_row.addWidget(self._lbz_tok_in); token_row.addWidget(save_btn)
        vb.addLayout(token_row)

        self._lbz_banner = Banner(); vb.addWidget(self._lbz_banner)
        clear_btn = QPushButton("Remove token"); clear_btn.setObjectName("danger"); clear_btn.setFixedWidth(120)
        clear_btn.clicked.connect(self._clear_lbz)
        vb.addWidget(clear_btn, alignment=Qt.AlignmentFlag.AlignRight)
        self._refresh_lbz_ui(); return card

    def _open_lbz_site(self):
        dlg = WebAuthDialog(
            "https://listenbrainz.org/profile/",
            "ListenBrainz",
            success_url_fragment="",   # no auto-detect for LBZ, manual token copy
            parent=self,
        )
        dlg.exec()

    def _save_lbz(self):
        token = self._lbz_tok_in.text().strip()
        if not token: QMessageBox.warning(self, "Empty token", "Paste your token first."); return
        c = self.conf(); c["lbz_token"] = token; save_conf(c); self._refresh_lbz_ui(); self.auth_changed.emit()

    def _clear_lbz(self):
        c = self.conf(); c.pop("lbz_token", None); save_conf(c); self._lbz_tok_in.clear()
        self._refresh_lbz_ui(); self.auth_changed.emit()

    def _refresh_lbz_ui(self):
        token = self.conf().get("lbz_token","")
        if token:
            self._lbz_dot.set_color(_current_theme["success"]); self._lbz_status.setText("Token saved")
            self._lbz_status.setStyleSheet(f"color:{_current_theme['success']};font-size:13px;")
            self._lbz_banner.set("Token saved — ready to submit to ListenBrainz.", "success")
        else:
            self._lbz_dot.set_color(_current_theme["txt2"]); self._lbz_status.setText("No token")
            self._lbz_status.setStyleSheet(f"color:{_current_theme['txt2']};font-size:13px;")
            self._lbz_banner.set("Paste your user token and click Save.", "muted")


# ─────────────────────────────────────────────────────────────
#  PAGE: APPEARANCE
# ─────────────────────────────────────────────────────────────

class AppearancePage(QWidget):
    theme_changed = pyqtSignal(dict)
    ACCENTS = {
        "Amber":    ("#c8861a","#e09d30"), "Teal":    ("#1a9a8a","#22b8a6"),
        "Crimson":  ("#c02040","#de2a52"), "Violet":  ("#7c3fc0","#9d52e0"),
        "Cobalt":   ("#1a66c8","#2882f0"), "Sage":    ("#4a8c5c","#5aaa70"),
        "Rose":     ("#c04080","#e05098"), "Slate":   ("#607090","#7888a8"),
        "Sunset":   ("#d4601a","#f07830"), "Forest":  ("#2d7a3a","#3aaa4e"),
    }

    def __init__(self, conf_ref: list, parent=None):
        super().__init__(parent)
        self.conf_ref = conf_ref
        self._build()

    def conf(self): return self.conf_ref[0]

    def _build(self):
        outer = QVBoxLayout(self); outer.setContentsMargins(0,0,0,0)
        scroll = QScrollArea(); scroll.setWidgetResizable(True); scroll.setFrameShape(QFrame.Shape.NoFrame)
        inner = QWidget(); root = QVBoxLayout(inner); root.setContentsMargins(28,24,28,20); root.setSpacing(20)

        title = QLabel("Appearance"); title.setObjectName("heading"); root.addWidget(title)

        # Color scheme card


        # Accent card
        ac_card = QWidget(); ac_card.setObjectName("card")
        ac = QVBoxLayout(ac_card); ac.setContentsMargins(18,14,18,16); ac.setSpacing(14)
        ac.addWidget(SectionLabel("Accent color"))
        presets = QHBoxLayout(); presets.setSpacing(8)
        for name, (c1, c2) in self.ACCENTS.items():
            sw = QPushButton(); sw.setFixedSize(32,32); sw.setToolTip(name)
            sw.setStyleSheet(f"background:{c1};border:2px solid rgba(255,255,255,0.08);border-radius:7px;")
            sw.clicked.connect(lambda _, a=c1, b=c2: self._apply_accent(a, b))
            presets.addWidget(sw)
        presets.addStretch(); ac.addLayout(presets)
        custom_row = QHBoxLayout(); custom_row.setSpacing(10)
        custom_lbl = QLabel("Custom:"); custom_lbl.setObjectName("secondary"); custom_lbl.setFixedWidth(65)
        self._accent_swatch = ColorSwatch(_current_theme["accent"])
        self._accent_swatch.color_changed.connect(lambda c: self._apply_accent(c, c))
        self._accent_hex = QLabel(_current_theme["accent"]); self._accent_hex.setObjectName("mono")
        custom_row.addWidget(custom_lbl); custom_row.addWidget(self._accent_swatch)
        custom_row.addWidget(self._accent_hex); custom_row.addStretch()
        ac.addLayout(custom_row); root.addWidget(ac_card)

        root.addStretch()
        scroll.setWidget(inner); outer.addWidget(scroll)

    def _apply_base(self, theme: dict, name: str):
        global _current_theme
        _current_theme = theme.copy()
        c = self.conf(); c["theme"] = name.lower()
        if "custom_accent" in c:
            a = c["custom_accent"]
            _current_theme["accent"] = a; _current_theme["accent2"] = a
            _current_theme["accentlo"] = a+"1a"; _current_theme["bordhi"] = a+"55"
        save_conf(c); self._sync_swatches(); self.theme_changed.emit(_current_theme)

    def _apply_accent(self, c1: str, c2: str):
        global _current_theme
        _current_theme["accent"] = c1; _current_theme["accent2"] = c2
        _current_theme["accentlo"] = c1+"1a"; _current_theme["bordhi"] = c1+"55"
        conf = self.conf(); conf["custom_accent"] = c1; conf["custom_accent2"] = c2; save_conf(conf)
        self._accent_swatch.set_color(c1); self._accent_hex.setText(c1)
        self.theme_changed.emit(_current_theme)

    def _reset_colors(self):
        global _current_theme
        _current_theme = DARK.copy()
        conf = self.conf()
        conf.pop("custom_accent", None); conf.pop("custom_accent2", None); conf.pop("color_overrides", None)
        save_conf(conf); self._sync_swatches(); self.theme_changed.emit(_current_theme)

    def _sync_swatches(self):
        self._accent_swatch.set_color(_current_theme["accent"]); self._accent_hex.setText(_current_theme["accent"])



# ─────────────────────────────────────────────────────────────
#  PAGE: SETTINGS  (merged Platforms + Appearance)
# ─────────────────────────────────────────────────────────────

class SettingsPage(QWidget):
    """Unified Settings page — Platforms and Appearance in one place."""
    auth_changed  = pyqtSignal()
    theme_changed = pyqtSignal(dict)

    def __init__(self, conf_ref: list, parent=None):
        super().__init__(parent)
        self.conf_ref = conf_ref
        self._tokens: dict[str, Optional[str]] = {P_LASTFM: None, P_LIBREFM: None}
        self._build()

    def conf(self) -> dict: return self.conf_ref[0]

    # ── Build ─────────────────────────────────────────────────

    def _build(self):
        outer = QVBoxLayout(self)
        outer.setContentsMargins(0, 0, 0, 0)
        outer.setSpacing(0)

        # Page header
        hdr = QWidget()
        hdr.setFixedHeight(56)
        hdr.setStyleSheet("background:rgba(5,7,11,0.65); border-bottom:1px solid rgba(255,255,255,0.07);")
        hb = QHBoxLayout(hdr)
        hb.setContentsMargins(28, 0, 28, 0)
        title = QLabel("Settings")
        tf = QFont(); tf.setPointSize(14); tf.setBold(True)
        title.setFont(tf)
        title.setStyleSheet("color:#fff;background:transparent;")
        hb.addWidget(title)
        hb.addStretch()
        outer.addWidget(hdr)

        # Tab bar
        tab_bar = QWidget()
        tab_bar.setFixedHeight(40)
        tab_bar.setStyleSheet("background:rgba(5,7,11,0.65); border-bottom:1px solid rgba(255,255,255,0.07);")
        tb = QHBoxLayout(tab_bar)
        tb.setContentsMargins(24, 0, 24, 0)
        tb.setSpacing(0)
        self._tab_btns: list[QPushButton] = []
        for i, lbl in enumerate(["Platforms", "Appearance", "Misc"]):
            btn = QPushButton(lbl)
            btn.setFlat(True)
            btn.setFixedHeight(40)
            btn.setFixedWidth(110)
            btn.setCheckable(True)
            btn.setChecked(i == 0)
            btn.setObjectName("tabbtn")
            btn.clicked.connect(lambda _, x=i: self._switch_tab(x))
            self._tab_btns.append(btn)
            tb.addWidget(btn)
        tb.addStretch()
        outer.addWidget(tab_bar)

        # Stacked content
        self._stack = QStackedWidget()
        self._stack.addWidget(self._build_platforms_tab())
        self._stack.addWidget(self._build_appearance_tab())
        self._stack.addWidget(self._build_misc_tab())
        outer.addWidget(self._stack, stretch=1)

    def _switch_tab(self, idx: int):
        for i, btn in enumerate(self._tab_btns):
            btn.setChecked(i == idx)
        self._stack.setCurrentIndex(idx)

    # ── Platforms tab ─────────────────────────────────────────

    def _build_platforms_tab(self) -> QWidget:
        w = QWidget()
        outer = QVBoxLayout(w); outer.setContentsMargins(0,0,0,0)
        scroll = QScrollArea(); scroll.setWidgetResizable(True)
        scroll.setFrameShape(QFrame.Shape.NoFrame)
        inner = QWidget()
        root  = QVBoxLayout(inner); root.setContentsMargins(28,24,28,20); root.setSpacing(20)

        sub = QLabel("Connect your scrobbling accounts and submit to each platform independently.")
        sub.setObjectName("secondary"); root.addWidget(sub)
        root.addWidget(self._make_lfm_card(P_LASTFM,  "Last.fm",  LASTFM_API,  LASTFM_AUTH,
                                           "https://www.last.fm/api/account/create"))
        root.addWidget(self._make_lfm_card(P_LIBREFM, "Libre.fm", LIBREFM_API, LIBREFM_AUTH,
                                           "https://libre.fm/join.php"))
        root.addWidget(self._make_lbz_card())
        root.addStretch()
        scroll.setWidget(inner); outer.addWidget(scroll)
        return w

    def _make_lfm_card(self, plat, display, api_url, auth_url, new_url) -> QWidget:
        card = QWidget(); card.setObjectName("card")
        vb = QVBoxLayout(card); vb.setContentsMargins(18,14,18,16); vb.setSpacing(12)
        hdr = QHBoxLayout(); badge = PlatformBadge(plat); hdr.addWidget(badge); hdr.addStretch()
        dot = StatusDot(); status_lbl = QLabel("Not connected"); status_lbl.setObjectName("secondary")
        hdr.addWidget(dot); hdr.addWidget(status_lbl); vb.addLayout(hdr)
        grid = QGridLayout(); grid.setHorizontalSpacing(12); grid.setVerticalSpacing(8)
        grid.setColumnMinimumWidth(0, 110)
        key_lbl = QLabel("API Key"); key_lbl.setObjectName("secondary")
        key_in = QLineEdit(self.conf().get(f"{_pk(plat)}_key", ""))
        key_in.setPlaceholderText("32-character hex key")
        grid.addWidget(key_lbl, 0, 0, Qt.AlignmentFlag.AlignRight|Qt.AlignmentFlag.AlignVCenter)
        grid.addWidget(key_in, 0, 1)
        sec_lbl = QLabel("Shared Secret"); sec_lbl.setObjectName("secondary")
        sec_in = QLineEdit(self.conf().get(f"{_pk(plat)}_secret", ""))
        sec_in.setPlaceholderText("Shared secret"); sec_in.setEchoMode(QLineEdit.EchoMode.Password)
        grid.addWidget(sec_lbl, 1, 0, Qt.AlignmentFlag.AlignRight|Qt.AlignmentFlag.AlignVCenter)
        grid.addWidget(sec_in, 1, 1); vb.addLayout(grid)
        new_link = QLabel(f'<a href="{new_url}" style="color:{_current_theme["accent"]};font-size:12px;">'
                          f'No account? Register here →</a>')
        new_link.setOpenExternalLinks(False)
        new_link.linkActivated.connect(lambda url: open_url(QUrl(url)))
        vb.addWidget(new_link)
        banner = Banner(); vb.addWidget(banner)

        btn_row = QHBoxLayout(); btn_row.setSpacing(8)
        connect_btn = QPushButton(f"Connect to {display}  →"); connect_btn.setObjectName("primary")
        connect_btn.setMinimumWidth(180)
        logout_btn = QPushButton("Disconnect"); logout_btn.setObjectName("danger")
        btn_row.addWidget(connect_btn); btn_row.addStretch(); btn_row.addWidget(logout_btn)
        vb.addLayout(btn_row)

        api_note = QLabel("Enter your API key & secret above, then click Connect.")
        api_note.setObjectName("muted"); api_note.setWordWrap(True)
        vb.addWidget(api_note)

        def refresh_ui():
            sess = load_session(plat)
            if sess:
                dot.set_color(_current_theme["success"]); status_lbl.setText("Connected")
                status_lbl.setStyleSheet(f"color:{_current_theme['success']};font-size:13px;")
                banner.set(f"Connected to {display}. You're all set to scrobble.", "success")
                connect_btn.setEnabled(False); logout_btn.setEnabled(True)
                api_note.setVisible(False)
            else:
                dot.set_color(_current_theme["txt2"]); status_lbl.setText("Not connected")
                status_lbl.setStyleSheet(f"color:{_current_theme['txt2']};font-size:13px;")
                banner.set("Enter your API credentials, then click Connect to authenticate.", "muted")
                connect_btn.setEnabled(True); logout_btn.setEnabled(False)
                api_note.setVisible(True)

        def save_creds():
            c = self.conf()
            c[f"{_pk(plat)}_key"]    = key_in.text().strip()
            c[f"{_pk(plat)}_secret"] = sec_in.text().strip()
            if plat == P_LASTFM:
                c["api_key"] = key_in.text().strip(); c["api_secret"] = sec_in.text().strip()
            save_conf(c)

        def do_connect():
            save_creds()
            k = key_in.text().strip(); s = sec_in.text().strip()
            if not k or not s:
                QMessageBox.warning(self, "Missing credentials",
                    "Enter your API key and shared secret first.")
                return
            banner.set(f"Getting auth token from {display}…", "info")
            try:
                resp = lfm_call({"method":"auth.getToken","api_key":k,"format":"json"}, api_url)
                if "error" in resp:
                    raise Exception(resp.get("message", "Could not get token"))
                token = resp["token"]
                self._tokens[plat] = token
            except Exception as e:
                banner.set(f"Error: {e}", "danger")
                QMessageBox.critical(self, "Auth error", str(e))
                return

            full_auth_url = f"{auth_url}?api_key={k}&token={token}"
            banner.set(f"Opening {display} in embedded browser — log in and click Authorise.", "warning")
            success_frag = "last.fm/user/" if plat == P_LASTFM else "libre.fm/user/"
            dlg = WebAuthDialog(full_auth_url, display, success_url_fragment=success_frag, parent=self)

            def on_approved():
                banner.set("Authorised! Completing login…", "info")
                params = {"method":"auth.getSession","api_key":k,"token":token}
                params["api_sig"] = api_sig(params, s); params["format"] = "json"
                try:
                    r = lfm_call(params, api_url)
                    if "error" in r:
                        code = r.get("error", 0); msg = r.get("message", "Error")
                        if code == 14:
                            msg = f"Not authorized yet — make sure you clicked 'Authorise' on {display}."
                        raise Exception(msg)
                    save_session(plat, r["session"]["key"])
                    self._tokens[plat] = None
                    refresh_ui(); self.auth_changed.emit()
                    banner.set(f"✓ Successfully connected to {display}!", "success")
                except Exception as e:
                    banner.set(f"Login failed: {e}", "danger")
                    QMessageBox.critical(self, "Login error", str(e))

            dlg.auth_approved.connect(on_approved)
            result = dlg.exec()
            if result == QDialog.DialogCode.Accepted and not load_session(plat):
                on_approved()

        def logout():
            clear_session(plat); refresh_ui(); self.auth_changed.emit()

        connect_btn.clicked.connect(do_connect)
        logout_btn.clicked.connect(logout)
        refresh_ui()
        return card

    def _make_lbz_card(self) -> QWidget:
        card = QWidget(); card.setObjectName("card")
        vb = QVBoxLayout(card); vb.setContentsMargins(18,14,18,16); vb.setSpacing(12)
        hdr = QHBoxLayout(); badge = PlatformBadge(P_LISTENBRAINZ); hdr.addWidget(badge); hdr.addStretch()
        self._lbz_dot = StatusDot()
        self._lbz_status = QLabel("No token"); self._lbz_status.setObjectName("secondary")
        hdr.addWidget(self._lbz_dot); hdr.addWidget(self._lbz_status); vb.addLayout(hdr)

        tok_lbl = QLabel("User Token"); tok_lbl.setObjectName("secondary")
        vb.addWidget(tok_lbl)
        self._lbz_tok_in = QLineEdit(self.conf().get("lbz_token",""))
        self._lbz_tok_in.setPlaceholderText("Paste your ListenBrainz user token here…")
        self._lbz_tok_in.setEchoMode(QLineEdit.EchoMode.Password)
        vb.addWidget(self._lbz_tok_in)
        link = QLabel(f'<a href="https://listenbrainz.org/profile/" style="color:{tok("accent")};font-size:12px;">'
                      'Get your token at listenbrainz.org/profile/ →</a>')
        link.setOpenExternalLinks(False)
        link.linkActivated.connect(lambda url: open_url(QUrl(url)))
        vb.addWidget(link)
        self._lbz_banner = Banner(); vb.addWidget(self._lbz_banner)
        btn_row = QHBoxLayout(); btn_row.setSpacing(8)
        save_btn = QPushButton("Save Token"); save_btn.setObjectName("primary"); save_btn.setMinimumWidth(130)
        clear_btn = QPushButton("Clear"); clear_btn.setObjectName("danger")
        btn_row.addWidget(save_btn); btn_row.addStretch(); btn_row.addWidget(clear_btn)
        vb.addLayout(btn_row)
        save_btn.clicked.connect(self._save_lbz)
        clear_btn.clicked.connect(self._clear_lbz)
        self._refresh_lbz_ui()
        return card

    def _save_lbz(self):
        tok_val = self._lbz_tok_in.text().strip()
        if not tok_val:
            QMessageBox.warning(self, "No token", "Paste your ListenBrainz user token first."); return
        c = self.conf(); c["lbz_token"] = tok_val; save_conf(c)
        self._refresh_lbz_ui(); self.auth_changed.emit()

    def _clear_lbz(self):
        c = self.conf(); c.pop("lbz_token", None); save_conf(c); self._lbz_tok_in.clear()
        self._refresh_lbz_ui(); self.auth_changed.emit()

    def _refresh_lbz_ui(self):
        token = self.conf().get("lbz_token","")
        if token:
            self._lbz_dot.set_color(_current_theme["success"]); self._lbz_status.setText("Token saved")
            self._lbz_status.setStyleSheet(f"color:{_current_theme['success']};font-size:13px;")
            self._lbz_banner.set("Token saved — ready to submit to ListenBrainz.", "success")
        else:
            self._lbz_dot.set_color(_current_theme["txt2"]); self._lbz_status.setText("No token")
            self._lbz_status.setStyleSheet(f"color:{_current_theme['txt2']};font-size:13px;")
            self._lbz_banner.set("Paste your user token and click Save.", "muted")

    # ── Appearance tab ────────────────────────────────────────

    ACCENTS = {
        "Amber":    ("#c8861a","#e09d30"), "Teal":    ("#1a9a8a","#22b8a6"),
        "Crimson":  ("#c02040","#de2a52"), "Violet":  ("#7c3fc0","#9d52e0"),
        "Cobalt":   ("#1a66c8","#2882f0"), "Sage":    ("#4a8c5c","#5aaa70"),
        "Rose":     ("#c04080","#e05098"), "Slate":   ("#607090","#7888a8"),
        "Sunset":   ("#d4601a","#f07830"), "Forest":  ("#2d7a3a","#3aaa4e"),
    }

    def _build_appearance_tab(self) -> QWidget:
        w = QWidget()
        outer = QVBoxLayout(w); outer.setContentsMargins(0,0,0,0)
        scroll = QScrollArea(); scroll.setWidgetResizable(True)
        scroll.setFrameShape(QFrame.Shape.NoFrame)
        inner = QWidget(); root = QVBoxLayout(inner)
        root.setContentsMargins(28,24,28,20); root.setSpacing(20)

        # Color scheme card


        # Accent card
        ac_card = QWidget(); ac_card.setObjectName("card")
        ac = QVBoxLayout(ac_card); ac.setContentsMargins(18,14,18,16); ac.setSpacing(14)
        ac.addWidget(SectionLabel("Accent color"))
        presets = QHBoxLayout(); presets.setSpacing(8)
        for name, (c1, c2) in self.ACCENTS.items():
            sw = QPushButton(); sw.setFixedSize(32,32); sw.setToolTip(name)
            sw.setStyleSheet(f"background:{c1};border:2px solid rgba(255,255,255,0.08);border-radius:7px;")
            sw.clicked.connect(lambda _, a=c1, b=c2: self._apply_accent(a, b))
            presets.addWidget(sw)
        presets.addStretch(); ac.addLayout(presets)
        custom_row = QHBoxLayout(); custom_row.setSpacing(10)
        custom_lbl = QLabel("Custom:"); custom_lbl.setObjectName("secondary"); custom_lbl.setFixedWidth(65)
        self._accent_swatch = ColorSwatch(_current_theme["accent"])
        self._accent_swatch.color_changed.connect(lambda c: self._apply_accent(c, c))
        self._accent_hex = QLabel(_current_theme["accent"]); self._accent_hex.setObjectName("mono")
        custom_row.addWidget(custom_lbl); custom_row.addWidget(self._accent_swatch)
        custom_row.addWidget(self._accent_hex); custom_row.addStretch()
        ac.addLayout(custom_row); root.addWidget(ac_card)

        root.addStretch()
        scroll.setWidget(inner); outer.addWidget(scroll)
        return w

    def _apply_base(self, theme: dict, name: str):
        global _current_theme
        _current_theme = theme.copy()
        c = self.conf(); c["theme"] = name.lower()
        if "custom_accent" in c:
            a = c["custom_accent"]
            _current_theme["accent"] = a; _current_theme["accent2"] = a
            _current_theme["accentlo"] = a+"1a"; _current_theme["bordhi"] = a+"55"
        save_conf(c); self._sync_swatches(); self.theme_changed.emit(_current_theme)

    def _apply_accent(self, c1: str, c2: str):
        global _current_theme
        _current_theme["accent"] = c1; _current_theme["accent2"] = c2
        _current_theme["accentlo"] = c1+"1a"; _current_theme["bordhi"] = c1+"55"
        conf = self.conf(); conf["custom_accent"] = c1; conf["custom_accent2"] = c2; save_conf(conf)
        self._accent_swatch.set_color(c1); self._accent_hex.setText(c1)
        self.theme_changed.emit(_current_theme)

    def _reset_colors(self):
        global _current_theme
        _current_theme = DARK.copy()
        conf = self.conf()
        conf.pop("custom_accent", None); conf.pop("custom_accent2", None); conf.pop("color_overrides", None)
        save_conf(conf); self._sync_swatches(); self.theme_changed.emit(_current_theme)

    def _sync_swatches(self):
        self._accent_swatch.set_color(_current_theme["accent"]); self._accent_hex.setText(_current_theme["accent"])

    # ── Miscellaneous tab ─────────────────────────────────────

    def _build_misc_tab(self) -> QWidget:
        w = QWidget()
        outer = QVBoxLayout(w); outer.setContentsMargins(0,0,0,0)
        scroll = QScrollArea(); scroll.setWidgetResizable(True)
        scroll.setFrameShape(QFrame.Shape.NoFrame)
        inner = QWidget(); root = QVBoxLayout(inner)
        root.setContentsMargins(28,24,28,20); root.setSpacing(20)

        c = self.conf

        # ── History Settings ──────────────────────────────────
        hist_card = QWidget(); hist_card.setObjectName("card")
        hv = QVBoxLayout(hist_card); hv.setContentsMargins(18,14,18,16); hv.setSpacing(12)
        hv.addWidget(SectionLabel("History"))

        save_hist_chk = QCheckBox("Save scrobbles to local history")
        save_hist_chk.setToolTip("When disabled, scrobbles are submitted but not stored locally")
        save_hist_chk.setChecked(c().get("save_history", True))
        def _toggle_save_hist(v): cc = c(); cc["save_history"] = v; save_conf(cc)
        save_hist_chk.toggled.connect(_toggle_save_hist)
        hv.addWidget(save_hist_chk)

        hist_lim_row = QHBoxLayout()
        hist_lim_lbl = QLabel("History limit (rows loaded)"); hist_lim_lbl.setObjectName("secondary")
        hist_lim_spin = QSpinBox(); hist_lim_spin.setRange(100, 50000); hist_lim_spin.setSingleStep(500)
        hist_lim_spin.setValue(c().get("history_limit", 5000))
        hist_lim_spin.setFixedWidth(100)
        def _save_hist_lim(v): cc = c(); cc["history_limit"] = v; save_conf(cc)
        hist_lim_spin.valueChanged.connect(_save_hist_lim)
        hist_lim_row.addWidget(hist_lim_lbl); hist_lim_row.addWidget(hist_lim_spin); hist_lim_row.addStretch()
        hv.addLayout(hist_lim_row)
        root.addWidget(hist_card)

        # ── Scrobbling Settings ───────────────────────────────
        scr_card = QWidget(); scr_card.setObjectName("card")
        sv = QVBoxLayout(scr_card); sv.setContentsMargins(18,14,18,16); sv.setSpacing(12)
        sv.addWidget(SectionLabel("Scrobbling"))

        retry_row = QHBoxLayout()
        retry_lbl = QLabel("Retry failed scrobbles (attempts)"); retry_lbl.setObjectName("secondary")
        retry_spin = QSpinBox(); retry_spin.setRange(0, 10); retry_spin.setValue(c().get("scrobble_retries", 3))
        retry_spin.setFixedWidth(80)
        def _save_retry(v): cc = c(); cc["scrobble_retries"] = v; save_conf(cc)
        retry_spin.valueChanged.connect(_save_retry)
        retry_row.addWidget(retry_lbl); retry_row.addWidget(retry_spin); retry_row.addStretch()
        sv.addLayout(retry_row)

        batch_row = QHBoxLayout()
        batch_lbl = QLabel("Batch size per submission"); batch_lbl.setObjectName("secondary")
        batch_spin = QSpinBox(); batch_spin.setRange(1, 50); batch_spin.setValue(c().get("batch_size", 50))
        batch_spin.setFixedWidth(80)
        def _save_batch(v): cc = c(); cc["batch_size"] = v; save_conf(cc)
        batch_spin.valueChanged.connect(_save_batch)
        batch_row.addWidget(batch_lbl); batch_row.addWidget(batch_spin); batch_row.addStretch()
        sv.addLayout(batch_row)

        skip_dup_chk = QCheckBox("Skip duplicate scrobbles (same track within 30 s)")
        skip_dup_chk.setChecked(c().get("skip_duplicates", True))
        def _toggle_skip_dup(v): cc = c(); cc["skip_duplicates"] = v; save_conf(cc)
        skip_dup_chk.toggled.connect(_toggle_skip_dup)
        sv.addWidget(skip_dup_chk)
        root.addWidget(scr_card)

        # ── TIDAL Settings ────────────────────────────────────
        tidal_card = QWidget(); tidal_card.setObjectName("card")
        tv = QVBoxLayout(tidal_card); tv.setContentsMargins(18,14,18,16); tv.setSpacing(12)
        tv.addWidget(SectionLabel("TIDAL Downloader"))

        retries_row = QHBoxLayout()
        retries_lbl = QLabel("Max download retries per quality"); retries_lbl.setObjectName("secondary")
        retries_spin = QSpinBox(); retries_spin.setRange(0, 10)
        retries_spin.setValue(c().get("tidal", {}).get("dl_retries", 2))
        retries_spin.setFixedWidth(80)
        def _save_tidal_retries(v):
            cc = c(); cc.setdefault("tidal", {})["dl_retries"] = v; save_conf(cc)
        retries_spin.valueChanged.connect(_save_tidal_retries)
        retries_row.addWidget(retries_lbl); retries_row.addWidget(retries_spin); retries_row.addStretch()
        tv.addLayout(retries_row)

        primary_artist_chk = QCheckBox("Use primary artist only for folder structure")
        primary_artist_chk.setToolTip(
            "When enabled, albums are stored under the main artist only,\n"
            "not all featured artists (e.g. Kanye West/MBDTF not 'Kanye West, Jay-Z, Kid Cudi/MBDTF')"
        )
        primary_artist_chk.setChecked(c().get("tidal", {}).get("use_primary_artist", True))
        def _toggle_primary(v):
            cc = c(); cc.setdefault("tidal", {})["use_primary_artist"] = v; save_conf(cc)
        primary_artist_chk.toggled.connect(_toggle_primary)
        tv.addWidget(primary_artist_chk)
        root.addWidget(tidal_card)

        # ── Interface Settings ────────────────────────────────
        ui_card = QWidget(); ui_card.setObjectName("card")
        uv = QVBoxLayout(ui_card); uv.setContentsMargins(18,14,18,16); uv.setSpacing(12)
        uv.addWidget(SectionLabel("Interface"))

        font_row = QHBoxLayout()
        font_lbl = QLabel("Font size (pt)"); font_lbl.setObjectName("secondary")
        font_spin = QSpinBox(); font_spin.setRange(9, 18); font_spin.setValue(c().get("font_size", 13))
        font_spin.setFixedWidth(80)
        font_note = QLabel("(restart required)"); font_note.setObjectName("muted")
        def _save_font(v): cc = c(); cc["font_size"] = v; save_conf(cc)
        font_spin.valueChanged.connect(_save_font)
        font_row.addWidget(font_lbl); font_row.addWidget(font_spin); font_row.addWidget(font_note); font_row.addStretch()
        uv.addLayout(font_row)
        root.addWidget(ui_card)

        # ── Data Management ───────────────────────────────────
        data_card = QWidget(); data_card.setObjectName("card")
        dv = QVBoxLayout(data_card); dv.setContentsMargins(18,14,18,16); dv.setSpacing(12)
        dv.addWidget(SectionLabel("Data Management"))

        db_info_lbl = QLabel(f"Database: {DB_FILE}")
        db_info_lbl.setObjectName("muted"); db_info_lbl.setWordWrap(True)
        dv.addWidget(db_info_lbl)
        cfg_info_lbl = QLabel(f"Config: {CONF_FILE}")
        cfg_info_lbl.setObjectName("muted"); cfg_info_lbl.setWordWrap(True)
        dv.addWidget(cfg_info_lbl)

        def _open_config_dir():
            open_url(QUrl.fromLocalFile(str(CONFIG_DIR)))
        open_dir_btn = QPushButton("Open Config Folder")
        open_dir_btn.setObjectName("ghost"); open_dir_btn.setFixedWidth(160)
        open_dir_btn.clicked.connect(_open_config_dir)
        dv.addWidget(open_dir_btn)
        root.addWidget(data_card)

        root.addStretch()
        scroll.setWidget(inner); outer.addWidget(scroll)
        return w


# ─────────────────────────────────────────────────────────────
#  TIDAL DOWNLOADER  –  constants & helpers
# ─────────────────────────────────────────────────────────────

_TIDAL_BASES = [
    "https://arran.monochrome.tf",
    "https://triton.squid.wtf",
    "https://hifi-one.spotisaver.net",
    "https://hifi-two.spotisaver.net",
    "https://tidal.kinoplus.online",
    "https://hund.qqdl.site",
    "https://katze.qqdl.site",
    "https://maus.qqdl.site",
    "https://vogel.qqdl.site",
    "https://wolf.qqdl.site",
]

# Custom hifi-api instance (set in TIDAL settings)
_TIDAL_CUSTOM_BASE: Optional[str] = None

_TIDAL_QUALITY_OPTIONS = [
    ("HI_RES_LOSSLESS", "Hi-Res",      "24-bit FLAC (DASH) up to 192 kHz"),
    ("LOSSLESS",        "CD Lossless", "16-bit / 44.1 kHz FLAC"),
    ("HIGH",            "320kbps AAC", "High quality AAC streaming"),
    ("LOW",             "96kbps AAC",  "Data saver AAC streaming"),
]
_TIDAL_QUALITY_EXT = {
    "HI_RES_LOSSLESS": "flac", "LOSSLESS": "flac",
    "HIGH": "m4a", "LOW": "m4a",
}


def _tidal_normalize_cover_id(cover_id: str) -> str:
    if not cover_id:
        return ""
    cid = str(cover_id).strip()
    if re.fullmatch(r"[0-9a-fA-F]{32}", cid):
        cid = f"{cid[0:8]}-{cid[8:12]}-{cid[12:16]}-{cid[16:20]}-{cid[20:32]}"
    return cid.lower()

def _tidal_cover(cover_id: str, size: int = 320) -> str:
    cid = _tidal_normalize_cover_id(cover_id)
    if not cid:
        return ""
    return f"https://resources.tidal.com/images/{cid.replace('-', '/')}/{size}x{size}.jpg"

def _tidal_find_cover_id(obj) -> str:
    """Best-effort extraction of cover id from differing proxy payload shapes."""
    seen = set()
    def walk(x):
        try:
            xid=id(x)
        except Exception:
            xid=None
        if xid is not None and xid in seen:
            return ""
        if xid is not None:
            seen.add(xid)
        if isinstance(x, dict):
            for k in ("cover", "coverId", "cover_id", "imageCover", "image_cover", "albumCover", "album_cover"):
                v = x.get(k)
                if isinstance(v, str) and v.strip():
                    return v.strip()
            for k in ("album", "release", "item", "resource", "data", "attributes"):
                if k in x:
                    got = walk(x[k])
                    if got:
                        return got
            for v in x.values():
                got = walk(v)
                if got:
                    return got
        elif isinstance(x, list):
            for it in x:
                got = walk(it)
                if got:
                    return got
        return ""
    return walk(obj)



def _tidal_resolve_stream_url(data: dict) -> str:
    """
    Resolve a playable stream URL from a TIDAL API response.
    hifi-api /track/ returns the raw Tidal playbackInfoPostPaywallResponse, which has:
      - manifest: base64-encoded DASH MPD (for LOSSLESS/HI_RES) or JSON (for AAC)
      - manifestMimeType: 'application/dash+xml' or 'application/vnd.tidal.bts'
    """
    if not isinstance(data, dict):
        return ""

    # 1. Unwrap hifi-api "data" key if present
    if "data" in data and isinstance(data["data"], dict):
        result = _tidal_resolve_stream_url(data["data"])
        if result:
            return result

    # 2. Direct URL fields (some proxies return these)
    for field in ("url", "streamUrl", "originalTrackUrl"):
        v = data.get(field)
        if v and isinstance(v, str) and v.startswith("http"):
            return v

    # 3. Nested stream objects
    for key in ("stream", "audio", "trackStreamInfo", "streamData", "stream_data"):
        sub = data.get(key)
        if isinstance(sub, dict):
            found = _tidal_resolve_stream_url(sub)
            if found:
                return found

    # 4. Base64-encoded manifest — primary path for hifi-api
    manifest_b64 = data.get("manifest") or data.get("encodedManifest") or ""
    mime = data.get("manifestMimeType", "")

    if manifest_b64 and isinstance(manifest_b64, str):
        try:
            import base64 as _b64
            padded  = manifest_b64 + "=" * (-len(manifest_b64) % 4)
            decoded = _b64.b64decode(padded).decode("utf-8", "ignore")

            # 4a. JSON BTS manifest (AAC streams — application/vnd.tidal.bts)
            if "vnd.tidal.bts" in mime or decoded.lstrip().startswith("{"):
                try:
                    bts  = json.loads(decoded)
                    urls = bts.get("urls") or []
                    if urls and isinstance(urls[0], str):
                        return urls[0]
                    if isinstance(bts.get("url"), str):
                        return bts["url"]
                except Exception:
                    pass

            # 4b. DASH MPD (FLAC) — parse XML properly
            # Priority: BaseURL > SegmentTemplate > any https with audio extension

            # BaseURL element (most common for Tidal FLAC)
            base_url_re = re.search(
                r'<BaseURL[^>]*>\s*(https?://[^\s<]+)\s*</BaseURL>',
                decoded, re.IGNORECASE
            )
            if base_url_re:
                candidate = base_url_re.group(1).strip().rstrip("/")
                if candidate.startswith("http"):
                    return candidate

            # media="..." attribute in SegmentTemplate
            seg_re = re.search(r'media="(https?://[^"]+)"', decoded)
            if seg_re:
                candidate = seg_re.group(1)
                # Strip DASH template vars like $Number$
                candidate = re.sub(r'\$[^$]+\$', '', candidate).rstrip("/")
                if candidate.startswith("http"):
                    return candidate

            # Direct file URLs with audio extensions
            direct_re = re.findall(
                r'(https://[^\s"<>\x00-\x1f]+\.(?:flac|m4a|mp4|aac)(?:\?[^\s"<>]*)?)',
                decoded,
            )
            if direct_re:
                return direct_re[0]

            # Signed S3/CDN URLs — look for audio-related patterns
            # Tidal CDN URLs often contain: pa.crunchyroll.com or similar
            cdn_re = re.findall(
                r'(https://(?:sp-prod|listening|audio)[^\s"<>]+)',
                decoded
            )
            if cdn_re:
                return cdn_re[0]

            # Last resort: any https URL that's not the manifest itself
            any_urls = re.findall(r'(https://[^\s"<>\x00-\x1f]{20,})', decoded)
            # Filter out MPD/manifest URLs
            audio_candidates = [u for u in any_urls
                                if not u.endswith('.mpd') and '.mpd?' not in u
                                and 'manifest' not in u.lower()]
            if audio_candidates:
                return audio_candidates[0]

        except Exception:
            pass

    return ""


def _tidal_req(path: str, timeout: int = 14) -> Optional[dict]:
    """Try custom base first, then every cluster endpoint; return first successful JSON."""
    import time as _time
    bases_to_try = []
    if _TIDAL_CUSTOM_BASE:
        bases_to_try.append(_TIDAL_CUSTOM_BASE.rstrip("/"))
    bases_to_try.extend(_TIDAL_BASES)
    for base in bases_to_try:
        try:
            r = requests.get(base + path, timeout=timeout,
                             headers={"X-Client": "Scrobbox/4.0",
                                      "Accept": "application/json"})
            if r.status_code == 200:
                try:
                    return r.json()
                except Exception:
                    continue
            if r.status_code in (400, 404):
                # Track/resource genuinely not found — no point trying other bases
                return None
            if r.status_code == 429:
                # Rate limited — wait briefly and retry this same base once before moving on
                _time.sleep(2)
                try:
                    r2 = requests.get(base + path, timeout=timeout,
                                      headers={"X-Client": "Scrobbox/4.0",
                                               "Accept": "application/json"})
                    if r2.status_code == 200:
                        return r2.json()
                except Exception:
                    pass
                continue
            if r.status_code in (401, 403):
                # Auth failure on this base — skip to next
                continue
        except Exception:
            continue
    return None


def _tidal_extract_tracks(data) -> list:
    """Pull track list from any TIDAL API response shape (handles hifi-api wrappers)."""
    if isinstance(data, list):
        return data
    if not isinstance(data, dict):
        return []

    # hifi-api /album/ wraps everything in a "data" key
    # The top-level album_data has "cover" which items' track.album may lack
    if "data" in data and isinstance(data["data"], dict):
        inner  = data["data"]
        result = _tidal_extract_tracks(inner)
        if result:
            # Try to inject the album cover/title into each track if missing
            album_cover = inner.get("cover") or ""
            album_title = inner.get("title") or ""
            album_id    = inner.get("id")
            if album_cover:
                for tr in result:
                    if isinstance(tr, dict):
                        alb = tr.setdefault("album", {})
                        if not alb.get("cover"):
                            alb["cover"] = album_cover
                        if not alb.get("title") and album_title:
                            alb["title"] = album_title
                        if not alb.get("id") and album_id:
                            alb["id"] = album_id
            return result

    # hifi-api /artist/?f= returns tracks directly
    if "tracks" in data:
        sub = data["tracks"]
        if isinstance(sub, list) and sub:
            first = sub[0]
            if isinstance(first, dict) and ("title" in first or "id" in first):
                return sub

    # items array — unwrap item wrapper if present
    # Also inject cover from parent if this is an album data dict
    if "items" in data:
        items = data["items"]
        if not items:
            return []
        parent_cover = data.get("cover") or ""
        parent_title = data.get("title") or ""
        parent_id    = data.get("id")
        if isinstance(items[0], dict):
            if "title" in items[0]:
                return items
            if "item" in items[0]:
                tracks = [i["item"] for i in items
                          if isinstance(i.get("item"), dict) and "title" in i.get("item", {})]
                # Inject album cover from parent album data
                if parent_cover:
                    for tr in tracks:
                        if isinstance(tr, dict):
                            alb = tr.setdefault("album", {})
                            if not alb.get("cover"):
                                alb["cover"] = parent_cover
                            if not alb.get("title") and parent_title:
                                alb["title"] = parent_title
                            if not alb.get("id") and parent_id:
                                alb["id"] = parent_id
                return tracks

    return []


def _tidal_extract_albums(data) -> list:
    if isinstance(data, list):
        return data
    if not isinstance(data, dict):
        return []

    # hifi-api /artist/?f= returns {"albums": {"items": [...]}, ...}
    if "albums" in data:
        sub = data["albums"]
        if isinstance(sub, dict) and "items" in sub:
            items = sub["items"]
            if isinstance(items, list) and items:
                return items
        if isinstance(sub, list) and sub:
            return sub

    # Unwrap hifi-api "data" wrapper
    if "data" in data and isinstance(data["data"], dict):
        result = _tidal_extract_albums(data["data"])
        if result:
            return result

    # Direct items
    for key in ("items",):
        if key in data:
            sub = data[key]
            if isinstance(sub, list) and sub:
                first = sub[0]
                if isinstance(first, dict) and ("title" in first or "numberOfTracks" in first):
                    return sub
            if isinstance(sub, dict):
                result = _tidal_extract_albums(sub)
                if result:
                    return result

    return []


def _tidal_extract_artists(data) -> list:
    if isinstance(data, list):
        return data
    if not isinstance(data, dict):
        return []

    # hifi-api /search/?a= returns top-hits with artists embedded
    if "artists" in data:
        sub = data["artists"]
        if isinstance(sub, list) and sub:
            return sub
        if isinstance(sub, dict) and "items" in sub:
            return sub["items"]

    # Unwrap hifi-api "data" wrapper
    if "data" in data and isinstance(data["data"], dict):
        result = _tidal_extract_artists(data["data"])
        if result:
            return result

    for key in ("items",):
        if key in data:
            sub = data[key]
            if isinstance(sub, list) and sub:
                first = sub[0]
                if isinstance(first, dict) and "name" in first:
                    return sub
            if isinstance(sub, dict):
                result = _tidal_extract_artists(sub)
                if result:
                    return result

    return []


def _fmt_dur(secs) -> str:
    try:
        m, s = divmod(int(secs), 60)
        return f"{m}:{s:02d}"
    except Exception:
        return ""


def _sanitize_path(s: str) -> str:
    return re.sub(r'[<>:"/\\|?*\x00-\x1f]', "_", str(s)).strip() or "Unknown"


# ─────────────────────────────────────────────────────────────
#  TIDAL – async workers
# ─────────────────────────────────────────────────────────────

class _TidalWorker(QThread):
    ok   = pyqtSignal(object)
    fail = pyqtSignal(str)

    def __init__(self, path: str, parent=None):
        super().__init__(parent)
        self._path = path
        self.setObjectName(f"TidalWorker:{path[:40]}")

    def run(self):
        data = _tidal_req(self._path)
        if data is not None:
            self.ok.emit(data)
        else:
            self.fail.emit(f"All endpoints failed for: {self._path}")


class _TidalDlWorker(QThread):
    progress = pyqtSignal(int, int)   # received, total
    done     = pyqtSignal(bool, str)  # ok, path-or-error

    def __init__(self, url: str, dest: str, track: dict = None,
                 prefs: dict = None, stream_data: dict = None, parent=None):
        super().__init__(parent)
        self._url    = url
        self._dest   = dest
        self._track  = track or {}
        self._prefs  = prefs or {}
        sd = stream_data or {}
        if isinstance(sd.get("data"), dict):
            sd = sd["data"]
        self._stream = sd
        self._cancel = False
        self._pause_event = _threading.Event()
        self._pause_event.set()   # not paused initially

    def cancel(self):
        self._cancel = True
        self._pause_event.set()   # unblock if paused so thread can exit cleanly

    def pause(self):
        self._pause_event.clear()

    def resume(self):
        self._pause_event.set()

    @property
    def is_paused(self) -> bool:
        return not self._pause_event.is_set()

    def run(self):
        try:
            self._download_file()
        except Exception as e:
            self.done.emit(False, str(e))

    def _download_file(self):
        """Download audio file, then handle post-processing based on prefs."""
        # ── Post-processing variables (needed for both DASH and normal paths) ──
        dest_path = Path(self._dest)
        p     = self._prefs
        track = self._track
        tid   = track.get("id")

        _COV_HEADERS = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "image/webp,image/jpeg,image/*,*/*;q=0.8",
            "Referer": "https://listen.tidal.com/",
        }

        def _fetch_cover(cover_id: str, preferred_dim: int):
            for d in [preferred_dim, 1280, 640, 320, 160]:
                url = f"https://resources.tidal.com/images/{cover_id.replace('-', '/')}/{d}x{d}.jpg"
                try:
                    rc = requests.get(url, timeout=15, headers=_COV_HEADERS)
                    if rc.status_code == 200 and len(rc.content) > 500:
                        ct = rc.headers.get("content-type", "")
                        if "image" in ct or rc.content[:3] == b"\xff\xd8\xff":
                            return rc.content
                except Exception:
                    continue
            return None

        # ── DASH download (HI_RES_LOSSLESS / LOSSLESS via MPD manifest) ──────
        mime = (self._stream.get("manifestMimeType") or self._stream.get("manifest_mime_type") or "").lower()
        manifest_b64 = self._stream.get("manifest") or self._stream.get("encodedManifest") or ""
        if manifest_b64 and "dash+xml" in mime:
            import base64 as _b64
            padded = manifest_b64 + "=" * (-len(manifest_b64) % 4)
            decoded = _b64.b64decode(padded).decode("utf-8", "ignore")
            if "<MPD" not in decoded:
                raise RuntimeError("DASH manifest was not valid MPD XML.")
            tmp_mpd = str(Path(self._dest).with_suffix(".mpd"))
            with open(tmp_mpd, "w", encoding="utf-8") as f:
                f.write(decoded)
            # Use -progress pipe:1 so we can parse time and emit progress signals
            duration_s = float(self._track.get("duration") or 0)
            cmd = [
                "ffmpeg", "-y",
                "-protocol_whitelist", "file,https,tls,tcp,crypto",
                "-i", tmp_mpd,
                "-vn", "-c:a", "flac",
                "-progress", "pipe:1",
                "-loglevel", "error",
                self._dest,
            ]
            ff_stderr_lines = []
            proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                bufsize=1,
            )
            # Emit progress from ffmpeg -progress output (out_time_ms=...)
            try:
                for line in proc.stdout:
                    self._pause_event.wait()
                    if self._cancel:
                        proc.kill()
                        try:
                            Path(self._dest).unlink(missing_ok=True)
                        except Exception:
                            pass
                        self.done.emit(False, "Cancelled")
                        return
                    line = line.strip()
                    if line.startswith("out_time_ms=") and duration_s > 0:
                        try:
                            elapsed_ms = int(line.split("=", 1)[1])
                            elapsed_s  = elapsed_ms / 1_000_000
                            pct = min(int(elapsed_s / duration_s * 100), 99)
                            # Emit as fraction of duration (recv/total in bytes equivalent)
                            self.progress.emit(pct, 100)
                        except Exception:
                            pass
                proc.wait()
            except Exception:
                proc.wait()
            # Collect any stderr
            try:
                raw_err = proc.stderr.read()
                if raw_err:
                    ff_stderr_lines.append(raw_err.decode("utf-8", "ignore").strip())
            except Exception:
                pass
            try:
                Path(tmp_mpd).unlink(missing_ok=True)
            except Exception:
                pass
            if proc.returncode != 0:
                err = " ".join(ff_stderr_lines).strip()
                raise RuntimeError(f"ffmpeg DASH download failed: {err or 'unknown error'}")
            self.progress.emit(100, 100)
            # DASH done — skip plain HTTP download, go straight to post-processing

        else:
            # ── Plain HTTP download (AAC / non-DASH streams) ─────────────────
            headers = {
                "User-Agent": "Scrobbox/4.0",
                "Accept": "audio/flac, audio/mp4, audio/*, */*",
            }
            try:
                r = requests.get(self._url, stream=True, timeout=90, headers=headers)
                r.raise_for_status()
            except requests.RequestException as e:
                raise RuntimeError(f"Download failed: {e}")

            content_type = r.headers.get("content-type", "").lower()
            total = int(r.headers.get("content-length", 0))

            if any(bad in content_type for bad in ("xml", "mpd", "json", "text/html", "text/plain")):
                raise RuntimeError(
                    f"Server returned non-audio content ({content_type}). "
                    "Try a different quality setting (CD Lossless instead of Hi-Res)."
                )

            recv = 0
            with open(self._dest, "wb") as f:
                for chunk in r.iter_content(65536):
                    self._pause_event.wait()
                    if self._cancel:
                        try:
                            Path(self._dest).unlink(missing_ok=True)
                        except Exception:
                            pass
                        self.done.emit(False, "Cancelled")
                        return
                    if chunk:
                        f.write(chunk)
                        recv += len(chunk)
                        if total:
                            self.progress.emit(recv, total)

            min_bytes = 50 * 1024
            actual_size = Path(self._dest).stat().st_size
            if actual_size < min_bytes:
                Path(self._dest).unlink(missing_ok=True)
                raise RuntimeError(
                    f"Downloaded file is too small ({actual_size} bytes) — "
                    "likely a manifest or error response, not audio. "
                    "Try a different quality setting."
                )

        # ── Post-processing ───────────────────────────────────
        dim_str = p.get("metadata_cover_dimension", "Px1280")
        dim     = int(dim_str.replace("Px", "")) if "Px" in dim_str else 1280

        # 1. Fetch cover bytes once (used for both saving to disk and embedding)
        cover_bytes = None
        cover_id = (track.get("album") or {}).get("cover") or ""
        if cover_id and (p.get("cover_album_file", True) or p.get("dl_covers", False) or p.get("metadata_cover_embed", True)):
            try:
                cover_bytes = _fetch_cover(cover_id, dim)
            except Exception:
                pass

        # 2. Save cover.jpg alongside
        if cover_bytes and (p.get("cover_album_file", True) or p.get("dl_covers", False)):
            try:
                cov_path = dest_path.parent / "cover.jpg"
                cov_path.write_bytes(cover_bytes)
            except Exception:
                pass

        # 3. Fetch lyrics once (used for both .lrc file and embedding)
        lyrics_obj = None
        plain_lyrics = ""
        if tid and (p.get("lyrics_file", True) or p.get("lyrics_embed", True)):
            try:
                lrc_data = _tidal_req(f"/lyrics/?id={tid}")
                if isinstance(lrc_data, dict):
                    inner = lrc_data.get("lyrics")
                    lyrics_obj   = inner if isinstance(inner, dict) else lrc_data
                    plain_lyrics = lyrics_obj.get("lyrics") or ""
            except Exception:
                pass

        # 4. Save lyrics as .lrc
        if p.get("lyrics_file", True) and lyrics_obj:
            try:
                lrc_text = self._build_lrc(lyrics_obj, track)
                if lrc_text:
                    lrc_path = dest_path.with_suffix(".lrc")
                    lrc_path.write_text(lrc_text, encoding="utf-8")
            except Exception:
                pass

        # 5. Write all metadata (tags + cover + lyrics) in one pass to avoid overwrites
        try:
            embed_cover = cover_bytes if p.get("metadata_cover_embed", True) else None
            embed_lyrics = plain_lyrics if (p.get("lyrics_embed", True) and plain_lyrics) else ""
            self._embed_tags(dest_path, track, p, cover_bytes=embed_cover, plain_lyrics=embed_lyrics)
        except Exception:
            pass

        # 6. Delay between downloads
        if p.get("download_delay", True):
            import time as _time
            _time.sleep(0.4)

        self.done.emit(True, self._dest)

    def _build_lrc(self, lyrics_obj: dict, track: dict) -> str:
        """Build an LRC file from TIDAL lyrics data."""
        lines = []
        title   = track.get("title", "")
        artists = ", ".join(a.get("name","") for a in (track.get("artists") or []))
        album   = (track.get("album") or {}).get("title", "")
        if title:   lines.append(f"[ti:{title}]")
        if artists: lines.append(f"[ar:{artists}]")
        if album:   lines.append(f"[al:{album}]")
        lines.append("")

        # Priority 1: parsed synced lines
        synced = lyrics_obj.get("subtitlesLines") or lyrics_obj.get("lyricsLines") or []
        if synced and isinstance(synced, list):
            lines.append("")
            for line in synced:
                try:
                    offset_ms = int(line.get("offset", 0) or 0)
                    text      = line.get("text", "") or ""
                    mins      = offset_ms // 60000
                    secs      = (offset_ms % 60000) / 1000
                    lines.append(f"[{mins:02d}:{secs:05.2f}]{text}")
                except Exception:
                    continue
            return "\n".join(lines)

        # Priority 2: pre-built LRC string
        subtitles = lyrics_obj.get("subtitles") or ""
        if isinstance(subtitles, str) and subtitles.strip():
            sub_lines = [l for l in subtitles.splitlines()
                         if not l.startswith(("[ti:", "[ar:", "[al:", "[re:"))]
            lines.append("")
            lines.extend(sub_lines)
            return "\n".join(lines)

        # Priority 3: plain unsynced
        plain = lyrics_obj.get("lyrics") or ""
        if isinstance(plain, str) and plain.strip():
            lines.append("")
            lines.append(plain)
        return "\n".join(lines)

    def _embed_tags(self, dest: Path, track: dict, p: dict,
                    cover_bytes: bytes = None, plain_lyrics: str = ""):
        """Embed ID3/FLAC/MP4 metadata tags, cover art, and lyrics in one save call."""
        try:
            suffix = dest.suffix.lower()
            title   = track.get("title") or ""
            artists = ", ".join(a.get("name","") for a in (track.get("artists") or []))
            album   = (track.get("album") or {}).get("title") or ""
            year    = (str(track.get("streamStartDate") or "")[:4]
                     or str((track.get("album") or {}).get("releaseDate") or "")[:4]
                     or str(track.get("releaseDate") or "")[:4])
            track_n = str(track.get("trackNumber") or "")
            disc_n  = str(track.get("volumeNumber") or "")
            explicit = track.get("explicit", False)
            mark_exp = p.get("mark_explicit", False)

            # Album artist: use album.artist (single primary), fall back to track.artist (singular)
            _alb = track.get("album") or {}
            _alb_artist_name = (_alb.get("artist") or {}).get("name") or ""
            album_artist = _alb_artist_name or (track.get("artist") or {}).get("name") or ""

            if suffix == ".flac":
                from mutagen.flac import FLAC, Picture
                audio = FLAC(str(dest))
                if title:        audio["TITLE"]          = title
                if artists:      audio["ARTIST"]         = artists
                if album:        audio["ALBUM"]          = album
                if album_artist: audio["ALBUMARTIST"]    = album_artist
                if year:         audio["DATE"]           = year
                if track_n:      audio["TRACKNUMBER"]    = track_n
                if disc_n:       audio["DISCNUMBER"]     = disc_n
                if plain_lyrics: audio["UNSYNCEDLYRICS"] = plain_lyrics
                if explicit and mark_exp:
                    audio["ITUNESADVISORY"] = "1"
                if cover_bytes:
                    pic = Picture()
                    pic.type = 3  # front cover
                    pic.mime = "image/jpeg"
                    pic.data = cover_bytes
                    audio.clear_pictures()
                    audio.add_picture(pic)
                audio.save()
            elif suffix in (".m4a", ".mp4", ".aac"):
                from mutagen.mp4 import MP4, MP4Cover
                audio = MP4(str(dest))
                if title:        audio["©nam"] = [title]
                if artists:      audio["©ART"] = [artists]
                if album:        audio["©alb"] = [album]
                if album_artist: audio["aART"]   = [album_artist]
                if year:         audio["©day"] = [year]
                if plain_lyrics: audio["©lyr"] = [plain_lyrics]
                if track_n:
                    try:
                        audio["trkn"] = [(int(track_n), 0)]
                    except Exception:
                        pass
                if disc_n:
                    try:
                        audio["disk"] = [(int(disc_n), 0)]
                    except Exception:
                        pass
                if cover_bytes:
                    audio["covr"] = [MP4Cover(cover_bytes, imageformat=MP4Cover.FORMAT_JPEG)]
                audio.save()
            elif suffix == ".mp3":
                from mutagen.id3 import ID3, TIT2, TPE1, TPE2, TALB, TDRC, TRCK, TPOS, APIC, USLT
                audio = ID3(str(dest))
                if title:        audio["TIT2"] = TIT2(encoding=3, text=title)
                if artists:      audio["TPE1"] = TPE1(encoding=3, text=artists)
                if album:        audio["TALB"] = TALB(encoding=3, text=album)
                if album_artist: audio["TPE2"] = TPE2(encoding=3, text=album_artist)
                if year:         audio["TDRC"] = TDRC(encoding=3, text=year)
                if track_n:      audio["TRCK"] = TRCK(encoding=3, text=track_n)
                if disc_n:       audio["TPOS"] = TPOS(encoding=3, text=disc_n)
                if plain_lyrics:
                    audio["USLT"] = USLT(encoding=3, lang="eng", desc="", text=plain_lyrics)
                if cover_bytes:
                    audio["APIC"] = APIC(encoding=3, mime="image/jpeg",
                                         type=3, desc="Cover", data=cover_bytes)
                audio.save()
        except ImportError:
            pass  # mutagen not installed
        except Exception:
            pass  # tag writing failed — file is still downloaded


# ─────────────────────────────────────────────────────────────
#  TIDAL – Settings panel  (mirrors tidal-ui glass popover)
# ─────────────────────────────────────────────────────────────

class _TidalSettingsPanel(QScrollArea):
    """
    Floating settings panel shown when the ⚙ Settings button is clicked.
    Uses a QScrollArea so it can scroll on small screens.
    Sections:
      • Streaming & Downloads  – quality selector
      • Conversions            – AAC→MP3, separate covers
      • Queue exports          – individual / ZIP / CSV
      • Queue actions          – download queue, export CSV
    All state is persisted under conf["tidal"].
    """
    quality_changed    = pyqtSignal(str)
    download_queue_req = pyqtSignal()
    export_csv_req     = pyqtSignal()

    def __init__(self, conf: dict, parent=None):
        super().__init__(parent)
        self._conf = conf
        self.setFixedWidth(400)
        self.setMinimumHeight(200)
        self.setMaximumHeight(700)
        self.setWidgetResizable(True)
        self.setFrameShape(QFrame.Shape.NoFrame)
        self.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)

        inner = QWidget()
        inner.setStyleSheet("background: #0a0c10;")
        self._root = QVBoxLayout(inner)
        self._root.setContentsMargins(14, 12, 14, 12)
        self._root.setSpacing(8)
        self.setWidget(inner)

        # Floating frame look
        self.setStyleSheet(
            f"QScrollArea{{background:#0a0c10;border:1px solid rgba(255,255,255,0.25);border-radius:10px;}}"
        )

        self._build()
        self._load()

    def _p(self) -> dict:
        return self._conf.setdefault("tidal", {})

    def _heading(self, text: str) -> QLabel:
        lbl = QLabel(text.upper())
        lbl.setObjectName("sectiontitle")
        return lbl

    def _hr(self) -> QFrame:
        f = QFrame()
        f.setFrameShape(QFrame.Shape.HLine)
        f.setFixedHeight(1)
        f.setStyleSheet("background:rgba(255,255,255,0.09);")
        return f

    def _build(self):
        r = self._root

        def _le(placeholder, key):
            le = QLineEdit()
            le.setPlaceholderText(placeholder)
            if key:
                def _save_key(v, k=key):
                    self._p()[k] = v
                    save_conf(self._conf)
                    if k == "custom_api_url":
                        global _TIDAL_CUSTOM_BASE
                        _TIDAL_CUSTOM_BASE = v.strip() if v.strip() else None
                le.textChanged.connect(_save_key)
            return le

        def _combo(options, key):
            cb = QComboBox()
            cb.addItems(options)
            cb.currentTextChanged.connect(lambda v, k=key: (self._p().__setitem__(k, v), save_conf(self._conf)))
            return cb

        def _row_lbl(text):
            lbl = QLabel(text)
            lbl.setFixedWidth(185)
            lbl.setStyleSheet("color:rgba(255,255,255,0.55);font-size:11px;background:transparent;")
            return lbl

        # ── Audio Quality ─────────────────────────────────────
        r.addWidget(self._heading("Audio Quality"))
        self._qual_btns: dict[str, QPushButton] = {}
        for val, label, desc in _TIDAL_QUALITY_OPTIONS:
            btn = QPushButton(f"{label}  –  {desc}")
            btn.setFixedHeight(36)
            btn.setCheckable(True)
            btn.setObjectName("toggle")
            btn.clicked.connect(lambda _, v=val: self._sel_quality(v))
            self._qual_btns[val] = btn
            r.addWidget(btn)

        r.addWidget(self._hr())

        # ── Download Options ──────────────────────────────────
        r.addWidget(self._heading("Download Options"))
        self._flag_btns: dict[str, QPushButton] = {}
        flag_defs = [
            ("skip_existing",        "Skip existing files",      True),
            ("download_delay",       "Delay between downloads",  True),
            ("lyrics_file",          "Save .lrc lyrics file",    True),
            ("lyrics_embed",         "Embed lyrics in tags",     True),
            ("metadata_cover_embed", "Embed cover art in tags",  True),
            ("cover_album_file",     "Save cover.jpg alongside", True),
            ("mark_explicit",        "Mark explicit in filename", False),
            ("use_primary_artist",   "Use primary artist only",  True),
        ]
        grid = QGridLayout(); grid.setSpacing(5); grid.setContentsMargins(0,0,0,0)
        for i, (key, label, _def) in enumerate(flag_defs):
            btn = QPushButton(label)
            btn.setFixedHeight(30)
            btn.setCheckable(True)
            btn.setObjectName("toggle")
            btn.clicked.connect(lambda _, k=key, b=btn: self._toggle(k, b))
            self._flag_btns[key] = btn
            grid.addWidget(btn, i // 2, i % 2)
        r.addLayout(grid)
        self._flag_defaults = {k: d for k, _, d in flag_defs}

        r.addWidget(self._hr())

        # ── Cover Art Resolution ──────────────────────────────
        r.addWidget(self._heading("Cover Art"))
        hb_cov = QHBoxLayout(); hb_cov.setSpacing(8)
        hb_cov.addWidget(_row_lbl("Resolution"))
        self._cb_metadata_cover_dimension = _combo(
            ["Px1280", "Px640", "Px320", "Px160"], "metadata_cover_dimension"
        )
        hb_cov.addWidget(self._cb_metadata_cover_dimension, stretch=1)
        r.addLayout(hb_cov)

        r.addWidget(self._hr())

        # ── Download Folder ───────────────────────────────────
        r.addWidget(self._heading("Paths"))

        lbl_dl = QLabel("Download folder")
        lbl_dl.setStyleSheet("color:rgba(255,255,255,0.55);font-size:10px;background:transparent;margin-top:3px;")
        r.addWidget(lbl_dl)
        hb_dl = QHBoxLayout(); hb_dl.setSpacing(4); hb_dl.setContentsMargins(0,0,0,0)
        self._le_dl_base_path = _le(str(Path.home() / "Music" / "TIDAL"), "dl_base_path")
        hb_dl.addWidget(self._le_dl_base_path, stretch=1)
        br_dl = QPushButton("…")
        br_dl.setFixedWidth(28)
        br_dl.setObjectName("ghost")
        br_dl.clicked.connect(lambda: self._browse_dir(self._le_dl_base_path))
        hb_dl.addWidget(br_dl)
        r.addLayout(hb_dl)

        lbl_api = QLabel("Custom hifi-api URL  (leave blank to use public proxies)")
        lbl_api.setWordWrap(True)
        lbl_api.setStyleSheet("color:rgba(255,255,255,0.55);font-size:10px;background:transparent;margin-top:6px;")
        r.addWidget(lbl_api)
        self._le_custom_api_url = _le("http://localhost:8000", "custom_api_url")
        r.addWidget(self._le_custom_api_url)

        r.addWidget(self._hr())

        # ── Queue Export Mode ─────────────────────────────────
        r.addWidget(self._heading("Queue Export Mode"))
        mode_row = QHBoxLayout(); mode_row.setSpacing(6)
        self._mode_btns: dict[str, QPushButton] = {}
        for val, label in [("individual","⬇ Individual"), ("zip","🗜 ZIP Archive"), ("csv","📄 URL List")]:
            btn = QPushButton(label)
            btn.setFixedHeight(30)
            btn.setCheckable(True)
            btn.setObjectName("toggle")
            btn.clicked.connect(lambda _, v=val: self._sel_mode(v))
            self._mode_btns[val] = btn; mode_row.addWidget(btn)
        r.addLayout(mode_row)

        r.addWidget(self._hr())

        # ── Queue Actions ─────────────────────────────────────
        r.addWidget(self._heading("Queue Actions"))
        dl_btn = QPushButton("⬇  Download Queue Now")
        dl_btn.setFixedHeight(34)
        dl_btn.setObjectName("primary")
        dl_btn.clicked.connect(lambda: (self.hide(), self.download_queue_req.emit()))
        r.addWidget(dl_btn)
        csv_btn = QPushButton("Export Queue as CSV")
        csv_btn.setFixedHeight(30)
        csv_btn.setObjectName("ghost")
        csv_btn.clicked.connect(lambda: (self.hide(), self.export_csv_req.emit()))
        r.addWidget(csv_btn)
        r.addStretch()

    def _browse_dir(self, le: QLineEdit):
        d = QFileDialog.getExistingDirectory(self, "Select folder", le.text() or str(Path.home()))
        if d: le.setText(d)

    def _set_btn_active(self, btn: QPushButton, on: bool):
        btn.setChecked(on)

    def _sel_quality(self, val: str):
        self._p()["quality"] = val
        save_conf(self._conf)
        for k, b in self._qual_btns.items():
            b.setChecked(k == val)
        self.quality_changed.emit(val)

    def _toggle(self, key: str, btn: QPushButton):
        fd = getattr(self, "_flag_defaults", {})
        p  = self._p()
        current = bool(p.get(key, fd.get(key, False)))
        p[key]  = not current
        save_conf(self._conf)
        btn.setChecked(p[key])

    def _sel_mode(self, val: str):
        self._p()["dl_mode"] = val
        save_conf(self._conf)
        for k, b in self._mode_btns.items():
            b.setChecked(k == val)

    def _load(self):
        p = self._p()

        # Quality buttons
        q = p.get("quality", "HI_RES_LOSSLESS")
        for k, b in self._qual_btns.items():
            b.setChecked(k == q)

        # Flag toggle buttons
        fd = getattr(self, "_flag_defaults", {})
        for key, btn in self._flag_btns.items():
            btn.setChecked(bool(p.get(key, fd.get(key, False))))

        # Cover art resolution combo
        cb = getattr(self, "_cb_metadata_cover_dimension", None)
        if cb:
            idx = cb.findText(p.get("metadata_cover_dimension", "Px1280"))
            if idx >= 0: cb.setCurrentIndex(idx)

        # Text fields
        for attr, conf_key, default_val in [
            ("_le_dl_base_path",   "dl_base_path",   ""),
            ("_le_custom_api_url", "custom_api_url", ""),
        ]:
            le = getattr(self, attr, None)
            if le: le.setText(p.get(conf_key, default_val))

        # Queue export mode
        mode = p.get("dl_mode", "individual")
        for k, b in self._mode_btns.items():
            b.setChecked(k == mode)

    def current_quality(self) -> str:
        return self._p().get("quality", "HI_RES_LOSSLESS")



class _AlbumFlowWidget(QWidget):
    """
    Responsive album grid that reflows into as many columns as fit.
    Cards are fixed width (CARD_W); column count is recalculated on every
    resizeEvent so the grid always fills the available width.
    """
    CARD_W  = 190
    COVER_H = 190
    GAP     = 14
    MIN_COLS = 2

    _GRAD_PAIRS = [
        ("#1a1a2e", "#e94560"), ("#0f3460", "#533483"),
        ("#16213e", "#0f3460"), ("#1b262c", "#0a3d62"),
        ("#2c003e", "#8e24aa"), ("#1a237e", "#283593"),
        ("#880e4f", "#4a0072"), ("#004d40", "#00695c"),
        ("#bf360c", "#e64a19"), ("#37474f", "#546e7a"),
    ]

    def __init__(self, albums: list, on_open, on_dl, on_queue, parent=None):
        super().__init__(parent)
        self._cards   = []
        self._on_open  = on_open
        self._on_dl    = on_dl
        self._on_queue = on_queue
        self.setStyleSheet("background:transparent;")
        self._build_cards(albums)

    def _build_cards(self, albums):
        t = _current_theme
        CW = self.CARD_W
        CH = self.COVER_H

        for alb in albums[:200]:
            title  = (alb.get("title") or alb.get("name") or "Untitled").strip()
            artist = ""
            if isinstance(alb.get("artist"), dict):
                artist = (alb["artist"].get("name") or alb["artist"].get("title") or "").strip()
            if not artist and isinstance(alb.get("artists"), list) and alb["artists"]:
                a0 = alb["artists"][0]
                if isinstance(a0, dict):
                    artist = (a0.get("name") or a0.get("title") or "").strip()
            year = str(alb.get("releaseDate") or alb.get("release_date") or alb.get("year") or "")
            year = year.split("-")[0] if year else ""

            card = QFrame(self)
            card.setFixedWidth(CW)
            card.setStyleSheet(
                "QFrame#alb_card{background:rgba(255,255,255,0.05);border:1px solid rgba(255,255,255,0.09);"
                f"border-radius:10px;}}"
                f"QFrame#alb_card:hover{{border-color:rgba(255,255,255,0.25);background:rgba(255,255,255,0.08);}}"
                f"QFrame#alb_card QLabel{{background:transparent;border:none;}}"
                f"QFrame#alb_card QPushButton{{font-size:11px;padding:3px 8px;}}"
            )
            card.setObjectName("alb_card")
            card.setCursor(Qt.CursorShape.PointingHandCursor)

            vb = QVBoxLayout(card)
            vb.setContentsMargins(0, 0, 0, 8)
            vb.setSpacing(0)

            # Cover art
            art_container = QWidget()
            art_container.setFixedSize(CW, CH)
            art_container.setStyleSheet("background:transparent;")
            cover_lbl = QLabel(art_container)
            cover_lbl.setFixedSize(CW, CH)
            cover_lbl.move(0, 0)
            cover_lbl.setAlignment(Qt.AlignmentFlag.AlignCenter)
            cover_lbl.setStyleSheet("background:transparent;border:none;")

            c1, c2 = self._GRAD_PAIRS[abs(hash(title)) % len(self._GRAD_PAIRS)]
            first  = (title[:1] or "♪").upper()
            pix    = QPixmap(CW, CH)
            pp = QPainter(pix)
            pp.setRenderHint(QPainter.RenderHint.Antialiasing)
            grad = QLinearGradient(0, 0, CW, CH)
            grad.setColorAt(0.0, QColor(c1)); grad.setColorAt(1.0, QColor(c2))
            pp.fillRect(0, 0, CW, CH, QBrush(grad))
            pp.setPen(QColor(255, 255, 255, 50))
            ff = QFont(); ff.setPointSize(52); ff.setWeight(QFont.Weight.Black)
            pp.setFont(ff)
            pp.drawText(pix.rect(), Qt.AlignmentFlag.AlignCenter, first)
            pp.end()
            rounded = QPixmap(CW, CH); rounded.fill(Qt.GlobalColor.transparent)
            rp = QPainter(rounded); rp.setRenderHint(QPainter.RenderHint.Antialiasing)
            path = QPainterPath()
            path.addRoundedRect(QRectF(0, 0, CW, CH), 9, 9)
            rp.setClipPath(path); rp.drawPixmap(0, 0, pix); rp.end()
            cover_lbl.setPixmap(rounded)

            alb_cover_id = (alb.get("cover") or alb.get("coverId") or alb.get("cover_id")
                            or _tidal_find_cover_id(alb))
            if alb_cover_id:
                _load_cover_into_label(str(alb_cover_id), cover_lbl, CW, corner_radius=9)

            vb.addWidget(art_container)

            # Text
            info = QWidget(); info.setStyleSheet("background:transparent;")
            iv = QVBoxLayout(info); iv.setContentsMargins(10, 6, 10, 0); iv.setSpacing(1)
            ttl_lbl = QLabel()
            ttl_lbl.setStyleSheet("color:rgba(255,255,255,0.85);font-weight:600;font-size:12px;background:transparent;border:none;")
            fm = QFontMetrics(ttl_lbl.font())
            ttl_lbl.setText(fm.elidedText(title, Qt.TextElideMode.ElideRight, CW - 20))
            sub_parts = ([artist] if artist else []) + ([year] if year else [])
            sub_lbl = QLabel()
            sub_lbl.setStyleSheet("color:rgba(255,255,255,0.35);font-size:11px;background:transparent;border:none;")
            fm2 = QFontMetrics(sub_lbl.font())
            sub_lbl.setText(fm2.elidedText(" · ".join(sub_parts), Qt.TextElideMode.ElideRight, CW - 20))
            iv.addWidget(ttl_lbl); iv.addWidget(sub_lbl)
            vb.addWidget(info)

            # Actions
            acts = QWidget(); acts.setStyleSheet("background:transparent;")
            ah = QHBoxLayout(acts); ah.setContentsMargins(8, 4, 8, 0); ah.setSpacing(4)
            open_btn = QPushButton("Open"); open_btn.setFixedHeight(26)
            open_btn.clicked.connect(lambda _=False, a=alb: self._on_open(a))
            dl_btn = QPushButton("⬇"); dl_btn.setFixedSize(26, 26)
            dl_btn.setToolTip("Download album"); dl_btn.setObjectName("dl_btn")
            dl_btn.clicked.connect(lambda _=False, a=alb: self._on_dl(a))
            q_btn = QPushButton("+"); q_btn.setFixedSize(26, 26)
            q_btn.setToolTip("Add to queue")
            q_btn.clicked.connect(lambda _=False, a=alb: self._on_queue(a))
            ah.addWidget(open_btn, stretch=1); ah.addWidget(dl_btn); ah.addWidget(q_btn)
            vb.addWidget(acts)

            card.mousePressEvent = (lambda e, a=alb:
                self._on_open(a) if e.button() == Qt.MouseButton.LeftButton else None)

            card.hide()   # positioned manually in _reflow
            self._cards.append(card)

        self._reflow_cols = 0   # force reflow on first show

    def _cols_for_width(self, w: int) -> int:
        if w <= 0:
            return self.MIN_COLS
        cols = max(self.MIN_COLS, (w + self.GAP) // (self.CARD_W + self.GAP))
        return cols

    def _reflow(self):
        w = self.width()
        cols = self._cols_for_width(w)
        if cols == self._reflow_cols and self._cards and self._cards[0].isVisible():
            return   # nothing changed
        self._reflow_cols = cols

        # Centre the grid block horizontally
        total_w = cols * self.CARD_W + (cols - 1) * self.GAP
        x0 = max(0, (w - total_w) // 2)

        card_h = self._cards[0].sizeHint().height() if self._cards else 260
        row_h  = card_h + self.GAP
        y0     = self.GAP

        for i, card in enumerate(self._cards):
            col = i % cols
            row = i // cols
            card.move(x0 + col * (self.CARD_W + self.GAP), y0 + row * row_h)
            card.show()

        rows  = (len(self._cards) + cols - 1) // cols if self._cards else 0
        total_h = y0 + rows * row_h + self.GAP
        self.setMinimumHeight(total_h)
        self.updateGeometry()

    def resizeEvent(self, e):
        super().resizeEvent(e)
        self._reflow()

    def showEvent(self, e):
        super().showEvent(e)
        self._reflow()


# Module-level set that holds all live _CoverFetchWorker instances.
# This is the only reliable way to prevent PyQt6/Qt from destroying a QThread
# while it is still running: keep a Python-side strong reference so the GC
# (and Qt's parent-child ownership) cannot collect it prematurely.
_active_cover_workers: set = set()


class _CoverFetchWorker(QThread):
    """
    Fetches a TIDAL cover on a background QThread and delivers raw bytes to
    the main thread via a Qt signal.  QTimer.singleShot() posted from a plain
    Python threading.Thread silently drops the callback in PyQt6 because plain
    threads have no Qt event-loop affinity — signals are the correct mechanism.

    Lifetime is managed by _active_cover_workers: the worker adds itself on
    start and removes itself (via finished signal) when the thread exits,
    preventing "QThread destroyed while still running" SIGABRT crashes.
    """
    done = pyqtSignal(bytes)

    _HEADERS = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/120 Safari/537.36",
        "Accept": "image/webp,image/jpeg,image/*,*/*;q=0.8",
        "Referer": "https://listen.tidal.com/",
    }

    def __init__(self, cid: str, size: int):
        super().__init__(None)   # no QObject parent — avoids cross-thread ownership crash
        self._cid  = cid
        self._size = size
        # Keep ourselves alive until the thread finishes
        _active_cover_workers.add(self)
        self.finished.connect(self._on_finished)

    def _on_finished(self):
        _active_cover_workers.discard(self)
        self.deleteLater()

    def run(self):
        for sz in sorted({self._size, 640, 320, 160}, reverse=True):
            url = f"https://resources.tidal.com/images/{self._cid.replace('-', '/')}/{sz}x{sz}.jpg"
            try:
                r = requests.get(url, timeout=10, headers=self._HEADERS)
                ct = r.headers.get("content-type", "")
                ok = (r.status_code == 200 and len(r.content) > 500
                      and ("image" in ct or r.content[:3] in (b"\xff\xd8\xff", b"\x89PN")))
                if ok:
                    self.done.emit(r.content)
                    return
            except Exception:
                continue


def _load_cover_into_label(cover_id: str, label: QLabel, size: int,
                           corner_radius: int = 4, circle: bool = False):
    """
    Async cover art loader.  Uses a QThread + signal so Qt guarantees the
    callback runs on the main thread.  The old plain-threading.Thread approach
    with QTimer.singleShot() is unreliable in PyQt6 and silently drops covers.
    Worker lifetime is managed by _active_cover_workers to prevent SIGABRT.
    """
    if not cover_id:
        return
    cid = _tidal_normalize_cover_id(cover_id)
    if not cid:
        return

    worker = _CoverFetchWorker(cid, size)

    def _on_done(raw: bytes):
        try:
            if not sip.isdeleted(label):
                _set_cover_on_label(label, raw, size, corner_radius, circle)
        except Exception:
            pass
        try:
            worker.done.disconnect(_on_done)
        except Exception:
            pass

    worker.done.connect(_on_done, Qt.ConnectionType.QueuedConnection)
    worker.start()


def _set_cover_on_label(label: QLabel, raw: bytes, size: int,
                        corner_radius: int = 4, circle: bool = False):
    """Paint cover bytes onto a QLabel — must be called on the main thread."""
    try:
        # Guard: widget may have been destroyed between the thread finishing and
        # this QTimer callback firing (common when cards are rebuilt rapidly).
        if sip.isdeleted(label):
            return
        # Use the plugin-free decoder so covers work in AppImage/PyInstaller
        # where Qt imageformat plugins are often absent.
        img = _qimage_from_bytes_plugin_free(raw)
        if img is None or img.isNull():
            return
        w = size
        h = size
        src = QPixmap.fromImage(img).scaled(
            w, h,
            Qt.AspectRatioMode.KeepAspectRatioByExpanding,
            Qt.TransformationMode.SmoothTransformation,
        )
        if src.width() > w or src.height() > h:
            src = src.copy((src.width() - w) // 2, (src.height() - h) // 2, w, h)
        radius = w // 2 if circle else corner_radius
        result = QPixmap(w, h)
        result.fill(Qt.GlobalColor.transparent)
        painter = QPainter(result)
        painter.setRenderHint(QPainter.RenderHint.Antialiasing)
        path = QPainterPath()
        path.addRoundedRect(QRectF(0, 0, w, h), radius, radius)
        painter.setClipPath(path)
        painter.drawPixmap(0, 0, src)
        painter.end()
        # Final guard before touching the widget
        if not sip.isdeleted(label):
            label.setPixmap(result)
            label.setText("")
            label.setStyleSheet("background:transparent;border:none;")
    except Exception:
        pass

def _qimage_from_bytes_plugin_free(raw: bytes) -> QImage:
    """
    Decode image bytes into QImage.
    Tries Qt first. If Qt imageformat plugins are missing (common in AppImage/PyInstaller),
    falls back to Pillow, then to ffmpeg+ffprobe (raw RGBA) so cover art *still renders*.
    """
    if not raw:
        return QImage()

    # 1) Qt decode (fast path)
    img = QImage()
    try:
        if img.loadFromData(raw) and not img.isNull():
            return img
    except Exception:
        pass

    # 2) Pillow fallback (no Qt plugins needed)
    try:
        from PIL import Image  # type: ignore
        im = Image.open(io.BytesIO(raw)).convert("RGBA")
        w, h = im.size
        buf = im.tobytes("raw", "RGBA")
        q = QImage(buf, w, h, 4*w, QImage.Format.Format_RGBA8888)
        return q.copy()
    except Exception:
        pass

    # 3) ffprobe + ffmpeg raw RGBA fallback (requires ffmpeg)
    try:
        # probe dimensions
        probe = subprocess.run(
            ["ffprobe","-v","error","-select_streams","v:0",
             "-show_entries","stream=width,height","-of","json","-i","pipe:0"],
            input=raw, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        if probe.returncode != 0:
            return QImage()
        meta = json.loads((probe.stdout or b"{}").decode("utf-8","ignore") or "{}")
        st = (meta.get("streams") or [{}])[0]
        w, h = int(st.get("width") or 0), int(st.get("height") or 0)
        if w <= 0 or h <= 0:
            return QImage()

        dec = subprocess.run(
            ["ffmpeg","-v","error","-i","pipe:0","-f","rawvideo","-pix_fmt","rgba","pipe:1"],
            input=raw, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        if dec.returncode != 0:
            return QImage()
        buf = dec.stdout or b""
        if len(buf) < w*h*4:
            return QImage()
        q = QImage(buf, w, h, 4*w, QImage.Format.Format_RGBA8888)
        return q.copy()
    except Exception:
        return QImage()


def _apply_cover_raw(lbl: QLabel, size: int, corner_radius: int = 4, circle: bool = False):
    raw = getattr(lbl, "_cover_raw", None)
    if not raw:
        return
    img = _qimage_from_bytes_plugin_free(raw)
    if img.isNull():
        return

    # Scale to fill the label's actual display dimensions
    w = lbl.width() or size
    h = lbl.height() or size
    src = QPixmap.fromImage(img).scaled(
        w, h,
        Qt.AspectRatioMode.KeepAspectRatioByExpanding,
        Qt.TransformationMode.SmoothTransformation,
    )
    # Centre-crop to exact label size
    if src.width() > w or src.height() > h:
        src = src.copy((src.width() - w) // 2, (src.height() - h) // 2, w, h)

    # Paint with rounded/circle clip into a transparent pixmap so it actually clips
    radius = w // 2 if circle else corner_radius
    result = QPixmap(w, h)
    result.fill(Qt.GlobalColor.transparent)
    painter = QPainter(result)
    painter.setRenderHint(QPainter.RenderHint.Antialiasing)
    path = QPainterPath()
    path.addRoundedRect(QRectF(0, 0, w, h), radius, radius)
    painter.setClipPath(path)
    painter.drawPixmap(0, 0, src)
    painter.end()

    lbl.setPixmap(result)
    lbl.setText("")
    lbl.setStyleSheet("background:transparent;border:none;")


# ─────────────────────────────────────────────────────────────
#  TIDAL – Track row widget
# ─────────────────────────────────────────────────────────────

class _TidalTrackRow(QWidget):
    download_req = pyqtSignal(dict)
    queue_req    = pyqtSignal(dict)
    album_open   = pyqtSignal(dict)   # emitted when cover thumbnail is clicked

    def __init__(self, track: dict, parent=None):
        super().__init__(parent)
        self._track = track
        self.setFixedHeight(58)
        self.setCursor(Qt.CursorShape.PointingHandCursor)
        self._build()

    def _build(self):
        t  = _current_theme
        hb = QHBoxLayout(self)
        hb.setContentsMargins(12, 0, 10, 0)
        hb.setSpacing(8)
        self.setStyleSheet(
            f"_TidalTrackRow{{background:transparent;border-bottom:1px solid rgba(255,255,255,0.09);}}"
            f"_TidalTrackRow:hover{{background:rgba(255,255,255,0.08);}}"
            f"_TidalTrackRow QLabel{{background:transparent;border:none;}}"
            f"_TidalTrackRow QPushButton{{background:transparent;border:1px solid rgba(255,255,255,0.09);"
            f"color:rgba(255,255,255,0.87);border-radius:5px;font-size:12px;padding:0 10px;"
            f"min-height:26px;max-height:28px;font-weight:500;}}"
            f"_TidalTrackRow QPushButton:hover{{border-color:{t['accent']};color:{t['accent']};"
            f"background:{t['accentlo']};}}"
            f"_TidalTrackRow QPushButton:disabled{{color:rgba(255,255,255,0.35);background:transparent;"
            f"border-color:rgba(255,255,255,0.09)66;}}"
        )

        # Track number or placeholder
        tnum = self._track.get("trackNumber") or 0
        num_lbl = QLabel(str(tnum) if tnum else "·")
        num_lbl.setFixedWidth(24)
        num_lbl.setAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
        num_lbl.setStyleSheet("color:rgba(255,255,255,0.35);font-size:12px;background:transparent;")
        hb.addWidget(num_lbl)

        # Cover — clickable to open the parent album
        self._cover = QLabel("♪")
        self._cover.setFixedSize(40, 40)
        self._cover.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self._cover.setStyleSheet(
            f"background:rgba(255,255,255,0.08);border-radius:6px;color:rgba(255,255,255,0.35);font-size:16px;"
        )
        self._cover.setCursor(Qt.CursorShape.PointingHandCursor)
        self._cover.setToolTip("Open album")
        hb.addWidget(self._cover)

        alb = self._track.get("album") or {}
        cid = alb.get("cover") or alb.get("coverId") or alb.get("cover_id") or self._track.get("cover") or self._track.get("coverId") or ""
        if cid:
            _load_cover_into_label(cid, self._cover, 40, corner_radius=6)

        # Make cover emit album_open when clicked
        _alb_ref = dict(alb)
        def _cover_press(event, _a=_alb_ref):
            if event.button() == Qt.MouseButton.LeftButton and _a.get("id"):
                self.album_open.emit(_a)
        self._cover.mousePressEvent = _cover_press

        # Info column
        info = QVBoxLayout()
        info.setSpacing(2)
        info.setContentsMargins(0, 0, 0, 0)

        title = self._track.get("title", "Unknown")
        if self._track.get("version"):
            title += f"  ({self._track['version']})"
        tl = QLabel(title)
        tl.setStyleSheet("font-weight:600;color:rgba(255,255,255,0.85);font-size:13px;background:transparent;")
        tl.setMaximumWidth(500)
        info.addWidget(tl)

        artists = ", ".join(a.get("name", "") for a in (self._track.get("artists") or []))
        if not artists:
            artists = (self._track.get("artist") or {}).get("name", "")
        alb = self._track.get("album") or {}
        alb_title = alb.get("title", "")
        sub_text  = "  ·  ".join(filter(None, [artists, alb_title]))
        sl = QLabel(sub_text)
        sl.setStyleSheet("color:rgba(255,255,255,0.55);font-size:11px;background:transparent;")
        info.addWidget(sl)

        hb.addLayout(info, stretch=1)

        # Audio quality badge
        aq_map = {
            "HI_RES_LOSSLESS": ("Hi-Res", t["accent"]),
            "LOSSLESS":         ("FLAC",   t["txt1"]),
            "HIGH":             ("320k",   t["txt2"]),
            "LOW":              ("96k",    t["txt2"]),
        }
        aq_key = self._track.get("audioQuality", "")
        if aq_key in aq_map:
            badge_text, badge_color = aq_map[aq_key]
            ql = QLabel(badge_text)
            ql.setStyleSheet(
                f"color:{badge_color};font-size:9px;font-weight:700;letter-spacing:0.6px;"
                f"background:{badge_color}22;border:1px solid {badge_color}44;"
                f"border-radius:3px;padding:1px 6px;"
            )
            hb.addWidget(ql)

        # Duration
        dur = self._track.get("duration", 0)
        if dur:
            dl = QLabel(_fmt_dur(dur))
            dl.setFixedWidth(38)
            dl.setAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
            dl.setStyleSheet("color:rgba(255,255,255,0.35);font-size:11px;background:transparent;")
            hb.addWidget(dl)

        # Progress bar (hidden until downloading)
        self._prog = QProgressBar()
        self._prog.setRange(0, 100)
        self._prog.setValue(0)
        self._prog.setFixedSize(60, 4)
        self._prog.setTextVisible(False)
        self._prog.setVisible(False)
        hb.addWidget(self._prog)

        # Download button
        self._dl_btn = QPushButton("⬇  Download")
        self._dl_btn.setFixedHeight(28)
        self._dl_btn.setMinimumWidth(100)
        self._dl_btn.setToolTip("Download this track")
        self._dl_btn.clicked.connect(lambda: self.download_req.emit(self._track))
        hb.addWidget(self._dl_btn)

        # Queue button
        q_btn = QPushButton("+ Queue")
        q_btn.setFixedHeight(28)
        q_btn.setMinimumWidth(66)
        q_btn.setToolTip("Add to download queue")
        q_btn.clicked.connect(lambda: self.queue_req.emit(self._track))
        hb.addWidget(q_btn)

    def set_downloading(self, pct: int):
        try:
            self._dl_btn.setEnabled(False)
            self._dl_btn.setText("Downloading…")
            self._prog.setVisible(True)
            self._prog.setValue(pct)
        except Exception:
            pass

    def set_done(self, ok: bool):
        try:
            t = _current_theme
            self._prog.setVisible(False)
            self._dl_btn.setEnabled(True)
            self._dl_btn.setText("✓  Done" if ok else "✗  Failed")
            c = t["success"] if ok else t["danger"]
            self._dl_btn.setStyleSheet(
                f"QPushButton{{background:{c}14;border:1px solid {c}55;"
                f"border-radius:5px;font-size:12px;color:{c};padding:0 10px;"
                f"min-height:26px;max-height:28px;font-weight:600;}}"
            )
        except Exception:
            pass

    def reset_state(self):
        try:
            self._prog.setVisible(False)
            self._dl_btn.setEnabled(True)
            self._dl_btn.setText("Download")
            self._dl_btn.setStyleSheet("")
        except Exception:
            pass


# ─────────────────────────────────────────────────────────────
#  TIDAL – Album card
# ─────────────────────────────────────────────────────────────

class _TidalAlbumCard(QFrame):
    """Clickable album card. Separate signals for open vs download."""
    open_req     = pyqtSignal(dict)
    download_req = pyqtSignal(dict)

    # Palette of gradient pairs for placeholder art
    _GRAD_PAIRS = [
        ("#1a1a2e", "#e94560"), ("#0f3460", "#533483"),
        ("#16213e", "#0f3460"), ("#1b262c", "#0a3d62"),
        ("#2c003e", "#8e24aa"), ("#1a237e", "#283593"),
        ("#880e4f", "#4a0072"), ("#004d40", "#00695c"),
        ("#bf360c", "#e64a19"), ("#37474f", "#546e7a"),
        ("#1b5e20", "#2e7d32"), ("#b71c1c", "#c62828"),
    ]

    def __init__(self, album: dict, parent=None):
        super().__init__(parent)
        self._album = album
        self.setFixedSize(172, 220)
        self.setCursor(Qt.CursorShape.PointingHandCursor)
        self._build()

    def _grad_colors(self):
        """Pick a consistent gradient based on album title hash."""
        title = self._album.get("title") or ""
        idx = hash(title) % len(self._GRAD_PAIRS)
        return self._GRAD_PAIRS[abs(idx)]

    def _build(self):
        t  = _current_theme
        ART_W, ART_H = 172, 152
        vb = QVBoxLayout(self)
        vb.setContentsMargins(0, 0, 0, 0)
        vb.setSpacing(0)
        self.setStyleSheet(
            f"_TidalAlbumCard{{background:rgba(255,255,255,0.05);border:1px solid rgba(255,255,255,0.09);"
            f"border-radius:10px;}}"
            f"_TidalAlbumCard:hover{{border-color:rgba(255,255,255,0.25);background:rgba(255,255,255,0.08);}}"
        )

        # Art container — absolute positioning
        art_container = QWidget()
        art_container.setFixedSize(ART_W, ART_H)
        art_container.setStyleSheet("background:transparent;")

        # Cover label — fills entire art area
        self._cover_lbl = QLabel(art_container)
        self._cover_lbl.setFixedSize(ART_W, ART_H)
        self._cover_lbl.move(0, 0)
        self._cover_lbl.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self._cover_lbl.setStyleSheet("background:transparent;border:none;")

        # Draw gradient placeholder with QPainter (not CSS, so art can replace it cleanly)
        c1, c2 = self._grad_colors()
        first = (self._album.get("title") or "♪")[:1].upper()
        pix = QPixmap(ART_W, ART_H)
        painter = QPainter(pix)
        painter.setRenderHint(QPainter.RenderHint.Antialiasing)
        grad = QLinearGradient(0, 0, ART_W, ART_H)
        grad.setColorAt(0.0, QColor(c1))
        grad.setColorAt(1.0, QColor(c2))
        painter.fillRect(0, 0, ART_W, ART_H, QBrush(grad))
        painter.setPen(QColor(255, 255, 255, 55))
        ff = QFont(); ff.setPointSize(52); ff.setWeight(QFont.Weight.Black)
        painter.setFont(ff)
        painter.drawText(pix.rect(), Qt.AlignmentFlag.AlignCenter, first)
        painter.end()
        # Round top corners
        rounded = QPixmap(ART_W, ART_H)
        rounded.fill(Qt.GlobalColor.transparent)
        rp = QPainter(rounded)
        rp.setRenderHint(QPainter.RenderHint.Antialiasing)
        path = QPainterPath()
        path.addRoundedRect(QRectF(0, 0, ART_W, ART_H), 9, 9)
        rp.setClipPath(path)
        rp.drawPixmap(0, 0, pix)
        rp.end()
        self._cover_lbl.setPixmap(rounded)

        # Load real cover art (async). Placeholder stays until it arrives.
        cover_id = (self._album.get('cover') or self._album.get('coverId') or self._album.get('cover_id')
                    or (self._album.get('image') or {}).get('cover') or (self._album.get('image') or {}).get('coverId'))
        if cover_id:
            _load_cover_into_label(str(cover_id), self._cover_lbl, 640, corner_radius=9, circle=False)

        # Download overlay button
        self._dl_overlay = QPushButton("⬇", art_container)
        self._dl_overlay.setGeometry(ART_W - 38, 8, 30, 30)
        self._dl_overlay.setStyleSheet(
            "QPushButton{background:rgba(0,0,0,0.6);color:#fff;"
            "border:none;border-radius:15px;font-size:13px;}"
            f"QPushButton:hover{{background:{t['accent']};color:#000;}}"
        )
        self._dl_overlay.clicked.connect(self._on_dl_click)
        vb.addWidget(art_container)

        # Info section
        info_w = QWidget()
        info_w.setStyleSheet("background:transparent;")
        info_l = QVBoxLayout(info_w)
        info_l.setContentsMargins(10, 8, 10, 10)
        info_l.setSpacing(3)

        title_lbl = QLabel(self._album.get("title", ""))
        title_lbl.setWordWrap(True)
        title_lbl.setMaximumHeight(36)
        title_lbl.setStyleSheet(
            f"font-weight:700;color:rgba(255,255,255,0.87);font-size:11px;background:transparent;"
        )
        info_l.addWidget(title_lbl)

        artist = (self._album.get("artist") or {}).get("name", "")
        year   = (self._album.get("releaseDate") or "")[:4]
        meta   = "  ·  ".join(filter(None, [artist, year]))
        if meta:
            ml = QLabel(meta)
            ml.setStyleSheet("color:rgba(255,255,255,0.35);font-size:10px;background:transparent;")
            info_l.addWidget(ml)
        vb.addWidget(info_w)

        # Async art load — replaces gradient if successful
        if self._album.get("cover"):
            self._fetch_art(self._album["cover"], ART_W, ART_H)

    def _fetch_art(self, cover_id: str, w: int, h: int):
        import threading as _th
        _HEADERS = {
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/120 Safari/537.36",
            "Accept": "image/webp,image/jpeg,image/*,*/*;q=0.8",
            "Referer": "https://listen.tidal.com/",
        }
        def _run():
            for size in [w, 320, 640, 160]:
                url = f"https://resources.tidal.com/images/{cover_id.replace('-','/')}/{size}x{size}.jpg"
                try:
                    r = requests.get(url, timeout=10, headers=_HEADERS)
                    ct = r.headers.get("content-type","")
                    ok = r.status_code == 200 and len(r.content) > 500
                    ok = ok and ("image" in ct or r.content[:3] in (b"\xff\xd8\xff", b"\x89PN"))
                    if ok:
                        raw = r.content
                        QTimer.singleShot(0, lambda raw=raw: self._set_art(raw, w, h))
                        return
                except Exception:
                    continue
        _th.Thread(target=_run, daemon=True).start()

    def _set_art(self, raw: bytes, w: int, h: int):
        try:
            img = QImage(); img.loadFromData(raw)
            if img.isNull(): return
            src = QPixmap.fromImage(img).scaled(w, h,
                Qt.AspectRatioMode.KeepAspectRatioByExpanding,
                Qt.TransformationMode.SmoothTransformation)
            if src.width() > w or src.height() > h:
                src = src.copy((src.width()-w)//2, (src.height()-h)//2, w, h)
            result = QPixmap(w, h); result.fill(Qt.GlobalColor.transparent)
            rp = QPainter(result); rp.setRenderHint(QPainter.RenderHint.Antialiasing)
            path = QPainterPath()
            path.addRoundedRect(QRectF(0,0,w,h), 9, 9)
            rp.setClipPath(path); rp.drawPixmap(0,0,src); rp.end()
            self._cover_lbl.setPixmap(result)
        except Exception:
            pass

    def mousePressEvent(self, e):
        if e.button() == Qt.MouseButton.LeftButton:
            self.open_req.emit(self._album)
        super().mousePressEvent(e)

    def _on_dl_click(self):
        self.download_req.emit(self._album)


# ─────────────────────────────────────────────────────────────
#  TIDAL – Queue sidebar
# ─────────────────────────────────────────────────────────────

class _TidalQueueSidebar(QWidget):
    """
    Right sidebar with two sections:
      • Active downloads — tracks currently being fetched/downloaded, each with
        a progress bar and individual cancel button. Shows "Cancel All" at top.
      • Queued tracks   — tracks waiting to be downloaded, with remove buttons.
    Bottom bar has Download All and Export CSV.
    """
    download_all_req = pyqtSignal()
    export_csv_req   = pyqtSignal()
    cancel_one_req   = pyqtSignal(dict)   # cancel a single active download
    cancel_all_req   = pyqtSignal()       # cancel all active downloads
    pause_one_req    = pyqtSignal(dict)   # pause/resume a single download
    pause_all_req    = pyqtSignal()

    def __init__(self, parent=None):
        super().__init__(parent)
        self._tracks: list[dict] = []           # queued (not yet started)
        self._active: dict[int, dict] = {}      # tid -> {track, pct, row_widget}
        self.setFixedWidth(256)
        self._build()

    # ── Build ─────────────────────────────────────────────────

    def _build(self):
        t  = _current_theme
        vb = QVBoxLayout(self)
        vb.setContentsMargins(0, 0, 0, 0)
        vb.setSpacing(0)

        # Header
        hdr = QWidget()
        hdr.setObjectName("queue_hdr")
        hdr.setFixedHeight(44)
        hb  = QHBoxLayout(hdr)
        hb.setContentsMargins(12, 0, 10, 0)
        ql = QLabel("Downloads")
        ql.setObjectName("subheading")
        hb.addWidget(ql)
        hb.addStretch()

        self._cancel_all_btn = QPushButton("Cancel")
        self._cancel_all_btn.setObjectName("danger")
        self._cancel_all_btn.setFixedHeight(24)
        self._cancel_all_btn.setVisible(False)
        self._cancel_all_btn.setToolTip("Cancel all active downloads")
        self._cancel_all_btn.clicked.connect(self.cancel_all_req.emit)
        hb.addWidget(self._cancel_all_btn)

        self._pause_all_btn = QPushButton("Pause")
        self._pause_all_btn.setObjectName("ghost")
        self._pause_all_btn.setCheckable(True)
        self._pause_all_btn.setFixedHeight(24)
        self._pause_all_btn.setVisible(False)
        self._pause_all_btn.setToolTip("Pause / Resume all active downloads")
        self._pause_all_btn.clicked.connect(self._on_pause_all)
        hb.addWidget(self._pause_all_btn)

        clr = QPushButton("Clear")
        clr.setObjectName("ghost")
        clr.setFixedHeight(24)
        clr.setToolTip("Clear the waiting queue")
        clr.clicked.connect(self._clear_queue)
        hb.addWidget(clr)
        vb.addWidget(hdr)

        # Scroll area holds both sections
        self._scroll = QScrollArea()
        self._scroll.setWidgetResizable(True)
        self._scroll.setFrameShape(QFrame.Shape.NoFrame)
        self._scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)
        self._scroll.setStyleSheet("QScrollArea{background:transparent;border:none;}")

        self._inner = QWidget()
        self._inner.setObjectName("queue_inner")
        self._inner.setStyleSheet("QWidget#queue_inner{background:transparent;}")
        iv = QVBoxLayout(self._inner)
        iv.setContentsMargins(6, 6, 6, 6)
        iv.setSpacing(8)

        # Active downloads section
        self._active_section = QWidget()
        self._active_section.setVisible(False)
        av = QVBoxLayout(self._active_section)
        av.setContentsMargins(0, 0, 0, 0)
        av.setSpacing(3)

        active_lbl = QLabel("ACTIVE")
        active_lbl.setObjectName("sectiontitle")
        av.addWidget(active_lbl)

        self._active_list_l = QVBoxLayout()
        self._active_list_l.setSpacing(3)
        av.addLayout(self._active_list_l)
        iv.addWidget(self._active_section)

        # Queued tracks section
        self._queue_section = QWidget()
        self._queue_section.setVisible(False)
        qv = QVBoxLayout(self._queue_section)
        qv.setContentsMargins(0, 0, 0, 0)
        qv.setSpacing(3)

        queue_lbl = QLabel("QUEUED")
        queue_lbl.setObjectName("sectiontitle")
        qv.addWidget(queue_lbl)

        self._queue_list_l = QVBoxLayout()
        self._queue_list_l.setSpacing(3)
        qv.addLayout(self._queue_list_l)
        iv.addWidget(self._queue_section)

        # Empty state
        self._empty_lbl = QLabel("♫\n\nNo downloads.\nPress Download or\n+ Queue on a track.")
        self._empty_lbl.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self._empty_lbl.setObjectName("muted")
        self._empty_lbl.setWordWrap(True)
        fi = QFont(); fi.setPointSize(22)
        self._empty_lbl.setFont(fi)
        iv.addWidget(self._empty_lbl, stretch=1)

        self._scroll.setWidget(self._inner)
        vb.addWidget(self._scroll, stretch=1)

        # Bottom action bar
        acts = QWidget()
        acts.setObjectName("queue_acts")
        acts.setFixedHeight(88)
        al  = QVBoxLayout(acts)
        al.setContentsMargins(8, 8, 8, 8)
        al.setSpacing(6)

        dl_all = QPushButton("⬇  Download All Queued")
        dl_all.setObjectName("primary")
        dl_all.setFixedHeight(32)
        dl_all.setToolTip("Start downloading all queued tracks")
        dl_all.clicked.connect(self.download_all_req.emit)
        al.addWidget(dl_all)

        csv_btn = QPushButton("Export Queue as CSV")
        csv_btn.setObjectName("ghost")
        csv_btn.setFixedHeight(28)
        csv_btn.clicked.connect(self.export_csv_req.emit)
        al.addWidget(csv_btn)
        vb.addWidget(acts)

    # ── Active download tracking ───────────────────────────────

    def notify_download_start(self, track: dict):
        """Called by TidalDownloaderPage when a download begins."""
        tid = track.get("id")
        if tid is None or tid in self._active:
            return
        row = self._make_active_row(track)
        self._active[tid] = {"track": track, "pct": 0, "row": row}
        self._active_list_l.addWidget(row)
        self._refresh_visibility()

    def notify_download_progress(self, tid: int, pct: int):
        info = self._active.get(tid)
        if not info:
            return
        info["pct"] = pct
        row = info["row"]
        try:
            if not sip.isdeleted(row):
                row._prog.setValue(pct)
                row._pct_lbl.setText(f"{pct}%")
        except Exception:
            pass

    def notify_download_done(self, tid: int, ok: bool):
        info = self._active.pop(tid, None)
        if not info:
            return
        row = info.get("row")
        if row:
            try:
                if not sip.isdeleted(row):
                    row.deleteLater()
            except Exception:
                pass
        self._refresh_visibility()

    def _make_active_row(self, track: dict) -> QWidget:
        t   = _current_theme
        row = QWidget()
        row.setObjectName("active_dl_row")
        row.setFixedHeight(66)
        row.setStyleSheet(
            f"QWidget#active_dl_row {{ background:rgba(255,255,255,0.05); border:1px solid {t['accent']}44;"
            f" border-radius:6px; }}"
            f"QWidget#active_dl_row QLabel {{ background:transparent; border:none; }}"
            f"QWidget#active_dl_row QPushButton {{ background:transparent; border:1px solid rgba(255,255,255,0.09);"
            f" color:rgba(255,255,255,0.87); border-radius:4px; font-size:13px; padding:0; "
            f" min-height:22px; max-height:22px; min-width:22px; max-width:22px; }}"
            f"QWidget#active_dl_row QPushButton:hover {{ border-color:{t['accent']}; color:{t['accent']};"
            f" background:{t['accentlo']}; }}"
            f"QWidget#active_dl_row QPushButton#cancel_btn {{ border-color:{t['danger']}66;"
            f" color:{t['danger']}; background:{t['danger']}14; }}"
            f"QWidget#active_dl_row QPushButton#cancel_btn:hover {{ border-color:{t['danger']}cc;"
            f" background:{t['danger']}28; }}"
        )
        vb = QVBoxLayout(row)
        vb.setContentsMargins(8, 6, 8, 6)
        vb.setSpacing(4)

        # Top row: title + pause + cancel
        top = QHBoxLayout()
        top.setSpacing(4)

        title = track.get("title", "Unknown")
        tl = QLabel(title)
        tl.setStyleSheet("font-weight:600; color:rgba(255,255,255,0.85); font-size:11px;")
        tl.setMaximumWidth(140)
        top.addWidget(tl, stretch=1)

        pause_btn = QPushButton("⏸")
        pause_btn.setObjectName("pause_btn")
        pause_btn.setFixedSize(22, 22)
        pause_btn.setCheckable(True)
        pause_btn.setToolTip("Pause / resume this download")
        pause_btn.clicked.connect(lambda checked: self._on_pause_one(track, pause_btn))
        top.addWidget(pause_btn)

        cancel_btn = QPushButton("✕")
        cancel_btn.setObjectName("cancel_btn")
        cancel_btn.setFixedSize(22, 22)
        cancel_btn.setToolTip("Cancel this download")
        cancel_btn.clicked.connect(lambda: self.cancel_one_req.emit(track))
        top.addWidget(cancel_btn)
        vb.addLayout(top)

        # Bottom row: progress bar + percent
        bot = QHBoxLayout()
        bot.setSpacing(6)

        prog = QProgressBar()
        prog.setRange(0, 100)
        prog.setValue(0)
        prog.setFixedHeight(4)
        prog.setTextVisible(False)
        bot.addWidget(prog, stretch=1)

        pct_lbl = QLabel("0%")
        pct_lbl.setFixedWidth(30)
        pct_lbl.setAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
        pct_lbl.setStyleSheet("color:rgba(255,255,255,0.35); font-size:9px;")
        bot.addWidget(pct_lbl)

        vb.addLayout(bot)

        row._prog      = prog
        row._pct_lbl   = pct_lbl
        row._pause_btn = pause_btn
        return row

    def _on_pause_one(self, track: dict, btn: QPushButton):
        paused = btn.isChecked()
        btn.setText("▶" if paused else "⏸")
        btn.setToolTip("Resume download" if paused else "Pause download")
        self.pause_one_req.emit(track)

    def _on_pause_all(self):
        paused = self._pause_all_btn.isChecked()
        self._pause_all_btn.setText("▶ Resume" if paused else "⏸ Pause")
        # Sync individual pause buttons
        for info in self._active.values():
            row = info.get("row")
            if row:
                try:
                    if not sip.isdeleted(row):
                        row._pause_btn.setChecked(paused)
                        row._pause_btn.setText("▶" if paused else "⏸")
                except Exception:
                    pass
        self.pause_all_req.emit()

    # ── Queued track management ────────────────────────────────

    def add_track(self, track: dict):
        if any(t.get("id") == track.get("id") for t in self._tracks):
            return
        self._tracks.append(track)
        self._rebuild_queue_list()
        self._refresh_visibility()

    def tracks(self) -> list[dict]:
        return list(self._tracks)

    def _clear_queue(self):
        self._tracks.clear()
        self._rebuild_queue_list()
        self._refresh_visibility()

    def _remove_queued(self, idx: int):
        if 0 <= idx < len(self._tracks):
            self._tracks.pop(idx)
            self._rebuild_queue_list()
            self._refresh_visibility()

    def _rebuild_queue_list(self):
        t = _current_theme
        # Clear existing rows
        while self._queue_list_l.count():
            item = self._queue_list_l.takeAt(0)
            if w := item.widget():
                w.deleteLater()

        for idx, track in enumerate(self._tracks):
            row = QWidget()
            row.setObjectName("queue_row")
            row.setFixedHeight(48)
            row.setStyleSheet(
                f"QWidget#queue_row {{ background:rgba(255,255,255,0.05); border:1px solid rgba(255,255,255,0.09);"
                f" border-radius:6px; }}"
                f"QWidget#queue_row QLabel {{ background:transparent; border:none; }}"
                f"QWidget#queue_row QPushButton {{ background:{t['danger']}14;"
                f" border:1px solid {t['danger']}55; color:{t['danger']}; border-radius:4px;"
                f" font-size:12px; padding:0; min-height:20px; max-height:20px;"
                f" min-width:20px; max-width:20px; font-weight:600; }}"
                f"QWidget#queue_row QPushButton:hover {{ background:{t['danger']}28;"
                f" border-color:{t['danger']}cc; }}"
            )
            hb = QHBoxLayout(row)
            hb.setContentsMargins(8, 4, 8, 4)
            hb.setSpacing(6)

            n = QLabel(str(idx + 1))
            n.setFixedWidth(16)
            n.setAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
            n.setStyleSheet("color:rgba(255,255,255,0.35); font-size:10px;")
            hb.addWidget(n)

            info = QVBoxLayout()
            info.setSpacing(1)
            tl = QLabel(track.get("title", "Unknown"))
            tl.setStyleSheet("font-weight:600; color:rgba(255,255,255,0.85); font-size:11px;")
            tl.setMaximumWidth(150)
            info.addWidget(tl)

            artists = ", ".join(a.get("name", "") for a in (track.get("artists") or []))
            if not artists:
                artists = (track.get("artist") or {}).get("name", "")
            if artists:
                al = QLabel(artists)
                al.setStyleSheet("color:rgba(255,255,255,0.35); font-size:10px;")
                al.setMaximumWidth(150)
                info.addWidget(al)
            hb.addLayout(info, stretch=1)

            rm = QPushButton("✕")
            rm.setFixedSize(20, 20)
            rm.setToolTip("Remove from queue")
            rm.clicked.connect(lambda _, i=idx: self._remove_queued(i))
            hb.addWidget(rm)

            self._queue_list_l.addWidget(row)

    def _refresh_visibility(self):
        has_active = bool(self._active)
        has_queue  = bool(self._tracks)
        self._active_section.setVisible(has_active)
        self._queue_section.setVisible(has_queue)
        self._empty_lbl.setVisible(not has_active and not has_queue)
        self._cancel_all_btn.setVisible(has_active)
        self._pause_all_btn.setVisible(has_active)
        if not has_active:
            # Reset pause all button state when no downloads
            self._pause_all_btn.setChecked(False)
            self._pause_all_btn.setText("⏸ Pause")


# ─────────────────────────────────────────────────────────────
#  TIDAL – Now-playing bar
# ─────────────────────────────────────────────────────────────

class _TidalNowPlayingBar(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setFixedHeight(68)
        t = _current_theme
        self.setStyleSheet(
            f"background:rgba(5,7,11,0.50);border-top:1px solid rgba(255,255,255,0.25);"
        )
        hb = QHBoxLayout(self)
        hb.setContentsMargins(14, 10, 14, 10)
        hb.setSpacing(12)

        self._cover = QLabel("♪")
        self._cover.setFixedSize(48, 48)
        self._cover.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self._cover.setStyleSheet(
            f"background:rgba(255,255,255,0.08);border-radius:6px;color:rgba(255,255,255,0.35);font-size:18px;"
        )
        hb.addWidget(self._cover)

        info = QVBoxLayout()
        info.setSpacing(3)
        self._title  = QLabel("—")
        self._title.setStyleSheet("font-weight:600;color:rgba(255,255,255,0.85);font-size:13px;")
        info.addWidget(self._title)
        self._artist = QLabel("—")
        self._artist.setStyleSheet("color:rgba(255,255,255,0.55);font-size:11px;")
        info.addWidget(self._artist)
        hb.addLayout(info, stretch=1)

        now_lbl = QLabel("NOW PLAYING")
        now_lbl.setStyleSheet(
            f"color:rgba(255,255,255,0.35);font-size:8px;font-weight:700;letter-spacing:1.5px;"
            f"background:transparent;"
        )
        hb.addWidget(now_lbl)

        self._badge = QLabel()
        self._badge.setStyleSheet(
            f"background:{tok('accentlo')};color:{tok('accent')};border-radius:4px;"
            f"padding:3px 10px;font-size:10px;font-weight:700;letter-spacing:0.5px;"
        )
        hb.addWidget(self._badge)
        self.hide()

    def set_track(self, track: dict, quality: str):
        title   = track.get("title", "")
        if track.get("version"):
            title += f"  ({track['version']})"
        artists = ", ".join(a.get("name", "") for a in (track.get("artists") or []))
        if not artists:
            artists = (track.get("artist") or {}).get("name", "")
        self._title.setText(title)
        self._artist.setText(artists)
        ql = {"HI_RES_LOSSLESS":"Hi-Res", "LOSSLESS":"CD", "HIGH":"320k", "LOW":"96k"}
        self._badge.setText(ql.get(quality, quality))
        alb = track.get("album") or {}
        cid = alb.get("cover") or alb.get("coverId") or alb.get("cover_id")
        if cid:
            self._load_cover(cid)
        self.show()

    def _load_cover(self, cid: str):
        _load_cover_into_label(cid, self._cover, 48, corner_radius=6)


# ─────────────────────────────────────────────────────────────
#  TIDAL DOWNLOADER  –  Main page
# ─────────────────────────────────────────────────────────────

class TidalDownloaderPage(QWidget):
    def __init__(self, conf, parent=None):
        super().__init__(parent)
        self._alive = True
        self._conf      = conf[0] if isinstance(conf, list) else conf
        self._workers:  list[_TidalWorker]   = []   # keep-alive refs
        self._dl_wkrs:  dict[int, _TidalDlWorker] = {}
        self._dl_jobs:  dict[int, dict] = {}
        self._track_rows: dict[int, _TidalTrackRow] = {}
        self._search_results: dict[int, int] = {}
        # Navigation history: list of (tab_idx, tracks, albums, status_text, breadcrumb)
        self._nav_history: list[dict] = []
        self._build()
        # Apply custom API URL from saved prefs
        self._apply_custom_api_url()

    def _prefs(self) -> dict:
        return self._conf.setdefault("tidal", {})



    # ── Download quality chain / retries ───────────────────────

    _QUALITY_CHAIN = ["HI_RES_LOSSLESS", "LOSSLESS", "HIGH", "LOW"]

    def _retry_max(self) -> int:
        try:
            return int(self._prefs().get("dl_retries", 2))
        except Exception:
            return 2

    def _job_init(self, track: dict):
        tid = track.get("id")
        if not tid:
            return
        # Start the quality chain at the user's chosen quality, not always HI_RES_LOSSLESS
        pref_quality = self._prefs().get("quality", "HI_RES_LOSSLESS")
        try:
            start_qi = self._QUALITY_CHAIN.index(pref_quality)
        except ValueError:
            start_qi = 0
        self._dl_jobs[tid] = {
            "track": track,
            "qi": start_qi,
            "retries": 0,
            "quality": None,
        }

    def _job_next_quality(self, tid: int):
        job = self._dl_jobs.get(tid)
        if not job:
            return None
        qi = int(job.get("qi", 0))
        if qi >= len(self._QUALITY_CHAIN):
            return None
        q = self._QUALITY_CHAIN[qi]
        job["quality"] = q
        return q

    def _job_advance_quality(self, tid: int):
        job = self._dl_jobs.get(tid)
        if not job:
            return None
        job["qi"] = int(job.get("qi", 0)) + 1
        job["retries"] = 0
        return self._job_next_quality(tid)

    def _fetch_stream_for(self, track: dict, quality: str):
        tid = track.get("id")
        if not tid:
            return
        self._st(f"Fetching stream ({quality}) for '{track.get('title','')}'…")
        w = _TidalWorker(f"/track/?id={tid}&quality={quality}")
        w.ok.connect(lambda d, tr=track, q=quality: self._got_stream_job(d, tr, q))
        w.fail.connect(lambda e, tr=track, q=quality: self._on_stream_fail(tr, q, str(e)))
        w.finished.connect(lambda: self._workers.remove(w) if w in self._workers else None)
        self._workers.append(w)
        w.start()

    def _on_stream_fail(self, track: dict, quality: str, err: str):
        tid = track.get("id")
        self._st(f"Stream error ({quality}): {err}")
        if tid is None:
            return
        nxt = self._job_advance_quality(tid)
        if nxt:
            self._fetch_stream_for(track, nxt)
        else:
            if tid in self._track_rows:
                self._track_rows[tid].set_done(False)
            self._dl_jobs.pop(tid, None)

    def _got_stream_job(self, data: dict, track: dict, quality: str):
        tid = track.get("id")
        url = _tidal_resolve_stream_url(data)
        if not url:
            # Try next quality in chain
            if tid is not None:
                nxt = self._job_advance_quality(tid)
                if nxt:
                    self._st(f"No stream URL ({quality}) — trying {nxt}…")
                    self._fetch_stream_for(track, nxt)
                    return
            self._st("No stream URL found — try later or check your proxy/backend.")
            if tid in self._track_rows:
                self._track_rows[tid].set_done(False)
            self._dl_jobs.pop(tid, None)
            return

        # Start download with explicit quality + full stream payload
        if tid is not None and tid in self._dl_jobs:
            self._dl_jobs[tid]["quality"] = quality
        self._start_dl(url, track, force_quality=quality, stream_data=data)

    def _apply_custom_api_url(self):
        global _TIDAL_CUSTOM_BASE
        url = self._prefs().get("custom_api_url", "").strip()
        _TIDAL_CUSTOM_BASE = url if url else None

    # ── Build ─────────────────────────────────────────────────

    def _build(self):
        root = QVBoxLayout(self)
        root.setContentsMargins(0, 0, 0, 0)
        root.setSpacing(0)

        # ── Header ────────────────────────────────────────────
        hdr = QWidget()
        hdr.setFixedHeight(60)
        hdr.setStyleSheet(
            f"background:rgba(5,7,11,0.65); border-bottom:1px solid rgba(255,255,255,0.07);"
        )
        hb  = QHBoxLayout(hdr)
        hb.setContentsMargins(16, 0, 14, 0)
        hb.setSpacing(10)

        # Back button (hidden until there's history)
        self._back_btn = QPushButton("← Back")
        self._back_btn.setObjectName("ghost")
        self._back_btn.setFixedHeight(30)
        self._back_btn.setVisible(False)
        self._back_btn.clicked.connect(self._go_back)
        hb.addWidget(self._back_btn)

        brand = QLabel("TIDAL DL")
        bf    = QFont(); bf.setPointSize(13); bf.setBold(True)
        brand.setFont(bf)
        brand.setStyleSheet(f"color:{tok('accent')};background:transparent;")
        hb.addWidget(brand)

        self._breadcrumb = QLabel("")
        self._breadcrumb.setStyleSheet("color:rgba(255,255,255,0.35);font-size:10px;background:transparent;")
        hb.addWidget(self._breadcrumb)
        hb.addStretch()

        self._search_in = QLineEdit()
        self._search_in.setPlaceholderText("Search tracks, albums, artists…")
        self._search_in.setMinimumWidth(300)
        self._search_in.returnPressed.connect(self._do_search)
        hb.addWidget(self._search_in, stretch=1)

        srch_btn = QPushButton("Search")
        srch_btn.setFixedHeight(32)
        srch_btn.setMinimumWidth(90)
        srch_btn.setObjectName("toggle")
        srch_btn.clicked.connect(self._do_search)
        hb.addWidget(srch_btn)

        self._settings_btn = QPushButton("Settings")
        self._settings_btn.setFixedHeight(32)
        self._settings_btn.setMinimumWidth(90)
        self._settings_btn.setCheckable(True)
        self._settings_btn.setObjectName("toggle")
        self._settings_btn.clicked.connect(self._toggle_settings)
        hb.addWidget(self._settings_btn)

        root.addWidget(hdr)

        # ── Tab bar ───────────────────────────────────────────
        tab_bar = QWidget()
        tab_bar.setFixedHeight(38)
        tab_bar.setStyleSheet(
            f"background:rgba(5,7,11,0.65); border-bottom:1px solid rgba(255,255,255,0.07);"
        )
        tb = QHBoxLayout(tab_bar)
        tb.setContentsMargins(12, 0, 12, 0)
        tb.setSpacing(0)

        self._tab_btns: list[QPushButton] = []
        for i, lbl in enumerate(["Tracks", "Albums", "Artists"]):
            btn = QPushButton(lbl)
            btn.setFlat(True)
            btn.setFixedHeight(38)
            btn.setFixedWidth(84)
            btn.setCheckable(True)
            btn.setChecked(i == 0)
            btn.setObjectName("tabbtn")
            btn.clicked.connect(lambda _, x=i: self._switch_tab(x))
            self._tab_btns.append(btn)
            tb.addWidget(btn)

        tb.addStretch()
        self._status_lbl = QLabel("Search for music above to get started.")
        self._status_lbl.setObjectName("muted")
        # Don't hardcode theme colors here; let the global stylesheet drive them.
        self._status_lbl.setStyleSheet("font-size:11px;padding-right:10px;")
        tb.addWidget(self._status_lbl)
        root.addWidget(tab_bar)

        # ── Body (results + queue sidebar) ────────────────────
        body_hb = QHBoxLayout()
        body_hb.setContentsMargins(0, 0, 0, 0)
        body_hb.setSpacing(0)

        # Results stack
        self._results_stack = QStackedWidget()

        def _scroll_page():
            sa = QScrollArea()
            sa.setWidgetResizable(True)
            sa.setFrameShape(QFrame.Shape.NoFrame)
            sa.setStyleSheet("QScrollArea{background:transparent;}")
            inner = QWidget()
            inner.setObjectName("results_inner")
            inner.setStyleSheet("QWidget#results_inner{background:transparent;}")
            layout = QVBoxLayout(inner)
            layout.setContentsMargins(10, 8, 10, 8)
            layout.setSpacing(4)
            layout.addStretch()
            sa.setWidget(inner)
            return sa, layout

        self._tracks_sa,  self._tracks_l  = _scroll_page()
        self._albums_sa,  self._albums_l  = _scroll_page()
        self._artists_sa, self._artists_l = _scroll_page()
        self._results_stack.addWidget(self._tracks_sa)
        self._results_stack.addWidget(self._albums_sa)
        self._results_stack.addWidget(self._artists_sa)
        body_hb.addWidget(self._results_stack, stretch=1)

        # Queue sidebar
        self._queue_side = _TidalQueueSidebar()
        self._queue_side.download_all_req.connect(self._download_queue)
        self._queue_side.export_csv_req.connect(self._export_csv)
        self._queue_side.cancel_one_req.connect(self._on_cancel_track)
        self._queue_side.cancel_all_req.connect(self._on_cancel_all)
        self._queue_side.pause_one_req.connect(self._on_pause_track)
        self._queue_side.pause_all_req.connect(self._on_pause_all)
        body_hb.addWidget(self._queue_side)

        body_w = QWidget()
        body_w.setLayout(body_hb)
        root.addWidget(body_w, stretch=1)

        # ── Floating settings panel ───────────────────────────
        # Parent to self so it floats over content
        self._settings_panel = _TidalSettingsPanel(self._conf, self)
        self._settings_panel.quality_changed.connect(self._on_quality_changed)
        self._settings_panel.download_queue_req.connect(self._download_queue)
        self._settings_panel.export_csv_req.connect(self._export_csv)
        self._settings_panel.hide()

    # ── Layout events ─────────────────────────────────────────

    def resizeEvent(self, e):
        super().resizeEvent(e)
        self._place_settings_panel()

    def showEvent(self, e):
        super().showEvent(e)
        self._place_settings_panel()

    def _place_settings_panel(self):
        """Position the floating settings panel below the ⚙ button."""
        w  = self._settings_panel.width()
        # Right-align with 12px margin from right edge, below header (54px + 38px tabs = 92px)
        x  = self.width() - w - 12
        y  = 96
        h  = min(700, self.height() - y - 12)
        self._settings_panel.setGeometry(x, y, w, h)

    # ── Tab switching ─────────────────────────────────────────

    def _switch_tab(self, idx: int):
        for i, btn in enumerate(self._tab_btns):
            btn.setChecked(i == idx)
        self._results_stack.setCurrentIndex(idx)

    # ── Settings toggle ───────────────────────────────────────

    def _toggle_settings(self):
        if self._settings_panel.isVisible():
            self._settings_panel.hide()
            self._settings_btn.setChecked(False)
        else:
            self._place_settings_panel()
            self._settings_panel.show()
            self._settings_panel.raise_()
            self._settings_btn.setChecked(True)

    def _on_quality_changed(self, q: str):
        lbl = {"HI_RES_LOSSLESS":"Hi-Res","LOSSLESS":"CD","HIGH":"320k","LOW":"96k"}
        self._settings_btn.setText(f"⚙  {lbl.get(q, q)}")

    # ── Status helper ─────────────────────────────────────────

    def _st(self, msg: str):
        # Truncate long messages (e.g. URLs in error strings)
        if len(msg) > 120:
            msg = msg[:117] + "…"
        active = len(self._dl_wkrs)
        if active > 0:
            self._status_lbl.setText(f"[{active} downloading]  {msg}")
        else:
            self._status_lbl.setText(msg)

    # ── Clear a tab's contents ────────────────────────────────

    def _clear_tab(self, layout: QVBoxLayout):
        # Guard against late worker callbacks after a UI restart.
        try:
            if layout is None or sip.isdeleted(layout):
                return
        except Exception:
            return
        while layout.count() > 1:          # keep the trailing stretch
            item = layout.takeAt(0)
            if w := item.widget():
                w.deleteLater()


    # ── Search ────────────────────────────────────────────────

    # ── Tidal URL patterns ───────────────────────────────────────────────────
    _TIDAL_URL_RE = re.compile(
        r"(?:https?://)?(?:www\.)?tidal\.com(?:/browse)?/(album|track|artist|playlist|mix)/([A-Za-z0-9]+)",
        re.IGNORECASE,
    )

    def _resolve_tidal_url(self, q: str) -> bool:
        """
        Detect a Tidal URL in q and resolve it directly by ID.
        Returns True if handled, False to fall through to normal text search.
        """
        m = self._TIDAL_URL_RE.search(q)
        if not m:
            return False

        kind = m.group(1).lower()
        tid  = m.group(2)

        self._search_results = {}
        self._nav_history.clear()
        self._back_btn.setVisible(False)
        self._breadcrumb.setText("")

        if kind == "album":
            # Reuse existing open flow: fetches /album/?id=X then shows tracks
            self._st(f"Loading album…")
            self._push_nav_state(f"Album {tid}")
            self._on_album_open({"id": tid, "title": ""})

        elif kind == "track":
            # Track URLs are not supported: the proxy only returns stream/playback
            # data for /track/?id=X (no title, artist, or album metadata).
            self._st("Track links aren't supported — paste an album or artist URL instead.")
            return False

        elif kind == "artist":
            self._st(f"Loading artist…")
            self._load_artist_albums({"id": tid, "name": ""})

        elif kind in ("playlist", "mix"):
            self._st(f"Loading {kind}…")
            w = _TidalWorker(f"/{kind}/?id={tid}")
            def _got_pl(data, _kind=kind):
                tracks = _tidal_extract_tracks(data)
                if tracks:
                    self._populate_tracks(tracks)
                    self._switch_tab(0)
                    self._st(f"{len(tracks)} tracks")
                else:
                    self._st(f"No tracks found — the proxy may not support {_kind} lookup.")
            w.ok.connect(_got_pl)
            w.fail.connect(lambda e: self._st(f"Could not load {kind}: {e}"))
            w.finished.connect(lambda: self._workers.remove(w) if w in self._workers else None)
            self._workers.append(w)
            w.start()

        else:
            return False

        return True

    def _do_search(self):
        q = self._search_in.text().strip()
        if not q:
            return

        # Check for a Tidal URL first
        if self._resolve_tidal_url(q):
            return

        qs = requests.utils.quote(q)
        self._st("Searching all tabs…")
        self._search_results = {}  # reset for new search
        self._nav_history.clear()
        self._back_btn.setVisible(False)
        self._breadcrumb.setText("")

        # Fire all 3 searches simultaneously so all tabs populate at once
        search_configs = [
            (f"/search/?s={qs}&limit=200",  0),   # tracks
            (f"/search/?al={qs}&limit=200", 1),   # albums
            (f"/search/?a={qs}&limit=200",  2),   # artists
        ]
        for path, tab_idx in search_configs:
            w = _TidalWorker(path)
            w.ok.connect(lambda d, t=tab_idx: self._on_search_ok(d, t))
            w.fail.connect(lambda e, t=tab_idx: self._on_search_fail(t))
            w.finished.connect(lambda: self._workers.remove(w) if w in self._workers else None)
            self._workers.append(w)
            w.start()

    def _on_search_fail(self, tab: int):
        self._search_results[tab] = 0
        if len(self._search_results) == 3:
            t = self._search_results.get(0, 0)
            al = self._search_results.get(1, 0)
            ar = self._search_results.get(2, 0)
            self._st(f"{t} tracks · {al} albums · {ar} artists")

    def _on_search_ok(self, data: dict, tab: int):
        if getattr(self, '_alive', True) is False:
            return
        try:
            if sip.isdeleted(self):
                return
        except Exception:
            pass
        if tab == 0:
            items = _tidal_extract_tracks(data)
            self._populate_tracks(items)
            count = len(items)
            self._search_results[0] = count
        elif tab == 1:
            items = _tidal_extract_albums(data)
            self._populate_albums(items)
            count = len(items)
            self._search_results[1] = count
        else:
            items = _tidal_extract_artists(data)
            self._populate_artists(items)
            count = len(items)
            self._search_results[2] = count

        # Update status once all 3 have responded
        if len(self._search_results) == 3:
            t = self._search_results.get(0, 0)
            al = self._search_results.get(1, 0)
            ar = self._search_results.get(2, 0)
            self._st(f"{t} tracks · {al} albums · {ar} artists")

    # ── Populate tracks ───────────────────────────────────────

    def _populate_tracks(self, tracks: list):
        self._clear_tab(self._tracks_l)
        self._clear_album_header()
        active_ids = set(self._dl_wkrs.keys())
        self._track_rows = {tid: row for tid, row in self._track_rows.items()
                            if tid in active_ids}
        for tr in tracks:
            row = _TidalTrackRow(tr)
            row.download_req.connect(self._on_dl_track)
            row.queue_req.connect(self._queue_side.add_track)
            row.album_open.connect(self._on_album_open)
            self._tracks_l.insertWidget(self._tracks_l.count() - 1, row)
            tid = tr.get("id")
            if tid is not None:
                self._track_rows[tid] = row

    # ── Populate albums ───────────────────────────────────────

    def _populate_albums(self, albums: list):
        """Responsive grid of album cards — reflows on window resize."""
        self._clear_tab(self._albums_l)
        t = _current_theme
        if not albums:
            empty = QLabel("No albums found.")
            empty.setObjectName("secondary")
            empty.setStyleSheet("padding:24px;color:rgba(255,255,255,0.55);")
            self._albums_l.insertWidget(self._albums_l.count() - 1, empty)
            return
        flow = _AlbumFlowWidget(
            albums,
            on_open=self._on_album_open,
            on_dl=self._on_dl_album,
            on_queue=self._on_queue_album,
        )
        self._albums_l.insertWidget(self._albums_l.count() - 1, flow)


    # ── Populate artists ──────────────────────────────────────────────────────

    def _populate_artists(self, artists: list):
        self._clear_tab(self._artists_l)
        t = _current_theme
        for art in artists:
            row = QWidget()
            row.setFixedHeight(58)
            row.setStyleSheet(
                f"background:rgba(255,255,255,0.05);border:1px solid rgba(255,255,255,0.09);border-radius:5px;"
            )
            hb = QHBoxLayout(row)
            hb.setContentsMargins(10, 8, 10, 8)
            hb.setSpacing(12)

            pic = QLabel()
            pic.setFixedSize(40, 40)
            pic.setAlignment(Qt.AlignmentFlag.AlignCenter)
            pic.setText("◉")
            pic.setStyleSheet(
                f"background:rgba(255,255,255,0.08);border-radius:20px;color:rgba(255,255,255,0.35);font-size:16px;"
            )
            hb.addWidget(pic)

            nl = QLabel(art.get("name", ""))
            nl.setStyleSheet("font-weight:600;color:rgba(255,255,255,0.85);font-size:13px;")
            hb.addWidget(nl, stretch=1)

            alb_btn = QPushButton("Browse albums")
            alb_btn.setFixedHeight(28)
            alb_btn.setObjectName("ghost")
            alb_btn.clicked.connect(lambda _, a=art: self._load_artist_albums(a))
            hb.addWidget(alb_btn)

            dl_btn = QPushButton("⬇  Download")
            dl_btn.setFixedHeight(28)
            dl_btn.setMinimumWidth(100)
            dl_btn.setToolTip("Download all tracks by this artist")
            dl_btn.clicked.connect(lambda _, a=art: self._on_dl_artist(a))
            hb.addWidget(dl_btn)

            q_btn = QPushButton("+ Queue")
            q_btn.setFixedHeight(28)
            q_btn.setMinimumWidth(66)
            q_btn.setToolTip("Add all tracks by this artist to the download queue")
            q_btn.clicked.connect(lambda _, a=art: self._on_queue_artist(a))
            hb.addWidget(q_btn)

            self._artists_l.insertWidget(self._artists_l.count() - 1, row)

            pid = art.get("picture", "")
            if pid:
                _load_cover_into_label(pid, pic, 40, circle=True)

    # ── Album → track list ────────────────────────────────────

    def _on_album_open(self, album: dict):
        aid = album.get("id")
        if not aid:
            return
        self._st(f"Loading '{album.get('title','')}'…")
        w = _TidalWorker(f"/album/?id={aid}")
        w.ok.connect(lambda d, a=album: self._show_album_tracks(d, a))
        w.fail.connect(lambda e: self._st(f"Error: {e}"))
        w.finished.connect(lambda: self._workers.remove(w) if w in self._workers else None)
        self._workers.append(w)
        w.start()

    def _push_nav_state(self, breadcrumb: str = ""):
        """Save current view state to history stack."""
        state = {
            "tab":        self._results_stack.currentIndex(),
            "breadcrumb": self._breadcrumb.text(),
            "status":     self._status_lbl.text(),
        }
        self._nav_history.append(state)
        self._breadcrumb.setText(f"  ›  {breadcrumb}" if breadcrumb else "")
        self._back_btn.setVisible(True)

    def _go_back(self):
        """Restore previous view state."""
        if not self._nav_history:
            return
        state = self._nav_history.pop()
        self._results_stack.setCurrentIndex(state["tab"])
        for i, btn in enumerate(self._tab_btns):
            btn.setChecked(i == state["tab"])
        self._breadcrumb.setText(state["breadcrumb"])
        self._status_lbl.setText(state["status"])
        self._back_btn.setVisible(bool(self._nav_history))

    def _show_album_tracks(self, data: dict, album: dict):
        self._push_nav_state(album.get("title", "Album"))
        tracks = _tidal_extract_tracks(data)
        n = len(tracks)
        self._st(f"{album.get('title', '')} · {n} track{'s' if n!=1 else ''}")
        self._switch_tab(0)
        self._show_album_header(album, n)
        self._populate_tracks(tracks)

    def _show_album_header(self, album: dict, track_count: int):
        """Inject a sticky album header with cover art at the top of the tracks list."""
        t = _current_theme
        # Remove any previous header
        self._clear_album_header()

        hdr = QWidget()
        hdr.setObjectName("alb_hdr")
        hdr.setStyleSheet(
            f"QWidget#alb_hdr{{background:rgba(255,255,255,0.05);border:1px solid rgba(255,255,255,0.09);"
            f"border-radius:10px;margin-bottom:4px;}}"
            f"QWidget#alb_hdr QLabel{{background:transparent;border:none;}}"
        )
        hb = QHBoxLayout(hdr)
        hb.setContentsMargins(14, 12, 14, 12)
        hb.setSpacing(14)

        # Cover art
        cover_lbl = QLabel("♪")
        cover_lbl.setFixedSize(72, 72)
        cover_lbl.setAlignment(Qt.AlignmentFlag.AlignCenter)
        cover_lbl.setStyleSheet(
            f"background:rgba(255,255,255,0.08);border-radius:8px;color:rgba(255,255,255,0.35);font-size:28px;"
        )
        hb.addWidget(cover_lbl)

        cover_id = (album.get("cover") or album.get("coverId") or
                    _tidal_find_cover_id(album))
        if cover_id:
            _load_cover_into_label(cover_id, cover_lbl, 72, corner_radius=8)

        # Info
        info = QVBoxLayout()
        info.setSpacing(3)
        info.setContentsMargins(0, 0, 0, 0)

        title_lbl = QLabel(album.get("title") or "Unknown Album")
        title_lbl.setStyleSheet(
            f"font-weight:700;font-size:15px;color:rgba(255,255,255,0.87);"
        )
        info.addWidget(title_lbl)

        artist = ""
        if isinstance(album.get("artist"), dict):
            artist = album["artist"].get("name", "")
        if not artist and isinstance(album.get("artists"), list) and album["artists"]:
            a0 = album["artists"][0]
            if isinstance(a0, dict):
                artist = a0.get("name", "")

        year = str(album.get("releaseDate") or album.get("release_date") or album.get("year") or "")
        year = year.split("-")[0] if year else ""

        meta_parts = [p for p in [artist, year,
                                   f"{track_count} track{'s' if track_count != 1 else ''}"] if p]
        meta_lbl = QLabel("  ·  ".join(meta_parts))
        meta_lbl.setStyleSheet("color:rgba(255,255,255,0.55);font-size:12px;")
        info.addWidget(meta_lbl)

        hb.addLayout(info, stretch=1)

        # Action buttons
        dl_btn = QPushButton("⬇  Download All")
        dl_btn.setFixedHeight(32)
        dl_btn.setMinimumWidth(120)
        dl_btn.clicked.connect(lambda: self._on_dl_album(album))
        hb.addWidget(dl_btn)

        q_btn = QPushButton("+ Queue All")
        q_btn.setFixedHeight(32)
        q_btn.setMinimumWidth(90)
        q_btn.clicked.connect(lambda: self._on_queue_album(album))
        hb.addWidget(q_btn)

        self._tracks_l.insertWidget(0, hdr)
        self._album_header_widget = hdr

    def _clear_album_header(self):
        hdr = getattr(self, "_album_header_widget", None)
        if hdr is not None:
            try:
                self._tracks_l.removeWidget(hdr)
                hdr.deleteLater()
            except Exception:
                pass
            self._album_header_widget = None

    # ── Artist → albums ───────────────────────────────────────

    def _load_artist_albums(self, artist: dict):
        aid = artist.get("id")
        if not aid:
            return
        self._push_nav_state(artist.get("name", "Artist"))
        self._st(f"Loading albums for {artist.get('name', '')}…")
        w = _TidalWorker(f"/artist/?f={aid}")
        w.ok.connect(self._show_artist_albums)
        w.fail.connect(lambda e: self._st(f"Error: {e}"))
        w.finished.connect(lambda: self._workers.remove(w) if w in self._workers else None)
        self._workers.append(w)
        w.start()

    def _show_artist_albums(self, data: dict):
        albums = _tidal_extract_albums(data)
        tracks = _tidal_extract_tracks(data)
        n_alb = len(albums)
        n_tr  = len(tracks)
        self._st(f"{n_alb} album{'s' if n_alb!=1 else ''} · {n_tr} track{'s' if n_tr!=1 else ''}")
        if tracks:
            self._populate_tracks(tracks)
        self._switch_tab(1)
        self._populate_albums(albums)

    def _on_dl_track(self, track: dict):
        tid = track.get("id")
        if not tid:
            return
        # Already downloading this track
        if tid in self._dl_wkrs:
            self._st(f"Already downloading '{track.get('title','')}'")
            return

        # Initialize job with full quality chain
        self._job_init(track)
        q0 = self._job_next_quality(tid)
        if not q0:
            self._st("No qualities configured.")
            return
        self._fetch_stream_for(track, q0)

    def _on_cancel_track(self, track: dict):
        """Cancel an active download for the given track."""
        tid = track.get("id")
        if tid is None:
            return
        w = self._dl_wkrs.get(tid)
        if w:
            w.cancel()
            self._st(f"Cancelling '{track.get('title', '')}'…")
        self._dl_jobs.pop(tid, None)
        self._queue_side.notify_download_done(tid, False)
        if tid in self._track_rows:
            self._track_rows[tid].set_done(False)

    def _on_cancel_all(self):
        """Cancel all active downloads."""
        tids = list(self._dl_wkrs.keys())
        for tid in tids:
            w = self._dl_wkrs.get(tid)
            if w:
                w.cancel()
            job = self._dl_jobs.pop(tid, None)
            self._queue_side.notify_download_done(tid, False)
            if tid in self._track_rows:
                self._track_rows[tid].set_done(False)
        self._st(f"Cancelled {len(tids)} download{'s' if len(tids) != 1 else ''}.")

    def _on_pause_track(self, track: dict):
        """Toggle pause/resume for a single active download."""
        tid = track.get("id")
        if tid is None:
            return
        w = self._dl_wkrs.get(tid)
        if w:
            if w.is_paused:
                w.resume()
                self._st(f"Resumed '{track.get('title', '')}'")
            else:
                w.pause()
                self._st(f"Paused '{track.get('title', '')}'")

    def _on_pause_all(self):
        """Toggle pause/resume for all active downloads."""
        workers = list(self._dl_wkrs.values())
        if not workers:
            return
        # If any is running, pause all; if all paused, resume all
        any_running = any(not w.is_paused for w in workers)
        for w in workers:
            if any_running:
                w.pause()
            else:
                w.resume()
        state = "Paused" if any_running else "Resumed"
        self._st(f"{state} {len(workers)} download{'s' if len(workers) != 1 else ''}.")

    def _got_stream(self, data: dict, track: dict):
        # Back-compat: route to job system using current pref quality
        tid = track.get('id')
        if tid and tid not in self._dl_jobs:
            self._job_init(track)
        q = self._prefs().get('quality','HI_RES_LOSSLESS')
        self._got_stream_job(data, track, q)

    def _got_stream_lossless(self, data: dict, track: dict):
        """Called when Hi-Res failed and we fell back to LOSSLESS."""
        url = _tidal_resolve_stream_url(data)
        if not url:
            self._st(f"No stream URL for '{track.get('title','')}' — endpoint may be down.")
            tid = track.get("id")
            if tid in self._track_rows:
                self._track_rows[tid].set_done(False)
            return
        # Force LOSSLESS quality for this track's file naming/extension
        track_copy = dict(track)
        self._start_dl(url, track_copy, stream_data=data, force_quality="LOSSLESS")

    def _start_dl(self, url: str, track: dict, force_quality: str = None, stream_data: dict = None):
        prefs   = self._prefs()
        quality = force_quality or prefs.get("quality", "HI_RES_LOSSLESS")

        # Check if this is a DASH stream — if so, output is always FLAC regardless of URL
        _sd = stream_data or {}
        if isinstance(_sd.get("data"), dict):
            _sd = _sd["data"]
        _is_dash = bool(_sd.get("manifest")) and "dash+xml" in (_sd.get("manifestMimeType") or "").lower()

        if _is_dash:
            ext = "flac"
        else:
            url_lower = url.lower().split("?")[0]
            if url_lower.endswith(".mp4") or url_lower.endswith(".m4a"):
                ext = "m4a"
            elif url_lower.endswith(".flac"):
                ext = "flac"
            else:
                ext = _TIDAL_QUALITY_EXT.get(quality, "flac")

        all_artists_list = track.get("artists") or []
        all_artists = ", ".join(a.get("name", "") for a in all_artists_list)
        if not all_artists:
            all_artists = (track.get("artist") or {}).get("name", "Unknown")

        # Use only primary (first) artist for folder/filename structure
        use_primary = prefs.get("use_primary_artist", True)
        if use_primary and all_artists_list:
            folder_artist = all_artists_list[0].get("name", "") or all_artists
        elif use_primary and not all_artists_list:
            raw = (track.get("artist") or {}).get("name", "")
            folder_artist = raw.split(",")[0].strip() if raw else "Unknown"
        else:
            folder_artist = all_artists or "Unknown"
        if not folder_artist:
            folder_artist = "Unknown"

        title   = track.get("title", "Unknown")
        if track.get("version"):
            title += f" ({track['version']})"
        alb_title = (track.get("album") or {}).get("title", "Unknown Album")
        tnum      = track.get("trackNumber") or 0
        tnum_s    = str(tnum).zfill(2) if tnum else "00"

        base_raw  = prefs.get("dl_base_path", "").strip()
        base_path = Path(base_raw) if base_raw else Path.home() / "Music" / "TIDAL"
        dest      = (base_path
                     / _sanitize_path(folder_artist)
                     / _sanitize_path(alb_title)
                     / f"{tnum_s} - {_sanitize_path(title)}.{ext}")
        try:
            dest.parent.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            self._st(f"Cannot create folder: {e}")
            return

        tid = track.get("id")
        if prefs.get("skip_existing", True) and dest.exists():
            self._st(f"Already exists: {dest.name}")
            if tid in self._track_rows:
                self._track_rows[tid].set_done(True)
            return

        self._st(f"Downloading: {all_artists} – {title}")
        if tid in self._track_rows:
            self._track_rows[tid].set_downloading(0)
        # Notify queue sidebar so it shows active download row
        self._queue_side.notify_download_start(track)

        w = _TidalDlWorker(url, str(dest), track=track, prefs=dict(prefs), stream_data=stream_data)
        w.progress.connect(lambda recv, tot, ti=tid: self._dl_prog(ti, recv, tot))
        w.done.connect(lambda ok, msg, ti=tid, d=dest: self._dl_done(ti, ok, msg, d))
        # Keep worker alive in both dicts
        if tid is not None:
            self._dl_wkrs[tid] = w
        self._workers.append(w)
        w.finished.connect(lambda: self._workers.remove(w) if w in self._workers else None)
        w.start()

    def _dl_prog(self, tid, recv: int, total: int):
        try:
            pct = int(recv * 100 / total) if total else 0
            if tid in self._track_rows:
                row = self._track_rows[tid]
                if row:
                    row.set_downloading(pct)
            self._queue_side.notify_download_progress(tid, pct)
        except Exception:
            pass

    def _dl_done(self, tid, ok: bool, msg: str, dest: Path):
        try:
            if tid is not None and tid in self._track_rows:
                row = self._track_rows[tid]
                if row and not row.isHidden():
                    row.set_done(ok)
            dest_name = dest.name if isinstance(dest, Path) else str(dest)
            self._st(f"✓ Saved: {dest_name}" if ok else f"Error: {msg}")
        except Exception:
            pass
        finally:
            if tid is not None:
                self._dl_wkrs.pop(tid, None)
                # Always remove from active downloads display
                self._queue_side.notify_download_done(tid, ok)

        # On failure: retry same quality (refresh stream) then fall back down the chain
        if ok or tid is None:
            if tid is not None:
                self._dl_jobs.pop(tid, None)
            return

        job = self._dl_jobs.get(tid)
        if not job:
            return

        track = job.get("track") or {}
        cur_q = job.get("quality") or self._QUALITY_CHAIN[min(int(job.get("qi", 0)), len(self._QUALITY_CHAIN)-1)]
        retries = int(job.get("retries", 0))
        if retries < self._retry_max():
            job["retries"] = retries + 1
            self._st(f"Retrying ({cur_q}) {job['retries']}/{self._retry_max()} …")
            self._fetch_stream_for(track, cur_q)
            return

        nxt = self._job_advance_quality(tid)
        if nxt:
            self._st(f"Falling back to {nxt} …")
            self._fetch_stream_for(track, nxt)
            return

        # Exhausted chain
        self._st("All qualities failed for this track.")
        self._dl_jobs.pop(tid, None)

    # ── Download album ────────────────────────────────────────

    def _on_dl_album(self, album: dict):
        aid = album.get("id")
        if not aid:
            return
        self._st(f"Fetching tracks for '{album.get('title','')}'…")
        w = _TidalWorker(f"/album/?id={aid}")
        w.ok.connect(lambda d, a=album: self._dl_album_tracks(d, a))
        w.fail.connect(lambda e: self._st(f"Error: {e}"))
        w.finished.connect(lambda: self._workers.remove(w) if w in self._workers else None)
        self._workers.append(w)
        w.start()

    def _dl_album_tracks(self, data: dict, album: dict):
        tracks = _tidal_extract_tracks(data)
        if not tracks:
            self._st("No tracks found for album.")
            return
        self._switch_tab(0)
        self._populate_tracks(tracks)
        n = len(tracks)
        self._st(f"Queuing {n} track{'s' if n!=1 else ''} for download…")
        # Stagger track fetch requests to avoid overwhelming the API
        def _enqueue(i=0):
            if i < len(tracks):
                self._on_dl_track(tracks[i])
                QTimer.singleShot(300, lambda: _enqueue(i + 1))
        _enqueue()

    def _on_queue_album(self, album: dict):
        """Fetch album tracks and add them all to the download queue."""
        aid = album.get("id")
        if not aid:
            return
        self._st(f"Fetching tracks for '{album.get('title', '')}'…")
        w = _TidalWorker(f"/album/?id={aid}")
        w.ok.connect(lambda d, a=album: self._queue_album_tracks(d, a))
        w.fail.connect(lambda e: self._st(f"Error fetching album: {e}"))
        w.finished.connect(lambda: self._workers.remove(w) if w in self._workers else None)
        self._workers.append(w)
        w.start()

    def _queue_album_tracks(self, data: dict, album: dict):
        tracks = _tidal_extract_tracks(data)
        if not tracks:
            self._st("No tracks found for album.")
            return
        for tr in tracks:
            self._queue_side.add_track(tr)
        n = len(tracks)
        self._st(f"Added {n} track{'s' if n!=1 else ''} from '{album.get('title', '')}' to queue.")

    def _on_dl_artist(self, artist: dict):
        """Fetch all artist albums then download every track."""
        aid = artist.get("id")
        if not aid:
            return
        self._st(f"Fetching discography for '{artist.get('name', '')}'…")
        w = _TidalWorker(f"/artist/?f={aid}")
        w.ok.connect(lambda d, a=artist: self._dl_artist_albums(d, a))
        w.fail.connect(lambda e: self._st(f"Error fetching artist: {e}"))
        w.finished.connect(lambda: self._workers.remove(w) if w in self._workers else None)
        self._workers.append(w)
        w.start()

    def _dl_artist_albums(self, data: dict, artist: dict):
        albums = _tidal_extract_albums(data)
        if not albums:
            self._st(f"No albums found for '{artist.get('name', '')}'.")
            return
        self._st(f"Downloading {len(albums)} album(s) by '{artist.get('name', '')}'…")
        # Fetch and download each album's tracks, staggered
        def _fetch_next(i=0):
            if i >= len(albums):
                return
            alb = albums[i]
            aid = alb.get("id")
            if not aid:
                _fetch_next(i + 1)
                return
            ww = _TidalWorker(f"/album/?id={aid}")
            ww.ok.connect(lambda d, a=alb, ni=i: (self._dl_album_tracks(d, a),
                                                    QTimer.singleShot(800, lambda: _fetch_next(ni + 1))))
            ww.fail.connect(lambda e, ni=i: QTimer.singleShot(400, lambda: _fetch_next(ni + 1)))
            ww.finished.connect(lambda: self._workers.remove(ww) if ww in self._workers else None)
            self._workers.append(ww)
            ww.start()
        _fetch_next()

    def _on_queue_artist(self, artist: dict):
        """Fetch all artist albums and queue every track."""
        aid = artist.get("id")
        if not aid:
            return
        self._st(f"Fetching discography for '{artist.get('name', '')}'…")
        w = _TidalWorker(f"/artist/?f={aid}")
        w.ok.connect(lambda d, a=artist: self._queue_artist_albums(d, a))
        w.fail.connect(lambda e: self._st(f"Error fetching artist: {e}"))
        w.finished.connect(lambda: self._workers.remove(w) if w in self._workers else None)
        self._workers.append(w)
        w.start()

    def _queue_artist_albums(self, data: dict, artist: dict):
        albums = _tidal_extract_albums(data)
        if not albums:
            self._st(f"No albums found for '{artist.get('name', '')}'.")
            return
        self._st(f"Queuing all tracks by '{artist.get('name', '')}'…")
        total_queued = [0]
        def _fetch_next(i=0):
            if i >= len(albums):
                self._st(f"Added {total_queued[0]} tracks by '{artist.get('name', '')}' to queue.")
                return
            alb = albums[i]
            aid = alb.get("id")
            if not aid:
                _fetch_next(i + 1)
                return
            ww = _TidalWorker(f"/album/?id={aid}")
            def _on_ok(d, ni=i):
                tracks = _tidal_extract_tracks(d)
                for tr in tracks:
                    self._queue_side.add_track(tr)
                total_queued[0] += len(tracks)
                QTimer.singleShot(400, lambda: _fetch_next(ni + 1))
            ww.ok.connect(_on_ok)
            ww.fail.connect(lambda e, ni=i: QTimer.singleShot(400, lambda: _fetch_next(ni + 1)))
            ww.finished.connect(lambda: self._workers.remove(ww) if ww in self._workers else None)
            self._workers.append(ww)
            ww.start()
        _fetch_next()

    # ── Queue download ────────────────────────────────────────

    def _download_queue(self):
        tracks = self._queue_side.tracks()
        if not tracks:
            self._st("Queue is empty.")
            return
        mode = self._prefs().get("dl_mode", "individual")
        if mode == "zip":
            self._queue_as_zip(tracks)
        elif mode == "csv":
            self._export_csv()
        else:
            n = len(tracks)
            self._st(f"Downloading {n} queued track{'s' if n!=1 else ''}…")
            def _enqueue(i=0):
                if i < len(tracks):
                    self._on_dl_track(tracks[i])
                    QTimer.singleShot(300, lambda: _enqueue(i + 1))
            _enqueue()

    def _queue_as_zip(self, tracks: list):
        import zipfile
        path, _ = QFileDialog.getSaveFileName(
            self, "Save queue as ZIP", str(Path.home() / "tidal_queue.zip"), "ZIP (*.zip)"
        )
        if not path:
            return
        q = self._prefs().get("quality", "LOSSLESS")
        self._st(f"Building ZIP with {len(tracks)} tracks (may take a while)…")
        def _work():
            with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as zf:
                for tr in tracks:
                    tid  = tr.get("id")
                    data = _tidal_req(f"/track/?id={tid}&quality={q}") or {}
                    url  = _tidal_resolve_stream_url(data)
                    if not url:
                        continue
                    ext  = _TIDAL_QUALITY_EXT.get(q, "flac")
                    name = _sanitize_path(tr.get("title", str(tid))) + "." + ext
                    try:
                        r = requests.get(url, timeout=90)
                        if r.status_code == 200:
                            zf.writestr(name, r.content)
                    except Exception:
                        pass
            QTimer.singleShot(0, lambda: self._st(f"✓ ZIP saved: {path}"))
        _threading.Thread(target=_work, daemon=True).start()

    def _export_csv(self):
        tracks = self._queue_side.tracks()
        if not tracks:
            self._st("Queue is empty.")
            return
        path, _ = QFileDialog.getSaveFileName(
            self, "Export queue as CSV", str(Path.home() / "tidal_queue.csv"), "CSV (*.csv)"
        )
        if not path:
            return
        q = self._prefs().get("quality", "LOSSLESS")
        self._st("Fetching URLs for CSV export…")
        def _work():
            import csv as _csv
            rows = [["#", "Title", "Artist", "Album", "Duration", "Stream URL"]]
            for i, tr in enumerate(tracks, 1):
                tid  = tr.get("id")
                data = _tidal_req(f"/track/?id={tid}&quality={q}") or {}
                url  = _tidal_resolve_stream_url(data)
                artists = ", ".join(a.get("name","") for a in (tr.get("artists") or []))
                rows.append([
                    str(i), tr.get("title",""), artists,
                    (tr.get("album") or {}).get("title",""),
                    _fmt_dur(tr.get("duration",0)), url,
                ])
            with open(path, "w", newline="", encoding="utf-8") as f:
                _csv.writer(f).writerows(rows)
            QTimer.singleShot(0, lambda: self._st(f"✓ CSV saved: {path}"))
        _threading.Thread(target=_work, daemon=True).start()

    def closeEvent(self, e):
        self._alive = False
        # Cancel all active downloads
        for w in list(self._dl_wkrs.values()):
            try:
                w.cancel()
            except Exception:
                pass
        super().closeEvent(e)


#  SPECTROGRAM ANALYZER  –  quality inspection tool
# ─────────────────────────────────────────────────────────────

_COLORMAPS = {
    "Magma": [
        (0,   0,   0),    # black
        (10,  0,  50),    # very deep purple
        (40,  0, 120),    # deep purple
        (100, 20, 160),   # purple
        (160, 40, 100),   # magenta-red
        (210, 80,  30),   # orange
        (245, 160,  0),   # amber
        (255, 220, 80),   # yellow
        (255, 255, 255),  # white
    ],
    "Inferno": [
        (0,   0,   4),
        (20,  11, 52),
        (58,  9,  99),
        (114, 0, 127),
        (167, 40, 97),
        (210, 90, 50),
        (242, 150, 15),
        (252, 214, 44),
        (255, 255, 255),
    ],
    "Viridis": [
        (68,  1,  84),
        (59,  82, 139),
        (33, 145, 140),
        (94, 201, 98),
        (253, 231, 37),
    ],
    "Classic": [
        (0,   0,   0),
        (0,   0, 180),
        (0, 180, 100),
        (200, 200,  0),
        (255, 100,  0),
        (255,   0,  20),
    ],
    "CoolWarm": [
        (59,  76, 192),
        (100, 160, 240),
        (220, 220, 220),
        (240, 130,  60),
        (180,  4,  38),
    ],
    "Mono": [
        (0, 0, 0),
        (255, 255, 255),
    ],
    "Green": [
        (0, 0, 0),
        (0, 40, 0),
        (0, 180, 0),
        (180, 255, 0),
        (255, 255, 255),
    ],
}


def _interp_colormap(stops: list, v: float) -> tuple:
    """Interpolate a list of RGB stops at position v in [0,1]."""
    n = len(stops)
    if n == 1:
        return stops[0]
    if v <= 0:
        return stops[0]
    if v >= 1:
        return stops[-1]
    seg   = v * (n - 1)
    lo    = int(seg)
    hi    = min(lo + 1, n - 1)
    t     = seg - lo
    r = int(stops[lo][0] + t * (stops[hi][0] - stops[lo][0]))
    g = int(stops[lo][1] + t * (stops[hi][1] - stops[lo][1]))
    b = int(stops[lo][2] + t * (stops[hi][2] - stops[lo][2]))
    return (r, g, b)


class _SpectrogramWorker(QThread):
    """
    Compute a spectrogram from an audio file using numpy if available,
    else falls back to subprocess + ffmpeg to extract PCM and compute manually.
    Emits dict: spec, sr, dur, fft, hop, n_frames, bit_depth, channels, file_size
    """
    done     = pyqtSignal(object)
    error    = pyqtSignal(str)
    progress = pyqtSignal(int)

    def __init__(self, path: str, fft_size: int = 4096,
                 window_fn: str = "Hann", parent=None):
        super().__init__(parent)
        self._path     = path
        self._fft_size = fft_size
        self._win_fn   = window_fn

    def run(self):
        try:
            self._compute()
        except Exception as e:
            self.error.emit(str(e))

    def _make_window(self, fft_size: int, name: str):
        try:
            import numpy as np
            n = fft_size
            if name == "Hann":      return np.hanning(n)
            elif name == "Hamming": return np.hamming(n)
            elif name == "Blackman":return np.blackman(n)
            else:                   return np.ones(n)
        except ImportError:
            import math
            n = fft_size
            if name == "Hann":
                return [0.5 - 0.5 * math.cos(2*math.pi*k/n) for k in range(n)]
            elif name == "Hamming":
                return [0.54 - 0.46 * math.cos(2*math.pi*k/n) for k in range(n)]
            elif name == "Blackman":
                return [0.42 - 0.5*math.cos(2*math.pi*k/n) + 0.08*math.cos(4*math.pi*k/n) for k in range(n)]
            else:
                return [1.0] * n

    def _compute(self):
        fft_size = self._fft_size
        hop      = fft_size // 4

        # ── 1. Probe metadata ──────────────────────────────────
        meta = {"bit_depth": "N/A", "channels": 2}
        try:
            probe = subprocess.run(
                ["ffprobe", "-v", "quiet", "-print_format", "json",
                 "-show_streams", self._path],
                capture_output=True, timeout=10
            )
            if probe.returncode == 0:
                pj = json.loads(probe.stdout.decode("utf-8", "ignore"))
                for st in pj.get("streams", []):
                    if st.get("codec_type") == "audio":
                        meta["channels"]  = st.get("channels", 2)
                        meta["bit_depth"] = (st.get("bits_per_raw_sample") or
                                             st.get("bits_per_coded_sample") or "N/A")
                        break
        except Exception:
            pass

        # ── 2. Decode PCM ──────────────────────────────────────
        cmd = ["ffmpeg", "-v", "quiet", "-i", self._path,
               "-ac", "1", "-ar", "44100", "-f", "s16le", "-"]
        try:
            proc = subprocess.run(cmd, capture_output=True, timeout=180)
            raw  = proc.stdout
        except FileNotFoundError:
            self.error.emit("ffmpeg not found. Install ffmpeg to use the spectrogram analyzer.")
            return
        except subprocess.TimeoutExpired:
            self.error.emit("ffmpeg timed out.")
            return

        if len(raw) < 4:
            self.error.emit("No audio data from ffmpeg. Is this a valid audio file?")
            return

        sr    = 44100
        n_smp = len(raw) // 2
        dur   = n_smp / sr
        fsize = Path(self._path).stat().st_size if Path(self._path).exists() else 0

        win = self._make_window(fft_size, self._win_fn)

        # ── 3. STFT — numpy fast path ──────────────────────────
        try:
            import numpy as np
            samples  = np.frombuffer(raw, dtype=np.int16).astype(np.float32) / 32768.0
            n        = len(samples)
            win_np   = np.array(win, dtype=np.float32)
            n_frames = max(1, (n - fft_size) // hop + 1)

            max_frames = 12000
            stride     = max(1, n_frames // max_frames)
            frame_idxs = np.arange(0, n_frames, stride)
            actual_frames = len(frame_idxs)

            spec = np.zeros((actual_frames, fft_size // 2 + 1), dtype=np.float32)
            for i, fi in enumerate(frame_idxs):
                start = fi * hop
                end   = start + fft_size
                if end > n:
                    break
                frame   = samples[start:end] * win_np
                spec[i] = np.abs(np.fft.rfft(frame)) / fft_size

            spec = spec.tolist()
            actual_n_frames = len(spec)

        except ImportError:
            # ── Fallback: pure Python Cooley-Tukey ────────────
            import cmath, math, array as _arr
            raw_arr  = _arr.array('h', raw)
            n        = len(raw_arr)
            n_frames = max(1, (n - fft_size) // hop + 1)
            max_frames = 3000
            stride   = max(1, n_frames // max_frames)

            def _fft(x):
                N = len(x)
                if N <= 1: return x
                if N & (N-1):
                    return [sum(x[k]*cmath.exp(-2j*math.pi*k*n/N) for k in range(N)) for n in range(N)]
                even = _fft(x[0::2]); odd = _fft(x[1::2])
                T = [cmath.exp(-2j*math.pi*k/N)*odd[k] for k in range(N//2)]
                return [even[k]+T[k] for k in range(N//2)] + [even[k]-T[k] for k in range(N//2)]

            spec = []
            for fi in range(0, n_frames, stride):
                start = fi * hop
                frame = [raw_arr[start+k]/32768.0 * win[k] for k in range(fft_size) if start+k < n]
                if len(frame) < fft_size:
                    frame += [0.0] * (fft_size - len(frame))
                F = _fft(frame)
                spec.append([abs(F[k])/fft_size for k in range(fft_size//2+1)])
            actual_n_frames = len(spec)

        self.done.emit({
            "spec":      spec,
            "sr":        sr,
            "dur":       dur,
            "fft":       fft_size,
            "hop":       hop,
            "n_frames":  actual_n_frames,
            "bit_depth": meta["bit_depth"],
            "channels":  meta["channels"],
            "file_size": fsize,
        })



class _SpectrogramRenderWorker(QThread):
    """
    Renders the COMPLETE annotated spectrogram image in a background thread —
    raw pixels + title + freq axis + time axis + colorbar — all baked into
    one QImage. paintEvent just blits it. Nothing can be clipped.
    """
    done = pyqtSignal(object)  # emits QImage (full annotated image)

    # Fixed margins inside the image (pixels)
    ML = 68   # left  — freq labels + rotated title
    MR = 78   # right — colorbar + dB labels
    MT = 32   # top   — filename + sample rate
    MB = 46   # bottom — time labels + "Time (seconds)"

    def __init__(self, spec, sr, fft, dur, freq_min, freq_max, freq_scale,
                 cmap_name, db_range, gamma, filename, img_w, img_h, parent=None):
        super().__init__(parent)
        self._spec = spec; self._sr = sr; self._fft = fft; self._dur = dur
        self._freq_min = freq_min; self._freq_max = freq_max
        self._freq_scale = freq_scale; self._cmap_name = cmap_name
        self._db_range = db_range; self._gamma = gamma
        self._filename = filename
        self._img_w = img_w; self._img_h = img_h

    def run(self):
        import math as _m
        ML, MR, MT, MB = self.ML, self.MR, self.MT, self.MB
        IW, IH = self._img_w, self._img_h
        # Plot area inside margins
        px0 = ML; py0 = MT
        pw  = IW - ML - MR
        ph  = IH - MT - MB
        if pw < 1 or ph < 1:
            return

        spec = self._spec
        n_frames = len(spec)
        n_bins   = len(spec[0]) if spec else 0
        sr, fft  = self._sr, self._fft
        nyq      = sr / 2
        fmin     = max(0, self._freq_min)
        fmax     = min(nyq, self._freq_max)
        log_scale = (self._freq_scale == "Log")
        db_range = self._db_range
        gamma    = self._gamma
        stops    = _COLORMAPS.get(self._cmap_name, _COLORMAPS["Magma"])

        try:
            gmax = max(max(row) for row in spec if row)
        except (ValueError, TypeError):
            gmax = 1e-12
        if gmax < 1e-12: gmax = 1e-12

        def freq_to_bin(hz):
            return min(max(int(hz * fft / sr), 0), n_bins - 1)

        def py_to_freq(py):
            t = 1.0 - py / ph
            if log_scale:
                flo = max(fmin, 20); fhi = fmax
                if flo <= 0: flo = 1
                return flo * (fhi / flo) ** t
            return fmin + t * (fmax - fmin)

        def freq_to_y(hz):
            hz = max(fmin, min(fmax, hz))
            if log_scale:
                flo = max(fmin, 20); fhi = fmax
                if flo <= 0: flo = 1
                t = _m.log(max(hz, flo) / flo) / _m.log(max(fhi / flo, 1.0001))
            else:
                t = (hz - fmin) / max(fmax - fmin, 1)
            return py0 + int((1.0 - t) * ph)

        # ── Step 1: render spectrogram pixels (numpy fast path) ─
        try:
            import numpy as np

            # Build lookup: for each pixel row py → bin index
            py_arr   = np.arange(ph, dtype=np.float32)
            t_arr    = 1.0 - py_arr / ph
            if log_scale:
                flo = max(fmin, 20.0); fhi = float(fmax)
                if flo <= 0: flo = 1.0
                freq_arr = flo * (fhi / flo) ** t_arr
            else:
                freq_arr = fmin + t_arr * (fmax - fmin)
            bin_arr = np.clip((freq_arr * fft / sr).astype(np.int32), 0, n_bins - 1)

            # Build frame index lookup: for each pixel col px → frame index
            px_arr  = np.arange(pw, dtype=np.float32)
            fi_arr  = np.clip((px_arr * n_frames / pw).astype(np.int32), 0, n_frames - 1)

            # spec as numpy array: shape (n_frames, n_bins)
            spec_np = np.array(spec, dtype=np.float32)  # (n_frames, n_bins)

            # Sample: result shape (ph, pw) — gather [fi, bin]
            sampled = spec_np[fi_arr, :]           # (pw, n_bins)
            sampled = sampled[:, bin_arr]           # (pw, ph)
            sampled = sampled.T / gmax              # (ph, pw)

            # dB + gamma
            db_arr = np.log10(np.maximum(sampled, 1e-10))
            db_arr = np.clip(1.0 + db_arr / (db_range / 20.0), 0.0, 1.0)
            db_arr = db_arr ** gamma  # (ph, pw)

            # Colormap: build RGB arrays
            n_stops = len(stops)
            stops_r = np.array([s[0] for s in stops], dtype=np.float32)
            stops_g = np.array([s[1] for s in stops], dtype=np.float32)
            stops_b = np.array([s[2] for s in stops], dtype=np.float32)

            seg   = db_arr * (n_stops - 1)
            lo    = np.floor(seg).astype(np.int32)
            hi    = np.minimum(lo + 1, n_stops - 1)
            t_seg = seg - lo

            r_arr = (stops_r[lo] + t_seg * (stops_r[hi] - stops_r[lo])).astype(np.uint8)
            g_arr = (stops_g[lo] + t_seg * (stops_g[hi] - stops_g[lo])).astype(np.uint8)
            b_arr = (stops_b[lo] + t_seg * (stops_b[hi] - stops_b[lo])).astype(np.uint8)

            # Pack into ARGB32 array: shape (ph, pw)
            alpha = np.full((ph, pw), 0xFF, dtype=np.uint32)
            argb  = (alpha << 24 | r_arr.astype(np.uint32) << 16 |
                     g_arr.astype(np.uint32) << 8 | b_arr.astype(np.uint32))

            # Full image buffer
            buf = np.full((IH, IW), 0xFF0A0A0D, dtype=np.uint32)
            buf[py0:py0+ph, px0:px0+pw] = argb

            img = QImage(buf.tobytes(), IW, IH, IW * 4, QImage.Format.Format_ARGB32)
            img = img.copy()  # detach from numpy buffer

        except ImportError:
            # ── Fallback: pure Python (slow but correct) ──────
            img = QImage(IW, IH, QImage.Format.Format_RGB32)
            img.fill(QColor(10, 10, 13).rgb())
            for px in range(pw):
                fi  = min(int(px * n_frames / pw), n_frames - 1)
                row = spec[fi]
                for py in range(ph):
                    freq_hz = py_to_freq(py)
                    bi  = min(max(freq_to_bin(freq_hz), 0), n_bins - 1)
                    val = row[bi] / gmax
                    db_val = max(0.0, min(1.0, 1.0 + _m.log10(max(val, 1e-10)) / (db_range / 20.0)))
                    db_val = db_val ** gamma
                    r, g, b = _interp_colormap(stops, db_val)
                    img.setPixel(px0 + px, py0 + py, 0xFF000000 | (r << 16) | (g << 8) | b)

        # ── Step 2: paint axes, labels, colorbar on top with QPainter ──
        p = QPainter(img)
        p.setRenderHint(QPainter.RenderHint.Antialiasing)

        BG    = QColor(10, 10, 13)
        TXT   = QColor(195, 195, 200)
        TXT2  = QColor(140, 140, 148)
        TICK  = QColor(100, 100, 108)
        GRID  = QColor(255, 255, 255, 18)
        BORDER= QColor(55, 55, 62)

        # Fill margin areas with background
        p.fillRect(0, 0, IW, MT, BG)               # top
        p.fillRect(0, py0 + ph, IW, MB, BG)         # bottom
        p.fillRect(0, 0, ML, IH, BG)                # left
        p.fillRect(px0 + pw, 0, MR, IH, BG)         # right

        # ── Title bar ─────────────────────────────────────────
        f_title = QFont(); f_title.setPointSize(9)
        p.setFont(f_title)
        fm = p.fontMetrics()
        p.setPen(TXT)
        p.drawText(px0, MT - 6, self._filename)
        sr_txt = f"Sample Rate: {sr:,} Hz"
        p.drawText(px0 + pw - fm.horizontalAdvance(sr_txt), MT - 6, sr_txt)

        # ── Plot border ───────────────────────────────────────
        p.setPen(BORDER)
        p.drawRect(px0, py0, pw, ph)

        # ── Frequency axis ────────────────────────────────────
        f_ax = QFont(); f_ax.setPointSize(8)
        p.setFont(f_ax)
        fm = p.fontMetrics()

        if log_scale:
            ticks = [f for f in [50, 100, 200, 500, 1000, 2000, 4000,
                                  8000, 12000, 16000, 20000, 22000]
                     if fmin <= f <= fmax]
        else:
            step = 2000
            ticks = list(range(0, int(fmax) + step, step))
            ticks = [f for f in ticks if fmin <= f <= fmax]

        last_ly = None
        for hz in ticks:
            y = freq_to_y(hz)
            if not (py0 <= y <= py0 + ph):
                continue
            # Gridline
            p.setPen(GRID)
            p.drawLine(px0 + 1, y, px0 + pw - 1, y)
            # Tick
            p.setPen(TICK)
            p.drawLine(px0 - 4, y, px0, y)
            # Label
            lbl = f"{hz//1000}k" if hz >= 1000 else str(hz)
            lw  = fm.horizontalAdvance(lbl)
            ly  = y + fm.ascent() // 2
            if last_ly is None or abs(ly - last_ly) >= fm.height() + 1:
                p.setPen(TXT)
                p.drawText(px0 - lw - 7, ly, lbl)
                last_ly = ly

        # "Frequency (Hz)" rotated
        p.save()
        p.setPen(TXT2)
        p.translate(12, py0 + ph // 2)
        p.rotate(-90)
        fm2 = p.fontMetrics()
        lf = "Frequency (Hz)"
        p.drawText(-fm2.horizontalAdvance(lf) // 2, fm2.ascent() // 2, lf)
        p.restore()

        # ── Time axis ─────────────────────────────────────────
        dur = self._dur
        if dur <= 30:    step_t = 5.0
        elif dur <= 60:  step_t = 10.0
        elif dur <= 120: step_t = 20.0
        elif dur <= 300: step_t = 30.0
        elif dur <= 600: step_t = 60.0
        else:            step_t = 120.0

        t_sec = 0.0
        p.setFont(f_ax)
        fm = p.fontMetrics()
        while t_sec <= dur + 0.01:
            x = px0 + int(t_sec / max(dur, 1e-6) * pw)
            if px0 <= x <= px0 + pw:
                p.setPen(GRID)
                p.drawLine(x, py0, x, py0 + ph)
                p.setPen(TICK)
                p.drawLine(x, py0 + ph, x, py0 + ph + 4)
                mm, ss = divmod(int(t_sec), 60)
                lbl = f"{mm}:{ss:02d}" if mm else f"{int(t_sec)}s"
                lw  = fm.horizontalAdvance(lbl)
                p.setPen(TXT)
                p.drawText(x - lw // 2, py0 + ph + fm.ascent() + 7, lbl)
            t_sec += step_t

        # "Time (seconds)" centered
        p.setPen(TXT2)
        lt = "Time (seconds)"
        p.drawText(px0 + (pw - fm.horizontalAdvance(lt)) // 2,
                   IH - 5, lt)

        # ── Colorbar ─────────────────────────────────────────
        cb_x = px0 + pw + 10
        cb_w = 16
        for cy in range(ph):
            v = 1.0 - cy / max(ph - 1, 1)
            r, g, b = _interp_colormap(stops, v ** gamma)
            p.fillRect(cb_x, py0 + cy, cb_w, 1, QColor(r, g, b))
        p.setPen(BORDER)
        p.drawRect(cb_x, py0, cb_w, ph)

        # dB ticks + labels — draw first so High/Low can overlay cleanly
        lh = fm.height()
        for frac, lbl in [(0.0, "0 dB"), (0.25, f"-{int(db_range*.25)}"),
                          (0.5, f"-{int(db_range*.5)}"),
                          (0.75, f"-{int(db_range*.75)}"),
                          (1.0, f"-{int(db_range)}")]:
            cy = py0 + int(frac * ph)
            p.setPen(TICK)
            p.drawLine(cb_x, cy, cb_x + cb_w, cy)
            # Skip label if it would overlap with "High" (top) or "Low" (bottom)
            skip_top    = frac == 0.0   # "0 dB" — "High" goes here instead
            skip_bottom = frac == 1.0   # "-96 dB" — "Low" goes here instead
            if not skip_top and not skip_bottom:
                p.setPen(TXT2)
                p.drawText(cb_x + cb_w + 4, cy + fm.ascent() // 2, lbl)

        # High at top (replaces "0 dB"), Low at bottom (replaces "-96 dB")
        p.setPen(TXT)
        p.drawText(cb_x + cb_w + 4, py0 + fm.ascent(), "High")
        p.drawText(cb_x + cb_w + 4, py0 + ph, "Low")

        p.end()
        self.done.emit(img)


class _SpectrogramWidget(QWidget):
    """
    Displays the annotated spectrogram image produced by _SpectrogramRenderWorker.
    paintEvent just scales and blits the pre-rendered image — nothing can be clipped.
    """
    colormap_name_changed = pyqtSignal(str)

    # Expose margin constants so export code can reference them
    ML = _SpectrogramRenderWorker.ML
    MR = _SpectrogramRenderWorker.MR
    MT = _SpectrogramRenderWorker.MT
    MB = _SpectrogramRenderWorker.MB

    def __init__(self, parent=None):
        super().__init__(parent)
        self._spec        = None
        self._sr          = 44100
        self._dur         = 0.0
        self._fft         = 4096
        self._n_frames    = 0
        self._freq_scale  = "Log"
        self._freq_min    = 0
        self._freq_max    = 22050
        self._cmap_name   = "Magma"
        self._db_range    = 96.0
        self._gamma       = 0.65
        self._full_img    = None   # QImage — full annotated render
        self._rendering   = False
        self._current_path = None
        self._hover       = None
        self._render_workers = []
        self.setMinimumHeight(280)
        self.setMouseTracking(True)

    # Backward-compat properties for export code
    @property
    def AXIS_W(self): return self.ML
    @property
    def AXIS_H(self): return self.MB
    @property
    def TOP_H(self):  return self.MT
    @property
    def CBAR_W(self): return self.MR

    def _freq_ticks(self):
        fmin = max(0, self._freq_min)
        fmax = min(self._sr / 2, self._freq_max)
        if self._freq_scale == "Log":
            return [f for f in [50,100,200,500,1000,2000,4000,
                                 8000,12000,16000,20000,22000]
                    if fmin <= f <= fmax]
        step = 2000
        return [f for f in range(0, int(fmax)+step, step) if fmin <= f <= fmax]

    def set_data(self, info: dict):
        self._spec     = info["spec"]
        self._sr       = info["sr"]
        self._dur      = info["dur"]
        self._fft      = info["fft"]
        self._n_frames = info["n_frames"]
        self._freq_max = self._sr // 2
        self._kick_render()

    def set_freq_scale(self, v):
        self._freq_scale = v
        if self._spec: self._kick_render()

    def set_freq_range(self, fmin, fmax):
        self._freq_min = fmin; self._freq_max = fmax
        if self._spec: self._kick_render()

    def set_colormap(self, name):
        self._cmap_name = name
        if self._spec: self._kick_render()

    def _kick_render(self):
        """Defer render by one event loop tick so the widget has its final size."""
        self._rendering = True
        self.update()
        QTimer.singleShot(0, self._do_render)

    def _do_render(self):
        if not self._spec:
            return
        # Use actual widget size — guaranteed correct after layout is done
        W = max(self.width(), 600)
        H = max(self.height(), 300)
        filename = Path(self._current_path).name if self._current_path else ""
        # Cancel any pending renders that haven't started yet by tracking generation
        self._render_gen = getattr(self, '_render_gen', 0) + 1
        gen = self._render_gen
        worker = _SpectrogramRenderWorker(
            self._spec, self._sr, self._fft, self._dur,
            self._freq_min, self._freq_max, self._freq_scale,
            self._cmap_name, self._db_range, self._gamma,
            filename, W, H
        )
        def _on_result(img, g=gen):
            # Discard stale renders (from old size or old data)
            if g == self._render_gen:
                self._on_done(img)
        worker.done.connect(_on_result)
        self._render_workers.append(worker)
        worker.start()

    def _on_done(self, img):
        self._full_img = img
        self._rendering = False
        # Clean up finished workers safely - don't deleteLater inside the signal handler
        still_running = []
        for w in self._render_workers:
            try:
                if w.isRunning():
                    still_running.append(w)
                else:
                    w.wait()
            except RuntimeError:
                pass
        self._render_workers = still_running
        self.update()

    def resizeEvent(self, e):
        super().resizeEvent(e)
        if self._spec:
            # Debounce: wait 120ms after resize stops before re-rendering
            if not hasattr(self, '_resize_timer'):
                self._resize_timer = QTimer()
                self._resize_timer.setSingleShot(True)
                self._resize_timer.timeout.connect(self._kick_render)
            self._resize_timer.start(120)

    def mouseMoveEvent(self, e):
        self._hover = (e.position().x(), e.position().y())
        self.update()

    def leaveEvent(self, e):
        self._hover = None
        self.update()

    def paintEvent(self, e):
        p = QPainter(self)
        W = self.width(); H = self.height()

        p.fillRect(self.rect(), QColor(10, 10, 13))

        if not self._full_img and not self._rendering:
            p.setPen(QColor(130, 130, 138))
            f = QFont(); f.setPointSize(12); p.setFont(f)
            p.drawText(self.rect(), Qt.AlignmentFlag.AlignCenter,
                       "Drop an audio file here or click 'Open file'\n\n"
                       "Supports FLAC, MP3, AAC, WAV, OGG and more")
            return

        if self._rendering and not self._full_img:
            p.setPen(QColor(130, 130, 138))
            f = QFont(); f.setPointSize(12); p.setFont(f)
            p.drawText(self.rect(), Qt.AlignmentFlag.AlignCenter, "Rendering…")
            return

        if self._full_img:
            # Scale the pre-rendered image to fit widget exactly
            pm = QPixmap.fromImage(self._full_img).scaled(
                W, H,
                Qt.AspectRatioMode.IgnoreAspectRatio,
                Qt.TransformationMode.SmoothTransformation
            )
            p.drawPixmap(0, 0, pm)

            # Hover crosshair — compute plot area scaled coords
            if self._hover and self._spec:
                ML = self.ML; MR = self.MR; MT = self.MT; MB = self.MB
                IW = self._full_img.width(); IH = self._full_img.height()
                # Scale factors
                sx = W / IW; sy = H / IH
                px0 = int(ML * sx); py0 = int(MT * sy)
                pw  = int((IW - ML - MR) * sx)
                ph  = int((IH - MT - MB) * sy)
                hx, hy = self._hover
                if px0 <= hx <= px0 + pw and py0 <= hy <= py0 + ph:
                    import math as _m
                    p.setPen(QColor(255, 255, 255, 60))
                    p.drawLine(int(hx), py0, int(hx), py0 + ph)
                    p.drawLine(px0, int(hy), px0 + pw, int(hy))
                    fmin = max(0, self._freq_min)
                    fmax = min(self._sr / 2, self._freq_max)
                    t_cur = (hx - px0) / max(pw, 1) * self._dur
                    t_norm = 1.0 - (hy - py0) / max(ph, 1)
                    if self._freq_scale == "Log":
                        flo = max(fmin, 20); fhi = fmax
                        freq_cur = flo * (fhi / flo) ** t_norm if flo > 0 else fmin
                    else:
                        freq_cur = fmin + t_norm * (fmax - fmin)
                    mm, ss = divmod(int(t_cur), 60)
                    tip = f"  {mm}:{ss:02d}  |  {int(freq_cur):,} Hz  "
                    f_tip = QFont(); f_tip.setPointSize(8); p.setFont(f_tip)
                    fm = p.fontMetrics()
                    tw = fm.horizontalAdvance(tip)
                    th = fm.height()
                    tx = min(int(hx) + 10, px0 + pw - tw - 4)
                    ty = max(int(hy) - th - 4, py0 + 4)
                    p.fillRect(tx-3, ty-fm.ascent()-2, tw+6, th+4, QColor(0,0,0,210))
                    p.setPen(QColor(230, 230, 230))
                    p.drawText(tx, ty, tip)



class SpectrogramPage(QWidget):
    """
    Audio quality inspection via spectrogram.
    Drag & drop or open a file (FLAC/MP3/AAC/WAV/AIFF/OGG etc).
    Shows the full frequency spectrogram so you can visually verify
    whether a 'lossless' file is genuine or an up-transcode.
    Requires: ffmpeg in PATH.
    """

    def __init__(self, parent=None):
        super().__init__(parent)
        self._worker: Optional[_SpectrogramWorker] = None
        self._current_path: Optional[str] = None
        self._current_info: Optional[dict] = None
        self._build()
        self.setAcceptDrops(True)

    def _build(self):
        root = QVBoxLayout(self)
        root.setContentsMargins(0, 0, 0, 0)
        root.setSpacing(0)

        # ── Combined header + controls bar ────────────────────
        header = QWidget()
        header.setFixedHeight(56)
        header.setStyleSheet("background:rgba(5,7,11,0.65); border-bottom:1px solid rgba(255,255,255,0.07);")
        h_lay = QHBoxLayout(header)
        h_lay.setContentsMargins(16, 0, 12, 0)
        h_lay.setSpacing(10)

        # Page title
        title_lbl = QLabel("Spectrogram")
        tf = QFont(); tf.setPointSize(12); tf.setWeight(QFont.Weight.Bold)
        title_lbl.setFont(tf)
        title_lbl.setStyleSheet("color:#fff;background:transparent;")
        h_lay.addWidget(title_lbl)

        # File label (flex)
        self._file_lbl = QLabel("Drop a file or click Open")
        self._file_lbl.setStyleSheet(
            f"color:rgba(255,255,255,0.35);font-size:10px;background:transparent;"
        )
        h_lay.addWidget(self._file_lbl, stretch=1)

        def _mk_combo(items, current=None):
            cb = QComboBox()
            cb.addItems(items)
            if current:
                cb.setCurrentText(current)
            cb.setFixedHeight(26)
            cb.setStyleSheet(
                f"QComboBox{{background:rgba(255,255,255,0.08);color:rgba(255,255,255,0.87);"
                f"border:1px solid rgba(255,255,255,0.09);border-radius:4px;"
                f"padding:1px 6px;font-size:11px;}}"
                f"QComboBox::drop-down{{border:none;width:16px;}}"
                f"QComboBox QAbstractItemView{{background:#1a1d24;color:rgba(255,255,255,0.87);"
                f"border:1px solid rgba(255,255,255,0.18);"
                f"selection-background-color:{tok('accent')};selection-color:#000;}}"
            )
            return cb

        def _labeled(text, widget):
            w = QWidget(); w.setStyleSheet("background:transparent;")
            vl = QVBoxLayout(w); vl.setContentsMargins(0,2,0,2); vl.setSpacing(1)
            lbl = QLabel(text)
            lbl.setStyleSheet(
                f"color:{tok('accent')};font-size:8px;font-weight:700;"
                f"letter-spacing:0.5px;background:transparent;"
            )
            vl.addWidget(lbl); vl.addWidget(widget)
            return w

        self._fft_combo = _mk_combo(["512","1024","2048","4096","8192"], "4096")
        self._fft_combo.setFixedWidth(72)
        h_lay.addWidget(_labeled("FFT", self._fft_combo))

        self._win_combo = _mk_combo(["Hann","Hamming","Blackman","Rectangular"])
        self._win_combo.setFixedWidth(88)
        h_lay.addWidget(_labeled("WINDOW", self._win_combo))

        self._scale_combo = _mk_combo(["Log","Linear"])
        self._scale_combo.setFixedWidth(68)
        h_lay.addWidget(_labeled("SCALE", self._scale_combo))
        self._scale_combo.currentTextChanged.connect(
            lambda v: self._spec_widget.set_freq_scale(v)
        )

        self._range_combo = _mk_combo([])
        self._range_combo.setFixedWidth(150)

        def _rebuild_ranges():
            nyq = getattr(self, "_nyq", 22050)
            self._range_combo.blockSignals(True)
            self._range_combo.clear()
            for label, fmin, fmax in [
                (f"Full  0–{nyq//1000} kHz", 0, nyq),
                ("0–20 kHz", 0, 20000),
                (f"20 Hz–{nyq//1000} kHz", 20, nyq),
                ("4–22 kHz  highs", 4000, nyq),
                ("0–8 kHz  lows", 0, 8000),
            ]:
                self._range_combo.addItem(label, (fmin, min(fmax, nyq)))
            self._range_combo.blockSignals(False)
            self._range_combo.setCurrentIndex(0)

        self._nyq = 22050
        _rebuild_ranges()
        self._rebuild_ranges = _rebuild_ranges

        def _on_range_changed():
            d = self._range_combo.currentData()
            if d:
                self._spec_widget.set_freq_range(d[0], d[1])
        self._range_combo.currentIndexChanged.connect(_on_range_changed)
        h_lay.addWidget(_labeled("RANGE", self._range_combo))

        analyze_btn = QPushButton("↺ Analyze")
        analyze_btn.setObjectName("toggle")
        analyze_btn.setFixedHeight(34)
        analyze_btn.setMinimumWidth(120)
        analyze_btn.clicked.connect(self._reanalyze)
        h_lay.addWidget(analyze_btn)

        open_btn = QPushButton("Open file…")
        open_btn.setObjectName("toggle")
        open_btn.setFixedHeight(34)
        open_btn.setMinimumWidth(120)
        open_btn.setMinimumWidth(120)
        open_btn.clicked.connect(self._open_file)
        h_lay.addWidget(open_btn)

        root.addWidget(header)

        # ── File meta bar (hidden until file loaded) ──────────
        self._sel_card = QWidget()
        self._sel_card.hide()
        self._sel_card.setFixedHeight(28)
        sc_lay = QHBoxLayout(self._sel_card)
        sc_lay.setContentsMargins(16, 0, 16, 0)
        sc_lay.setSpacing(20)
        self._sel_info = QLabel()
        self._sel_info.setStyleSheet(
            f"color:rgba(255,255,255,0.55);font-size:11px;background:transparent;"
        )
        sc_lay.addWidget(self._sel_info)
        sc_lay.addStretch()
        self._quality_badge = QLabel()
        self._quality_badge.setFixedHeight(18)
        self._quality_badge.setStyleSheet(
            f"background:rgba(255,255,255,0.08);color:rgba(255,255,255,0.87);border-radius:3px;"
            f"padding:1px 8px;font-size:10px;font-weight:700;border:1px solid rgba(255,255,255,0.09);"
        )
        sc_lay.addWidget(self._quality_badge)
        self._sel_card.setObjectName("inset")
        root.addWidget(self._sel_card)

        # ── Progress ──────────────────────────────────────────
        self._progress = QProgressBar()
        self._progress.setRange(0, 0)
        self._progress.setFixedHeight(3)
        self._progress.setTextVisible(False)
        self._progress.setStyleSheet(
            f"QProgressBar{{background:rgba(255,255,255,0.05);border:none;}}"
            f"QProgressBar::chunk{{background:{tok('accent')};}}"
        )
        self._progress.hide()
        root.addWidget(self._progress)

        # ── Spectrogram widget ────────────────────────────────
        self._spec_widget = _SpectrogramWidget()
        self._spec_widget.setStyleSheet("background:rgba(0,0,0,0.30);")
        root.addWidget(self._spec_widget, stretch=1)

        # ── Resolution & Export row ───────────────────────────
        res_row = QWidget()
        res_row.setObjectName("card_top")
        rr_lay = QVBoxLayout(res_row)
        rr_lay.setContentsMargins(16, 10, 16, 10)
        rr_lay.setSpacing(8)

        res_lbl = QLabel("Resolution")
        res_lbl.setStyleSheet("color:rgba(255,255,255,0.55);font-size:10px;font-weight:700;background:transparent;")
        rr_lay.addWidget(res_lbl)

        res_btns_row = QHBoxLayout()
        res_btns_row.setSpacing(8)
        self._res_group = []

        res_opts = [
            ("Standard", "1100×600"),
            ("High",     "1850×800"),
            ("Maximum",  "2200×1200"),
        ]
        self._res_choice = 0

        def _make_res_btn(idx, name, sz):
            btn = QPushButton()
            btn.setCheckable(True)
            btn.setFixedHeight(52)
            btn.setMinimumWidth(120)
            btn.setSizePolicy(QSizePolicy.Policy.Preferred, QSizePolicy.Policy.Fixed)
            btn.setStyleSheet(
                f"QPushButton{{background:rgba(255,255,255,0.05);border:1px solid rgba(255,255,255,0.09);"
                f"border-radius:6px;padding:0;text-align:left;}}"
                f"QPushButton:hover{{background:rgba(255,255,255,0.08);border-color:{tok('accent')}44;}}"
                f"QPushButton:checked{{background:{tok('accentlo')};border:2px solid {tok('accent')};"
                f"border-radius:6px;}}"
            )
            inner = QVBoxLayout(btn)
            inner.setContentsMargins(12, 6, 10, 6)
            inner.setSpacing(1)
            n_lbl = QLabel(name)
            nf = QFont(); nf.setWeight(QFont.Weight.Bold); nf.setPointSize(10)
            n_lbl.setFont(nf)
            n_lbl.setStyleSheet("background:transparent;color:rgba(255,255,255,0.87);")
            inner.addWidget(n_lbl)
            s_lbl = QLabel(sz)
            s_lbl.setStyleSheet("font-size:9px;background:transparent;color:rgba(255,255,255,0.35);")
            inner.addWidget(s_lbl)
            btn.setProperty("res_idx", idx)
            # Re-color labels when checked state changes
            def _sync_colors(checked, nl=n_lbl, sl=s_lbl):
                ac = "#ffffff" if checked else tok('txt0')
                sc = "#ffffffaa" if checked else tok('txt2')
                nl.setStyleSheet(f"background:transparent;color:{ac};")
                sl.setStyleSheet(f"font-size:9px;background:transparent;color:{sc};")
            btn.toggled.connect(_sync_colors)
            return btn

        for i, (name, sz) in enumerate(res_opts):
            b = _make_res_btn(i, name, sz)
            b.setChecked(i == 0)
            def _on_res(checked, idx=i):
                if checked:
                    self._res_choice = idx
                    for j, rb in enumerate(self._res_group):
                        rb.setChecked(j == idx)
            b.toggled.connect(_on_res)
            self._res_group.append(b)
            res_btns_row.addWidget(b)
            # Fire initial color sync
            if i == 0:
                b.toggled.emit(True)

        res_btns_row.addStretch()
        rr_lay.addLayout(res_btns_row)

        # Export format + color scheme row
        exp_row = QHBoxLayout()
        exp_row.setSpacing(16)

        # Shared combo style for the export row
        combo_ss = (
            f"QComboBox{{background:rgba(255,255,255,0.08);color:rgba(255,255,255,0.87);border:1px solid rgba(255,255,255,0.09);"
            f"border-radius:4px;padding:2px 8px;font-size:11px;}}"
            f"QComboBox::drop-down{{border:none;width:16px;}}"
            f"QComboBox QAbstractItemView{{background:#1a1d24;color:rgba(255,255,255,0.87);"
            f"border:1px solid rgba(255,255,255,0.18);selection-background-color:{tok('accent')};"
            f"selection-color:#000;}}"
        )

        fmt_lay = QVBoxLayout()
        fmt_lay.setSpacing(2)
        fmt_lbl = QLabel("Export Format")
        fmt_lbl.setStyleSheet("color:rgba(255,255,255,0.55);font-size:10px;font-weight:700;background:transparent;")
        fmt_lay.addWidget(fmt_lbl)
        self._fmt_combo = QComboBox()
        self._fmt_combo.setStyleSheet(combo_ss)
        self._fmt_combo.addItems(["PNG", "JPEG", "BMP"])
        fmt_lay.addWidget(self._fmt_combo)
        exp_row.addLayout(fmt_lay)

        cscheme_lay = QVBoxLayout()
        cscheme_lay.setSpacing(2)
        cs_lbl = QLabel("Color Scheme")
        cs_lbl.setStyleSheet("color:rgba(255,255,255,0.55);font-size:10px;font-weight:700;background:transparent;")
        cscheme_lay.addWidget(cs_lbl)

        cmap_btns = QHBoxLayout()
        cmap_btns.setSpacing(4)
        cmap_names = list(_COLORMAPS.keys())
        cmap_preview_colors = {
            "Magma":   ("#000000", "#7B0EA0", "#F8902C"),
            "Inferno": ("#000004", "#721F81", "#F8DF25"),
            "Viridis": ("#440154", "#31688E", "#FDE725"),
            "Classic": ("#000000", "#00B464", "#FF0000"),
            "CoolWarm":("#3B4CC0", "#DDDDDD", "#B40426"),
            "Mono":    ("#000000", "#888888", "#FFFFFF"),
            "Green":   ("#000000", "#00B400", "#FFFFFF"),
        }
        self._cmap_btns = {}

        for cname in cmap_names:
            btn = QPushButton()
            btn.setFixedSize(22, 22)
            btn.setToolTip(cname)
            colors = cmap_preview_colors.get(cname, ("#000", "#888", "#FFF"))
            btn.setStyleSheet(
                f"QPushButton{{border:2px solid transparent;border-radius:3px;"
                f"background:qlineargradient(x1:0,y1:0,x2:1,y2:0,"
                f"stop:0 {colors[0]},stop:0.5 {colors[1]},stop:1 {colors[2]});}}"
                f"QPushButton:checked{{border-color:{tok('accent')};}}"
            )
            btn.setCheckable(True)
            btn.setChecked(cname == "Magma")
            def _on_cmap(checked, n=cname):
                if checked:
                    self._spec_widget.set_colormap(n)
                    for k, b in self._cmap_btns.items():
                        b.setChecked(k == n)
            btn.toggled.connect(_on_cmap)
            self._cmap_btns[cname] = btn
            cmap_btns.addWidget(btn)

        cscheme_lay.addLayout(cmap_btns)
        exp_row.addLayout(cscheme_lay)
        exp_row.addStretch()

        export_btn = QPushButton("⬆ Export Spectrogram")
        export_btn.setObjectName("primary")
        export_btn.setFixedHeight(32)
        export_btn.clicked.connect(self._export)
        exp_row.addWidget(export_btn, alignment=Qt.AlignmentFlag.AlignBottom)

        rr_lay.addLayout(exp_row)
        root.addWidget(res_row)

        # ── Audio file info panel ─────────────────────────────
        self._info_panel = QWidget()
        self._info_panel.hide()
        self._info_panel.setStyleSheet("background:rgba(5,7,11,0.60); border-top:1px solid rgba(255,255,255,0.07);")
        ip_lay = QVBoxLayout(self._info_panel)
        ip_lay.setContentsMargins(16, 10, 16, 10)
        ip_lay.setSpacing(4)

        ip_head = QHBoxLayout()
        ip_icon = QLabel("▾ Audio File Information:")
        ip_icon.setStyleSheet("color:rgba(255,255,255,0.35);font-size:10px;font-weight:700;background:transparent;")
        ip_head.addWidget(ip_icon)
        ip_head.addStretch()
        ip_lay.addLayout(ip_head)

        self._info_lines = {}
        for key in ["Type", "Sample Rate", "Bit Depth", "Channels", "Duration",
                    "Nyquist", "Size", "Samples", "Analysis Frames", "FFT Size", "Freq Resolution"]:
            lbl = QLabel()
            lbl.setStyleSheet("color:rgba(255,255,255,0.55);font-size:10px;background:transparent;")
            ip_lay.addWidget(lbl)
            self._info_lines[key] = lbl

        root.addWidget(self._info_panel)

    # ── File open / drag-drop ─────────────────────────────────

    def _open_file(self):
        path, _ = QFileDialog.getOpenFileName(
            self, "Open audio file", "",
            "Audio files (*.flac *.mp3 *.aac *.m4a *.ogg *.wav *.aiff *.wv *.ape *.opus);;All files (*)"
        )
        if path:
            self._load(path)

    def dragEnterEvent(self, e):
        if e.mimeData().hasUrls():
            e.acceptProposedAction()

    def dropEvent(self, e):
        for url in e.mimeData().urls():
            p = url.toLocalFile()
            if p:
                self._load(p); break

    def _load(self, path: str):
        self._current_path = path
        name = Path(path).name
        ext  = Path(path).suffix.upper().lstrip(".")
        sz   = Path(path).stat().st_size if Path(path).exists() else 0
        sz_str = f"{sz/1024/1024:.2f} MB" if sz > 0 else "?"

        self._file_lbl.setText(f"▶ {name}")
        self._sel_info.setText(
            f"Selected File: {name}    Size: {sz_str}    Type: audio/{ext.lower()}"
        )
        self._sel_card.show()
        self._quality_badge.setText("")
        self._progress.show()

        if self._worker and self._worker.isRunning():
            self._worker.quit()
            self._worker.wait(300)

        fft_size = int(self._fft_combo.currentText())
        win_fn   = self._win_combo.currentText()

        self._worker = _SpectrogramWorker(path, fft_size=fft_size, window_fn=win_fn)
        self._worker.done.connect(self._on_done)
        self._worker.error.connect(self._on_error)
        self._worker.start()

    def _reanalyze(self):
        if self._current_path:
            self._load(self._current_path)
        else:
            self._open_file()

    def _on_done(self, info: dict):
        self._progress.hide()
        self._current_info = info
        self._spec_widget._current_path = self._current_path
        self._spec_widget.set_data(info)

        dur  = info["dur"]
        sr   = info["sr"]
        fft  = info["fft"]
        n_fr = info["n_frames"]
        fsize = info.get("file_size", 0)
        path = info.get("path", self._current_path or "")
        ext  = Path(path).suffix.upper().lstrip(".") if path else "?"
        bd   = info.get("bit_depth", "N/A")
        ch   = info.get("channels", 2)
        nyq  = sr // 2
        hop  = info.get("hop", fft // 4)
        freq_res = sr / fft

        self._nyq = nyq

        # Rebuild range combo with correct Nyquist
        self._range_combo.blockSignals(True)
        self._range_combo.clear()
        for label, fmin, fmax in [
            (f"Full range  (0–{nyq//1000} kHz)", 0, nyq),
            ("0–20 kHz", 0, 20000),
            (f"20 Hz–{nyq//1000} kHz", 20, nyq),
            ("4–22 kHz (highs)", 4000, nyq),
            ("0–8 kHz (lows)", 0, 8000),
        ]:
            self._range_combo.addItem(label, (fmin, min(fmax, nyq)))
        self._range_combo.blockSignals(False)
        self._range_combo.setCurrentIndex(0)
        self._spec_widget.set_freq_range(0, nyq)

        m, s = divmod(int(dur), 60)
        sz_str = f"{fsize/1024/1024:.2f} MB" if fsize > 0 else "?"

        self._info_lines["Type"].setText(f"Type: {ext}")
        self._info_lines["Sample Rate"].setText(f"Sample Rate: {sr} Hz")
        self._info_lines["Bit Depth"].setText(f"Bit Depth: {bd} bit")
        self._info_lines["Channels"].setText(f"Channels: {ch}")
        self._info_lines["Duration"].setText(f"Duration: {m}:{s:02d}.{int((dur % 1)*100):02d}s")
        self._info_lines["Nyquist"].setText(f"Nyquist: {nyq/1000:.1f} kHz")
        self._info_lines["Size"].setText(f"Size: {sz_str}")
        self._info_lines["Samples"].setText(f"Samples: {int(dur * sr):,}")
        self._info_lines["Analysis Frames"].setText(f"Analysis Frames: {n_fr:,}")
        self._info_lines["FFT Size"].setText(f"FFT Size: {fft}")
        self._info_lines["Freq Resolution"].setText(f"Freq Resolution: {freq_res:.2f} Hz/bin")
        self._info_panel.show()

        # Quality assessment
        spec    = info["spec"]
        n_bins  = len(spec[0]) if spec else 0
        try:
            gmax = max(max(row) for row in spec if row)
        except (ValueError, TypeError):
            gmax = 1.0
        if gmax < 1e-12: gmax = 1e-12
        ceil_bin = 0
        step = max(1, len(spec) // 200)
        for bi in range(n_bins - 1, -1, -1):
            avg = sum(spec[fi][bi] for fi in range(0, len(spec), step)) / max(len(spec) // step, 1)
            if avg > gmax * 0.005:
                ceil_bin = bi
                break
        ceil_hz = int(ceil_bin * sr / fft)

        if ceil_hz >= 19000:
            quality   = "Full-range (≥19 kHz)"
            badge_ss  = (f"background:{tok('accentlo')};color:{tok('accent')};border-radius:4px;"
                         f"padding:2px 10px;font-size:10px;font-weight:700;")
        elif ceil_hz >= 16000:
            quality   = "Partial (≥16 kHz)"
            badge_ss  = (f"background:{tok('warning')}18;color:{tok('warning')};border-radius:4px;"
                         f"padding:2px 10px;font-size:10px;font-weight:700;")
        else:
            quality   = f"Limited ({ceil_hz//1000} kHz ceiling)"
            badge_ss  = (f"background:{tok('danger')}18;color:{tok('danger')};border-radius:4px;"
                         f"padding:2px 10px;font-size:10px;font-weight:700;")

        self._quality_badge.setText(quality)
        self._quality_badge.setStyleSheet(badge_ss)

    def _on_error(self, msg: str):
        self._progress.hide()
        self._quality_badge.setText("Error")
        self._quality_badge.setStyleSheet(
            f"background:{tok('danger')}18;color:{tok('danger')};border-radius:4px;"
            f"padding:2px 10px;font-size:10px;font-weight:700;"
        )
        QMessageBox.critical(self, "Spectrogram Error", msg)

    def _export(self):
        if not self._spec_widget._full_img:
            QMessageBox.information(self, "Export", "Analyze a file first.")
            return

        res_sizes = [(1100, 600), (1850, 800), (2200, 1200)]
        export_w, export_h = res_sizes[self._res_choice]

        fmt_map = {"PNG": ("PNG Files (*.png)", ".png", "PNG"),
                   "JPEG": ("JPEG Files (*.jpg)", ".jpg", "JPEG"),
                   "BMP": ("BMP Files (*.bmp)", ".bmp", "BMP")}
        fmt_name = self._fmt_combo.currentText()
        flt, ext, fmt = fmt_map.get(fmt_name, fmt_map["PNG"])

        path, _ = QFileDialog.getSaveFileName(self, "Export Spectrogram", f"spectrogram{ext}", flt)
        if not path:
            return

        sw = self._spec_widget

        # Use the render worker to produce a full annotated image at export resolution
        # Run synchronously since we need to block for the file save
        from PyQt6.QtCore import QEventLoop
        loop = QEventLoop()
        result = [None]

        filename = Path(sw._current_path).name if sw._current_path else ""
        worker = _SpectrogramRenderWorker(
            sw._spec, sw._sr, sw._fft, sw._dur,
            sw._freq_min, sw._freq_max, sw._freq_scale,
            sw._cmap_name, sw._db_range, sw._gamma,
            filename, export_w, export_h
        )

        def _got(img):
            result[0] = img
            loop.quit()

        worker.done.connect(_got)
        worker.done.connect(worker.deleteLater)
        worker.start()
        loop.exec()

        if result[0] is None:
            QMessageBox.critical(self, "Export Failed", "Could not render spectrogram.")
            return

        pm = QPixmap.fromImage(result[0])
        if pm.save(path, fmt):
            QMessageBox.information(self, "Exported", f"Saved to:\n{path}")
        else:
            QMessageBox.critical(self, "Export Failed", "Could not save the file.")
# ─────────────────────────────────────────────────────────────
#  ALBUM COVER EXTRACTOR PAGE
# ─────────────────────────────────────────────────────────────

class AlbumCoverExtractorPage(QWidget):
    """
    Scans a music library folder and extracts embedded cover art from
    FLAC/MP3/M4A files. Can also resize/convert to BMP for Rockbox devices.
    Uses mutagen for reading tags and Pillow for image processing if available;
    gracefully degrades to ffmpeg if mutagen is not installed.
    """

    def __init__(self, parent=None):
        super().__init__(parent)
        self._worker: Optional[QThread] = None
        self._build()

    def _build(self):
        t = _current_theme
        root = QVBoxLayout(self)
        root.setContentsMargins(0, 0, 0, 0)
        root.setSpacing(0)

        # ── Page header ───────────────────────────────────────
        hdr = QWidget()
        hdr.setFixedHeight(56)
        hdr.setStyleSheet("background:rgba(5,7,11,0.65); border-bottom:1px solid rgba(255,255,255,0.07);")
        hb = QHBoxLayout(hdr)
        hb.setContentsMargins(20, 0, 20, 0)
        title_lbl = QLabel("Album Cover Extractor")
        tf = QFont(); tf.setPointSize(15); tf.setBold(True)
        title_lbl.setFont(tf)
        title_lbl.setStyleSheet("color:#fff;background:transparent;")
        hb.addWidget(title_lbl)
        hb.addStretch()
        sub = QLabel("Extract & resize cover art from your music library")
        sub.setObjectName("muted")
        hb.addWidget(sub)
        root.addWidget(hdr)

        # ── Body ─────────────────────────────────────────────
        body = QWidget()
        body.setObjectName("covers_body")
        body.setStyleSheet("QWidget#covers_body { background:transparent; border:none; }")
        bl = QHBoxLayout(body)
        bl.setContentsMargins(20, 20, 20, 20)
        bl.setSpacing(20)
        root.addWidget(body, stretch=1)

        # Left panel — config
        cfg_panel = QWidget()
        cfg_panel.setFixedWidth(320)
        cfg_panel.setObjectName("panel")
        cfg_panel.setAttribute(Qt.WidgetAttribute.WA_StyledBackground, True)
        cv = QVBoxLayout(cfg_panel)
        cv.setContentsMargins(16, 16, 16, 16)
        cv.setSpacing(12)

        def _section(text):
            lbl = QLabel(text.upper())
            lbl.setObjectName("sectiontitle")
            return lbl

        # Folder
        cv.addWidget(_section("Music Library Folder"))
        folder_row = QHBoxLayout()
        self._folder_edit = QLineEdit()
        self._folder_edit.setPlaceholderText("Select or type folder path…")
        self._folder_edit.setReadOnly(True)
        folder_row.addWidget(self._folder_edit, stretch=1)
        browse_btn = QPushButton("Folder…")
        browse_btn.setObjectName("ghost")
        browse_btn.setFixedHeight(30)
        browse_btn.clicked.connect(self._browse_folder)
        folder_row.addWidget(browse_btn)

        browse_file_btn = QPushButton("File…")
        browse_file_btn.setObjectName("ghost")
        browse_file_btn.setFixedHeight(30)
        browse_file_btn.clicked.connect(self._browse_file)
        folder_row.addWidget(browse_file_btn)
        cv.addLayout(folder_row)

        # Options
        cv.addSpacing(4)
        cv.addWidget(_section("Output Options"))

        self._opt_save_original = QCheckBox("Save original cover as Cover.jpg/png")
        self._opt_save_original.setChecked(True)
        cv.addWidget(self._opt_save_original)

        self._opt_bmp = QCheckBox("Generate resized BMP (for Rockbox)")
        self._opt_bmp.setChecked(True)
        cv.addWidget(self._opt_bmp)

        self._opt_dry = QCheckBox("Dry run — report only, don't write files")
        cv.addWidget(self._opt_dry)

        self._opt_overwrite = QCheckBox("Overwrite existing cover files")
        self._opt_overwrite.setChecked(False)
        cv.addWidget(self._opt_overwrite)

        # BMP size
        cv.addSpacing(4)
        cv.addWidget(_section("BMP Dimensions"))
        size_row = QHBoxLayout()
        size_row.setSpacing(8)
        size_row.addWidget(QLabel("W"))
        self._bmp_w = QSpinBox()
        self._bmp_w.setRange(64, 2048); self._bmp_w.setValue(500)
        self._bmp_w.setFixedWidth(70)
        size_row.addWidget(self._bmp_w)
        size_row.addWidget(QLabel("H"))
        self._bmp_h = QSpinBox()
        self._bmp_h.setRange(64, 2048); self._bmp_h.setValue(500)
        self._bmp_h.setFixedWidth(70)
        size_row.addWidget(self._bmp_h)
        size_row.addStretch()
        cv.addLayout(size_row)

        # Output name templates
        cv.addSpacing(4)
        cv.addWidget(_section("Output Filenames"))
        cv.addWidget(QLabel("Original cover filename:"))
        self._name_orig = QLineEdit("Cover")
        cv.addWidget(self._name_orig)
        cv.addWidget(QLabel("BMP filename (no extension):"))
        self._name_bmp = QLineEdit("")
        self._name_bmp.setPlaceholderText("Leave blank to use album folder name")
        cv.addWidget(self._name_bmp)

        cv.addStretch()

        # Run controls
        btn_row = QHBoxLayout(); btn_row.setSpacing(8)
        self._run_btn = QPushButton("Extract covers")
        self._run_btn.setObjectName("primary")
        self._run_btn.setFixedHeight(36)
        self._run_btn.setMinimumWidth(130)
        self._run_btn.clicked.connect(self._start)
        btn_row.addWidget(self._run_btn)

        self._pause_btn = QPushButton("⏸ Pause")
        self._pause_btn.setObjectName("ghost")
        self._pause_btn.setFixedHeight(36)
        self._pause_btn.setEnabled(False)
        self._pause_btn.setCheckable(True)
        self._pause_btn.clicked.connect(self._toggle_pause)
        btn_row.addWidget(self._pause_btn)

        self._cancel_btn = QPushButton("Cancel")
        self._cancel_btn.setObjectName("danger")
        self._cancel_btn.setFixedHeight(36)
        self._cancel_btn.setEnabled(False)
        self._cancel_btn.clicked.connect(self._cancel)
        btn_row.addWidget(self._cancel_btn)
        cv.addLayout(btn_row)

        bl.addWidget(cfg_panel)

        # Right panel — progress + results
        right = QVBoxLayout()
        right.setSpacing(10)

        # Progress section
        prog_widget = QWidget()
        prog_widget.setObjectName("panel")
        prog_widget.setAttribute(Qt.WidgetAttribute.WA_StyledBackground, True)
        pv = QVBoxLayout(prog_widget)
        pv.setContentsMargins(16, 12, 16, 12)
        pv.setSpacing(6)
        prog_hdr = QHBoxLayout()
        prog_hdr.addWidget(QLabel("Progress"))
        prog_hdr.addStretch()
        self._prog_lbl = QLabel("Ready")
        self._prog_lbl.setObjectName("muted")
        prog_hdr.addWidget(self._prog_lbl)
        pv.addLayout(prog_hdr)
        self._prog_bar = QProgressBar()
        self._prog_bar.setRange(0, 100)
        self._prog_bar.setValue(0)
        self._prog_bar.setTextVisible(True)
        self._prog_bar.setFixedHeight(8)
        pv.addWidget(self._prog_bar)
        right.addWidget(prog_widget)

        # Log output
        log_widget = QWidget()
        log_widget.setObjectName("panel")
        log_widget.setAttribute(Qt.WidgetAttribute.WA_StyledBackground, True)
        lv = QVBoxLayout(log_widget)
        lv.setContentsMargins(0, 0, 0, 0)
        log_hdr = QWidget()
        log_hdr.setStyleSheet("background:rgba(255,255,255,0.04); border-radius:8px 8px 0 0; border-bottom:1px solid rgba(255,255,255,0.07);")
        lhb = QHBoxLayout(log_hdr)
        lhb.setContentsMargins(14, 8, 14, 8)
        lhb.addWidget(QLabel("Log"))
        lhb.addStretch()
        clr = QPushButton("Clear")
        clr.setObjectName("ghost")
        clr.setFixedHeight(24)
        clr.clicked.connect(lambda: self._log.clear())
        lhb.addWidget(clr)
        lv.addWidget(log_hdr)
        self._log = QPlainTextEdit()
        self._log.setReadOnly(True)
        self._log.setPlaceholderText("Extraction log will appear here…")
        self._log.setStyleSheet(
            "QPlainTextEdit { background:rgba(0,0,0,0.25); color:rgba(255,255,255,0.75);"
            " font-family:'Cascadia Code','SF Mono','Consolas',monospace; font-size:11px;"
            " border:none; border-radius:0 0 8px 8px; padding:10px; }"
        )
        lv.addWidget(self._log)

        # Stats row
        stats_row = QHBoxLayout()
        stats_row.setSpacing(20)
        for attr, label in [("_stat_found","Found"), ("_stat_saved","Saved"), ("_stat_skipped","Skipped"), ("_stat_errors","Errors")]:
            w = QWidget()
            w.setObjectName("card")
            w.setAttribute(Qt.WidgetAttribute.WA_StyledBackground, True)
            wl = QVBoxLayout(w); wl.setContentsMargins(12, 8, 12, 8); wl.setSpacing(2)
            num = QLabel("0")
            nf = QFont(); nf.setPointSize(18); nf.setBold(True)
            num.setFont(nf)
            num.setAlignment(Qt.AlignmentFlag.AlignCenter)
            num.setStyleSheet(f"color:{tok('accent')};background:transparent;")
            lbl2 = QLabel(label)
            lbl2.setAlignment(Qt.AlignmentFlag.AlignCenter)
            lbl2.setObjectName("muted")
            wl.addWidget(num); wl.addWidget(lbl2)
            setattr(self, attr, num)
            stats_row.addWidget(w)
        right.addLayout(stats_row)
        right.addWidget(log_widget, stretch=1)
        bl.addLayout(right, stretch=1)

    def _browse_folder(self):
        d = QFileDialog.getExistingDirectory(self, "Select Folder", str(Path.home()))
        if d:
            self._folder_edit.setText(d)

    def _browse_file(self):
        f, _ = QFileDialog.getOpenFileName(
            self,
            "Select Audio File",
            str(Path.home()),
            "Audio files (*.mp3 *.flac *.m4a *.aac *.ogg *.opus *.wma);;All files (*)",
        )
        if f:
            self._folder_edit.setText(f)

    def _log_msg(self, msg: str):
        self._log.appendPlainText(msg)

    def _toggle_pause(self):
        if not self._worker:
            return
        if self._pause_btn.isChecked():
            self._pause_btn.setText("▶ Resume")
            if hasattr(self._worker, 'pause'):
                self._worker.pause()
        else:
            self._pause_btn.setText("⏸ Pause")
            if hasattr(self._worker, 'resume'):
                self._worker.resume()

    def _cancel(self):
        if self._worker and hasattr(self._worker, 'cancel'):
            self._worker.cancel()
        self._set_running(False)
        self._log_msg("— Cancelled —")

    def _start(self):
        folder = self._folder_edit.text().strip()
        p = Path(folder) if folder else None
        if not folder or not p.exists() or not (p.is_dir() or p.is_file()):
            QMessageBox.warning(self, "No input", "Please select a valid folder or an audio file.")
            return
        self._stat_found.setText("0")
        self._stat_saved.setText("0")
        self._stat_skipped.setText("0")
        self._stat_errors.setText("0")
        self._log.clear()
        self._prog_bar.setValue(0)
        self._prog_lbl.setText("Starting…")
        self._set_running(True)

        opts = {
            "save_original": self._opt_save_original.isChecked(),
            "gen_bmp":       self._opt_bmp.isChecked(),
            "dry_run":       self._opt_dry.isChecked(),
            "overwrite":     self._opt_overwrite.isChecked(),
            "bmp_w":         self._bmp_w.value(),
            "bmp_h":         self._bmp_h.value(),
            "name_orig":     self._name_orig.text().strip() or "Cover",
            "name_bmp":      self._name_bmp.text().strip(),  # empty = use album folder name
        }
        self._worker = _CoverExtractWorker(folder, opts)
        self._worker.progress.connect(self._on_progress)
        self._worker.log.connect(self._log_msg)
        self._worker.stats.connect(self._on_stats)
        self._worker.finished.connect(self._on_done)
        self._worker.start()

    def _set_running(self, running: bool):
        self._run_btn.setEnabled(not running)
        self._pause_btn.setEnabled(running)
        self._cancel_btn.setEnabled(running)
        if not running:
            self._pause_btn.setChecked(False)
            self._pause_btn.setText("⏸ Pause")

    def _on_progress(self, pct: int, status: str):
        self._prog_bar.setValue(pct)
        self._prog_lbl.setText(status)

    def _on_stats(self, found: int, saved: int, skipped: int, errors: int):
        self._stat_found.setText(str(found))
        self._stat_saved.setText(str(saved))
        self._stat_skipped.setText(str(skipped))
        self._stat_errors.setText(str(errors))

    def _on_done(self):
        self._set_running(False)
        self._prog_lbl.setText("Complete")


class _CoverExtractWorker(QThread):
    progress = pyqtSignal(int, str)
    log      = pyqtSignal(str)
    stats    = pyqtSignal(int, int, int, int)
    # finished inherited from QThread

    def __init__(self, folder: str, opts: dict):
        super().__init__()
        self._folder = folder
        self._opts   = opts
        self._cancel_flag = False
        self._pause_event = _threading.Event()
        self._pause_event.set()

    def cancel(self): self._cancel_flag = True; self._pause_event.set()
    def pause(self):  self._pause_event.clear()
    def resume(self): self._pause_event.set()

    def run(self):
        try:
            self._run()
        except Exception as e:
            self.log.emit(f"[ERROR] {e}")

    def _run(self):
        opts      = self._opts
        dry       = opts["dry_run"]
        overwrite = opts["overwrite"]
        bmp_w, bmp_h = opts["bmp_w"], opts["bmp_h"]
        name_orig = opts["name_orig"]
        name_bmp  = opts["name_bmp"]

        # Collect music files
        exts = {".flac", ".mp3", ".m4a", ".ogg", ".opus", ".aac", ".wma"}
        root = Path(self._folder)
        if root.is_file():
            files = [root] if root.suffix.lower() in exts else []
        else:
            files = [p for p in root.rglob("*") if p.suffix.lower() in exts]
        total  = len(files)
        found  = saved = skipped = errors = 0
        self.log.emit(f"Found {total} music files to scan.\n")

        # Try to import mutagen for metadata reading
        try:
            import mutagen
            from mutagen.flac import FLAC
            from mutagen.mp3  import MP3
            from mutagen.id3  import ID3
            from mutagen.mp4  import MP4
            _HAS_MUTAGEN = True
        except ImportError:
            _HAS_MUTAGEN = False
            self.log.emit("[WARN] mutagen not installed — falling back to ffmpeg for cover extraction.")

        # PIL for resizing
        try:
            from PIL import Image
            import io as _io
            _HAS_PIL = True
        except ImportError:
            _HAS_PIL = False
            if opts["gen_bmp"]:
                self.log.emit("[WARN] Pillow not installed — BMP resize will use ffmpeg.")

        for i, fpath in enumerate(files):
            self._pause_event.wait()
            if self._cancel_flag:
                break

            pct = int((i + 1) * 100 / total)
            self.progress.emit(pct, f"{i+1}/{total}  {fpath.name}")

            album_dir = fpath.parent
            cover_data: Optional[bytes] = None
            cover_ext  = "jpg"
            album_title: Optional[str] = None

            # ── Extract cover bytes ──────────────────────────
            if _HAS_MUTAGEN:
                try:
                    suf = fpath.suffix.lower()
                    if suf == ".flac":
                        import mutagen.flac as mflac
                        audio = mflac.FLAC(str(fpath))
                        # Album name for default BMP naming
                        alb = audio.get("album")
                        album_title = (alb[0] if alb else None)
                        pics = audio.pictures
                        if pics:
                            cover_data = pics[0].data
                            if pics[0].mime and "png" in pics[0].mime:
                                cover_ext = "png"
                    elif suf == ".mp3":
                        from mutagen.id3 import ID3
                        tags = ID3(str(fpath))
                        try:
                            talb = tags.get("TALB")
                            album_title = (str(talb.text[0]) if talb and getattr(talb, "text", None) else None)
                        except Exception:
                            album_title = None
                        apics = tags.getall("APIC")
                        if apics:
                            cover_data = apics[0].data
                            if apics[0].mime and "png" in apics[0].mime:
                                cover_ext = "png"
                    elif suf in (".m4a", ".aac"):
                        from mutagen.mp4 import MP4
                        audio = MP4(str(fpath))
                        try:
                            alb = audio.tags.get("\xa9alb") if audio.tags else None
                            album_title = (str(alb[0]) if alb else None)
                        except Exception:
                            album_title = None
                        covr = audio.tags.get("covr") if audio.tags else None
                        if covr:
                            cover_data = bytes(covr[0])
                except Exception as e:
                    errors += 1
                    self.log.emit(f"  [SKIP] {fpath.name}: {e}")
                    self.stats.emit(found, saved, skipped, errors)
                    continue
            else:
                # Fallback: use ffmpeg to extract cover
                try:
                    tmp = album_dir / f"_sbxtmp_cover_{fpath.stem}.jpg"
                    result = subprocess.run(
                        ["ffmpeg", "-y", "-i", str(fpath), "-an", "-vcodec", "copy", str(tmp)],
                        capture_output=True, timeout=15
                    )
                    if tmp.exists():
                        cover_data = tmp.read_bytes()
                        tmp.unlink(missing_ok=True)
                except Exception:
                    pass

            if not cover_data:
                skipped += 1
                self.stats.emit(found, saved, skipped, errors)
                continue

            found += 1

            # ── Save original cover ──────────────────────────
            if opts["save_original"]:
                dest_orig = album_dir / f"{name_orig}.{cover_ext}"
                if dest_orig.exists() and not overwrite:
                    skipped += 1
                    self.log.emit(f"  [SKIP] {album_dir.name}/{dest_orig.name} (exists)")
                else:
                    if not dry:
                        dest_orig.write_bytes(cover_data)
                    saved += 1
                    self.log.emit(f"  {'[DRY] ' if dry else ''}→ {album_dir.name}/{dest_orig.name}")

            # ── Generate BMP ─────────────────────────────────
            if opts["gen_bmp"]:
                # Use album folder name as default BMP filename if not specified
                bmp_filename = name_bmp if name_bmp else (album_title or album_dir.name)
                # Sanitize for filesystem safety
                bmp_filename = re.sub(r'[\\/:*?"<>|]+', "_", bmp_filename).strip().strip(".")
                if not bmp_filename:
                    bmp_filename = album_dir.name
                dest_bmp = album_dir / f"{bmp_filename}.bmp"
                if dest_bmp.exists() and not overwrite:
                    skipped += 1
                    self.log.emit(f"  [SKIP] {album_dir.name}/{dest_bmp.name} (exists)")
                elif _HAS_PIL:
                    try:
                        from PIL import Image
                        import io as _io
                        img = Image.open(_io.BytesIO(cover_data)).convert("RGB")
                        img = img.resize((bmp_w, bmp_h), Image.LANCZOS)
                        if not dry:
                            img.save(str(dest_bmp), "BMP")
                        saved += 1
                        self.log.emit(f"  {'[DRY] ' if dry else ''}→ {album_dir.name}/{dest_bmp.name}  ({bmp_w}×{bmp_h})")
                    except Exception as e:
                        errors += 1
                        self.log.emit(f"  [ERR] BMP: {e}")
                else:
                    # PIL not available — ffmpeg fallback
                    if not dry:
                        try:
                            import tempfile, io as _io
                            with tempfile.NamedTemporaryFile(suffix=cover_ext, delete=False) as tf:
                                tf.write(cover_data); tmp_in = tf.name
                            try:
                                subprocess.run(
                                    ["ffmpeg", "-y", "-i", tmp_in,
                                     "-vf", f"scale={bmp_w}:{bmp_h}",
                                     str(dest_bmp)],
                                    capture_output=True, timeout=15
                                )
                            finally:
                                Path(tmp_in).unlink(missing_ok=True)
                            saved += 1
                        except Exception as e:
                            errors += 1
                            self.log.emit(f"  [ERR] BMP ffmpeg: {e}")

            self.stats.emit(found, saved, skipped, errors)

        self.log.emit(f"\n✓ Done. Found: {found}  Saved: {saved}  Skipped: {skipped}  Errors: {errors}")


# ─────────────────────────────────────────────────────────────
#  MUSIC TAG EDITOR PAGE
# ─────────────────────────────────────────────────────────────

def _peek_image_size(data: bytes):
    """
    Read image dimensions from header bytes without decoding the full image.
    Returns (width, height) or None if format is unrecognised.
    Handles JPEG, PNG, WebP (VP8/VP8L), BMP, GIF.
    """
    import struct
    if len(data) < 12:
        return None
    if data[:8] == b'\x89PNG\r\n\x1a\n' and len(data) >= 24:
        w, h = struct.unpack('>II', data[16:24])
        return w, h
    if data[:4] == b'RIFF' and data[8:12] == b'WEBP' and len(data) >= 30:
        if data[12:16] == b'VP8 ':
            w = struct.unpack_from('<H', data, 26)[0] & 0x3FFF
            h = struct.unpack_from('<H', data, 28)[0] & 0x3FFF
            return w, h
        if data[12:16] == b'VP8L' and len(data) >= 25:
            bits = struct.unpack_from('<I', data, 21)[0]
            return (bits & 0x3FFF) + 1, ((bits >> 14) & 0x3FFF) + 1
    if data[:2] == b'BM' and len(data) >= 26:
        w, h = struct.unpack_from('<ii', data, 18)
        return w, abs(h)
    if data[:6] in (b'GIF87a', b'GIF89a') and len(data) >= 10:
        w, h = struct.unpack_from('<HH', data, 6)
        return w, h
    if data[:2] == b'\xff\xd8':
        i = 2; end = min(len(data), 65536)
        while i + 4 < end:
            if data[i] != 0xFF: break
            marker = data[i + 1]
            if marker in (0xC0, 0xC1, 0xC2, 0xC3, 0xC5, 0xC6, 0xC7,
                          0xC9, 0xCA, 0xCB, 0xCD, 0xCE, 0xCF):
                if i + 9 <= end:
                    h, w = struct.unpack_from('>HH', data, i + 5)
                    return w, h
            if i + 3 >= end: break
            seg_len = struct.unpack_from('>H', data, i + 2)[0]
            i += 2 + seg_len
    return None


class _MbClusterWorker(QThread):
    """
    Groups a list of paths into album clusters by reading their tags.
    Emits clusters_ready(list[dict]) where each dict has:
      'key'   : cluster label (albumartist – album)
      'files' : list[Path]
    """
    clusters_ready = pyqtSignal(object)   # list[dict]
    progress       = pyqtSignal(int, int) # done, total

    def __init__(self, files: list):
        super().__init__()
        self.files = files

    def run(self):
        try:
            import mutagen
        except ImportError:
            self.clusters_ready.emit([])
            return

        buckets: dict[str, list] = {}
        total = len(self.files)
        for i, path in enumerate(self.files, 1):
            if self.isInterruptionRequested():
                break
            self.progress.emit(i, total)
            try:
                audio = mutagen.File(str(path), easy=True)
                tags  = audio.tags if audio else {}
                def _t(k):
                    v = (tags or {}).get(k)
                    return str(v[0]).strip() if isinstance(v, list) and v else str(v).strip() if v else ""
                aa    = _t("albumartist") or _t("artist") or "Unknown Artist"
                al    = _t("album") or "Unknown Album"
                key   = f"{aa}  —  {al}"
            except Exception:
                key   = "Unreadable"
            buckets.setdefault(key, []).append(path)

        result = [{"key": k, "files": v} for k, v in sorted(buckets.items())]
        self.clusters_ready.emit(result)


class _FileRenameWorker(QThread):
    """
    Renames files according to a template.
    Template tokens: %title% %artist% %albumartist% %album%
                     %tracknumber% %discnumber% %date% %genre%
    Emits log_line(msg, level) and finished(renamed, errors).
    """
    log_line = pyqtSignal(str, str)
    finished = pyqtSignal(int, int)

    def __init__(self, files: list, template: str):
        super().__init__()
        self.files    = files
        self.template = template

    @staticmethod
    def _safe(s: str) -> str:
        """Strip characters that are illegal in filenames."""
        return re.sub(r'[\\/:*?"<>|]', '_', s).strip()

    def run(self):
        try:
            import mutagen
        except ImportError:
            self.log_line.emit("mutagen not installed — cannot rename.", "error")
            self.finished.emit(0, len(self.files))
            return

        def _t(tags, k):
            v = (tags or {}).get(k)
            return str(v[0]).strip() if isinstance(v, list) and v else str(v).strip() if v else ""

        renamed = errors = 0
        for path in self.files:
            if self.isInterruptionRequested():
                break
            try:
                audio = mutagen.File(str(path), easy=True)
                tags  = audio.tags if audio else {}
                tpl = self.template
                for token, val in [
                    ("%title%",       _t(tags, "title")),
                    ("%artist%",      _t(tags, "artist")),
                    ("%albumartist%", _t(tags, "albumartist") or _t(tags, "artist")),
                    ("%album%",       _t(tags, "album")),
                    ("%tracknumber%", _t(tags, "tracknumber").split("/")[0].zfill(2)),
                    ("%discnumber%",  _t(tags, "discnumber").split("/")[0]),
                    ("%date%",        _t(tags, "date")[:4]),
                    ("%genre%",       _t(tags, "genre")),
                ]:
                    tpl = tpl.replace(token, self._safe(val) if val else "")

                tpl = re.sub(r'_+', '_', tpl).strip("_").strip()
                if not tpl:
                    self.log_line.emit(f"Empty name from template: {path.name}", "warn")
                    errors += 1
                    continue
                new_path = path.with_name(tpl + path.suffix)
                if new_path == path:
                    self.log_line.emit(f"Unchanged: {path.name}", "info")
                    continue
                if new_path.exists():
                    self.log_line.emit(f"Skipped (target exists): {new_path.name}", "warn")
                    errors += 1
                    continue
                path.rename(new_path)
                self.log_line.emit(f"Renamed: {path.name}  →  {new_path.name}", "ok")
                renamed += 1
            except Exception as e:
                self.log_line.emit(f"Rename error — {path.name}: {e}", "error")
                errors += 1
        self.finished.emit(renamed, errors)




class MusicTagEditorPage(QWidget):
    """
    Rich metadata editor. Select a folder to load all music files,
    click a file to edit all its tags, view/replace embedded cover art,
    and save changes via mutagen.

    Picard-inspired additions
    ─────────────────────────
    • Album clustering    — auto-group loaded files by album in a tree
    • File renaming       — rename files from a tag template
    • Original-values diff — amber highlight on changed fields
    • Batch tag ops       — capitalize, trim, swap artist↔albumartist, etc.
    • Sort file list      — click column header to sort
    • Missing-tag highlights — red dot on files missing Title/Artist/Album
    """

    _FIELDS = [
        ("title",        "Title"),
        ("artist",       "Artist"),
        ("albumartist",  "Album Artist"),
        ("album",        "Album"),
        ("date",         "Year / Date"),
        ("tracknumber",  "Track Number"),
        ("discnumber",   "Disc Number"),
        ("genre",        "Genre"),
        ("composer",     "Composer"),
        ("comment",      "Comment"),
        ("copyright",    "Copyright"),
        ("lyrics",       "Lyrics (first verse)"),
        ("bpm",          "BPM"),
        ("grouping",     "Grouping"),
        ("isrc",         "ISRC"),
    ]

    # Fields we consider "required" for the missing-tag highlight
    _REQUIRED_FIELDS = {"title", "artist", "album"}

    def __init__(self, parent=None):
        super().__init__(parent)
        self._files: list[Path]    = []

        self._current: Optional[Path] = None
        self._cover_data: Optional[bytes] = None
        # original values loaded from disk — for diff highlight
        self._orig_vals: dict[str, str] = {}
        # cluster mode: list of {"key":str, "files":list[Path]}
        self._clusters: list[dict] = []
        self._cluster_mode: bool            = False
        self._cluster_multi_paths: list     = []   # tracks selected in cluster tree
        self._build()

    # ─────────────────────────────────────────────────────────
    #  BUILD UI
    # ─────────────────────────────────────────────────────────

    def _build(self):
        t = _current_theme
        root = QVBoxLayout(self)
        root.setContentsMargins(0, 0, 0, 0)
        root.setSpacing(0)

        # ── Page header ───────────────────────────────────────
        hdr = QWidget()
        hdr.setFixedHeight(56)
        hdr.setStyleSheet("background:rgba(5,7,11,0.65); border-bottom:1px solid rgba(255,255,255,0.07);")
        hb = QHBoxLayout(hdr)
        hb.setContentsMargins(20, 0, 20, 0)
        hb.setSpacing(8)
        title_lbl = QLabel("Music Tag Editor")
        tf = QFont(); tf.setPointSize(15); tf.setBold(True)
        title_lbl.setFont(tf)
        title_lbl.setStyleSheet("color:#fff;background:transparent;")
        hb.addWidget(title_lbl)
        hb.addStretch()

        open_file_btn = QPushButton("🎵  Open File")
        open_file_btn.setObjectName("toggle")
        open_file_btn.setFixedHeight(32)
        open_file_btn.setMinimumWidth(120)
        open_file_btn.clicked.connect(self._open_file)
        hb.addWidget(open_file_btn)

        open_btn = QPushButton("📂  Open Folder")
        open_btn.setObjectName("toggle")
        open_btn.setFixedHeight(32)
        open_btn.setMinimumWidth(130)
        open_btn.clicked.connect(self._open_folder)
        hb.addWidget(open_btn)
        root.addWidget(hdr)

        # ── Body — three-panel splitter ───────────────────────
        body = QSplitter(Qt.Orientation.Horizontal)
        body.setHandleWidth(3)
        body.setStyleSheet("""
            QSplitter::handle { background: rgba(255,255,255,0.06); }
            QSplitter::handle:hover { background: rgba(200,134,26,0.50); }
            QSplitter::handle:pressed { background: rgba(200,134,26,0.85); }
        """)
        root.addWidget(body, stretch=1)

        # ── LEFT: file list / cluster tree ────────────────────
        file_panel = QWidget()
        file_panel.setStyleSheet("background:rgba(5,7,11,0.50); border-right:1px solid rgba(255,255,255,0.07);")
        fv = QVBoxLayout(file_panel)
        fv.setContentsMargins(0, 0, 0, 0)
        fv.setSpacing(0)

        # Sub-header with count + sort combo + cluster toggle
        file_hdr = QWidget()
        file_hdr.setFixedHeight(38)
        file_hdr.setStyleSheet("background:rgba(5,7,11,0.65); border-bottom:1px solid rgba(255,255,255,0.07);")
        fhb = QHBoxLayout(file_hdr)
        fhb.setContentsMargins(8, 0, 8, 0)
        fhb.setSpacing(6)
        self._file_count_lbl = QLabel("No folder loaded")
        self._file_count_lbl.setObjectName("muted")
        fhb.addWidget(self._file_count_lbl)
        fhb.addStretch()

        fv.addWidget(file_hdr)

        # Search bar
        search_bar = QWidget()
        search_bar.setStyleSheet("background:rgba(5,7,11,0.50); border-bottom:1px solid rgba(255,255,255,0.07);")
        sb_layout = QHBoxLayout(search_bar)
        sb_layout.setContentsMargins(8, 4, 8, 4)
        sb_layout.setSpacing(4)
        self._search_box = QLineEdit()
        self._search_box.setPlaceholderText("Search files…")
        self._search_box.setFixedHeight(24)
        self._search_box.setStyleSheet("font-size:12px; padding: 0 6px;")
        self._search_box.setCompleter(None)
        self._search_box.textChanged.connect(self._filter_file_list)
        sb_layout.addWidget(self._search_box)
        clr_search = QPushButton("✕"); clr_search.setObjectName("ghost")
        clr_search.setFixedSize(24, 24)
        clr_search.setToolTip("Clear search")
        clr_search.clicked.connect(self._search_box.clear)
        sb_layout.addWidget(clr_search)
        fv.addWidget(search_bar)

        # Stacked widget: flat list vs cluster tree
        self._left_stack = QStackedWidget()
        fv.addWidget(self._left_stack, stretch=1)

        # Page 0: flat file list
        list_page = QWidget()
        list_page.setStyleSheet("background:rgba(5,7,11,0.35);")
        lp_v = QVBoxLayout(list_page)
        lp_v.setContentsMargins(0, 0, 0, 0)
        lp_v.setSpacing(0)

        self._file_list = QListWidget()
        self._file_list.setSelectionMode(QAbstractItemView.SelectionMode.ExtendedSelection)
        self._file_list.setStyleSheet(
            f"QListWidget{{background:rgba(5,7,11,0.50);border:none;outline:none;}}"
            f"QListWidget::item{{padding:5px 12px;border-bottom:1px solid rgba(255,255,255,0.09);"
            f"color:rgba(255,255,255,0.87);}}"
            f"QListWidget::item:selected{{background:{t['accentlo']};color:{t['accent']};}}"
            f"QListWidget::item:hover:!selected{{background:rgba(255,255,255,0.08);}}"
        )
        self._file_list.currentRowChanged.connect(self._on_file_selected)
        self._file_list.itemSelectionChanged.connect(self._on_selection_changed)
        self._file_list.keyPressEvent = self._file_list_key_press
        lp_v.addWidget(self._file_list)
        self._left_stack.addWidget(list_page)   # index 0

        # Page 1: cluster tree
        tree_page = QWidget()
        tree_page.setStyleSheet("background:rgba(5,7,11,0.35);")
        tp_v = QVBoxLayout(tree_page)
        tp_v.setContentsMargins(0, 0, 0, 0)

        self._cluster_tree = QTreeWidget()
        self._cluster_tree.setColumnCount(1)
        self._cluster_tree.setHeaderHidden(True)
        self._cluster_tree.setSelectionMode(QAbstractItemView.SelectionMode.ExtendedSelection)
        self._cluster_tree.setStyleSheet(
            f"QTreeWidget{{background:rgba(5,7,11,0.50);border:none;outline:none;}}"
            f"QTreeWidget::item{{padding:4px 8px;color:rgba(255,255,255,0.87);}}"
            f"QTreeWidget::item:selected{{background:{t['accentlo']};color:{t['accent']};}}"
            f"QTreeWidget::item:hover:!selected{{background:rgba(255,255,255,0.08);}}"
        )
        self._cluster_tree.itemClicked.connect(self._on_cluster_item_clicked)
        self._cluster_tree.itemSelectionChanged.connect(self._on_cluster_selection_changed)
        self._cluster_tree.keyPressEvent = self._cluster_tree_key_press
        tp_v.addWidget(self._cluster_tree)
        self._left_stack.addWidget(tree_page)   # index 1

        # Bottom bar: Select All / Remove / Clear / Cluster button
        sel_all_row = QHBoxLayout()
        sel_all_row.setContentsMargins(8, 4, 8, 4)
        sel_all_row.setSpacing(4)
        sel_all_btn = QPushButton("Select All"); sel_all_btn.setObjectName("ghost")
        sel_all_btn.setFixedHeight(26)
        sel_all_btn.clicked.connect(self._select_all)
        sel_all_row.addWidget(sel_all_btn)
        del_sel_btn = QPushButton("Remove"); del_sel_btn.setObjectName("ghost")
        del_sel_btn.setFixedHeight(26)
        del_sel_btn.setToolTip("Remove selected files from list")
        del_sel_btn.clicked.connect(self._remove_selected_files)
        sel_all_row.addWidget(del_sel_btn)
        clear_list_btn = QPushButton("Clear"); clear_list_btn.setObjectName("ghost")
        clear_list_btn.setFixedHeight(26)
        clear_list_btn.clicked.connect(self._clear_file_list)
        sel_all_row.addWidget(clear_list_btn)
        reload_btn = QPushButton("↻ Reload"); reload_btn.setObjectName("ghost")
        reload_btn.setFixedHeight(26)
        reload_btn.setToolTip("Reload file list from disk")
        reload_btn.clicked.connect(self._reload_file_list)
        sel_all_row.addWidget(reload_btn)
        sel_all_row.addStretch()
        self._cluster_btn = QPushButton("⊞ Cluster")
        self._cluster_btn.setObjectName("ghost")
        self._cluster_btn.setFixedHeight(26)
        self._cluster_btn.setToolTip("Group files by album")
        self._cluster_btn.setCheckable(True)
        self._cluster_btn.toggled.connect(self._toggle_cluster_mode)
        sel_all_row.addWidget(self._cluster_btn)
        self._cancel_cluster_btn = QPushButton("✕ Cancel")
        self._cancel_cluster_btn.setObjectName("danger")
        self._cancel_cluster_btn.setFixedHeight(26)
        self._cancel_cluster_btn.setToolTip("Cancel clustering")
        self._cancel_cluster_btn.setVisible(False)
        self._cancel_cluster_btn.clicked.connect(self._cancel_clustering)
        sel_all_row.addWidget(self._cancel_cluster_btn)
        fv.addLayout(sel_all_row)
        body.addWidget(file_panel)

        # ── CENTRE: tag fields + log ───────────────────────────
        fields_panel = QWidget()
        fields_panel.setStyleSheet("background:rgba(0,0,0,0.20);")
        fld_v = QVBoxLayout(fields_panel)
        fld_v.setContentsMargins(0, 0, 0, 0)
        fld_v.setSpacing(0)

        # Toolbar
        tbar = QWidget()
        tbar.setFixedHeight(48)
        tbar.setStyleSheet("background:rgba(5,7,11,0.65); border-bottom:1px solid rgba(255,255,255,0.07);")
        tb = QHBoxLayout(tbar)
        tb.setContentsMargins(16, 0, 16, 0)
        tb.setSpacing(8)
        self._file_label = QLabel("Select a file to edit")
        self._file_label.setStyleSheet("font-weight:600;color:rgba(255,255,255,0.85);")
        tb.addWidget(self._file_label)
        tb.addStretch()

        # Batch ops button
        self._batch_btn = QPushButton("⚙ Batch Ops")
        self._batch_btn.setObjectName("ghost")
        self._batch_btn.setFixedHeight(32)
        self._batch_btn.setToolTip("Apply batch tag operations to selected files")
        self._batch_btn.clicked.connect(self._show_batch_ops)
        tb.addWidget(self._batch_btn)

        # Rename button
        self._rename_btn = QPushButton("✎ Rename Files")
        self._rename_btn.setObjectName("ghost")
        self._rename_btn.setFixedHeight(32)
        self._rename_btn.setToolTip("Rename files using a tag template")
        self._rename_btn.clicked.connect(self._show_rename_dialog)
        tb.addWidget(self._rename_btn)

        self._save_all_btn = QPushButton("💾  Save to All Selected")
        self._save_all_btn.setObjectName("ghost")
        self._save_all_btn.setFixedHeight(32)
        self._save_all_btn.setEnabled(False)
        self._save_all_btn.setToolTip("Save changed fields to all selected files")
        self._save_all_btn.clicked.connect(self._save_tags_multi)
        tb.addWidget(self._save_all_btn)

        self._save_btn = QPushButton("💾  Save Changes")
        self._save_btn.setObjectName("ghost")
        self._save_btn.setFixedHeight(32)
        self._save_btn.setEnabled(False)
        self._save_btn.clicked.connect(self._save_tags)
        tb.addWidget(self._save_btn)
        # Keyboard shortcut: Ctrl+S saves (single file) or all selected
        _save_shortcut = QShortcut(QKeySequence("Ctrl+S"), self)
        _save_shortcut.activated.connect(self._ctrl_s_save)

        # Tab switcher
        _tab_btn_style = f"""
            QPushButton {{
                background: transparent; border: none;
                border-bottom: 2px solid transparent;
                color: rgba(255,255,255,0.35); font-size: 12px; font-weight: 600;
                padding: 0 12px; border-radius: 0; min-height: 48px;
            }}
            QPushButton:checked {{
                color: {t['accent']}; border-bottom: 2px solid {t['accent']};
            }}
            QPushButton:hover:!checked {{ color: rgba(255,255,255,0.87); }}
        """
        self._tab_tags_btn = QPushButton("Tags")
        self._tab_log_btn  = QPushButton("Log  (0)")
        for btn in (self._tab_tags_btn, self._tab_log_btn):
            btn.setCheckable(True); btn.setFlat(True)
            btn.setStyleSheet(_tab_btn_style)
        self._tab_tags_btn.setChecked(True)
        tb.addWidget(self._tab_tags_btn)
        tb.addWidget(self._tab_log_btn)
        fld_v.addWidget(tbar)

        # Stacked body: tags page / log page
        self._centre_stack = QStackedWidget()
        fld_v.addWidget(self._centre_stack, stretch=1)

        # ── Page 0: tag fields ────────────────────────────────
        tags_page = QWidget()
        tags_page.setStyleSheet("background:rgba(0,0,0,0.15);")
        tags_page_v = QVBoxLayout(tags_page)
        tags_page_v.setContentsMargins(0, 0, 0, 0)
        tags_page_v.setSpacing(0)

        fields_scroll = QScrollArea()
        fields_scroll.setWidgetResizable(True)
        fields_scroll.setFrameShape(QFrame.Shape.NoFrame)
        fields_scroll.setStyleSheet("QScrollArea{background:transparent;border:none;}")

        fields_inner = QWidget()
        fields_inner.setStyleSheet("background:transparent;")
        form = QGridLayout(fields_inner)
        form.setContentsMargins(20, 16, 20, 16)
        form.setSpacing(10)
        form.setColumnMinimumWidth(0, 120)
        form.setColumnStretch(1, 1)
        form.setColumnMinimumWidth(2, 110)  # original-value column

        self._tag_edits: dict[str, QLineEdit] = {}
        self._orig_lbls: dict[str, QLabel]    = {}   # per-field orig-value label

        for row_idx, (key, label) in enumerate(self._FIELDS):
            lbl = QLabel(label)
            lbl.setAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
            lbl.setStyleSheet("color:rgba(255,255,255,0.55);font-size:12px;background:transparent;")
            edit = QLineEdit()
            edit.setPlaceholderText(f"Enter {label}…")
            # Connect to diff-highlight handler
            edit.textChanged.connect(lambda text, k=key: self._on_field_changed(k, text))
            self._tag_edits[key] = edit

            orig_lbl = QLabel("—")
            orig_lbl.setStyleSheet(
                f"color:rgba(255,255,255,0.35);font-size:11px;background:transparent;"
                f"padding:0 6px;"
            )
            orig_lbl.setAlignment(Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter)
            orig_lbl.setToolTip("Value currently saved on disk")
            self._orig_lbls[key] = orig_lbl

            form.addWidget(lbl,      row_idx, 0)
            form.addWidget(edit,     row_idx, 1)
            form.addWidget(orig_lbl, row_idx, 2)

        # Column header for original-value column
        orig_col_hdr = QLabel("ON DISK")
        orig_col_hdr.setObjectName("sectiontitle")
        orig_col_hdr.setAlignment(Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter)
        form.addWidget(orig_col_hdr, 0, 2)   # overlaps first row label — we move it above

        # Reorder: put the column header in a dedicated row above the fields
        # Re-do the grid: header row 0, fields from row 1
        # Rebuild the form properly with a header row
        # (Clear and redo with header row)
        while form.count():
            item = form.takeAt(0)
            if item.widget():
                item.widget().setParent(None)  # type: ignore

        # Add "ON DISK" column label in header row
        disk_hdr = QLabel("ON DISK")
        disk_hdr.setObjectName("sectiontitle")
        disk_hdr.setAlignment(Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter)
        disk_hdr.setStyleSheet(
            f"color:rgba(255,255,255,0.35);font-size:10px;font-weight:600;letter-spacing:1.5px;"
            "background:transparent;padding:0 6px;"
        )
        form.addWidget(disk_hdr, 0, 2)

        for row_idx, (key, label) in enumerate(self._FIELDS):
            grid_row = row_idx + 1
            lbl = QLabel(label)
            lbl.setAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
            lbl.setStyleSheet("color:rgba(255,255,255,0.55);font-size:12px;background:transparent;")
            edit = self._tag_edits[key]
            edit.setParent(fields_inner)
            orig_lbl = self._orig_lbls[key]
            orig_lbl.setParent(fields_inner)
            form.addWidget(lbl,      grid_row, 0)
            form.addWidget(edit,     grid_row, 1)
            form.addWidget(orig_lbl, grid_row, 2)

        # Technical info (read-only)
        tech_row = len(self._FIELDS) + 1
        sep = QFrame()
        sep.setFrameShape(QFrame.Shape.HLine)
        sep.setStyleSheet("background:rgba(255,255,255,0.09);")
        form.addWidget(sep, tech_row, 0, 1, 3)
        tech_lbl = QLabel("Technical Info")
        tech_lbl.setObjectName("sectiontitle")
        form.addWidget(tech_lbl, tech_row + 1, 0, 1, 3)

        tech_fields = [("_info_format","Format/Codec"), ("_info_bitrate","Bitrate"),
                       ("_info_samplerate","Sample Rate"), ("_info_channels","Channels"),
                       ("_info_duration","Duration"), ("_info_filesize","File Size")]
        for ti, (attr, lbl_txt) in enumerate(tech_fields):
            lbl = QLabel(lbl_txt)
            lbl.setAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
            lbl.setStyleSheet("color:rgba(255,255,255,0.35);font-size:11px;background:transparent;")
            val = QLabel("—")
            val.setStyleSheet("color:rgba(255,255,255,0.85);font-size:11px;background:transparent;")
            setattr(self, attr, val)
            form.addWidget(lbl, tech_row + 2 + ti, 0)
            form.addWidget(val, tech_row + 2 + ti, 1)

        fields_scroll.setWidget(fields_inner)
        tags_page_v.addWidget(fields_scroll, stretch=1)

        self._status_bar = QLabel("Ready")
        self._status_bar.setFixedHeight(22)
        self._status_bar.setStyleSheet(
            f"background:rgba(0,0,0,0.30);color:rgba(255,255,255,0.35);font-size:10px;"
            f"padding:0 12px;border-top:1px solid rgba(255,255,255,0.09);"
        )
        tags_page_v.addWidget(self._status_bar)
        self._centre_stack.addWidget(tags_page)   # index 0

        # ── Page 1: log ───────────────────────────────────────
        log_page = QWidget()
        log_page.setStyleSheet("background:rgba(0,0,0,0.15);")
        log_page_v = QVBoxLayout(log_page)
        log_page_v.setContentsMargins(0, 0, 0, 0)
        log_page_v.setSpacing(0)

        log_sub_hdr = QWidget()
        log_sub_hdr.setFixedHeight(36)
        log_sub_hdr.setStyleSheet("background:rgba(5,7,11,0.65); border-bottom:1px solid rgba(255,255,255,0.07);")
        lsh = QHBoxLayout(log_sub_hdr)
        lsh.setContentsMargins(16, 0, 12, 0); lsh.setSpacing(8)
        self._log_count_lbl = QLabel("No entries yet")
        self._log_count_lbl.setStyleSheet("color:rgba(255,255,255,0.35);font-size:11px;background:transparent;")
        lsh.addWidget(self._log_count_lbl)
        lsh.addStretch()
        self._log_filter_combo = QComboBox()
        self._log_filter_combo.addItems(["All", "Errors only", "OK only"])
        self._log_filter_combo.setFixedHeight(24)
        self._log_filter_combo.setFixedWidth(110)
        self._log_filter_combo.setStyleSheet("font-size:11px;")
        self._log_filter_combo.currentIndexChanged.connect(self._apply_tag_log_filter)
        lsh.addWidget(self._log_filter_combo)
        log_clear_btn = QPushButton("Clear")
        log_clear_btn.setObjectName("ghost")
        log_clear_btn.setFixedHeight(24); log_clear_btn.setFixedWidth(52)
        log_clear_btn.setStyleSheet("font-size:11px;")
        log_clear_btn.clicked.connect(self._clear_tag_log)
        lsh.addWidget(log_clear_btn)
        log_page_v.addWidget(log_sub_hdr)

        self._log_table = QTableWidget()
        self._log_table.setColumnCount(5)
        self._log_table.setHorizontalHeaderLabels(["Time", "●", "Operation", "File", "Detail"])
        self._log_table.setColumnWidth(0, 68)
        self._log_table.setColumnWidth(1, 22)
        self._log_table.setColumnWidth(2, 100)
        self._log_table.horizontalHeader().setSectionResizeMode(3, QHeaderView.ResizeMode.Stretch)
        self._log_table.horizontalHeader().setSectionResizeMode(4, QHeaderView.ResizeMode.Stretch)
        self._log_table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self._log_table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self._log_table.setAlternatingRowColors(True)
        self._log_table.setSortingEnabled(False)
        self._log_table.verticalHeader().setVisible(False)
        self._log_table.verticalHeader().setDefaultSectionSize(24)
        self._log_table.setStyleSheet(
            f"QTableWidget{{background:rgba(0,0,0,0.30);border:none;font-size:11px;gridline-color:rgba(255,255,255,0.09);}}"
            f"QTableWidget::item{{padding:0 6px;}}"
            f"QHeaderView::section{{background:rgba(255,255,255,0.05);color:rgba(255,255,255,0.35);"
            f"font-size:10px;font-weight:600;border:none;padding:3px 6px;"
            f"border-bottom:1px solid rgba(255,255,255,0.09);}}"
            f"QTableWidget::item:alternate{{background:rgba(5,7,11,0.50);}}"
        )
        log_page_v.addWidget(self._log_table, stretch=1)

        self._log_stats_bar = QLabel("")
        self._log_stats_bar.setFixedHeight(22)
        self._log_stats_bar.setStyleSheet(
            f"background:rgba(5,7,11,0.50);color:rgba(255,255,255,0.35);font-size:10px;"
            f"padding:0 16px;border-top:1px solid rgba(255,255,255,0.09);"
        )
        log_page_v.addWidget(self._log_stats_bar)
        self._centre_stack.addWidget(log_page)   # index 1

        def _switch_centre(idx):
            self._tab_tags_btn.setChecked(idx == 0)
            self._tab_log_btn.setChecked(idx == 1)
            self._centre_stack.setCurrentIndex(idx)
        self._tab_tags_btn.clicked.connect(lambda: _switch_centre(0))
        self._tab_log_btn.clicked.connect(lambda: _switch_centre(1))
        self._switch_to_log = lambda: _switch_centre(1)

        body.addWidget(fields_panel)

        # ── RIGHT: cover / resize / verify / rg / file details ─
        right_outer = QWidget()
        right_outer.setMinimumWidth(180)
        right_outer.setStyleSheet("background:rgba(5,7,11,0.50); border-left:1px solid rgba(255,255,255,0.07);")
        right_outer_v = QVBoxLayout(right_outer)
        right_outer_v.setContentsMargins(0, 0, 0, 0)
        right_outer_v.setSpacing(0)

        right_scroll = QScrollArea()
        right_scroll.setWidgetResizable(True)
        right_scroll.setFrameShape(QFrame.Shape.NoFrame)
        right_scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)
        right_scroll.setStyleSheet(
            f"QScrollArea{{background:rgba(5,7,11,0.50);border:none;}}"
            f"QScrollArea > QWidget > QWidget{{background:rgba(5,7,11,0.50);}}"
        )

        cover_panel = QWidget()
        cover_panel.setStyleSheet("background:rgba(5,7,11,0.35);")
        cov_v = QVBoxLayout(cover_panel)
        cov_v.setContentsMargins(14, 16, 14, 16)
        cov_v.setSpacing(10)

        # ── Cover Art ─────────────────────────────────────────
        cov_section = QLabel("COVER ART")
        cov_section.setObjectName("sectiontitle")
        cov_v.addWidget(cov_section)

        self._cover_lbl = QLabel("No Cover")
        self._cover_lbl.setFixedSize(192, 192)
        self._cover_lbl.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self._cover_lbl.setStyleSheet(
            f"background:rgba(255,255,255,0.08);border-radius:8px;color:rgba(255,255,255,0.35);"
            f"font-size:13px;border:1px solid rgba(255,255,255,0.09);"
        )
        cov_v.addWidget(self._cover_lbl)

        cov_info = QLabel("")
        cov_info.setObjectName("muted")
        cov_info.setWordWrap(True)
        cov_info.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self._cover_info_lbl = cov_info
        cov_v.addWidget(cov_info)

        set_cover_btn = QPushButton("Set New Cover…")
        set_cover_btn.setObjectName("ghost")
        set_cover_btn.clicked.connect(self._set_cover)
        cov_v.addWidget(set_cover_btn)

        cover_row2 = QHBoxLayout(); cover_row2.setSpacing(6)
        export_cover_btn = QPushButton("Export…")
        export_cover_btn.setObjectName("ghost")
        export_cover_btn.clicked.connect(self._export_cover)
        remove_cover_btn = QPushButton("Remove")
        remove_cover_btn.setObjectName("danger")
        remove_cover_btn.clicked.connect(self._remove_cover)
        cover_row2.addWidget(export_cover_btn)
        cover_row2.addWidget(remove_cover_btn)
        cov_v.addLayout(cover_row2)

        # ── Resize Cover ──────────────────────────────────────
        _sep1 = QFrame(); _sep1.setFrameShape(QFrame.Shape.HLine)
        _sep1.setStyleSheet("background:rgba(255,255,255,0.09);max-height:1px;")
        cov_v.addWidget(_sep1)
        resize_section_lbl = QLabel("RESIZE COVER")
        resize_section_lbl.setObjectName("sectiontitle")
        cov_v.addWidget(resize_section_lbl)

        sz_row = QHBoxLayout(); sz_row.setSpacing(6)
        sz_lbl = QLabel("Max size:")
        sz_lbl.setStyleSheet("color:rgba(255,255,255,0.55);font-size:12px;background:transparent;")
        self._resize_spin = QSpinBox()
        self._resize_spin.setRange(50, 2000)
        self._resize_spin.setSingleStep(50)
        self._resize_spin.setValue(500)
        self._resize_spin.setSuffix(" px")
        self._resize_spin.setFixedHeight(28)
        self._resize_spin.setToolTip("Target maximum dimension. Images already smaller are skipped.")
        sz_row.addWidget(sz_lbl)
        sz_row.addWidget(self._resize_spin)
        cov_v.addLayout(sz_row)

        resize_one_btn = QPushButton("Apply to This File")
        resize_one_btn.setObjectName("ghost")
        resize_one_btn.setToolTip("Resize the cover in the currently selected file")
        resize_one_btn.clicked.connect(self._resize_cover)
        cov_v.addWidget(resize_one_btn)

        self._resize_sel_btn = QPushButton("Apply to Selected Files")
        self._resize_sel_btn.setObjectName("ghost")
        self._resize_sel_btn.setToolTip("Resize embedded covers in the currently selected files")
        self._resize_sel_btn.clicked.connect(self._bulk_resize_selected)
        cov_v.addWidget(self._resize_sel_btn)

        self._resize_all_btn = QPushButton("Apply to All Files")
        self._resize_all_btn.setObjectName("ghost")
        self._resize_all_btn.setToolTip("Resize embedded covers across every file in the list")
        self._resize_all_btn.clicked.connect(self._bulk_resize_covers)
        cov_v.addWidget(self._resize_all_btn)

        self._revert_cover_btn = QPushButton("Revert Cover")
        self._revert_cover_btn.setObjectName("ghost")
        self._revert_cover_btn.setToolTip("Restore the cover to what it was before the last resize (single file only)")
        self._revert_cover_btn.setEnabled(False)
        self._revert_cover_btn.clicked.connect(self._revert_cover)
        cov_v.addWidget(self._revert_cover_btn)

        self._revert_all_btn = QPushButton("Revert All Covers")
        self._revert_all_btn.setObjectName("ghost")
        self._revert_all_btn.setToolTip("Restore all covers that were changed by the last bulk resize")
        self._revert_all_btn.setEnabled(False)
        self._revert_all_btn.clicked.connect(self._revert_bulk_covers)
        cov_v.addWidget(self._revert_all_btn)

        self._resize_progress = QProgressBar()
        self._resize_progress.setRange(0, 100)
        self._resize_progress.setFixedHeight(4)
        self._resize_progress.setTextVisible(False)
        self._resize_progress.setVisible(False)
        cov_v.addWidget(self._resize_progress)
        self._cancel_resize_btn = QPushButton("✕ Cancel Resize")
        self._cancel_resize_btn.setObjectName("danger")
        self._cancel_resize_btn.setVisible(False)
        self._cancel_resize_btn.clicked.connect(self._cancel_bulk_resize)
        cov_v.addWidget(self._cancel_resize_btn)

        # ── Verify Integrity ──────────────────────────────────
        _sep2 = QFrame(); _sep2.setFrameShape(QFrame.Shape.HLine)
        _sep2.setStyleSheet("background:rgba(255,255,255,0.09);max-height:1px;")
        cov_v.addWidget(_sep2)
        verify_lbl = QLabel("VERIFY INTEGRITY")
        verify_lbl.setObjectName("sectiontitle")
        cov_v.addWidget(verify_lbl)

        verify_one_btn = QPushButton("Verify This File")
        verify_one_btn.setObjectName("ghost")
        verify_one_btn.setToolTip(
            "Fully decode the audio stream and report any errors (uses ffmpeg).\n"
            "FLAC files are also checked against their built-in MD5 checksum."
        )
        verify_one_btn.clicked.connect(self._verify_current)
        cov_v.addWidget(verify_one_btn)

        self._verify_all_btn = QPushButton("Verify All Files")
        self._verify_all_btn.setObjectName("ghost")
        self._verify_all_btn.setToolTip("Check every file in the list for corruption")
        self._verify_all_btn.clicked.connect(self._verify_all)
        cov_v.addWidget(self._verify_all_btn)

        self._verify_cancel_btn = QPushButton("Cancel Verify")
        self._verify_cancel_btn.setObjectName("ghost")
        self._verify_cancel_btn.setVisible(False)
        self._verify_cancel_btn.clicked.connect(self._cancel_verify)
        cov_v.addWidget(self._verify_cancel_btn)

        self._verify_progress = QProgressBar()
        self._verify_progress.setRange(0, 100)
        self._verify_progress.setFixedHeight(4)
        self._verify_progress.setTextVisible(False)
        self._verify_progress.setVisible(False)
        cov_v.addWidget(self._verify_progress)

        # ── ReplayGain ────────────────────────────────────────
        _sep3 = QFrame(); _sep3.setFrameShape(QFrame.Shape.HLine)
        _sep3.setStyleSheet("background:rgba(255,255,255,0.09);max-height:1px;")
        cov_v.addWidget(_sep3)
        rg_lbl = QLabel("REPLAYGAIN")
        rg_lbl.setObjectName("sectiontitle")
        cov_v.addWidget(rg_lbl)

        self._rg_strip_btn = QPushButton("Strip ReplayGain Tags")
        self._rg_strip_btn.setObjectName("danger")
        self._rg_strip_btn.setToolTip(
            "Remove all ReplayGain tags from every file in the list.\n"
            "Uses mutagen — no external tools required."
        )
        self._rg_strip_btn.clicked.connect(self._rg_strip)
        cov_v.addWidget(self._rg_strip_btn)

        self._rg_progress = QProgressBar()
        self._rg_progress.setRange(0, 100)
        self._rg_progress.setFixedHeight(4)
        self._rg_progress.setTextVisible(False)
        self._rg_progress.setVisible(False)
        cov_v.addWidget(self._rg_progress)

        # ── File Details ──────────────────────────────────────
        _sep4 = QFrame(); _sep4.setFrameShape(QFrame.Shape.HLine)
        _sep4.setStyleSheet("background:rgba(255,255,255,0.09);max-height:1px;")
        cov_v.addWidget(_sep4)
        info_section = QLabel("FILE DETAILS")
        info_section.setObjectName("sectiontitle")
        cov_v.addWidget(info_section)
        self._file_info_lbl = QLabel("—")
        self._file_info_lbl.setObjectName("muted")
        self._file_info_lbl.setWordWrap(True)
        cov_v.addWidget(self._file_info_lbl)

        cov_v.addStretch()

        right_scroll.setWidget(cover_panel)
        right_outer_v.addWidget(right_scroll)
        body.addWidget(right_outer)
        body.setSizes([200, 700, 224])
        body.setCollapsible(0, False)
        body.setCollapsible(1, False)
        body.setCollapsible(2, False)
        file_panel.setMinimumWidth(150)
        fields_panel.setMinimumWidth(300)

    # ─────────────────────────────────────────────────────────
    #  LOG HELPERS (unchanged from original)
    # ─────────────────────────────────────────────────────────

    def _log(self, msg: str, level: str = "info", operation: str = ""):
        t = _current_theme
        ts = datetime.now().strftime("%H:%M:%S")
        if not operation:
            low = msg.lower()
            if "save" in low or "saved" in low:               operation = "Save Tags"
            elif "strip" in low or "stripped" in low:         operation = "Strip RG"
            elif "no rg" in low:                              operation = "Strip RG"
            elif "resized cover" in low or "resize" in low:   operation = "Resize Cover"
            elif "cover" in low:                              operation = "Cover"
            elif "ok:" in low or "verify" in low:             operation = "Verify"
            elif "rename" in low:                             operation = "Rename"
            elif "lookup" in low:                             operation = "Info"
            elif "cluster" in low:                            operation = "Cluster"
            elif "batch" in low:                              operation = "Batch Ops"
            elif "error:" in low:                             operation = "Verify"
            else:                                             operation = "Info"

        dot       = {"ok": "✓", "warn": "⚠", "error": "✗", "info": "·"}.get(level, "·")
        dot_color = {"ok": t["success"], "warn": t["warning"],
                     "error": t["danger"], "info": t["txt2"]}.get(level, t["txt2"])
        op_color  = dot_color if level != "info" else t["txt1"]

        filename = ""; detail = msg
        for sep in (" — ", ": "):
            parts = msg.split(sep, 1)
            if len(parts) == 2 and len(parts[0]) < 60:
                filename = parts[0]; detail = parts[1]; break

        tbl = self._log_table
        row = tbl.rowCount()
        tbl.insertRow(row)

        ts_item = QTableWidgetItem(ts)
        ts_item.setForeground(QColor(t["txt2"]))
        ts_item.setData(Qt.ItemDataRole.UserRole, level)
        tbl.setItem(row, 0, ts_item)

        dot_item = QTableWidgetItem(dot)
        dot_item.setForeground(QColor(dot_color))
        dot_item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
        tbl.setItem(row, 1, dot_item)

        op_item = QTableWidgetItem(operation)
        op_item.setForeground(QColor(op_color))
        tbl.setItem(row, 2, op_item)

        file_item = QTableWidgetItem(filename or msg[:60])
        file_item.setForeground(QColor(t["txt0"]))
        file_item.setToolTip(msg)
        tbl.setItem(row, 3, file_item)

        detail_item = QTableWidgetItem(detail if filename else "")
        detail_item.setForeground(QColor(t["txt2"]))
        detail_item.setToolTip(msg)
        tbl.setItem(row, 4, detail_item)

        self._apply_tag_log_filter_row(row, level)
        self._update_log_stats()
        if level == "error" and self._centre_stack.currentIndex() != 1:
            self._switch_to_log()
        tbl.scrollToBottom()

    def _apply_tag_log_filter_row(self, row: int, level: str):
        f = self._log_filter_combo.currentText()
        hide = (f == "Errors only" and level not in ("error", "warn")) or \
               (f == "OK only"     and level not in ("ok", "info"))
        self._log_table.setRowHidden(row, hide)

    def _apply_tag_log_filter(self):
        tbl = self._log_table
        for row in range(tbl.rowCount()):
            item = tbl.item(row, 0)
            level = item.data(Qt.ItemDataRole.UserRole) if item else "info"
            self._apply_tag_log_filter_row(row, level)
        self._update_log_stats()

    def _update_log_stats(self):
        tbl   = self._log_table
        total = tbl.rowCount()
        ok    = sum(1 for r in range(total)
                    if tbl.item(r, 0) and tbl.item(r, 0).data(Qt.ItemDataRole.UserRole) == "ok")
        errs  = sum(1 for r in range(total)
                    if tbl.item(r, 0) and tbl.item(r, 0).data(Qt.ItemDataRole.UserRole) == "error")
        warns = sum(1 for r in range(total)
                    if tbl.item(r, 0) and tbl.item(r, 0).data(Qt.ItemDataRole.UserRole) == "warn")
        self._log_count_lbl.setText(
            f"{total} entr{'y' if total == 1 else 'ies'}"
            + (f"  ·  {ok} ok"       if ok    else "")
            + (f"  ·  {warns} warn"  if warns else "")
            + (f"  ·  {errs} errors" if errs  else "")
        )
        self._log_stats_bar.setText(
            f"Total: {total}   ✓ {ok}   ⚠ {warns}   ✗ {errs}" if total else ""
        )
        self._tab_log_btn.setText(
            f"Log  ({total})" if not errs else f"Log  ({total})  ✗{errs}"
        )

    def _clear_tag_log(self):
        self._log_table.setRowCount(0)
        self._log_count_lbl.setText("No entries yet")
        self._log_stats_bar.setText("")
        self._tab_log_btn.setText("Log  (0)")

    # ─────────────────────────────────────────────────────────
    #  FILE LOADING (unchanged + missing-tag highlight)
    # ─────────────────────────────────────────────────────────

    def _open_folder(self):
        d = QFileDialog.getExistingDirectory(self, "Open Music Folder", str(Path.home()))
        if not d:
            return
        self._loaded_folder = d
        exts = {".flac", ".mp3", ".m4a", ".ogg", ".opus", ".aac", ".wma", ".wav", ".aiff"}
        self._files = sorted(p for p in Path(d).rglob("*") if p.suffix.lower() in exts)
        self._cluster_multi_paths = []
        self._populate_file_list()
        if self._cluster_mode:
            self._build_clusters()
        elif self._files:
            self._file_list.setCurrentRow(0)

    def _open_file(self):
        exts = "Audio files (*.flac *.mp3 *.m4a *.ogg *.opus *.aac *.wma *.wav *.aiff);;All files (*)"
        paths, _ = QFileDialog.getOpenFileNames(self, "Open Audio File(s)", str(Path.home()), exts)
        if not paths:
            return
        new_files = [Path(p) for p in paths]
        existing  = set(self._files)
        added     = [p for p in new_files if p not in existing]
        self._files = sorted(self._files + added)
        self._populate_file_list()
        if self._cluster_mode:
            self._build_clusters()
        elif added:
            idx = self._files.index(added[0])
            self._file_list.setCurrentRow(idx)

    def _populate_file_list(self):
        """Rebuild the list widget instantly with filenames."""
        self._file_list.blockSignals(True)
        self._file_list.clear()
        self._file_count_lbl.setText(f"{len(self._files)} files" if self._files else "No folder loaded")
        for path in self._files:
            self._file_list.addItem(path.name)
        self._file_list.blockSignals(False)

    def _filter_file_list(self, text: str):
        """Show only files whose name contains the search text (case-insensitive)."""
        text = text.strip().lower()
        self._file_list.blockSignals(True)
        for i in range(self._file_list.count()):
            item = self._file_list.item(i)
            item.setHidden(bool(text) and text not in item.text().lower())
        self._file_list.blockSignals(False)

    def _select_all(self):
        """Select all items in whichever list mode is active."""
        if self._cluster_mode:
            # Select all leaf (track) items in the tree
            root = self._cluster_tree.invisibleRootItem()
            self._cluster_tree.blockSignals(True)
            self._cluster_tree.clearSelection()
            for i in range(root.childCount()):
                album_item = root.child(i)
                for j in range(album_item.childCount()):
                    album_item.child(j).setSelected(True)
            self._cluster_tree.blockSignals(False)
            self._on_cluster_selection_changed()
        else:
            self._file_list.selectAll()

    def _file_list_key_press(self, event):
        if event.key() in (Qt.Key.Key_Delete, Qt.Key.Key_Backspace):
            self._remove_selected_files()
        else:
            QListWidget.keyPressEvent(self._file_list, event)

    def _cluster_tree_key_press(self, event):
        if event.key() in (Qt.Key.Key_Delete, Qt.Key.Key_Backspace):
            self._remove_selected_files()
        else:
            QTreeWidget.keyPressEvent(self._cluster_tree, event)

    def _remove_selected_files(self):
        if self._cluster_mode:
            # In cluster mode: remove selected leaf paths from self._files, then rebuild
            to_remove = {
                item.data(0, Qt.ItemDataRole.UserRole)
                for item in self._cluster_tree.selectedItems()
                if item.data(0, Qt.ItemDataRole.UserRole) is not None
            }
            if not to_remove:
                return
            self._files = [p for p in self._files if p not in to_remove]
            self._cluster_multi_paths = [p for p in self._cluster_multi_paths if p not in to_remove]
            self._file_count_lbl.setText(
                f"{len(self._files)} files" if self._files else "No folder loaded"
            )
            self._populate_file_list()
            if self._files:
                self._build_clusters()
            else:
                self._cluster_tree.clear()
                self._clusters = []
                self._current = None
                self._file_label.setText("Select a file to edit")
                self._save_btn.setEnabled(False)
                self._save_all_btn.setEnabled(False)
                for edit in self._tag_edits.values():
                    edit.clear(); edit.setPlaceholderText("")
                self._cover_lbl.setPixmap(QPixmap()); self._cover_lbl.setText("No Cover")
                self._cover_data = None; self._cover_info_lbl.setText("")
                self._status_bar.setText("Ready")
            return
        selected_rows = sorted(
            [self._file_list.row(it) for it in self._file_list.selectedItems()],
            reverse=True
        )
        for row in selected_rows:
            if 0 <= row < len(self._files):
                self._files.pop(row)
                self._file_list.takeItem(row)
        self._file_count_lbl.setText(
            f"{len(self._files)} files" if self._files else "No folder loaded"
        )
        if not self._files:
            self._current = None
            self._file_label.setText("Select a file to edit")
            self._save_btn.setEnabled(False)
            self._save_all_btn.setEnabled(False)
            for edit in self._tag_edits.values():
                edit.clear(); edit.setPlaceholderText("")
            self._cover_lbl.setPixmap(QPixmap()); self._cover_lbl.setText("No Cover")
            self._cover_data = None; self._cover_info_lbl.setText("")
            self._file_info_lbl.setText("—")
            for attr in ("_info_format","_info_bitrate","_info_samplerate",
                         "_info_channels","_info_duration","_info_filesize"):
                lbl = getattr(self, attr, None)
                if lbl: lbl.setText("—")
            self._status_bar.setText("Ready")

    def _reload_file_list(self):
        """Re-scan the original loaded folder and rebuild the list from scratch."""
        folder = getattr(self, "_loaded_folder", None)
        if not folder:
            return
        exts = {".flac", ".mp3", ".m4a", ".ogg", ".opus", ".aac", ".wma", ".wav", ".aiff"}
        self._files = sorted(p for p in Path(folder).rglob("*") if p.suffix.lower() in exts)
        self._cluster_multi_paths = []
        self._populate_file_list()
        self._status_bar.setText(f"Reloaded — {len(self._files)} files")

    def _clear_file_list(self):
        self._files = []
        self._file_list.clear()
        # If cluster mode is on, untoggle it — that cleans up the tree and resets state.
        # Block signals so _toggle_cluster_mode doesn't try to rebuild with empty file list.
        if self._cluster_mode:
            self._cluster_btn.blockSignals(True)
            self._cluster_btn.setChecked(False)
            self._cluster_btn.blockSignals(False)
            self._cluster_btn.setText("⊞ Cluster")
            self._cluster_mode = False
            self._cluster_tree.clear()
            self._clusters = []
            self._cluster_multi_paths = []
            self._left_stack.setCurrentIndex(0)
        self._file_count_lbl.setText("No folder loaded")
        self._current = None
        self._file_label.setText("Select a file to edit")
        self._save_btn.setEnabled(False)
        self._save_all_btn.setEnabled(False)
        for edit in self._tag_edits.values():
            edit.clear(); edit.setPlaceholderText("")
        self._cover_lbl.setPixmap(QPixmap()); self._cover_lbl.setText("No Cover")
        self._cover_data = None; self._cover_info_lbl.setText("")
        self._file_info_lbl.setText("—")
        for attr in ("_info_format","_info_bitrate","_info_samplerate",
                     "_info_channels","_info_duration","_info_filesize"):
            lbl = getattr(self, attr, None)
            if lbl: lbl.setText("—")
        self._status_bar.setText("Ready")
        self._orig_vals = {}
        self._clear_diff_highlights()

    # ─────────────────────────────────────────────────────────
    #  CLUSTER MODE
    # ─────────────────────────────────────────────────────────

    def _toggle_cluster_mode(self, checked: bool):
        self._cluster_mode = checked
        self._cluster_multi_paths = []
        if checked:
            self._left_stack.setCurrentIndex(1)
            self._cluster_btn.setText("⋟ Cluster")
            self._build_clusters()
        else:
            self._left_stack.setCurrentIndex(0)
            self._cluster_btn.setText("⋞ Cluster")
            self._save_all_btn.setEnabled(False)
            self._save_all_btn.setText("💾  Save to All Selected")
            # Clear the cluster tree so stale data doesn't linger
            self._cluster_tree.clear()
            self._clusters = []
            # Re-sync the right panel to whatever is selected in the flat list
            selected = self._file_list.selectedItems()
            if len(selected) == 1:
                row = self._file_list.row(selected[0])
                if 0 <= row < len(self._files):
                    self._current = self._files[row]
                    self._load_tags(self._current)
                    return
            # Nothing selected or multi-selected: clear fields
            self._current = None
            self._file_label.setText("Select a file to edit")
            self._save_btn.setEnabled(False)
            for edit in self._tag_edits.values():
                edit.clear(); edit.setPlaceholderText("")
            self._orig_vals = {}
            self._clear_diff_highlights()
            self._cover_lbl.setPixmap(QPixmap()); self._cover_lbl.setText("No Cover")
            self._cover_data = None; self._cover_info_lbl.setText("")
            self._file_info_lbl.setText("—")
            for attr in ("_info_format","_info_bitrate","_info_samplerate",
                         "_info_channels","_info_duration","_info_filesize"):
                lbl = getattr(self, attr, None)
                if lbl: lbl.setText("—")
            self._status_bar.setText("Ready")

    def _build_clusters(self):
        if not self._files:
            return
        self._cluster_tree.clear()
        self._status_bar.setText("Clustering…")
        self._cancel_cluster_btn.setVisible(True)
        worker = _MbClusterWorker(list(self._files))
        worker.clusters_ready.connect(self._on_clusters_ready)
        worker.clusters_ready.connect(worker.deleteLater)
        self._cluster_worker = worker
        worker.start()

    def _cancel_clustering(self):
        w = getattr(self, "_cluster_worker", None)
        if w and w.isRunning():
            w.requestInterruption()
            w.wait(500)
        self._cancel_cluster_btn.setVisible(False)
        self._cluster_btn.blockSignals(True)
        self._cluster_btn.setChecked(False)
        self._cluster_btn.setText("⊞ Cluster")
        self._cluster_btn.blockSignals(False)
        self._cluster_mode = False
        self._left_stack.setCurrentIndex(0)
        self._status_bar.setText("Clustering cancelled.")

    def _on_clusters_ready(self, clusters: list):
        self._cancel_cluster_btn.setVisible(False)
        self._clusters = clusters
        tree = self._cluster_tree
        tree.clear()
        for bucket in clusters:
            parent = QTreeWidgetItem([f"📀  {bucket['key']}  ({len(bucket['files'])})"])
            parent.setData(0, Qt.ItemDataRole.UserRole, None)
            for path in bucket["files"]:
                child = QTreeWidgetItem([path.name])
                child.setData(0, Qt.ItemDataRole.UserRole, path)
                parent.addChild(child)
            tree.addTopLevelItem(parent)
        tree.expandAll()
        self._status_bar.setText(f"Clustered into {len(clusters)} albums.")
        self._log(f"Clustered {len(self._files)} files → {len(clusters)} albums.", "ok", "Cluster")

    def _on_cluster_item_clicked(self, item: QTreeWidgetItem, col: int):
        """Single click: load tags for a track; clicking an album node selects all its tracks."""
        item_path = item.data(0, Qt.ItemDataRole.UserRole)
        if item_path is None:
            # Album parent node clicked - select all child tracks
            child_count = item.childCount()
            if child_count == 0:
                return
            self._cluster_tree.blockSignals(True)
            self._cluster_tree.clearSelection()
            for i in range(child_count):
                item.child(i).setSelected(True)
            self._cluster_tree.blockSignals(False)
            self._on_cluster_selection_changed()
            return
        # Leaf track: if multiple tracks already selected let selection handler keep the view
        leaf_selected = [
            i for i in self._cluster_tree.selectedItems()
            if i.data(0, Qt.ItemDataRole.UserRole) is not None
        ]
        if len(leaf_selected) > 1:
            return
        self._current = item_path
        self._load_tags(item_path)

    def _on_cluster_selection_changed(self):
        """Multi-select in cluster tree: update save-all button + populate multi-field view."""
        selected = self._cluster_tree.selectedItems()
        # Only count leaf items (tracks), not album parent nodes
        leaf_paths = [
            item.data(0, Qt.ItemDataRole.UserRole)
            for item in selected
            if item.data(0, Qt.ItemDataRole.UserRole) is not None
        ]
        n = len(leaf_paths)
        if n == 0:
            self._cluster_multi_paths = []
            self._save_all_btn.setEnabled(False)
            self._save_all_btn.setText("\U0001f4be  Save to All Selected")
            return
        if n == 1:
            # Single track: normal single-file editing mode
            self._save_all_btn.setEnabled(False)
            self._save_all_btn.setText("\U0001f4be  Save to All Selected")
            self._current = leaf_paths[0]
            self._load_tags(leaf_paths[0])
            return
        # Multiple tracks selected: show aggregate field view
        self._save_all_btn.setEnabled(True)
        self._save_all_btn.setText(f"\U0001f4be  Save to {n} Files")
        self._file_label.setText(f"{n} files selected")
        self._save_btn.setEnabled(False)
        # Populate fields with shared values (same logic as flat-list multi-select)
        try:
            import mutagen
        except ImportError:
            return
        all_vals: dict[str, list[str]] = {key: [] for key, _ in self._FIELDS}
        for path in leaf_paths:
            try:
                audio = mutagen.File(str(path), easy=True)
                if audio is None:
                    continue
                for key, _ in self._FIELDS:
                    val = audio.tags.get(key) if audio.tags else None
                    if isinstance(val, list):
                        text = str(val[0]).strip() if val else ""
                    elif val is not None:
                        text = str(val).strip()
                    else:
                        text = ""
                    all_vals[key].append(text)
            except Exception:
                pass
        # Override save_tags_multi to use cluster selection
        self._loading_multi = True
        self._clear_diff_highlights()
        for key, _ in self._FIELDS:
            vals = all_vals[key]
            edit = self._tag_edits[key]
            if not vals:
                edit.clear(); edit.setPlaceholderText("—")
                continue
            non_empty = [v for v in vals if v]
            unique_ne = set(non_empty)
            if len(unique_ne) <= 1:
                edit.setText(next(iter(unique_ne), "")); edit.setPlaceholderText("")
            else:
                edit.clear(); edit.setPlaceholderText("— multiple values —")
        self._loading_multi = False
        self._cluster_multi_paths = leaf_paths
        self._status_bar.setText(
            f"{n} files selected  ·  Matching values shown  ·  "
            "Blank fields keep existing values on save"
        )

    # ─────────────────────────────────────────────────────────
    #  TAG LOADING + DIFF HIGHLIGHT
    # ─────────────────────────────────────────────────────────

    def _on_file_selected(self, row: int):
        if row < 0 or row >= len(self._files):
            return
        # If multiple items are selected, let _on_selection_settled handle display
        if len(self._file_list.selectedItems()) > 1:
            return
        self._current = self._files[row]
        self._load_tags(self._current)

    def _load_tags(self, path: Path):
        for edit in self._tag_edits.values():
            edit.clear()
        self._cover_lbl.setText("No Cover")
        self._cover_lbl.setPixmap(QPixmap())
        self._cover_data = None
        self._cover_info_lbl.setText("")
        self._orig_vals = {}
        self._clear_diff_highlights()

        self._file_label.setText(path.name)
        self._save_btn.setEnabled(True)

        self._cover_data_before_resize = None
        if hasattr(self, "_revert_cover_btn"):
            self._revert_cover_btn.setEnabled(False)

        if not path.exists():
            self._status_bar.setText(f"File not found (deleted?): {path.name}")
            self._save_btn.setEnabled(False)
            return
        size_mb = path.stat().st_size / 1048576
        self._file_info_lbl.setText(f"{path.suffix.upper()[1:]}  ·  {size_mb:.1f} MB\n{path.parent.name}")

        try:
            import mutagen
            audio = mutagen.File(str(path), easy=True)
            if audio is None:
                self._status_bar.setText(f"Could not read tags from {path.name}")
                return

            # Fill tag fields + store originals
            for key, _ in self._FIELDS:
                val  = audio.tags.get(key) if audio.tags else None
                text = str(val[0]) if isinstance(val, list) and val else str(val) if val else ""
                self._tag_edits[key].setText(text)
                self._orig_vals[key] = text
                self._orig_lbls[key].setText(text if text else "—")

            # Technical info
            info = audio.info
            self._info_duration.setText(_fmt_dur(int(getattr(info, "length", 0))))
            br = getattr(info, "bitrate", 0)
            self._info_bitrate.setText(f"{br//1000} kbps" if br else "—")
            sr = getattr(info, "sample_rate", 0)
            self._info_samplerate.setText(f"{sr:,} Hz" if sr else "—")
            ch = getattr(info, "channels", 0)
            self._info_channels.setText(str(ch) if ch else "—")
            self._info_filesize.setText(f"{size_mb:.2f} MB")
            self._info_format.setText(type(info).__name__)

            # Cover art
            audio_raw = mutagen.File(str(path))
            cover_data = self._extract_cover(audio_raw, path.suffix.lower())
            if cover_data:
                self._cover_data = cover_data
                self._show_cover(cover_data)

            self._status_bar.setText(f"Loaded: {path.name}")
        except ImportError:
            self._status_bar.setText("mutagen not installed — install it for full tag editing")
        except Exception as e:
            self._status_bar.setText(f"Error: {e}")

    def _ctrl_s_save(self):
        """Ctrl+S: save multi if multiple selected, else save single."""
        if self._save_all_btn.isEnabled():
            self._save_tags_multi()
        elif self._save_btn.isEnabled():
            self._save_tags()

    def _on_field_changed(self, key: str, text: str):
        """Highlight fields that differ from the on-disk value."""
        # Skip highlights while populating multi-select or cluster aggregate view
        if getattr(self, '_loading_multi', False):
            return
        t      = _current_theme
        orig   = self._orig_vals.get(key, "")
        edit   = self._tag_edits[key]
        if text != orig:
            edit.setStyleSheet(
                f"background:rgba(255,255,255,0.08);border:1px solid {t['accent']};"
                f"border-radius:4px;padding:8px 11px;color:rgba(255,255,255,0.87);"
            )
        else:
            edit.setStyleSheet("")   # revert to stylesheet default

    def _clear_diff_highlights(self):
        for edit in self._tag_edits.values():
            edit.setStyleSheet("")

    # ─────────────────────────────────────────────────────────
    #  SAVE TAGS (unchanged)
    # ─────────────────────────────────────────────────────────

    def _save_tags(self):
        if not self._current:
            return
        try:
            import mutagen
            audio = mutagen.File(str(self._current), easy=True)
            if audio is None:
                QMessageBox.warning(self, "Error", "Could not open file for writing.")
                return
            if audio.tags is None:
                audio.add_tags()
            for key, _ in self._FIELDS:
                edit = self._tag_edits[key]
                val  = edit.text().strip()
                if val:
                    audio.tags[key] = [val]
                elif key in (audio.tags or {}):
                    del audio.tags[key]
            audio.save()
            # Update orig values + clear highlights
            for key, _ in self._FIELDS:
                self._orig_vals[key] = self._tag_edits[key].text().strip()
                self._orig_lbls[key].setText(self._orig_vals[key] or "—")
            self._clear_diff_highlights()
            self._status_bar.setText(f"✓ Saved: {self._current.name}")
            self._log(f"Saved tags: {self._current.name}", "ok")
            # Refresh file list item display name
            self._refresh_file_list_item(self._current)
        except ImportError:
            QMessageBox.warning(self, "mutagen required",
                "Please install mutagen:\n\n  pip install mutagen")
        except Exception as e:
            self._log(f"Save error — {self._current.name}: {e}", "error")
            QMessageBox.critical(self, "Save Error", str(e))

    def _refresh_file_list_item(self, path: Path):
        """Update a single list item's display name after a save."""
        try:
            idx = self._files.index(path)
        except ValueError:
            return
        item = self._file_list.item(idx)
        if item is None:
            return
        item.setText(path.name)
        item.setForeground(QColor(_current_theme["txt0"]))

    def _on_selection_changed(self):
        if not hasattr(self, '_sel_timer'):
            self._sel_timer = QTimer()
            self._sel_timer.setSingleShot(True)
            self._sel_timer.timeout.connect(self._on_selection_settled)
        self._sel_timer.start(120)

    def _on_selection_settled(self):
        selected = self._file_list.selectedItems()
        n = len(selected)
        if n <= 1:
            self._save_all_btn.setEnabled(False)
            self._save_all_btn.setText("💾  Save to All Selected")
            if n == 0:
                self._current = None
                self._file_label.setText("Select a file to edit")
                self._save_btn.setEnabled(False)
            return

        self._save_all_btn.setEnabled(True)
        self._save_all_btn.setText(f"💾  Save to {n} Files")
        self._file_label.setText(f"{n} files selected")
        self._save_btn.setEnabled(False)

        MAX_TAG_READ = 200
        if n > MAX_TAG_READ:
            for edit in self._tag_edits.values():
                edit.clear()
                edit.setPlaceholderText("— too many files to compare —")
            self._status_bar.setText(
                f"{n} files selected  ·  Tags not shown for selections > {MAX_TAG_READ} files  ·  "
                "Filled fields will be written to all selected files on save"
            )
            return

        try:
            import mutagen
        except ImportError:
            self._status_bar.setText("mutagen not installed")
            return

        selected_rows  = [self._file_list.row(it) for it in selected]
        selected_paths = [self._files[r] for r in selected_rows if 0 <= r < len(self._files)]
        if not selected_paths:
            return

        all_vals: dict[str, list[str]] = {key: [] for key, _ in self._FIELDS}
        for path in selected_paths:
            try:
                audio = mutagen.File(str(path), easy=True)
                if audio is None:
                    continue
                for key, _ in self._FIELDS:
                    val  = audio.tags.get(key) if audio.tags else None
                    if isinstance(val, list):
                        text = str(val[0]).strip() if val else ""
                    elif val is not None:
                        text = str(val).strip()
                    else:
                        text = ""
                    all_vals[key].append(text)
            except Exception:
                pass

        self._loading_multi = True
        self._clear_diff_highlights()
        for key, _ in self._FIELDS:
            vals = all_vals[key]
            edit = self._tag_edits[key]
            if not vals:
                edit.clear(); edit.setPlaceholderText("—")
                continue
            non_empty       = [v for v in vals if v]
            unique_non_empty = set(non_empty)
            if len(unique_non_empty) <= 1:
                common = next(iter(unique_non_empty), "")
                edit.setText(common); edit.setPlaceholderText("")
            else:
                edit.clear(); edit.setPlaceholderText("— multiple values —")
        self._loading_multi = False

        self._status_bar.setText(
            f"{n} files selected  ·  Matching values shown  ·  "
            "Blank fields keep existing values on save"
        )

    def _save_tags_multi(self):
        # Support both flat-list multi-select and cluster-tree multi-select
        cluster_paths = getattr(self, '_cluster_multi_paths', None)
        if self._cluster_mode and cluster_paths:
            selected_paths = cluster_paths
        else:
            selected = self._file_list.selectedItems()
            if not selected:
                return
            selected_rows  = [self._file_list.row(it) for it in selected]
            selected_paths = [self._files[r] for r in selected_rows if 0 <= r < len(self._files)]
        if not selected_paths:
            return

        fields_to_write: dict[str, str] = {}
        fields_to_delete: list[str]     = []
        for key, _ in self._FIELDS:
            edit        = self._tag_edits[key]
            val         = edit.text().strip()
            placeholder = edit.placeholderText()
            if "multiple values" in placeholder and not val:
                continue
            if val:
                fields_to_write[key] = val
            else:
                fields_to_delete.append(key)

        if not fields_to_write and not fields_to_delete:
            self._status_bar.setText("No fields to write (all blank/unchanged)")
            return

        try:
            import mutagen
        except ImportError:
            QMessageBox.warning(self, "mutagen required", "pip install mutagen")
            return

        saved_count = 0
        errors      = []
        for path in selected_paths:
            try:
                audio = mutagen.File(str(path), easy=True)
                if audio is None:
                    errors.append(path.name); continue
                if audio.tags is None:
                    audio.add_tags()
                for key, val in fields_to_write.items():
                    audio.tags[key] = [val]
                for key in fields_to_delete:
                    if key in (audio.tags or {}):
                        del audio.tags[key]
                audio.save()
                saved_count += 1
                self._refresh_file_list_item(path)
            except Exception as e:
                errors.append(f"{path.name}: {e}")

        if errors:
            self._status_bar.setText(f"✓ Saved {saved_count} files  ·  {len(errors)} errors")
            self._log(f"Saved {saved_count} files. Errors ({len(errors)}):", "warn")
            for e in errors:
                self._log(f"  {e}", "error")
        else:
            self._status_bar.setText(f"✓ Saved {saved_count} files")
            self._log(f"Saved tags to {saved_count} files.", "ok")

    # ─────────────────────────────────────────────────────────
    #  BATCH TAG OPERATIONS
    # ─────────────────────────────────────────────────────────

    def _show_batch_ops(self):
        if self._cluster_mode:
            cluster_paths = getattr(self, '_cluster_multi_paths', [])
            selected_paths = list(cluster_paths) if cluster_paths else (
                [self._current] if self._current else []
            )
        else:
            selected = self._file_list.selectedItems()
            selected_rows  = [self._file_list.row(it) for it in selected]
            selected_paths = [self._files[r] for r in selected_rows if 0 <= r < len(self._files)]
        targets = selected_paths or list(self._files)
        if not targets:
            QMessageBox.information(self, "Batch Ops", "No files loaded.")
            return

        t   = _current_theme
        dlg = QDialog(self)
        dlg.setWindowTitle(f"Batch Tag Operations  ({len(targets)} files)")
        dlg.setMinimumWidth(400)
        dlg.setStyleSheet("background:rgba(0,0,0,0.35);color:rgba(255,255,255,0.85);")
        v = QVBoxLayout(dlg)
        v.setContentsMargins(20, 16, 20, 16)
        v.setSpacing(8)

        scope_lbl = QLabel(
            f"Applies to: {'selected' if selected_paths else 'all'} "
            f"{len(targets)} file(s)"
        )
        scope_lbl.setStyleSheet("color:rgba(255,255,255,0.35);font-size:11px;background:transparent;")
        v.addWidget(scope_lbl)

        ops = [
            ("capitalize_titles",  "Capitalize Title (Title Case)"),
            ("capitalize_artists", "Capitalize Artist & Album Artist"),
            ("trim_whitespace",    "Trim Whitespace (all fields)"),
            ("swap_artist_aa",     "Swap Artist ↔ Album Artist"),
            ("copy_artist_to_aa",  "Copy Artist → Album Artist (if AA empty)"),
            ("strip_track_total",  "Strip track total  (e.g. '3/12' → '3')"),
            ("strip_disc_total",   "Strip disc total   (e.g. '1/2' → '1')"),
            ("lowercase_genre",    "Lowercase Genre"),
            ("titlecase_genre",    "Title Case Genre"),
        ]
        checks = {}
        for key, label in ops:
            cb = QCheckBox(label)
            cb.setStyleSheet("color:rgba(255,255,255,0.55);background:transparent;")
            v.addWidget(cb)
            checks[key] = cb

        btn_row = QHBoxLayout()
        run_btn    = QPushButton("Run")
        run_btn.setObjectName("toggle")
        cancel_btn = QPushButton("Cancel")
        cancel_btn.setObjectName("ghost")
        btn_row.addStretch()
        btn_row.addWidget(cancel_btn)
        btn_row.addWidget(run_btn)
        v.addLayout(btn_row)

        def _run():
            selected_ops = {k for k, cb in checks.items() if cb.isChecked()}
            if not selected_ops:
                QMessageBox.information(dlg, "Batch Ops", "Select at least one operation.")
                return
            dlg.accept()
            self._run_batch_ops(targets, selected_ops)

        run_btn.clicked.connect(_run)
        cancel_btn.clicked.connect(dlg.reject)
        dlg.exec()

    def _run_batch_ops(self, files: list, ops: set):
        try:
            import mutagen
        except ImportError:
            QMessageBox.warning(self, "mutagen required", "pip install mutagen")
            return

        def _get(tags, k):
            v = tags.get(k)
            return str(v[0]).strip() if isinstance(v, list) and v else str(v).strip() if v else ""

        changed = errors = 0
        for path in files:
            try:
                audio = mutagen.File(str(path), easy=True)
                if audio is None or audio.tags is None:
                    continue
                tags    = audio.tags
                dirty   = False

                if "trim_whitespace" in ops:
                    for key in list(tags.keys()):
                        v = tags.get(key)
                        if isinstance(v, list) and v:
                            stripped = str(v[0]).strip()
                            if stripped != str(v[0]):
                                tags[key] = [stripped]; dirty = True

                if "capitalize_titles" in ops:
                    v = _get(tags, "title")
                    if v: tags["title"] = [v.title()]; dirty = True

                if "capitalize_artists" in ops:
                    for k in ("artist", "albumartist"):
                        v = _get(tags, k)
                        if v: tags[k] = [v.title()]; dirty = True

                if "swap_artist_aa" in ops:
                    a  = _get(tags, "artist")
                    aa = _get(tags, "albumartist")
                    if a or aa:
                        # Write directly so an empty value clears the field
                        if aa:
                            tags["artist"] = [aa]; dirty = True
                        elif "artist" in tags:
                            del tags["artist"]; dirty = True
                        if a:
                            tags["albumartist"] = [a]; dirty = True
                        elif "albumartist" in tags:
                            del tags["albumartist"]; dirty = True

                if "copy_artist_to_aa" in ops:
                    a  = _get(tags, "artist")
                    aa = _get(tags, "albumartist")
                    if a and not aa:
                        tags["albumartist"] = [a]; dirty = True

                if "strip_track_total" in ops:
                    v = _get(tags, "tracknumber")
                    if "/" in v: tags["tracknumber"] = [v.split("/")[0].strip()]; dirty = True

                if "strip_disc_total" in ops:
                    v = _get(tags, "discnumber")
                    if "/" in v: tags["discnumber"] = [v.split("/")[0].strip()]; dirty = True

                if "lowercase_genre" in ops:
                    v = _get(tags, "genre")
                    if v: tags["genre"] = [v.lower()]; dirty = True

                if "titlecase_genre" in ops:
                    v = _get(tags, "genre")
                    if v: tags["genre"] = [v.title()]; dirty = True

                if dirty:
                    audio.save()
                    changed += 1
                    self._log(f"Batch ops applied: {path.name}", "ok", "Batch Ops")
                    self._refresh_file_list_item(path)
            except Exception as e:
                errors += 1
                self._log(f"Batch error — {path.name}: {e}", "error", "Batch Ops")

        msg = f"Batch ops done  ·  {changed} files changed"
        if errors:
            msg += f"  ·  {errors} errors"
        self._status_bar.setText(msg)
        self._log(msg, "ok" if not errors else "warn", "Batch Ops")
        # Reload view so fields reflect changes
        if self._cluster_mode and getattr(self, '_cluster_multi_paths', []):
            self._on_cluster_selection_changed()
        elif len(self._file_list.selectedItems()) > 1:
            self._on_selection_settled()
        elif self._current:
            self._load_tags(self._current)

    # ─────────────────────────────────────────────────────────
    #  FILE RENAMING
    # ─────────────────────────────────────────────────────────

    def _show_rename_dialog(self):
        if self._cluster_mode:
            cluster_paths = getattr(self, '_cluster_multi_paths', [])
            selected_paths = list(cluster_paths) if cluster_paths else (
                [self._current] if self._current else []
            )
        else:
            selected       = self._file_list.selectedItems()
            selected_rows  = [self._file_list.row(it) for it in selected]
            selected_paths = [self._files[r] for r in selected_rows if 0 <= r < len(self._files)]
        targets = selected_paths or list(self._files)
        if not targets:
            QMessageBox.information(self, "Rename Files", "No files loaded.")
            return

        t   = _current_theme
        dlg = QDialog(self)
        dlg.setWindowTitle(f"Rename Files  ({len(targets)} files)")
        dlg.setMinimumWidth(500)
        dlg.setStyleSheet("background:rgba(0,0,0,0.35);color:rgba(255,255,255,0.85);")
        v = QVBoxLayout(dlg)
        v.setContentsMargins(20, 16, 20, 16)
        v.setSpacing(10)

        info = QLabel(
            "Template tokens:  %tracknumber%  %title%  %artist%  %albumartist%\n"
            "                  %album%  %date%  %discnumber%  %genre%\n\n"
            "File extension is preserved automatically."
        )
        info.setStyleSheet("color:rgba(255,255,255,0.35);font-size:11px;background:transparent;")
        v.addWidget(info)

        tpl_edit = QLineEdit()
        tpl_edit.setText("%tracknumber% - %title%")
        tpl_edit.setPlaceholderText("e.g. %tracknumber% - %artist% - %title%")
        v.addWidget(tpl_edit)

        preview_lbl = QLabel("Preview: —")
        preview_lbl.setStyleSheet("color:rgba(255,255,255,0.55);font-size:11px;background:transparent;")
        v.addWidget(preview_lbl)

        # Live preview from the first target file
        def _update_preview():
            tpl = tpl_edit.text().strip()
            if not tpl or not targets:
                preview_lbl.setText("Preview: —")
                return
            try:
                import mutagen as _mut
                audio = _mut.File(str(targets[0]), easy=True)
                tags  = audio.tags if audio else {}
                def _tg(tags_inner, k):
                    v2 = (tags_inner or {}).get(k)
                    return str(v2[0]).strip() if isinstance(v2, list) and v2 else str(v2).strip() if v2 else ""
                out = tpl
                for token, val in [
                    ("%title%",       _tg(tags, "title")),
                    ("%artist%",      _tg(tags, "artist")),
                    ("%albumartist%", _tg(tags, "albumartist") or _tg(tags, "artist")),
                    ("%album%",       _tg(tags, "album")),
                    ("%tracknumber%", _tg(tags, "tracknumber").split("/")[0].zfill(2)),
                    ("%discnumber%",  _tg(tags, "discnumber").split("/")[0]),
                    ("%date%",        _tg(tags, "date")[:4]),
                    ("%genre%",       _tg(tags, "genre")),
                ]:
                    out = out.replace(token, re.sub(r'[\\/:*?"<>|]', '_', val) if val else "")
                out = re.sub(r'_+', '_', out).strip("_").strip()
                preview_lbl.setText(f"Preview: {out}{targets[0].suffix}")
            except Exception as ex:
                preview_lbl.setText(f"Preview error: {ex}")

        tpl_edit.textChanged.connect(_update_preview)
        _update_preview()

        scope_lbl = QLabel(
            f"Applies to: {'selected' if selected_paths else 'all'} {len(targets)} file(s)"
        )
        scope_lbl.setStyleSheet("color:rgba(255,255,255,0.35);font-size:11px;background:transparent;")
        v.addWidget(scope_lbl)

        btn_row = QHBoxLayout()
        run_btn    = QPushButton("Rename")
        run_btn.setObjectName("toggle")
        cancel_btn = QPushButton("Cancel")
        cancel_btn.setObjectName("ghost")
        btn_row.addStretch()
        btn_row.addWidget(cancel_btn)
        btn_row.addWidget(run_btn)
        v.addLayout(btn_row)

        def _start_rename():
            tpl = tpl_edit.text().strip()
            if not tpl:
                QMessageBox.warning(dlg, "Rename", "Enter a template first.")
                return
            dlg.accept()
            self._start_rename_worker(targets, tpl)

        run_btn.clicked.connect(_start_rename)
        cancel_btn.clicked.connect(dlg.reject)
        dlg.exec()

    def _start_rename_worker(self, files: list, template: str):
        self._rename_btn.setEnabled(False)
        self._status_bar.setText("Renaming files…")
        worker = _FileRenameWorker(files, template)
        # Only connect errors/warnings to the log — skip per-file "Renamed: x → y"
        # lines to avoid flooding the log table with 10k+ insertions and freezing the UI.
        worker.log_line.connect(lambda msg, lvl: self._log(msg, lvl) if lvl in ("warn", "error") else None)
        worker.finished.connect(self._on_rename_done)
        worker.finished.connect(worker.deleteLater)
        self._rename_worker = worker
        worker.start()

    def _on_rename_done(self, renamed: int, errors: int):
        self._rename_btn.setEnabled(True)
        msg = f"Renamed {renamed} files"
        if errors:
            msg += f"  ·  {errors} errors"
        self._status_bar.setText(msg)
        self._log(msg, "ok" if not errors else "warn", "Rename")

    # ─────────────────────────────────────────────────────────
    #  COVER OPERATIONS (unchanged from original)
    # ─────────────────────────────────────────────────────────

    def _extract_cover(self, audio, suffix: str) -> Optional[bytes]:
        try:
            if suffix == ".flac":
                pics = audio.pictures
                return pics[0].data if pics else None
            elif suffix == ".mp3":
                apics = audio.tags.getall("APIC") if audio.tags else []
                return apics[0].data if apics else None
            elif suffix in (".m4a", ".aac"):
                covr = (audio.tags or {}).get("covr")
                return bytes(covr[0]) if covr else None
            elif suffix in (".ogg", ".opus", ".oga"):
                import base64
                from mutagen.flac import Picture
                tags = audio.tags or {}
                mbp  = tags.get("metadata_block_picture") or tags.get("METADATA_BLOCK_PICTURE")
                if mbp:
                    entries = mbp if isinstance(mbp, list) else [mbp]
                    for entry in entries:
                        try:
                            pic = Picture(base64.b64decode(entry))
                            if pic.data:
                                return pic.data
                        except Exception:
                            pass
                for key in (audio.tags or {}).keys():
                    if "apic" in key.lower() or "picture" in key.lower():
                        val = (audio.tags or {})[key]
                        if isinstance(val, list) and val:
                            val = val[0]
                        if hasattr(val, "data"):
                            return val.data
            else:
                if hasattr(audio, "tags") and audio.tags:
                    for key in audio.tags.keys():
                        if "APIC" in key or "picture" in key.lower():
                            val = audio.tags[key]
                            if hasattr(val, "data"):
                                return val.data
        except Exception:
            pass
        return None

    def _show_cover(self, data: bytes):
        try:
            img = QImage()
            img.loadFromData(data)
            if not img.isNull():
                pix = QPixmap.fromImage(img).scaled(
                    192, 192,
                    Qt.AspectRatioMode.KeepAspectRatio,
                    Qt.TransformationMode.SmoothTransformation,
                )
                self._cover_lbl.setPixmap(pix)
                size_kb = len(data) / 1024
                self._cover_info_lbl.setText(
                    f"{img.width()}×{img.height()}  ·  {size_kb:.0f} KB"
                )
        except Exception:
            pass

    def _set_cover(self):
        if not self._current:
            return
        path, _ = QFileDialog.getOpenFileName(
            self, "Select Cover Image", str(Path.home()),
            "Images (*.jpg *.jpeg *.png *.bmp *.webp)"
        )
        if not path:
            return
        try:
            import mutagen
            from mutagen.id3 import APIC, ID3
            data = Path(path).read_bytes()
            mime = "image/jpeg" if path.lower().endswith((".jpg", ".jpeg")) else "image/png"
            suf  = self._current.suffix.lower()
            audio_raw = mutagen.File(str(self._current))

            if suf == ".mp3":
                if audio_raw.tags is None:
                    audio_raw.add_tags()
                audio_raw.tags.delall("APIC")
                audio_raw.tags.add(APIC(encoding=3, mime=mime, type=3, desc="Cover", data=data))
            elif suf == ".flac":
                import mutagen.flac as mflac
                pic = mflac.Picture()
                pic.data = data; pic.type = 3; pic.mime = mime
                audio_raw.clear_pictures(); audio_raw.add_picture(pic)
            elif suf in (".m4a", ".aac"):
                from mutagen.mp4 import MP4Cover
                fmt = MP4Cover.FORMAT_JPEG if "jpeg" in mime or "jpg" in mime else MP4Cover.FORMAT_PNG
                if audio_raw.tags is None:
                    audio_raw.add_tags()
                audio_raw.tags["covr"] = [MP4Cover(data, imageformat=fmt)]
            elif suf in (".ogg", ".opus", ".oga"):
                import base64
                from mutagen.flac import Picture
                pic = Picture()
                pic.data = data; pic.type = 3; pic.mime = mime
                pic.width = pic.height = pic.depth = pic.colors = 0
                enc = base64.b64encode(pic.write()).decode("ascii")
                if audio_raw.tags is None:
                    audio_raw.add_tags()
                audio_raw.tags["metadata_block_picture"] = [enc]
            else:
                QMessageBox.warning(self, "Unsupported Format",
                    f"Cover embedding is not supported for {suf} files.")
                return

            audio_raw.save()
            self._cover_data = data
            self._show_cover(data)
            self._status_bar.setText("✓ Cover updated")
            self._log(f"Cover updated: {self._current.name}", "ok")
        except ImportError:
            QMessageBox.warning(self, "mutagen required", "pip install mutagen")
        except Exception as e:
            self._log(f"Cover set error — {self._current.name}: {e}", "error")
            QMessageBox.critical(self, "Error", str(e))

    def _remove_cover(self):
        if not self._current:
            return
        try:
            import mutagen
            suf   = self._current.suffix.lower()
            audio = mutagen.File(str(self._current))
            if suf == ".mp3" and audio.tags:
                audio.tags.delall("APIC")
            elif suf == ".flac":
                audio.clear_pictures()
            elif suf in (".m4a", ".aac") and audio.tags:
                audio.tags.pop("covr", None)
            elif suf in (".ogg", ".opus", ".oga") and audio.tags:
                audio.tags.pop("metadata_block_picture", None)
                audio.tags.pop("METADATA_BLOCK_PICTURE", None)
            audio.save()
            self._cover_data = None
            self._cover_lbl.setPixmap(QPixmap()); self._cover_lbl.setText("No Cover")
            self._cover_info_lbl.setText("")
            self._status_bar.setText("✓ Cover removed")
            self._log(f"Cover removed: {self._current.name}", "ok")
        except Exception as e:
            self._log(f"Cover remove error — {self._current.name}: {e}", "error")
            QMessageBox.critical(self, "Error", str(e))

    def _export_cover(self):
        if not self._cover_data:
            QMessageBox.information(self, "No Cover", "No cover art loaded.")
            return
        path, _ = QFileDialog.getSaveFileName(
            self, "Export Cover", str(Path.home() / "cover.jpg"),
            "JPEG (*.jpg);;PNG (*.png)"
        )
        if path:
            Path(path).write_bytes(self._cover_data)
            self._status_bar.setText(f"✓ Exported to {Path(path).name}")

    # ── Cover resize ──────────────────────────────────────────

    @staticmethod
    def _do_resize_bytes(data: bytes, target_px: int, quality: int):
        from PyQt6.QtCore import QByteArray, QBuffer
        peeked = _peek_image_size(data)
        if peeked is not None:
            pw, ph = peeked
            if pw <= target_px and ph <= target_px:
                return None
        img = QImage()
        if not img.loadFromData(data):
            return None
        if img.width() <= target_px and img.height() <= target_px:
            return None
        img = img.scaled(
            target_px, target_px,
            Qt.AspectRatioMode.KeepAspectRatio,
            Qt.TransformationMode.SmoothTransformation,
        )
        qba  = QByteArray()
        qbuf = QBuffer(qba)
        qbuf.open(QBuffer.OpenModeFlag.WriteOnly)
        img.save(qbuf, "JPEG", quality)
        qbuf.close()
        return bytes(qba), img.width(), img.height()

    def _embed_resized_cover(self, path: Path, new_data: bytes):
        import mutagen
        from mutagen.id3 import APIC
        suf   = path.suffix.lower()
        audio = mutagen.File(str(path))
        if audio is None:
            raise ValueError(f"mutagen could not open {path.name}")
        mime = "image/jpeg"
        if suf == ".mp3":
            if audio.tags is None:
                audio.add_tags()
            audio.tags.delall("APIC")
            audio.tags.add(APIC(encoding=3, mime=mime, type=3, desc="Cover", data=new_data))
        elif suf == ".flac":
            import mutagen.flac as mflac
            pic = mflac.Picture()
            pic.data = new_data; pic.type = 3; pic.mime = mime
            audio.clear_pictures(); audio.add_picture(pic)
        elif suf in (".m4a", ".aac"):
            from mutagen.mp4 import MP4Cover
            if audio.tags is None:
                audio.add_tags()
            audio.tags["covr"] = [MP4Cover(new_data, imageformat=MP4Cover.FORMAT_JPEG)]
        elif suf in (".ogg", ".opus", ".oga"):
            import base64
            from mutagen.flac import Picture
            pic = Picture()
            pic.data = new_data; pic.type = 3; pic.mime = mime
            pic.width = pic.height = pic.depth = pic.colors = 0
            enc = base64.b64encode(pic.write()).decode("ascii")
            if audio.tags is None:
                audio.add_tags()
            audio.tags["metadata_block_picture"] = [enc]
        else:
            raise ValueError(f"Cover embedding not supported for {suf} files")
        audio.save()

    def _resize_cover(self):
        if not self._current:
            self._status_bar.setText("Select a file first."); return
        if not self._cover_data:
            self._status_bar.setText("This file has no embedded cover art."); return
        try:
            import mutagen
        except ImportError:
            QMessageBox.warning(self, "mutagen required", "pip install mutagen"); return

        target_px = self._resize_spin.value()
        result    = self._do_resize_bytes(self._cover_data, target_px, 100)
        if result is None:
            img = QImage(); img.loadFromData(self._cover_data)
            self._status_bar.setText(
                f"Cover is already {img.width()}×{img.height()} — at or below {target_px} px, skipped."
            )
            return
        new_data, new_w, new_h = result
        old_kb = len(self._cover_data) / 1024
        new_kb = len(new_data) / 1024
        try:
            self._cover_data_before_resize = self._cover_data
            self._embed_resized_cover(self._current, new_data)
            self._cover_data = new_data
            self._show_cover(new_data)
            self._revert_cover_btn.setEnabled(True)
            self._status_bar.setText(
                f"✓ Cover resized to {new_w}×{new_h}  ·  {old_kb:.0f} KB → {new_kb:.0f} KB"
            )
        except Exception as e:
            self._cover_data_before_resize = None
            QMessageBox.critical(self, "Resize Error", str(e))

    def _revert_cover(self):
        original = getattr(self, "_cover_data_before_resize", None)
        if not original:
            self._status_bar.setText("Nothing to revert."); return
        if not self._current or not self._current.exists():
            self._status_bar.setText("File no longer available."); return
        try:
            self._embed_resized_cover(self._current, original)
            self._cover_data = original
            self._cover_data_before_resize = None
            self._show_cover(self._cover_data)
            self._revert_cover_btn.setEnabled(False)
            self._status_bar.setText("✓ Cover reverted to original.")
        except Exception as e:
            QMessageBox.critical(self, "Revert Error", str(e))

    def _revert_bulk_covers(self):
        originals = getattr(self, "_bulk_cover_originals", None)
        if not originals:
            self._status_bar.setText("No bulk revert data available."); return
        self._revert_all_btn.setEnabled(False)
        reverted = errors = 0
        for path, cover_data in originals.items():
            try:
                if not path.exists():
                    self._log(f"Skipped (missing): {path.name}", "warn"); continue
                self._embed_resized_cover(path, cover_data)
                self._log(f"Reverted: {path.name}", "ok")
                reverted += 1
            except Exception as e:
                self._log(f"Revert error — {path.name}: {e}", "error")
                errors += 1
        self._bulk_cover_originals = {}
        msg = f"✓ Bulk revert done  ·  {reverted} restored"
        if errors:
            msg += f"  ·  {errors} errors"
        self._status_bar.setText(msg)
        self._log(msg, "ok" if not errors else "warn")
        if self._current and self._current in originals:
            self._cover_data = originals[self._current]
            self._show_cover(self._cover_data)

    def _bulk_resize_selected(self):
        """Resize covers only for the currently selected files."""
        # Gather selected paths from flat list or cluster mode
        if self._cluster_mode:
            selected_paths = list(getattr(self, "_cluster_multi_paths", []))
            if not selected_paths and self._current:
                selected_paths = [self._current]
        else:
            selected_items = self._file_list.selectedItems()
            if selected_items:
                rows = [self._file_list.row(it) for it in selected_items]
                selected_paths = [self._files[r] for r in rows if 0 <= r < len(self._files)]
            elif self._current:
                selected_paths = [self._current]
            else:
                selected_paths = []

        if not selected_paths:
            self._status_bar.setText("No files selected."); return
        try:
            import mutagen
        except ImportError:
            QMessageBox.warning(self, "mutagen required", "pip install mutagen"); return

        supported = {".mp3", ".flac", ".m4a", ".aac"}
        files_to_process = [f for f in selected_paths if f.suffix.lower() in supported]
        if not files_to_process:
            self._status_bar.setText("No supported files (MP3/FLAC/M4A/AAC) in selection."); return

        self._resize_sel_btn.setEnabled(False)
        self._resize_all_btn.setEnabled(False)
        self._resize_progress.setVisible(True)
        self._resize_progress.setValue(0)
        self._cancel_resize_btn.setVisible(True)

        worker = _CoverResizeWorker(
            files_to_process, self._resize_spin.value(), 100,
            self._extract_cover, self._do_resize_bytes, self._embed_resized_cover,
        )
        worker.progress.connect(self._on_bulk_resize_progress)
        worker.log_line.connect(self._on_bulk_resize_log)
        worker.originals_ready.connect(self._on_bulk_originals_ready)
        worker.finished.connect(self._on_bulk_resize_done)
        worker.finished.connect(worker.deleteLater)
        self._bulk_worker = worker
        self._bulk_total  = len(files_to_process)
        worker.start()

    def _bulk_resize_covers(self):
        if not self._files:
            self._status_bar.setText("Load some files first."); return
        try:
            import mutagen
        except ImportError:
            QMessageBox.warning(self, "mutagen required", "pip install mutagen"); return

        supported = {".mp3", ".flac", ".m4a", ".aac"}
        files_to_process = [f for f in self._files if f.suffix.lower() in supported]
        if not files_to_process:
            self._status_bar.setText("No supported files (MP3/FLAC/M4A/AAC) in list."); return

        self._resize_sel_btn.setEnabled(False)
        self._resize_all_btn.setEnabled(False)
        self._resize_progress.setVisible(True)
        self._resize_progress.setValue(0)
        self._cancel_resize_btn.setVisible(True)

        worker = _CoverResizeWorker(
            files_to_process, self._resize_spin.value(), 100,
            self._extract_cover, self._do_resize_bytes, self._embed_resized_cover,
        )
        worker.progress.connect(self._on_bulk_resize_progress)
        worker.log_line.connect(self._on_bulk_resize_log)
        worker.originals_ready.connect(self._on_bulk_originals_ready)
        worker.finished.connect(self._on_bulk_resize_done)
        worker.finished.connect(worker.deleteLater)
        self._bulk_worker = worker
        self._bulk_total  = len(files_to_process)
        worker.start()

    def _cancel_bulk_resize(self):
        w = getattr(self, "_bulk_worker", None)
        if w and w.isRunning():
            w.requestInterruption()
            w.wait(500)
        self._cancel_resize_btn.setVisible(False)
        self._resize_progress.setVisible(False)
        self._resize_sel_btn.setEnabled(True)
        self._resize_all_btn.setEnabled(True)
        self._status_bar.setText("Cover resize cancelled.")

    def _on_bulk_resize_progress(self, i, total, name):
        pct = int(i / total * 100) if total else 0
        self._resize_progress.setValue(pct)
        self._status_bar.setText(f"Resizing… {i}/{total}  —  {name}")

    def _on_bulk_resize_log(self, msg, level):
        self._log(msg, level)

    def _on_bulk_originals_ready(self, originals):
        self._bulk_cover_originals = originals

    def _on_bulk_resize_done(self, resized, skipped, errors):
        self._resize_sel_btn.setEnabled(True)
        self._resize_all_btn.setEnabled(True)
        self._resize_progress.setVisible(False)
        self._cancel_resize_btn.setVisible(False)
        msg = f"Cover resize done  ·  {resized} resized  ·  {skipped} already small"
        if errors:
            msg += f"  ·  {errors} errors"
        self._status_bar.setText(msg)
        self._log(msg, "ok" if not errors else "warn")
        if resized > 0 and self._bulk_cover_originals:
            self._revert_all_btn.setEnabled(True)
        if self._current:
            try:
                import mutagen
                audio_raw  = mutagen.File(str(self._current))
                cover_data = self._extract_cover(audio_raw, self._current.suffix.lower())
                if cover_data:
                    self._cover_data = cover_data
                    self._show_cover(cover_data)
            except Exception:
                pass

    # ── Verify Integrity ──────────────────────────────────────

    def _verify_current(self):
        if not self._current:
            self._status_bar.setText("Select a file first."); return
        if not shutil.which("ffmpeg"):
            QMessageBox.warning(self, "ffmpeg not found",
                "Integrity verification requires ffmpeg on your PATH.\n\n"
                "Install it via your package manager or from https://ffmpeg.org/"); return
        self._log(f"Verifying: {self._current.name} …")
        self._verify_all_btn.setEnabled(False)
        worker = _IntegrityCheckWorker([self._current])
        worker.log_line.connect(self._log)
        worker.finished.connect(self._on_verify_done_single)
        worker.finished.connect(worker.deleteLater)
        self._verify_worker = worker
        worker.start()

    def _verify_all(self):
        if not self._files:
            self._status_bar.setText("Load some files first."); return
        if not shutil.which("ffmpeg"):
            QMessageBox.warning(self, "ffmpeg not found",
                "Integrity verification requires ffmpeg on your PATH.\n\n"
                "Install it via your package manager or from https://ffmpeg.org/"); return
        self._log(f"Verifying {len(self._files)} files …")
        self._verify_all_btn.setEnabled(False)
        self._verify_cancel_btn.setVisible(True)
        self._verify_progress.setVisible(True)
        self._verify_progress.setValue(0)
        worker = _IntegrityCheckWorker(list(self._files))
        worker.progress.connect(self._on_verify_progress)
        worker.log_line.connect(self._log)
        worker.finished.connect(self._on_verify_done_all)
        worker.finished.connect(worker.deleteLater)
        self._verify_worker = worker
        worker.start()

    def _cancel_verify(self):
        w = getattr(self, '_verify_worker', None)
        if w and w.isRunning():
            w.requestInterruption(); w.wait(2000)
        self._verify_all_btn.setEnabled(True)
        self._verify_cancel_btn.setVisible(False)
        self._verify_progress.setVisible(False)
        self._status_bar.setText("Verify cancelled.")

    def _on_verify_progress(self, i, total, name):
        pct = int(i / total * 100) if total else 0
        self._verify_progress.setValue(pct)
        self._status_bar.setText(f"Verifying… {i}/{total}  —  {name}")

    def _on_verify_done_single(self, ok, errors):
        self._verify_all_btn.setEnabled(True)
        self._status_bar.setText("✓ No errors found." if not errors else f"✗ {errors} error(s) detected — see log.")

    def _on_verify_done_all(self, ok, errors):
        self._verify_all_btn.setEnabled(True)
        self._verify_cancel_btn.setVisible(False)
        self._verify_progress.setVisible(False)
        msg = f"Verify done  ·  {ok} OK  ·  {errors} with errors"
        self._status_bar.setText(msg)
        self._log(msg, "ok" if not errors else "warn")

    # ── ReplayGain ────────────────────────────────────────────

    def _rg_strip(self):
        if not self._files:
            self._status_bar.setText("Load some files first."); return
        try:
            import mutagen
        except ImportError:
            QMessageBox.warning(self, "mutagen required", "pip install mutagen"); return
        self._rg_strip_btn.setEnabled(False)
        self._rg_progress.setVisible(True)
        self._rg_progress.setValue(0)
        worker = _RgStripWorker(list(self._files))
        worker.progress.connect(self._on_rg_progress)
        worker.log_line.connect(self._log)
        worker.finished.connect(self._on_rg_done)
        worker.finished.connect(worker.deleteLater)
        self._rg_worker = worker
        worker.start()

    def _on_rg_progress(self, i, total, name):
        pct = int(i / total * 100) if total else 0
        self._rg_progress.setValue(pct)
        self._status_bar.setText(f"Stripping RG tags… {i}/{total}  —  {name}")

    def _on_rg_done(self, stripped, errors):
        self._rg_strip_btn.setEnabled(True)
        self._rg_progress.setVisible(False)
        msg = f"Strip done  ·  {stripped} files cleared"
        if errors:
            msg += f"  ·  {errors} errors"
        self._status_bar.setText(msg)
        self._log(msg, "ok" if not errors else "warn")

class _RgStripWorker(QThread):
    """
    Background worker that strips ReplayGain tags from a list of files.
    progress emits (files_done, total, current_filename).
    log_line emits (message, level).
    finished emits (stripped, errors).
    """
    progress = pyqtSignal(int, int, str)
    log_line = pyqtSignal(str, str)
    finished = pyqtSignal(int, int)

    _RG_PATTERNS = [
        "replaygain", "rva2", "r128_track_gain", "r128_album_gain"
    ]

    def __init__(self, files):
        super().__init__()
        self.files = files

    def run(self):
        stripped = errors = 0
        total = len(self.files)
        for i, path in enumerate(self.files, 1):
            if self.isInterruptionRequested():
                break
            self.progress.emit(i, total, path.name)
            try:
                import mutagen
                audio = mutagen.File(str(path))
                if audio is None:
                    self.log_line.emit(f"Cannot open: {path.name}", "error")
                    errors += 1
                    continue
                changed = False
                tags = audio.tags or {}
                for key in list(tags.keys()):
                    low = key.lower()
                    if any(pat in low for pat in self._RG_PATTERNS):
                        del tags[key]
                        changed = True
                if changed:
                    audio.save()
                    stripped += 1
                    self.log_line.emit(f"Stripped RG tags: {path.name}", "ok")
                else:
                    self.log_line.emit(f"No RG tags: {path.name}", "info")
            except Exception as e:
                errors += 1
                self.log_line.emit(f"Strip error — {path.name}: {e}", "error")
        self.finished.emit(stripped, errors)


class _CoverResizeWorker(QThread):
    """
    Background worker that resizes embedded covers in a list of files.
    progress        emits (files_done, total, current_filename).
    log_line        emits (message, level) — for display in the log panel.
    originals_ready emits dict[Path, bytes] of original covers for resized files.
    finished        emits (resized, skipped, errors).
    """
    progress        = pyqtSignal(int, int, str)
    log_line        = pyqtSignal(str, str)
    originals_ready = pyqtSignal(object)   # dict[Path, bytes]
    finished        = pyqtSignal(int, int, int)

    def __init__(self, files, target_px, quality, extract_fn, resize_fn, embed_fn):
        super().__init__()
        self.files     = files
        self.target_px = target_px
        self.quality   = quality
        self._extract  = extract_fn
        self._resize   = resize_fn
        self._embed    = embed_fn

    def run(self):
        resized = skipped = errors = 0
        originals = {}   # Path -> original bytes, only for files actually resized
        total = len(self.files)
        for i, path in enumerate(self.files, 1):
            if self.isInterruptionRequested():
                break
            self.progress.emit(i, total, path.name)
            try:
                import mutagen
                suf       = path.suffix.lower()
                audio_raw = mutagen.File(str(path))
                if audio_raw is None:
                    self.log_line.emit(f"Cannot open: {path.name}", "error")
                    errors += 1
                    continue
                cover_data = self._extract(audio_raw, suf)
                if not cover_data:
                    skipped += 1
                    continue
                result = self._resize(cover_data, self.target_px, self.quality)
                if result is None:
                    skipped += 1
                    continue
                new_data, new_w, new_h = result
                old_kb = len(cover_data) / 1024
                new_kb = len(new_data) / 1024
                self._embed(path, new_data)
                originals[path] = cover_data   # stash for revert
                self.log_line.emit(
                    f"Resized cover: {path.name}  ({new_w}×{new_h}, "
                    f"{old_kb:.0f}→{new_kb:.0f} KB)", "ok"
                )
                resized += 1
            except Exception as e:
                self.log_line.emit(f"Resize error — {path.name}: {e}", "error")
                errors += 1
        self.originals_ready.emit(originals)
        self.finished.emit(resized, skipped, errors)


class _IntegrityCheckWorker(QThread):
    """
    Verifies audio file integrity by fully decoding each file through ffmpeg.
    FLAC files additionally get a --test pass (checks built-in MD5).

    progress emits (files_done, total, current_filename).
    log_line emits (message, level).
    finished emits (ok_count, error_count).
    """
    progress = pyqtSignal(int, int, str)
    log_line = pyqtSignal(str, str)
    finished = pyqtSignal(int, int)

    def __init__(self, files: list):
        super().__init__()
        self.files = files

    def run(self):
        ok = errors = 0
        total = len(self.files)
        for i, path in enumerate(self.files, 1):
            if self.isInterruptionRequested():
                break
            self.progress.emit(i, total, path.name)
            try:
                self._check_file(path)
                self.log_line.emit(f"OK: {path.name}", "ok")
                ok += 1
            except Exception as e:
                self.log_line.emit(f"ERROR: {path.name}  —  {e}", "error")
                errors += 1
        self.finished.emit(ok, errors)

    def _check_file(self, path: Path):
        """Raises RuntimeError if the file is corrupt, returns None if clean."""
        # Primary check: ffmpeg full decode
        cmd = ["ffmpeg", "-v", "error", "-i", str(path), "-f", "null", "-"]
        try:
            result = subprocess.run(
                cmd, capture_output=True, text=True, timeout=120
            )
        except FileNotFoundError:
            raise RuntimeError("ffmpeg not found on PATH")
        except subprocess.TimeoutExpired:
            raise RuntimeError("ffmpeg timed out (>120s)")

        stderr = result.stderr.strip()
        if stderr:
            # Filter benign informational lines ffmpeg sometimes emits
            bad_lines = [
                ln for ln in stderr.splitlines()
                if ln.strip() and not ln.startswith("    ")
                and "Last message repeated" not in ln
            ]
            if bad_lines:
                summary = bad_lines[0][:120]
                raise RuntimeError(f"ffmpeg: {summary}")

        # Extra FLAC check: use flac --test if available (checks MD5)
        if path.suffix.lower() == ".flac" and shutil.which("flac"):
            try:
                flac_result = subprocess.run(
                    ["flac", "--test", "--silent", str(path)],
                    capture_output=True, text=True, timeout=120
                )
                if flac_result.returncode != 0:
                    raise RuntimeError("flac --test failed (MD5 mismatch or corrupt frame)")
            except subprocess.TimeoutExpired:
                raise RuntimeError("flac --test timed out (>120s)")




# ─────────────────────────────────────────────────────────────
#  FILE CONVERTER PAGE
# ─────────────────────────────────────────────────────────────

_CONV_DB_TABLE   = "converted_files"
_CONV_SNAP_TABLE = "conv_snapshot"
_CONV_PRESET_KEY = "conv_presets"   # key in config.json

# ── Format/codec catalogue ────────────────────────────────────
_CONV_FORMATS = {
    "FLAC":      {"ext": "flac", "codec": "flac",       "lossless": True},
    "AAC / M4A": {"ext": "m4a",  "codec": "aac",        "lossless": False},
    "MP3":       {"ext": "mp3",  "codec": "libmp3lame",  "lossless": False},
    "OGG Vorbis":{"ext": "ogg",  "codec": "libvorbis",  "lossless": False},
    "Opus":      {"ext": "opus", "codec": "libopus",    "lossless": False},
    "WAV":       {"ext": "wav",  "codec": "pcm_s16le",  "lossless": True},
    "AIFF":      {"ext": "aiff", "codec": "pcm_s16be",  "lossless": True},
}

_CONV_BITRATES = {
    "aac":        [("96 kbps","96k"),("128 kbps","128k"),("192 kbps","192k"),
                   ("256 kbps","256k"),("320 kbps","320k")],
    "libmp3lame": [("128 kbps","128k"),("192 kbps","192k"),("256 kbps","256k"),
                   ("320 kbps","320k"),("V0 VBR",None)],
    "libvorbis":  [("Q3 ~112 kbps","q3"),("Q5 ~160 kbps","q5"),
                   ("Q6 ~192 kbps","q6"),("Q8 ~256 kbps","q8")],
    "libopus":    [("64 kbps","64k"),("96 kbps","96k"),("128 kbps","128k"),
                   ("192 kbps","192k"),("256 kbps","256k")],
}

_CONV_DEFAULT_BITRATE = {
    "aac": "256k", "libmp3lame": "320k", "libvorbis": "q6", "libopus": "128k",
}

_CONV_SRC_EXTS = {".flac",".mp3",".m4a",".aac",".ogg",".opus",".wav",".aiff",".wma"}


def _build_ffmpeg_cmd(src: str, dst: str, opts: dict) -> list:
    """
    Build a correct ffmpeg command from opts dict.
    Handles the cover/filter flag ordering that previously caused silent failures.
    """
    codec    = opts["codec"]
    lossless = opts.get("lossless", False)
    cmd = ["ffmpeg", "-y", "-i", src]

    # Audio codec + quality
    cmd += ["-c:a", codec]
    q = opts.get("quality_args", [])
    if q:
        cmd += q

    # Audio filters
    filters = []
    if opts.get("normalize"):
        filters.append("loudnorm=I=-16:TP=-1.5:LRA=11")
    if opts.get("resample"):
        cmd += ["-ar", opts["resample"]]

    if filters:
        cmd += ["-af", ",".join(filters)]

    # Metadata
    if opts.get("preserve_tags"):
        cmd += ["-map_metadata", "0"]

    # Cover art
    cover_resize = opts.get("cover_resize", False)
    cover_size   = opts.get("cover_size", 500)

    if opts.get("embed_cover"):
        if cover_resize:
            # Scale down only — if source cover is already smaller than target, pass through unchanged.
            # scale uses -2 so the other dimension is auto-calculated keeping aspect ratio.
            # The 'if(gt(iw,N)...' guards mean we never upscale.
            vf = (f"scale='if(gt(iw,{cover_size}),{cover_size},-2)':"
                  f"'if(gt(ih,{cover_size}),{cover_size},-2)':"
                  f"force_original_aspect_ratio=decrease")
            cmd += [
                "-filter_complex", f"[0:v]{vf}[vout]",
                "-map", "0:a:0",
                "-map", "[vout]",
                "-c:v", "mjpeg",
                "-disposition:v", "attached_pic",
            ]
        else:
            cmd += ["-map", "0:a:0", "-map", "0:v?", "-c:v", "copy"]

    # Extra user args
    extra = opts.get("extra_args", [])
    if extra:
        cmd += extra

    cmd.append(dst)
    return cmd


class FileConverterPage(QWidget):
    """
    FFmpeg-powered audio converter with format presets and conversion memory.
    """

    def __init__(self, parent=None):
        super().__init__(parent)
        self._worker: Optional[QThread] = None
        self._scan_worker: Optional[QThread] = None
        self._pending_convert_after_scan = False
        self._scan_batch_buf: list = []
        self._queue_files: list[Path]   = []
        self._presets: list[dict]       = []
        self._ensure_db()
        self._load_presets()
        self._build()

    # ── DB / persistence ──────────────────────────────────────

    def _ensure_db(self):
        try:
            con = sqlite3.connect(DB_FILE)
            # Legacy migration: add preset_name column if missing
            existing_conv = {r[1] for r in con.execute("PRAGMA table_info(converted_files)")}
            if existing_conv and "preset_name" not in existing_conv:
                con.execute("ALTER TABLE converted_files ADD COLUMN preset_name TEXT NOT NULL DEFAULT ''")
            existing_snap = {r[1] for r in con.execute("PRAGMA table_info(conv_snapshot)")}
            if existing_snap and "preset_name" not in existing_snap:
                con.execute("ALTER TABLE conv_snapshot ADD COLUMN preset_name TEXT NOT NULL DEFAULT ''")
            con.execute(f"""CREATE TABLE IF NOT EXISTS {_CONV_DB_TABLE} (
                src_path TEXT NOT NULL, src_mtime REAL,
                dst_path TEXT, converted_at TEXT, format TEXT,
                preset_name TEXT NOT NULL DEFAULT '',
                PRIMARY KEY (src_path, preset_name))""")
            # Snapshot table: tracks every file seen in a source folder
            # src_folder  = the root folder being converted (key for this library)
            # preset_name = name of the active preset (isolates memories per preset)
            # src_path    = absolute path to individual file
            # src_mtime   = mtime at last scan (change detection)
            # src_size    = size at last scan (extra change detection)
            con.execute(f"""CREATE TABLE IF NOT EXISTS {_CONV_SNAP_TABLE} (
                src_folder  TEXT NOT NULL,
                preset_name TEXT NOT NULL DEFAULT '',
                src_path    TEXT NOT NULL,
                src_mtime   REAL,
                src_size    INTEGER,
                PRIMARY KEY (src_folder, preset_name, src_path))""")
            con.commit(); con.close()
        except Exception:
            pass

    def _load_presets(self):
        c = load_conf()
        self._presets = c.get(_CONV_PRESET_KEY, [])

    def _save_presets(self):
        c = load_conf(); c[_CONV_PRESET_KEY] = self._presets; save_conf(c)

    # ── Build UI ──────────────────────────────────────────────

    def _build(self):
        t = _current_theme
        root = QVBoxLayout(self)
        root.setContentsMargins(0, 0, 0, 0)
        root.setSpacing(0)

        # Header
        hdr = QWidget(); hdr.setFixedHeight(56)
        hdr.setStyleSheet("background:rgba(5,7,11,0.65); border-bottom:1px solid rgba(255,255,255,0.07);")
        hb = QHBoxLayout(hdr); hb.setContentsMargins(20, 0, 20, 0)
        hl = QLabel("File Converter")
        hf = QFont(); hf.setPointSize(15); hf.setBold(True)
        hl.setFont(hf); hl.setStyleSheet("color:#fff;background:transparent;")
        hb.addWidget(hl); hb.addStretch()
        hb.addWidget(QLabel("FFmpeg-powered · remembers converted files"))
        root.addWidget(hdr)

        # Body: left config + right queue
        body = QHBoxLayout(); body.setContentsMargins(20,20,20,20); body.setSpacing(20)
        root.addWidget(self._w(body), stretch=1)

        # ── Left panel (scrollable so it works on small monitors) ──
        cfg_inner = QWidget(); cfg_inner.setObjectName("panel")
        cfg_inner.setAttribute(Qt.WidgetAttribute.WA_StyledBackground, True)
        cv  = QVBoxLayout(cfg_inner); cv.setContentsMargins(16,16,16,16); cv.setSpacing(10)
        cfg_scroll = QScrollArea()
        cfg_scroll.setWidget(cfg_inner)
        cfg_scroll.setWidgetResizable(True)
        cfg_scroll.setFixedWidth(350)
        cfg_scroll.setFrameShape(QFrame.Shape.NoFrame)
        cfg_scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)
        cfg_scroll.setStyleSheet("QScrollArea{background:transparent;border:none;}")
        cfg = cfg_scroll

        def sec(txt):
            l = QLabel(txt); l.setObjectName("sectiontitle"); return l

        # Source
        cv.addWidget(sec("SOURCE"))
        src_row = QHBoxLayout(); src_row.setSpacing(5)
        self._src_edit = QLineEdit()
        self._src_edit.setPlaceholderText("Folder or single audio file…")
        self._src_edit.setReadOnly(True)
        src_row.addWidget(self._src_edit, 1)
        for label, cb in (("Folder…", self._browse_src_folder),
                           ("File…",   self._browse_src_file)):
            b = QPushButton(label); b.setObjectName("ghost"); b.setFixedHeight(28)
            b.clicked.connect(cb); src_row.addWidget(b)
        cv.addLayout(src_row)

        # Destination
        cv.addWidget(sec("OUTPUT FOLDER"))
        dst_row = QHBoxLayout(); dst_row.setSpacing(5)
        self._dst_edit = QLineEdit(); self._dst_edit.setPlaceholderText("Output folder…"); self._dst_edit.setReadOnly(True)
        dst_row.addWidget(self._dst_edit, 1)
        db = QPushButton("Browse…"); db.setObjectName("ghost"); db.setFixedHeight(28)
        db.clicked.connect(self._browse_dst); dst_row.addWidget(db)
        cv.addLayout(dst_row)

        # ── Format + quality in one block ─────────────────────
        cv.addWidget(sec("FORMAT & QUALITY"))
        fmt_row = QHBoxLayout(); fmt_row.setSpacing(6)
        self._fmt_combo = QComboBox()
        self._fmt_combo.addItems(list(_CONV_FORMATS.keys()))
        self._fmt_combo.currentTextChanged.connect(self._on_fmt_changed)
        fmt_row.addWidget(self._fmt_combo, 1)

        self._bitrate_combo = QComboBox()
        self._bitrate_combo.setFixedWidth(130)
        fmt_row.addWidget(self._bitrate_combo)
        cv.addLayout(fmt_row)
        self._on_fmt_changed(self._fmt_combo.currentText())

        # ── Presets ────────────────────────────────────────────
        cv.addWidget(sec("PRESETS"))
        preset_row = QHBoxLayout(); preset_row.setSpacing(5)
        self._preset_combo = QComboBox()
        self._preset_combo.setPlaceholderText("— select a preset —")
        preset_row.addWidget(self._preset_combo, 1)
        load_p = QPushButton("Load"); load_p.setObjectName("ghost"); load_p.setFixedHeight(26)
        load_p.clicked.connect(self._load_preset)
        save_p = QPushButton("Save"); save_p.setObjectName("ghost"); save_p.setFixedHeight(26)
        save_p.clicked.connect(self._save_preset)
        del_p  = QPushButton("Del");  del_p.setObjectName("danger"); del_p.setFixedHeight(26)
        del_p.clicked.connect(self._delete_preset)
        for b in (load_p, save_p, del_p): preset_row.addWidget(b)
        cv.addLayout(preset_row)
        self._refresh_preset_combo()

        # ── Options ────────────────────────────────────────────
        cv.addWidget(sec("OPTIONS"))
        self._opt_skip      = QCheckBox("Skip already-converted files"); self._opt_skip.setChecked(True)
        self._opt_tags      = QCheckBox("Preserve metadata tags");       self._opt_tags.setChecked(True)
        cv.addWidget(self._opt_skip)
        cv.addWidget(self._opt_tags)
        # Re-embed cover art + inline resize option on the same row
        cover_row = QHBoxLayout(); cover_row.setSpacing(8); cover_row.setContentsMargins(0,0,0,0)
        self._opt_cover = QCheckBox("Re-embed cover art"); self._opt_cover.setChecked(True)
        cover_row.addWidget(self._opt_cover)
        self._opt_cover_resize = QCheckBox("Resize to:")
        self._opt_cover_resize.setToolTip(
            "Scale embedded cover art down to at most this size.\n"
            "If the source cover is already smaller, it is kept as-is.")
        self._cover_size_spin = QSpinBox()
        self._cover_size_spin.setRange(100, 4096)
        self._cover_size_spin.setValue(500)
        self._cover_size_spin.setSuffix(" px")
        self._cover_size_spin.setFixedWidth(82)
        self._cover_size_spin.setEnabled(False)
        cover_row.addWidget(self._opt_cover_resize)
        cover_row.addWidget(self._cover_size_spin)
        cover_row.addStretch()
        cv.addLayout(cover_row)
        self._opt_cover_resize.toggled.connect(self._cover_size_spin.setEnabled)
        self._opt_cover.toggled.connect(lambda on: (
            self._opt_cover_resize.setEnabled(on),
            self._opt_cover_resize.setChecked(False) if not on else None,
        ))

        self._opt_struct    = QCheckBox("Mirror folder structure");      self._opt_struct.setChecked(True)
        self._opt_del_src   = QCheckBox("Delete source after convert");  self._opt_del_src.setChecked(False)
        self._opt_normalize = QCheckBox("Loudness normalize (EBU R128)"); self._opt_normalize.setChecked(False)
        for cb in (self._opt_struct, self._opt_del_src, self._opt_normalize):
            cv.addWidget(cb)

        # Same-format handling
        sf_row = QHBoxLayout(); sf_row.setSpacing(6)
        sf_row.addWidget(QLabel("Same format files:"))
        self._opt_samefmt = QComboBox()
        self._opt_samefmt.addItems(["Skip", "Copy as-is", "Re-encode"])
        self._opt_samefmt.setCurrentIndex(0)
        self._opt_samefmt.setToolTip(
            "What to do with files already in the target format\n"
            "Skip: ignore them entirely\n"
            "Copy as-is: copy to output folder without re-encoding\n"
            "Re-encode: run through FFmpeg anyway (e.g. to normalize or resample)")
        sf_row.addWidget(self._opt_samefmt, 1)
        cv.addLayout(sf_row)

        # Resample
        rs_row = QHBoxLayout(); rs_row.setSpacing(6)
        self._opt_resample = QCheckBox("Resample to:")
        self._resample_combo = QComboBox()
        self._resample_combo.addItems(["44100 Hz","48000 Hz","88200 Hz","96000 Hz"])
        rs_row.addWidget(self._opt_resample); rs_row.addWidget(self._resample_combo)
        cv.addLayout(rs_row)

        # Workers
        cv.addWidget(sec("PARALLEL JOBS"))
        wr = QHBoxLayout(); wr.setSpacing(6)
        wr.addWidget(QLabel("Workers:"))
        self._threads_spin = QSpinBox(); self._threads_spin.setRange(1,16); self._threads_spin.setValue(4)
        self._threads_spin.setFixedWidth(55)
        wr.addWidget(self._threads_spin); wr.addStretch()
        cv.addLayout(wr)

        # Extra args
        cv.addWidget(sec("EXTRA FFMPEG ARGS"))
        self._extra_args = QLineEdit(); self._extra_args.setPlaceholderText("-af aecho=…")
        cv.addWidget(self._extra_args)

        cv.addStretch()

        # Action buttons
        cv.addWidget(sec(""))
        btn_row = QHBoxLayout(); btn_row.setSpacing(8)
        self._scan_btn = QPushButton("Scan & Queue"); self._scan_btn.setObjectName("ghost"); self._scan_btn.setFixedHeight(34)
        self._scan_btn.clicked.connect(self._scan)
        self._conv_btn = QPushButton("▶  Convert");   self._conv_btn.setObjectName("primary"); self._conv_btn.setFixedHeight(34)
        self._conv_btn.clicked.connect(self._start_convert)
        btn_row.addWidget(self._scan_btn); btn_row.addWidget(self._conv_btn)
        cv.addLayout(btn_row)

        # Sync button — full row, one-click scan+convert of new/changed files only
        self._sync_btn = QPushButton("⟳  Sync (find new & convert)")
        self._sync_btn.setObjectName("ghost")
        self._sync_btn.setFixedHeight(34)
        self._sync_btn.setToolTip(
            "Uses a snapshot of your library to instantly find only new or changed files.\n"
            "On first run it walks the full folder. After that it's near-instant.")
        self._sync_btn.clicked.connect(self._sync)
        cv.addWidget(self._sync_btn)

        ctrl_row = QHBoxLayout(); ctrl_row.setSpacing(8)
        self._pause_btn = QPushButton("⏸ Pause"); self._pause_btn.setObjectName("ghost")
        self._pause_btn.setFixedHeight(28); self._pause_btn.setCheckable(True); self._pause_btn.setEnabled(False)
        self._pause_btn.clicked.connect(self._toggle_pause)
        self._cancel_btn = QPushButton("Cancel"); self._cancel_btn.setObjectName("danger")
        self._cancel_btn.setFixedHeight(28); self._cancel_btn.setEnabled(False)
        self._cancel_btn.clicked.connect(self._cancel)
        clr_btn = QPushButton("Clear Memory"); clr_btn.setObjectName("ghost"); clr_btn.setFixedHeight(28)
        clr_btn.setToolTip("Forget all previously converted files")
        clr_btn.clicked.connect(self._clear_history)
        ctrl_row.addWidget(self._pause_btn); ctrl_row.addWidget(self._cancel_btn)
        ctrl_row.addWidget(clr_btn)
        cv.addLayout(ctrl_row)
        body.addWidget(cfg)

        # ── Right panel: stats + queue ─────────────────────────
        right = QVBoxLayout(); right.setSpacing(10)

        # Progress card
        prog_card = QWidget(); prog_card.setObjectName("panel")
        prog_card.setAttribute(Qt.WidgetAttribute.WA_StyledBackground, True)
        pv = QVBoxLayout(prog_card); pv.setContentsMargins(14,10,14,10); pv.setSpacing(6)
        pt = QHBoxLayout(); pt.addWidget(QLabel("Overall Progress")); pt.addStretch()
        self._prog_lbl = QLabel("Ready"); self._prog_lbl.setObjectName("muted")
        pt.addWidget(self._prog_lbl); pv.addLayout(pt)
        self._prog_bar = QProgressBar(); self._prog_bar.setRange(0,100); self._prog_bar.setValue(0)
        self._prog_bar.setFixedHeight(8); pv.addWidget(self._prog_bar)

        stats_row = QHBoxLayout(); stats_row.setSpacing(12)
        for attr, label, ck in [("_cs_queued","Queued","txt1"),("_cs_done","Done","success"),
                                  ("_cs_skipped","Skipped","txt2"),("_cs_errors","Errors","danger")]:
            w = QWidget(); w.setObjectName("card")
            w.setAttribute(Qt.WidgetAttribute.WA_StyledBackground, True)
            wl = QVBoxLayout(w); wl.setContentsMargins(10,6,10,6); wl.setSpacing(1)
            num = QLabel("0")
            nf = QFont(); nf.setPointSize(16); nf.setBold(True); num.setFont(nf)
            num.setAlignment(Qt.AlignmentFlag.AlignCenter)
            num.setStyleSheet(f"color:{t[ck]};background:transparent;")
            lbl2 = QLabel(label); lbl2.setAlignment(Qt.AlignmentFlag.AlignCenter); lbl2.setObjectName("muted")
            wl.addWidget(num); wl.addWidget(lbl2); setattr(self, attr, num); stats_row.addWidget(w)
        pv.addLayout(stats_row); right.addWidget(prog_card)

        # ── Queue + Skipped + Log tabs ─────────────────────────
        tabs_card = QWidget(); tabs_card.setObjectName("panel")
        tabs_card.setAttribute(Qt.WidgetAttribute.WA_StyledBackground, True)
        tabs_card_v = QVBoxLayout(tabs_card); tabs_card_v.setContentsMargins(0,0,0,0); tabs_card_v.setSpacing(0)

        # Tab bar header
        tab_hdr = QWidget(); tab_hdr.setFixedHeight(38)
        tab_hdr.setStyleSheet("background:rgba(255,255,255,0.05);border-radius:8px 8px 0 0;")
        tab_hdr_l = QHBoxLayout(tab_hdr); tab_hdr_l.setContentsMargins(14,0,8,0); tab_hdr_l.setSpacing(0)

        self._tab_queue_btn  = QPushButton("Conversion Queue")
        self._tab_skip_btn   = QPushButton("Skipped  (0)")
        self._tab_log_btn    = QPushButton("Log  (0)")
        for btn in (self._tab_queue_btn, self._tab_skip_btn, self._tab_log_btn):
            btn.setCheckable(True); btn.setFlat(True)
            btn.setFixedHeight(30)
            btn.setStyleSheet("""
                QPushButton {{
                    background: transparent; border: none; border-bottom: 2px solid transparent;
                    color: rgba(255,255,255,0.35); font-size: 12px; font-weight: 600;
                    padding: 0 14px; border-radius: 0;
                }}
                QPushButton:checked {{
                    color: {t['accent']}; border-bottom: 2px solid {t['accent']};
                }}
                QPushButton:hover:!checked {{ color: rgba(255,255,255,0.87); }}
            """)
        self._tab_queue_btn.setChecked(True)
        tab_hdr_l.addWidget(self._tab_queue_btn)
        tab_hdr_l.addWidget(self._tab_skip_btn)
        tab_hdr_l.addWidget(self._tab_log_btn)
        tab_hdr_l.addStretch()

        # Log filter combo (only shown when Log tab is active)
        self._log_filter_combo = QComboBox()
        self._log_filter_combo.addItems(["All", "Errors only"])
        self._log_filter_combo.setFixedHeight(24)
        self._log_filter_combo.setFixedWidth(110)
        self._log_filter_combo.setStyleSheet("font-size:11px;")
        self._log_filter_combo.setVisible(False)
        self._log_filter_combo.currentIndexChanged.connect(self._apply_log_filter)
        tab_hdr_l.addWidget(self._log_filter_combo)

        clrq = QPushButton("Clear Queue"); clrq.setObjectName("ghost"); clrq.setFixedHeight(24)
        clrq.clicked.connect(self._clear_queue)
        self._clrq_btn = clrq
        clrs = QPushButton("Clear"); clrs.setObjectName("ghost"); clrs.setFixedHeight(24)
        clrs.clicked.connect(self._clear_active_log)
        self._clrs_btn = clrs
        clrs.hide()
        tab_hdr_l.addWidget(clrq); tab_hdr_l.addWidget(clrs)
        tabs_card_v.addWidget(tab_hdr)

        self._queue_title_lbl = self._tab_queue_btn  # keep compat ref

        # Stacked body
        self._tab_stack = QStackedWidget()

        # Page 0: queue table
        self._queue_table = QTableWidget()
        self._queue_table.setColumnCount(5)
        self._queue_table.setHorizontalHeaderLabels(["File","Format","Size","Status","Progress"])
        self._queue_table.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeMode.Stretch)
        self._queue_table.setColumnWidth(1,80); self._queue_table.setColumnWidth(2,70)
        self._queue_table.setColumnWidth(3,80); self._queue_table.setColumnWidth(4,120)
        self._queue_table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self._queue_table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self._queue_table.setAlternatingRowColors(True)
        self._queue_table.setSortingEnabled(False)
        self._queue_table.verticalHeader().setVisible(False)
        self._queue_table.verticalHeader().setDefaultSectionSize(26)
        # Pagination nav bar for queue
        queue_page_widget = QWidget()
        queue_page_v = QVBoxLayout(queue_page_widget)
        queue_page_v.setContentsMargins(0, 0, 0, 0)
        queue_page_v.setSpacing(0)
        queue_page_v.addWidget(self._queue_table, stretch=1)

        queue_nav = QWidget()
        queue_nav.setFixedHeight(32)
        queue_nav.setStyleSheet(
            f"background:rgba(255,255,255,0.05);border-top:1px solid rgba(255,255,255,0.09);"
        )
        queue_nav_l = QHBoxLayout(queue_nav)
        queue_nav_l.setContentsMargins(8, 0, 8, 0)
        queue_nav_l.setSpacing(6)
        self._queue_prev_btn = QPushButton("◄  Prev")
        self._queue_prev_btn.setObjectName("ghost")
        self._queue_prev_btn.setFixedHeight(24)
        self._queue_prev_btn.setEnabled(False)
        self._queue_prev_btn.clicked.connect(self._queue_prev_page)
        self._queue_page_lbl = QLabel("Page 1")
        self._queue_page_lbl.setStyleSheet("color:rgba(255,255,255,0.35);font-size:11px;background:transparent;")
        self._queue_page_lbl.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self._queue_next_btn = QPushButton("Next  ►")
        self._queue_next_btn.setObjectName("ghost")
        self._queue_next_btn.setFixedHeight(24)
        self._queue_next_btn.setEnabled(False)
        self._queue_next_btn.clicked.connect(self._queue_next_page)
        queue_nav_l.addWidget(self._queue_prev_btn)
        queue_nav_l.addStretch()
        queue_nav_l.addWidget(self._queue_page_lbl)
        queue_nav_l.addStretch()
        queue_nav_l.addWidget(self._queue_next_btn)
        queue_page_v.addWidget(queue_nav)

        self._queue_current_page = 0
        self._QUEUE_PAGE_SIZE    = 500

        self._tab_stack.addWidget(queue_page_widget)

        # Page 1: skipped log — paginated table
        skip_page_widget = QWidget()
        skip_page_v = QVBoxLayout(skip_page_widget)
        skip_page_v.setContentsMargins(0, 0, 0, 0)
        skip_page_v.setSpacing(0)

        self._skip_table = QTableWidget()
        self._skip_table.setColumnCount(2)
        self._skip_table.setHorizontalHeaderLabels(["File", "Reason"])
        self._skip_table.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeMode.Stretch)
        self._skip_table.setColumnWidth(1, 200)
        self._skip_table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self._skip_table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self._skip_table.setAlternatingRowColors(True)
        self._skip_table.setSortingEnabled(False)
        self._skip_table.verticalHeader().setVisible(False)
        self._skip_table.verticalHeader().setDefaultSectionSize(24)
        skip_page_v.addWidget(self._skip_table, stretch=1)

        # Pagination nav bar
        skip_nav = QWidget()
        skip_nav.setFixedHeight(32)
        skip_nav.setStyleSheet(
            f"background:rgba(255,255,255,0.05);border-top:1px solid rgba(255,255,255,0.09);"
        )
        skip_nav_l = QHBoxLayout(skip_nav)
        skip_nav_l.setContentsMargins(8, 0, 8, 0)
        skip_nav_l.setSpacing(6)
        self._skip_prev_btn = QPushButton("◀  Prev")
        self._skip_prev_btn.setObjectName("ghost")
        self._skip_prev_btn.setFixedHeight(24)
        self._skip_prev_btn.setEnabled(False)
        self._skip_prev_btn.clicked.connect(self._skip_prev_page)
        self._skip_page_lbl = QLabel("Page 1")
        self._skip_page_lbl.setStyleSheet("color:rgba(255,255,255,0.35);font-size:11px;background:transparent;")
        self._skip_page_lbl.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self._skip_next_btn = QPushButton("Next  ▶")
        self._skip_next_btn.setObjectName("ghost")
        self._skip_next_btn.setFixedHeight(24)
        self._skip_next_btn.setEnabled(False)
        self._skip_next_btn.clicked.connect(self._skip_next_page)
        skip_nav_l.addWidget(self._skip_prev_btn)
        skip_nav_l.addStretch()
        skip_nav_l.addWidget(self._skip_page_lbl)
        skip_nav_l.addStretch()
        skip_nav_l.addWidget(self._skip_next_btn)
        skip_page_v.addWidget(skip_nav)

        # Internal state for pagination
        self._skip_all_entries: list[tuple] = []   # full list of (path_str, reason)
        self._skip_current_page = 0
        self._SKIP_PAGE_SIZE    = 500

        self._tab_stack.addWidget(skip_page_widget)

        # Page 2: conversion log (done + errors with ffmpeg output)
        self._log_table = QTableWidget()
        self._log_table.setColumnCount(3)
        self._log_table.setHorizontalHeaderLabels(["File", "Status", "Detail"])
        self._log_table.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeMode.Stretch)
        self._log_table.horizontalHeader().setSectionResizeMode(2, QHeaderView.ResizeMode.Stretch)
        self._log_table.setColumnWidth(1, 70)
        self._log_table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self._log_table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self._log_table.setAlternatingRowColors(True)
        self._log_table.setSortingEnabled(False)
        self._log_table.verticalHeader().setVisible(False)
        self._log_table.verticalHeader().setDefaultSectionSize(24)
        # Pagination nav bar for log
        log_page_widget = QWidget()
        log_page_v = QVBoxLayout(log_page_widget)
        log_page_v.setContentsMargins(0, 0, 0, 0)
        log_page_v.setSpacing(0)
        log_page_v.addWidget(self._log_table, stretch=1)

        log_nav = QWidget()
        log_nav.setFixedHeight(32)
        log_nav.setStyleSheet(
            f"background:rgba(255,255,255,0.05);border-top:1px solid rgba(255,255,255,0.09);"
        )
        log_nav_l = QHBoxLayout(log_nav)
        log_nav_l.setContentsMargins(8, 0, 8, 0)
        log_nav_l.setSpacing(6)
        self._log_prev_btn = QPushButton("◄  Prev")
        self._log_prev_btn.setObjectName("ghost")
        self._log_prev_btn.setFixedHeight(24)
        self._log_prev_btn.setEnabled(False)
        self._log_prev_btn.clicked.connect(self._log_prev_page)
        self._log_page_lbl = QLabel("Page 1")
        self._log_page_lbl.setStyleSheet("color:rgba(255,255,255,0.35);font-size:11px;background:transparent;")
        self._log_page_lbl.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self._log_next_btn = QPushButton("Next  ►")
        self._log_next_btn.setObjectName("ghost")
        self._log_next_btn.setFixedHeight(24)
        self._log_next_btn.setEnabled(False)
        self._log_next_btn.clicked.connect(self._log_next_page)
        log_nav_l.addWidget(self._log_prev_btn)
        log_nav_l.addStretch()
        log_nav_l.addWidget(self._log_page_lbl)
        log_nav_l.addStretch()
        log_nav_l.addWidget(self._log_next_btn)
        log_page_v.addWidget(log_nav)

        self._log_all_entries: list[tuple] = []   # (filename, ok, detail, full_path)
        self._log_current_page = 0
        self._LOG_PAGE_SIZE    = 500

        self._tab_stack.addWidget(log_page_widget)

        tabs_card_v.addWidget(self._tab_stack)
        right.addWidget(tabs_card, stretch=1)

        # Tab switching logic
        def _switch_to(idx):
            for i, btn in enumerate((self._tab_queue_btn, self._tab_skip_btn, self._tab_log_btn)):
                btn.setChecked(i == idx)
            self._tab_stack.setCurrentIndex(idx)
            self._clrq_btn.setVisible(idx == 0)
            self._clrs_btn.setVisible(idx != 0)
            self._log_filter_combo.setVisible(idx == 2)
        self._tab_queue_btn.clicked.connect(lambda: _switch_to(0))
        self._tab_skip_btn.clicked.connect(lambda: _switch_to(1))
        self._tab_log_btn.clicked.connect(lambda: _switch_to(2))
        self._switch_to_queue = lambda: _switch_to(0)
        self._switch_to_skip  = lambda: _switch_to(1)
        self._switch_to_log   = lambda: _switch_to(2)
        self._switch_tab = _switch_to

        body.addLayout(right, stretch=1)

    def _w(self, layout) -> QWidget:
        w = QWidget(); w.setLayout(layout); return w

    # ── Format/bitrate helpers ─────────────────────────────────

    def _on_fmt_changed(self, name: str):
        info  = _CONV_FORMATS.get(name, {})
        codec = info.get("codec","")
        opts  = _CONV_BITRATES.get(codec)
        self._bitrate_combo.blockSignals(True)
        self._bitrate_combo.clear()
        if opts:
            default = _CONV_DEFAULT_BITRATE.get(codec,"")
            for label, val in opts:
                self._bitrate_combo.addItem(label)
                if val == default or (val is None and "V0" in label and not default):
                    self._bitrate_combo.setCurrentIndex(self._bitrate_combo.count()-1)
            self._bitrate_combo.setEnabled(True)
        else:
            self._bitrate_combo.addItem("Lossless")
            self._bitrate_combo.setEnabled(False)
        self._bitrate_combo.blockSignals(False)

    def _quality_args(self) -> list:
        name  = self._fmt_combo.currentText()
        info  = _CONV_FORMATS.get(name,{})
        codec = info.get("codec","")
        opts  = _CONV_BITRATES.get(codec)
        if not opts:
            return []
        idx = self._bitrate_combo.currentIndex()
        if idx < 0 or idx >= len(opts):
            return []
        _, val = opts[idx]
        if codec == "libmp3lame":
            return ["-q:a","0"] if val is None else ["-b:a", val, "-q:a","0"]
        if codec == "libvorbis":
            return ["-q:a", val.replace("q","")]
        return ["-b:a", val] if val else []

    # ── Preset helpers ─────────────────────────────────────────

    def _current_settings(self) -> dict:
        return {
            "format":    self._fmt_combo.currentText(),
            "bitrate_idx": self._bitrate_combo.currentIndex(),
            "skip":      self._opt_skip.isChecked(),
            "tags":      self._opt_tags.isChecked(),
            "cover":     self._opt_cover.isChecked(),
            "cover_resize": self._opt_cover_resize.isChecked(),
            "cover_size":   self._cover_size_spin.value(),
            "struct":    self._opt_struct.isChecked(),
            "del_src":   self._opt_del_src.isChecked(),
            "normalize": self._opt_normalize.isChecked(),
            "resample":  self._opt_resample.isChecked(),
            "resample_rate": self._resample_combo.currentText(),
            "workers":   self._threads_spin.value(),
            "extra":     self._extra_args.text().strip(),
            "src":       self._src_edit.text().strip(),
            "dst":       self._dst_edit.text().strip(),
            "samefmt":   self._opt_samefmt.currentIndex(),
        }

    def _apply_settings(self, s: dict):
        idx = self._fmt_combo.findText(s.get("format",""))
        if idx >= 0: self._fmt_combo.setCurrentIndex(idx)
        bi = s.get("bitrate_idx", -1)
        if 0 <= bi < self._bitrate_combo.count(): self._bitrate_combo.setCurrentIndex(bi)
        self._opt_skip.setChecked(s.get("skip", True))
        self._opt_tags.setChecked(s.get("tags", True))
        self._opt_cover.setChecked(s.get("cover", True))
        self._opt_cover_resize.setChecked(s.get("cover_resize", False))
        self._cover_size_spin.setValue(s.get("cover_size", 500))
        self._opt_struct.setChecked(s.get("struct", True))
        self._opt_del_src.setChecked(s.get("del_src", False))
        self._opt_normalize.setChecked(s.get("normalize", False))
        self._opt_resample.setChecked(s.get("resample", False))
        ri = self._resample_combo.findText(s.get("resample_rate","44100 Hz"))
        if ri >= 0: self._resample_combo.setCurrentIndex(ri)
        self._threads_spin.setValue(s.get("workers", 4))
        self._extra_args.setText(s.get("extra",""))
        if s.get("src"): self._src_edit.setText(s["src"])
        if s.get("dst"): self._dst_edit.setText(s["dst"])
        self._opt_samefmt.setCurrentIndex(s.get("samefmt", 0))

    def _refresh_preset_combo(self):
        self._preset_combo.blockSignals(True)
        self._preset_combo.clear()
        for p in self._presets:
            self._preset_combo.addItem(p["name"])
        self._preset_combo.blockSignals(False)

    def _save_preset(self):
        name, ok = QInputDialog.getText(self, "Save Preset", "Preset name:")
        if not ok or not name.strip():
            return
        name = name.strip()
        s = self._current_settings()
        s["name"] = name
        # Replace if exists
        for i, p in enumerate(self._presets):
            if p["name"] == name:
                self._presets[i] = s
                self._save_presets(); self._refresh_preset_combo()
                return
        self._presets.append(s)
        self._save_presets(); self._refresh_preset_combo()
        # Select the new one
        idx = self._preset_combo.findText(name)
        if idx >= 0: self._preset_combo.setCurrentIndex(idx)

    def _load_preset(self):
        idx = self._preset_combo.currentIndex()
        if idx < 0 or idx >= len(self._presets):
            return
        self._apply_settings(self._presets[idx])

    def _delete_preset(self):
        idx = self._preset_combo.currentIndex()
        if idx < 0 or idx >= len(self._presets):
            return
        name = self._presets[idx]["name"]
        ans  = QMessageBox.question(self, "Delete Preset", f"Delete preset '{name}'?",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No)
        if ans == QMessageBox.StandardButton.Yes:
            self._presets.pop(idx); self._save_presets(); self._refresh_preset_combo()

    # ── File browsing ──────────────────────────────────────────

    def _browse_src_folder(self):
        d = QFileDialog.getExistingDirectory(self, "Source Folder", str(Path.home()))
        if d: self._src_edit.setText(d)

    def _browse_src_file(self):
        f, _ = QFileDialog.getOpenFileName(
            self, "Source Audio File", str(Path.home()),
            "Audio files (*.mp3 *.flac *.m4a *.aac *.ogg *.opus *.wma *.wav *.aiff);;All (*)")
        if f: self._src_edit.setText(f)

    def _browse_dst(self):
        d = QFileDialog.getExistingDirectory(self, "Output Folder", str(Path.home()))
        if d: self._dst_edit.setText(d)

    # ── Conversion memory ──────────────────────────────────────

    def _is_converted(self, path: Path, dst_root: str, dst_ext: str) -> bool:
        """
        Returns True if this file should be skipped.
        Checks BOTH the conversion memory DB and whether the output file
        already exists on disk — so clearing memory doesn't re-queue files
        that are already sitting in the output folder.
        """
        if not self._opt_skip.isChecked():
            return False
        # 1. Check if output file already exists in the destination folder
        dst_root_p = Path(dst_root)
        src_p      = Path(self._src_edit.text().strip())
        if src_p.is_dir():
            try:    rel = path.relative_to(src_p)
            except: rel = Path(path.name)
        else:
            rel = Path(path.name)
        if self._opt_struct.isChecked():
            dst_candidate = dst_root_p / rel.with_suffix(f".{dst_ext}")
        else:
            dst_candidate = dst_root_p / path.with_suffix(f".{dst_ext}").name
        if dst_candidate.exists():
            return True
        # 2. Fall back to DB memory check (scoped to active preset)
        preset = self._preset_combo.currentText() if self._preset_combo.currentIndex() >= 0 else ""
        try:
            con = sqlite3.connect(DB_FILE)
            row = con.execute(
                f"SELECT src_mtime FROM {_CONV_DB_TABLE} WHERE src_path=? AND preset_name=?",
                (str(path), preset)
            ).fetchone(); con.close()
            return bool(row and abs(row[0] - path.stat().st_mtime) < 1.0)
        except Exception:
            return False

    def _active_preset_name(self) -> str:
        """Return the currently selected preset name, or '' if none."""
        idx = self._preset_combo.currentIndex()
        if idx >= 0 and idx < len(self._presets):
            return self._presets[idx].get("name", "")
        return ""

    def _mark_converted(self, src: Path, dst: Path, fmt: str, preset_name: str = ""):
        try:
            con = sqlite3.connect(DB_FILE)
            con.execute(
                f"INSERT OR REPLACE INTO {_CONV_DB_TABLE} VALUES (?,?,?,?,?,?)",
                (str(src), src.stat().st_mtime, str(dst),
                 datetime.now().isoformat(timespec="seconds"), fmt, preset_name))
            con.commit(); con.close()
        except Exception:
            pass

    # ── Queue / scan ───────────────────────────────────────────

    def _scan(self):
        src = self._src_edit.text().strip()
        if not src or not Path(src).exists():
            QMessageBox.warning(self, "No Source", "Select a source folder or file first.")
            return
        if self._worker and self._worker.isRunning():
            return

        # Reset UI
        self._queue_files = []
        self._queue_table.setRowCount(0)
        self._skip_table.setRowCount(0)
        self._log_table.setRowCount(0)
        self._queue_current_page = 0
        self._queue_prev_btn.setEnabled(False)
        self._queue_next_btn.setEnabled(False)
        self._queue_page_lbl.setText("Page 1 / 1")
        self._log_all_entries.clear()
        self._log_current_page = 0
        self._log_prev_btn.setEnabled(False)
        self._log_next_btn.setEnabled(False)
        self._log_page_lbl.setText("Page 1 / 1")
        self._row_statuses = []
        self._tab_skip_btn.setText("Skipped  (0)")
        self._tab_log_btn.setText("Log  (0)")
        self._cs_queued.setText("0"); self._cs_skipped.setText("0")
        self._cs_done.setText("0"); self._cs_errors.setText("0")
        self._prog_lbl.setText("Scanning…")
        self._prog_bar.setRange(0, 0)  # indeterminate spinner
        self._scan_btn.setEnabled(False)
        self._conv_btn.setEnabled(False)
        self._sync_btn.setEnabled(False)

        fmt_name = self._fmt_combo.currentText()
        dst_ext  = _CONV_FORMATS[fmt_name]["ext"]
        self._scan_batch_buf = []

        self._scan_worker = _FileScanWorker(
            src, dst_ext,
            self._dst_edit.text().strip(),
            self._opt_skip.isChecked(),
            self._opt_struct.isChecked(),
            samefmt=self._opt_samefmt.currentIndex(),
            preset_name=self._active_preset_name(),
        )
        self._scan_worker.batch.connect(self._on_scan_batch)
        self._scan_worker.skipped_batch.connect(self._on_skipped_batch)
        self._scan_worker.done.connect(self._on_scan_done)
        self._scan_worker.status.connect(self._prog_lbl.setText)
        self._scan_worker.start()

    _TABLE_MAX_ROWS = 500  # rows per page in queue and log tables

    def _queue_table_title(self):
        total = len(self._queue_files)
        n_pages = max(1, (total + self._TABLE_MAX_ROWS - 1) // self._TABLE_MAX_ROWS)
        if n_pages <= 1:
            self._tab_queue_btn.setText(f"Conversion Queue  ({total:,})" if total else "Conversion Queue")
            self._queue_page_lbl.setText("Page 1 / 1")
        else:
            self._tab_queue_btn.setText(f"Conversion Queue  ({total:,})")
            self._queue_page_lbl.setText(
                f"Page {self._queue_current_page + 1} / {n_pages}  ({total:,} total)"
            )
        self._queue_prev_btn.setEnabled(self._queue_current_page > 0)
        self._queue_next_btn.setEnabled(self._queue_current_page < n_pages - 1)

    def _on_scan_batch(self, paths: list, skipped_count: int):
        for p_str in paths:
            self._queue_files.append(Path(p_str))
            self._scan_batch_buf.append(Path(p_str))
        self._cs_skipped.setText(str(skipped_count))
        self._flush_scan_batch()

    def _on_skipped_batch(self, entries: list):
        """Accumulate all (path, reason) entries and refresh the current page."""
        self._skip_all_entries.extend(entries)
        self._render_skip_page(self._skip_current_page)
        total = len(self._skip_all_entries)
        n_pages = max(1, (total + self._SKIP_PAGE_SIZE - 1) // self._SKIP_PAGE_SIZE)
        self._tab_skip_btn.setText(f"Skipped  ({total:,})")
        self._skip_page_lbl.setText(
            f"Page {self._skip_current_page + 1} / {n_pages}"
        )

    def _render_skip_page(self, page: int):
        """Fill the skip table with the entries for the given page number."""
        tbl = self._skip_table
        tbl.setUpdatesEnabled(False)
        tbl.setRowCount(0)
        start = page * self._SKIP_PAGE_SIZE
        end   = start + self._SKIP_PAGE_SIZE
        page_entries = self._skip_all_entries[start:end]
        for path_str, reason in page_entries:
            row = tbl.rowCount()
            tbl.insertRow(row)
            name_item = QTableWidgetItem(Path(path_str).name)
            name_item.setToolTip(path_str)
            tbl.setItem(row, 0, name_item)
            reason_item = QTableWidgetItem(reason)
            reason_item.setForeground(QColor(tok("txt2")))
            reason_item.setToolTip(path_str)
            tbl.setItem(row, 1, reason_item)
        tbl.setUpdatesEnabled(True)

        total = len(self._skip_all_entries)
        n_pages = max(1, (total + self._SKIP_PAGE_SIZE - 1) // self._SKIP_PAGE_SIZE)
        self._skip_prev_btn.setEnabled(page > 0)
        self._skip_next_btn.setEnabled(page < n_pages - 1)
        self._skip_page_lbl.setText(f"Page {page + 1} / {n_pages}  ({total:,} total)")

    def _skip_prev_page(self):
        if self._skip_current_page > 0:
            self._skip_current_page -= 1
            self._render_skip_page(self._skip_current_page)

    def _skip_next_page(self):
        total = len(self._skip_all_entries)
        n_pages = max(1, (total + self._SKIP_PAGE_SIZE - 1) // self._SKIP_PAGE_SIZE)
        if self._skip_current_page < n_pages - 1:
            self._skip_current_page += 1
            self._render_skip_page(self._skip_current_page)

    def _apply_log_filter(self):
        """Show all rows or errors-only rows in the log table based on the filter combo."""
        errors_only = self._log_filter_combo.currentText() == "Errors only"
        tbl = self._log_table
        visible = 0
        for row in range(tbl.rowCount()):
            status_item = tbl.item(row, 1)
            is_error = status_item is not None and "Error" in status_item.text()
            hide = errors_only and not is_error
            tbl.setRowHidden(row, hide)
            if not hide:
                visible += 1
        total_all = len(self._log_all_entries)
        total_errors = sum(1 for _, ok, _, _ in self._log_all_entries if not ok)
        if errors_only:
            self._tab_log_btn.setText(f"Log  ({total_errors:,} errors of {total_all:,})")
        else:
            self._tab_log_btn.setText(f"Log  ({total_all:,})")

    def _clear_skip_log(self):
        self._skip_table.setRowCount(0)
        self._skip_all_entries.clear()
        self._skip_current_page = 0
        self._skip_prev_btn.setEnabled(False)
        self._skip_next_btn.setEnabled(False)
        self._skip_page_lbl.setText("Page 1 / 1")
        self._tab_skip_btn.setText("Skipped  (0)")

    def _flush_scan_batch(self):
        if not self._scan_batch_buf:
            return
        self._scan_batch_buf.clear()
        self._cs_queued.setText(str(len(self._queue_files)))
        self._render_queue_page(self._queue_current_page)
        self._queue_table_title()

    def _render_queue_page(self, page: int):
        """Fill the queue table with the entries for the given page number."""
        tbl = self._queue_table
        fmt_name = self._fmt_combo.currentText()
        dst_ext  = _CONV_FORMATS[fmt_name]["ext"]
        tbl.setUpdatesEnabled(False)
        tbl.setRowCount(0)
        start = page * self._TABLE_MAX_ROWS
        end   = start + self._TABLE_MAX_ROWS
        page_files = self._queue_files[start:end]
        for f in page_files:
            row = tbl.rowCount()
            tbl.insertRow(row)
            tbl.setItem(row, 0, QTableWidgetItem(f.name))
            tbl.setItem(row, 1, QTableWidgetItem(dst_ext.upper()))
            try:
                size_str = f"{f.stat().st_size/1048576:.1f} MB"
            except OSError:
                size_str = "?"
            tbl.setItem(row, 2, QTableWidgetItem(size_str))
            # Show correct status: may already be converted if we navigated pages mid-run
            global_row = start + tbl.rowCount() - 1
            if global_row < len(getattr(self, "_row_statuses", [])):
                status, pct = self._row_statuses[global_row]
            else:
                status, pct = "Queued", 0
            tbl.setItem(row, 3, QTableWidgetItem(status))
            pb = QProgressBar(); pb.setRange(0, 100); pb.setValue(pct)
            pb.setFixedHeight(6); pb.setTextVisible(False)
            tbl.setCellWidget(row, 4, pb)
        tbl.setUpdatesEnabled(True)

    def _queue_prev_page(self):
        if self._queue_current_page > 0:
            self._queue_current_page -= 1
            self._render_queue_page(self._queue_current_page)
            self._queue_table_title()

    def _queue_next_page(self):
        total = len(self._queue_files)
        n_pages = max(1, (total + self._TABLE_MAX_ROWS - 1) // self._TABLE_MAX_ROWS)
        if self._queue_current_page < n_pages - 1:
            self._queue_current_page += 1
            self._render_queue_page(self._queue_current_page)
            self._queue_table_title()

    def _on_scan_done(self, total_queued: int, total_skipped: int):
        self._flush_scan_batch()
        self._prog_bar.setRange(0, 100); self._prog_bar.setValue(0)
        self._cs_queued.setText(str(total_queued))
        self._cs_skipped.setText(str(total_skipped))
        self._prog_lbl.setText(f"{total_queued:,} queued, {total_skipped:,} skipped")
        self._queue_table_title()
        self._scan_btn.setEnabled(True)
        self._conv_btn.setEnabled(True)
        self._sync_btn.setEnabled(True)
        if getattr(self, "_pending_convert_after_scan", False):
            self._pending_convert_after_scan = False
            if not self._queue_files:
                QMessageBox.information(self, "Nothing to Convert",
                    "No files queued — all already converted or folder is empty.")
                return
            self._start_convert()

    def _clear_queue(self):
        self._queue_files.clear()
        self._queue_table.setRowCount(0)
        self._cs_queued.setText("0")
        self._queue_table_title()

    def _sync(self):
        """One-click: find new/changed files using snapshot, then auto-convert."""
        src = self._src_edit.text().strip()
        if not src or not Path(src).exists():
            QMessageBox.warning(self, "No Source", "Select a source folder first.")
            return
        dst = self._dst_edit.text().strip()
        if not dst:
            QMessageBox.warning(self, "No Output", "Select an output folder first.")
            return
        if self._worker and self._worker.isRunning():
            return

        self._queue_files = []
        self._queue_table.setRowCount(0)
        self._skip_table.setRowCount(0)
        self._log_table.setRowCount(0)
        self._queue_current_page = 0
        self._queue_prev_btn.setEnabled(False)
        self._queue_next_btn.setEnabled(False)
        self._queue_page_lbl.setText("Page 1 / 1")
        self._log_all_entries.clear()
        self._log_current_page = 0
        self._log_prev_btn.setEnabled(False)
        self._log_next_btn.setEnabled(False)
        self._log_page_lbl.setText("Page 1 / 1")
        self._row_statuses = []
        self._tab_skip_btn.setText("Skipped  (0)")
        self._tab_log_btn.setText("Log  (0)")
        self._cs_queued.setText("0"); self._cs_skipped.setText("0")
        self._cs_done.setText("0"); self._cs_errors.setText("0")
        self._prog_lbl.setText("Syncing…")
        self._prog_bar.setRange(0, 0)
        self._scan_btn.setEnabled(False)
        self._conv_btn.setEnabled(False)
        self._sync_btn.setEnabled(False)

        fmt_name = self._fmt_combo.currentText()
        dst_ext  = _CONV_FORMATS[fmt_name]["ext"]
        self._scan_batch_buf = []
        self._pending_convert_after_scan = True  # auto-start convert when done

        self._scan_worker = _FileScanWorker(
            src, dst_ext, dst,
            self._opt_skip.isChecked(),
            self._opt_struct.isChecked(),
            sync_mode=True,
            samefmt=self._opt_samefmt.currentIndex(),
            preset_name=self._active_preset_name(),
        )
        self._scan_worker.batch.connect(self._on_scan_batch)
        self._scan_worker.skipped_batch.connect(self._on_skipped_batch)
        self._scan_worker.done.connect(self._on_scan_done)
        self._scan_worker.status.connect(self._prog_lbl.setText)
        self._scan_worker.start()

    def _update_snapshot(self):
        """
        Save the full file list seen during the last sync scan to the DB.
        Called after conversion finishes (or immediately after sync scan if nothing to convert).
        This is what makes the next sync fast — it compares against this snapshot.
        """
        if not hasattr(self, "_scan_worker") or not self._scan_worker:
            return
        if not self._scan_worker.all_seen:
            return
        src = self._src_edit.text().strip()
        if not src:
            return
        preset = self._active_preset_name()
        try:
            con = sqlite3.connect(DB_FILE)
            # Upsert all seen files
            con.executemany(
                f"INSERT OR REPLACE INTO {_CONV_SNAP_TABLE} "
                f"(src_folder, preset_name, src_path, src_mtime, src_size) VALUES (?,?,?,?,?)",
                [(src, preset, path, mtime, size)
                 for path, (mtime, size) in self._scan_worker.all_seen.items()]
            )
            # Remove files that no longer exist (deleted from source)
            seen_paths = set(self._scan_worker.all_seen.keys())
            existing = {row[0] for row in con.execute(
                f"SELECT src_path FROM {_CONV_SNAP_TABLE} WHERE src_folder=? AND preset_name=?",
                (src, preset)
            )}
            deleted = existing - seen_paths
            if deleted:
                con.executemany(
                    f"DELETE FROM {_CONV_SNAP_TABLE} WHERE src_folder=? AND preset_name=? AND src_path=?",
                    [(src, preset, p) for p in deleted]
                )
            con.commit(); con.close()
        except Exception:
            pass

    def _clear_history(self):
        preset = self._active_preset_name()
        label  = f"preset '{preset}'" if preset else "unsaved settings (no preset)"
        ans = QMessageBox.question(self, "Clear Memory",
            f"Forget all previously converted files for {label}?\n"
            f"Next scan will re-queue everything for this preset.",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No)
        if ans == QMessageBox.StandardButton.Yes:
            try:
                con = sqlite3.connect(DB_FILE)
                con.execute(f"DELETE FROM {_CONV_DB_TABLE} WHERE preset_name=?", (preset,))
                src = self._src_edit.text().strip()
                if src:
                    con.execute(
                        f"DELETE FROM {_CONV_SNAP_TABLE} WHERE src_folder=? AND preset_name=?",
                        (src, preset))
                con.commit(); con.close()
            except Exception:
                pass
            self._prog_lbl.setText(f"Memory cleared for {label}.")

    # ── Convert ────────────────────────────────────────────────

    def _start_convert(self):
        if not self._queue_files:
            self._pending_convert_after_scan = True
            self._scan()
            return
        self._pending_convert_after_scan = False
        dst = self._dst_edit.text().strip()
        if not dst:
            QMessageBox.warning(self, "No Output", "Select an output folder.")
            return

        src    = self._src_edit.text().strip()
        src_p  = Path(src)
        fmtname = self._fmt_combo.currentText()
        fmtinfo = _CONV_FORMATS[fmtname]

        opts = {
            "src_root":    str(src_p if src_p.is_dir() else src_p.parent),
            "dst_root":    dst,
            "fmt_name":    fmtname,
            "ext":         fmtinfo["ext"],
            "codec":       fmtinfo["codec"],
            "lossless":    fmtinfo.get("lossless", False),
            "quality_args":self._quality_args(),
            "preserve_tags":self._opt_tags.isChecked(),
            "embed_cover": self._opt_cover.isChecked(),
            "cover_resize":self._opt_cover_resize.isChecked(),
            "cover_size":  self._cover_size_spin.value(),
            "same_struct": self._opt_struct.isChecked(),
            "delete_src":  self._opt_del_src.isChecked(),
            "workers":     self._threads_spin.value(),
            "normalize":   self._opt_normalize.isChecked(),
            "resample":    self._resample_combo.currentText().split()[0]
                           if self._opt_resample.isChecked() else None,
            "extra_args":  self._extra_args.text().strip().split()
                           if self._extra_args.text().strip() else [],
            # 0=Skip, 1=Copy as-is, 2=Re-encode
            "samefmt":     self._opt_samefmt.currentIndex(),
            "preset_name": self._preset_combo.currentText() if self._preset_combo.currentIndex() >= 0 else "",
        }
        self._worker = _FileConvWorker(list(self._queue_files), opts)
        self._worker.file_progress.connect(self._on_file_progress)
        self._worker.file_done.connect(self._on_file_done)
        self._worker.overall_progress.connect(self._on_overall_progress)
        self._worker.finished.connect(self._on_conv_done)
        self._set_running(True)
        self._worker.start()

    def _set_running(self, running: bool):
        self._conv_btn.setEnabled(not running)
        self._scan_btn.setEnabled(not running)
        self._sync_btn.setEnabled(not running)
        self._pause_btn.setEnabled(running)
        self._cancel_btn.setEnabled(running)
        if not running:
            self._pause_btn.setChecked(False)
            self._pause_btn.setText("⏸ Pause")

    def _toggle_pause(self):
        if not self._worker: return
        if self._pause_btn.isChecked():
            self._pause_btn.setText("▶ Resume"); self._worker.pause()
        else:
            self._pause_btn.setText("⏸ Pause"); self._worker.resume()

    def _cancel(self):
        if self._worker: self._worker.cancel()
        self._set_running(False); self._prog_lbl.setText("Cancelled")

    def _on_file_progress(self, row: int, pct: int):
        # Track status for page rendering
        if not hasattr(self, "_row_statuses"):
            self._row_statuses = []
        while len(self._row_statuses) <= row:
            self._row_statuses.append(("Queued", 0))
        status, _ = self._row_statuses[row]
        self._row_statuses[row] = (status, pct)
        # Update table if this row is on the current visible page
        page_start = self._queue_current_page * self._TABLE_MAX_ROWS
        table_row = row - page_start
        if 0 <= table_row < self._TABLE_MAX_ROWS:
            pb = self._queue_table.cellWidget(table_row, 4)
            if pb: pb.setValue(pct)

    def _on_file_done(self, row: int, ok: bool, src: str, dst: str, fmt: str, error_log: str):
        status_text = "✓ Done" if ok else "✗ Error"
        if not hasattr(self, "_row_statuses"):
            self._row_statuses = []
        while len(self._row_statuses) <= row:
            self._row_statuses.append(("Queued", 0))
        self._row_statuses[row] = (status_text, 100 if ok else 0)
        page_start = self._queue_current_page * self._TABLE_MAX_ROWS
        table_row = row - page_start
        if 0 <= table_row < self._TABLE_MAX_ROWS:
            item = self._queue_table.item(table_row, 3)
            if item: item.setText(status_text)
            pb = self._queue_table.cellWidget(table_row, 4)
            if pb: pb.setValue(100 if ok else 0)
        if ok:
            self._cs_done.setText(str(int(self._cs_done.text())+1))
            preset = self._active_preset_name()
            self._mark_converted(Path(src), Path(dst), fmt, preset)
        else:
            self._cs_errors.setText(str(int(self._cs_errors.text())+1))
        # Always add to log tab
        self._append_log(Path(src).name, ok, error_log, src)
        # Auto-switch to log on first error
        if not ok and self._tab_stack.currentIndex() != 2 and int(self._cs_errors.text()) == 1:
            self._switch_tab(2)

    def _append_log(self, filename: str, ok: bool, detail: str, full_path: str = ""):
        self._log_all_entries.append((filename, ok, detail, full_path))
        total = len(self._log_all_entries)
        # Stay on last page as entries come in
        n_pages = max(1, (total + self._LOG_PAGE_SIZE - 1) // self._LOG_PAGE_SIZE)
        last_page = n_pages - 1
        if self._log_current_page == last_page or total <= self._LOG_PAGE_SIZE:
            self._render_log_page(self._log_current_page)
        self._apply_log_filter()

    def _render_log_page(self, page: int):
        """Fill the log table with entries for the given page, respecting current filter."""
        errors_only = getattr(self, "_log_filter_combo", None) and                       self._log_filter_combo.currentText() == "Errors only"
        tbl = self._log_table
        tbl.setUpdatesEnabled(False)
        tbl.setRowCount(0)
        start = page * self._LOG_PAGE_SIZE
        end   = start + self._LOG_PAGE_SIZE
        for filename, ok, detail, full_path in self._log_all_entries[start:end]:
            row = tbl.rowCount()
            tbl.insertRow(row)
            name_item = QTableWidgetItem(filename)
            name_item.setToolTip(full_path or filename)
            tbl.setItem(row, 0, name_item)
            status_item = QTableWidgetItem("✓ Done" if ok else "✗ Error")
            status_item.setForeground(QColor(tok("success") if ok else tok("danger")))
            tbl.setItem(row, 1, status_item)
            last_line = detail.strip().splitlines()[-1] if detail.strip() else ("" if ok else "Unknown error")
            detail_item = QTableWidgetItem(last_line)
            detail_item.setToolTip(detail if detail else "")
            detail_item.setForeground(QColor(tok("txt2")))
            tbl.setItem(row, 2, detail_item)
            if errors_only and ok:
                tbl.setRowHidden(row, True)
        tbl.setUpdatesEnabled(True)
        total = len(self._log_all_entries)
        n_pages = max(1, (total + self._LOG_PAGE_SIZE - 1) // self._LOG_PAGE_SIZE)
        self._log_prev_btn.setEnabled(page > 0)
        self._log_next_btn.setEnabled(page < n_pages - 1)
        self._log_page_lbl.setText(f"Page {page + 1} / {n_pages}  ({total:,} total)")

    def _log_prev_page(self):
        if self._log_current_page > 0:
            self._log_current_page -= 1
            self._render_log_page(self._log_current_page)
            self._apply_log_filter()

    def _log_next_page(self):
        total = len(self._log_all_entries)
        n_pages = max(1, (total + self._LOG_PAGE_SIZE - 1) // self._LOG_PAGE_SIZE)
        if self._log_current_page < n_pages - 1:
            self._log_current_page += 1
            self._render_log_page(self._log_current_page)
            self._apply_log_filter()

    def _clear_active_log(self):
        idx = self._tab_stack.currentIndex()
        if idx == 1:
            self._clear_skip_log()
        elif idx == 2:
            self._log_table.setRowCount(0)
            self._log_all_entries.clear()
            self._log_current_page = 0
            self._log_prev_btn.setEnabled(False)
            self._log_next_btn.setEnabled(False)
            self._log_page_lbl.setText("Page 1 / 1")
            self._tab_log_btn.setText("Log  (0)")
            if hasattr(self, "_log_filter_combo"):
                self._log_filter_combo.setCurrentIndex(0)

    def _on_overall_progress(self, pct: int, status: str):
        self._prog_bar.setValue(pct); self._prog_lbl.setText(status)

    def _on_conv_done(self):
        self._set_running(False)
        self._prog_lbl.setText("✓ Conversion complete")
        self._update_snapshot()


class _FileScanWorker(QThread):
    """
    Scans a source folder for audio files in a background thread.

    Two modes:
      sync_mode=False  →  normal scan, checks skip/DB like before
      sync_mode=True   →  snapshot mode: loads previous snapshot from DB,
                          only emits files that are new or have changed mtime/size.
                          After scan completes, caller should call update_snapshot().
    """
    batch          = pyqtSignal(list, int)   # ([path_str, ...], running_skipped_count)
    skipped_batch  = pyqtSignal(list)        # [(path_str, reason_str), ...]
    done           = pyqtSignal(int, int)    # (total_queued, total_skipped)
    status         = pyqtSignal(str)         # status text updates during scan

    BATCH_SIZE = 500

    def __init__(self, src: str, dst_ext: str, dst_root: str,
                 skip: bool, same_struct: bool, sync_mode: bool = False,
                 samefmt: int = 0, preset_name: str = ""):
        super().__init__()
        self._src        = src
        self._dst_ext    = dst_ext
        self._dst_root   = dst_root
        self._skip       = skip
        self._same_struct = same_struct
        self._sync_mode  = sync_mode
        self._samefmt    = samefmt  # 0=skip, 1=copy, 2=re-encode
        self._preset_name = preset_name
        # Populated during run() for snapshot update after conversion
        self.all_seen: dict[str, tuple] = {}  # path -> (mtime, size)

    def run(self):
        src_p   = Path(self._src)
        dst_ext = self._dst_ext
        queued  = 0
        skipped = 0
        batch: list[str] = []
        skipped_log: list[tuple] = []  # (path_str, reason_str)

        SKIP_BATCH = 200  # emit skipped entries in chunks to avoid signal flood

        if self._sync_mode:
            # ── Snapshot mode ─────────────────────────────────────
            # Load snapshot for this source folder
            self.status.emit("Loading snapshot…")
            snapshot: dict[str, tuple] = {}  # path -> (mtime, size)
            try:
                con = sqlite3.connect(DB_FILE)
                for row in con.execute(
                    f"SELECT src_path, src_mtime, src_size FROM {_CONV_SNAP_TABLE} "
                    f"WHERE src_folder=? AND preset_name=?", (str(src_p), self._preset_name)
                ):
                    snapshot[row[0]] = (row[1], row[2])
                con.close()
            except Exception:
                pass

            first_run = len(snapshot) == 0
            if first_run:
                self.status.emit("First run — building snapshot (this takes a moment)…")
            else:
                self.status.emit(f"Snapshot has {len(snapshot):,} files — checking for changes…")

            candidates = [src_p] if src_p.is_file() else (
                p for p in src_p.rglob("*") if p.is_file()
            )

            for f in candidates:
                suf = f.suffix.lower()
                if suf not in _CONV_SRC_EXTS:
                    continue
                is_same_fmt = (suf == f".{dst_ext}")
                if is_same_fmt and self._samefmt == 0:
                    continue  # skip
                try:
                    st = f.stat()
                    mtime, size = st.st_mtime, st.st_size
                except OSError:
                    continue

                # Always record this file in all_seen for snapshot update
                self.all_seen[str(f)] = (mtime, size)

                prev = snapshot.get(str(f))
                if prev is not None:
                    prev_mtime, prev_size = prev
                    if abs(prev_mtime - mtime) < 1.0 and prev_size == size:
                        # Unchanged — skip
                        skipped += 1
                        skipped_log.append((str(f), "Unchanged (snapshot)"))
                        if len(skipped_log) >= SKIP_BATCH:
                            self.skipped_batch.emit(skipped_log[:])
                            skipped_log.clear()
                        continue

                # New or changed file
                batch.append(str(f))
                queued += 1
                if len(batch) >= self.BATCH_SIZE:
                    self.batch.emit(batch, skipped)
                    batch = []

        else:
            # ── Normal scan mode ──────────────────────────────────
            # Load entire conversion history into memory once
            db_mtimes: dict[str, float] = {}
            if self._skip:
                self.status.emit("Loading conversion history…")
                try:
                    con = sqlite3.connect(DB_FILE)
                    for row in con.execute(
                        f"SELECT src_path, src_mtime FROM {_CONV_DB_TABLE} WHERE preset_name=?",
                        (self._preset_name,)
                    ):
                        db_mtimes[row[0]] = row[1]
                    con.close()
                except Exception:
                    pass

            dst_root_p = Path(self._dst_root) if self._dst_root else None

            # Build a set of all existing output-format stems in the destination
            # folder (lowercase) so we can skip by filename even when the subfolder
            # structure differs from what we'd construct.  Built once, O(1) lookup.
            dst_existing_stems: set[str] = set()
            if self._skip and dst_root_p:
                self.status.emit("Indexing destination folder…")
                try:
                    for dp in dst_root_p.rglob("*"):
                        if dp.is_file() and dp.suffix.lower() == f".{dst_ext}":
                            dst_existing_stems.add(dp.stem.lower())
                except (PermissionError, OSError):
                    pass

            self.status.emit("Scanning files…")

            candidates = [src_p] if src_p.is_file() else (
                p for p in src_p.rglob("*") if p.is_file()
            )

            for f in candidates:
                suf = f.suffix.lower()
                if suf not in _CONV_SRC_EXTS:
                    continue
                is_same_fmt = (suf == f".{dst_ext}")
                if is_same_fmt and self._samefmt == 0:
                    continue  # skip

                if self._skip:
                    skip_this = False
                    skip_reason = ""
                    if dst_root_p:
                        if self._same_struct and src_p.is_dir():
                            try:    rel = f.relative_to(src_p)
                            except: rel = Path(f.name)
                            dst_c = dst_root_p / rel.with_suffix(f".{dst_ext}")
                        else:
                            dst_c = dst_root_p / f.with_suffix(f".{dst_ext}").name
                        try:
                            if dst_c.exists():
                                skip_this = True
                                skip_reason = "Output exists (exact path)"
                        except (PermissionError, OSError):
                            pass
                        # Fallback: skip if any file with the same stem already
                        # exists anywhere in the destination, regardless of the
                        # subfolder layout (handles files converted by other apps).
                        if not skip_this:
                            if f.stem.lower() in dst_existing_stems:
                                skip_this = True
                                skip_reason = "Output exists (filename match)"
                    if not skip_this:
                        rec = db_mtimes.get(str(f))
                        if rec is not None:
                            try:
                                if abs(rec - f.stat().st_mtime) < 1.0:
                                    skip_this = True
                                    skip_reason = "Previously converted (DB)"
                            except OSError:
                                pass
                    if skip_this:
                        skipped += 1
                        skipped_log.append((str(f), skip_reason))
                        if len(skipped_log) >= SKIP_BATCH:
                            self.skipped_batch.emit(skipped_log[:])
                            skipped_log.clear()
                        continue

                batch.append(str(f))
                queued += 1
                if len(batch) >= self.BATCH_SIZE:
                    self.batch.emit(batch, skipped)
                    batch = []

        if batch:
            self.batch.emit(batch, skipped)
        if skipped_log:
            self.skipped_batch.emit(skipped_log[:])
        self.done.emit(queued, skipped)


class _FileConvWorker(QThread):
    file_progress    = pyqtSignal(int, int)
    file_done        = pyqtSignal(int, bool, str, str, str, str)  # +error_log
    overall_progress = pyqtSignal(int, str)

    def __init__(self, files: list, opts: dict):
        super().__init__()
        self._files    = files
        self._opts     = opts
        self._cancel_f = False
        self._pause_ev = _threading.Event()
        self._pause_ev.set()

    def cancel(self): self._cancel_f = True;  self._pause_ev.set()
    def pause(self):  self._pause_ev.clear()
    def resume(self): self._pause_ev.set()

    def run(self):
        import concurrent.futures
        opts      = self._opts
        src_root  = Path(opts["src_root"])
        dst_root  = Path(opts["dst_root"])
        total     = len(self._files)
        done_count = 0

        def convert_one(args):
            idx, fpath = args
            self._pause_ev.wait()
            if self._cancel_f:
                return idx, False, str(fpath), "", opts["fmt_name"], ""

            # Build destination path — same-format files keep their original extension
            is_same_fmt = fpath.suffix.lower() == f".{opts['ext']}"
            dst_suffix  = fpath.suffix if is_same_fmt else f".{opts['ext']}"

            if opts["same_struct"]:
                try:    rel = fpath.relative_to(src_root)
                except: rel = Path(fpath.name)
                dst_path = dst_root / rel.with_suffix(dst_suffix)
            else:
                dst_path = dst_root / fpath.with_suffix(dst_suffix).name
            dst_path.parent.mkdir(parents=True, exist_ok=True)

            # samefmt=1 → copy as-is, samefmt=2 → re-encode, otherwise normal convert
            samefmt = opts.get("samefmt", 0)
            error_log = ""
            if is_same_fmt and samefmt == 1:
                # Plain file copy, no FFmpeg
                try:
                    import shutil as _shutil
                    _shutil.copy2(str(fpath), str(dst_path))
                    ok = True
                except Exception as e:
                    ok = False
                    error_log = str(e)
            else:
                cmd = _build_ffmpeg_cmd(str(fpath), str(dst_path), opts)
                try:
                    result = subprocess.run(cmd, capture_output=True, timeout=600)
                    ok = result.returncode == 0
                    if not ok:
                        # Grab last 20 lines of stderr — enough to see the error
                        stderr_lines = result.stderr.decode("utf-8", errors="replace").strip().splitlines()
                        error_log = "\n".join(stderr_lines[-20:])
                except Exception as e:
                    ok = False
                    error_log = str(e)

            if ok and opts.get("delete_src"):
                try: fpath.unlink()
                except: pass

            return idx, ok, str(fpath), str(dst_path), opts["fmt_name"], error_log

        with concurrent.futures.ThreadPoolExecutor(max_workers=opts["workers"]) as pool:
            futures = {pool.submit(convert_one, (i,f)): i for i,f in enumerate(self._files)}
            for fut in concurrent.futures.as_completed(futures):
                if self._cancel_f: break
                try:
                    idx, ok, src, dst, fmt, error_log = fut.result()
                    done_count += 1
                    pct = int(done_count * 100 / total)
                    self.file_progress.emit(idx, 100 if ok else 0)
                    self.file_done.emit(idx, ok, src, dst, fmt, error_log)
                    self.overall_progress.emit(pct, f"{done_count}/{total}")
                except Exception:
                    pass



# ─────────────────────────────────────────────────────────────
#  MAIN WINDOW
# ─────────────────────────────────────────────────────────────

class UpdateChecker(QThread):
    """
    Checks GitHub releases API in the background for a newer version.
    Emits update_available(latest_version, release_url) if one is found.
    Silently does nothing on network errors — never bothers the user on failure.
    """
    update_available = pyqtSignal(str, str)  # (latest_version, html_url)

    def run(self):
        try:
            url = f"https://api.github.com/repos/{GITHUB_REPO}/releases/latest"
            resp = requests.get(url, timeout=8,
                                headers={"Accept": "application/vnd.github+json",
                                         "User-Agent": f"{APP_NAME}/{APP_VERSION}"})
            if resp.status_code != 200:
                return
            data = resp.json()
            tag = data.get("tag_name", "").lstrip("v")
            html_url = data.get("html_url", f"https://github.com/{GITHUB_REPO}/releases/latest")
            if not tag:
                return
            # Simple tuple comparison works for semver x.y.z
            def _parse(v):
                try:
                    return tuple(int(x) for x in v.split("."))
                except Exception:
                    return (0,)
            if _parse(tag) > _parse(APP_VERSION):
                self.update_available.emit(tag, html_url)
        except Exception:
            pass  # network down, timeout, etc — stay silent


class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Scrobbox")
        self.setMinimumSize(1020, 680)
        self.resize(1280, 820)

        conf = load_conf()
        self._conf = [conf]

        global _current_theme
        saved_theme = conf.get("theme", "dark")
        _current_theme = DARK.copy()

        if "custom_accent" in conf:
            c = conf["custom_accent"]; c2 = conf.get("custom_accent2", c)
            _current_theme["accent"] = c; _current_theme["accent2"] = c2
            _current_theme["accentlo"] = c+"1a"; _current_theme["bordhi"] = c+"55"

        self.setStyleSheet(build_stylesheet(_current_theme))
        self._current_platform = conf.get("last_platform", P_LASTFM)
        self._build_ui()
        self._nav_to(0)

        # Check for updates in the background — non-blocking
        self._update_checker = UpdateChecker()
        self._update_checker.update_available.connect(self._on_update_available)
        self._update_checker.start()

    def _build_ui(self):
        root = QWidget(); self.setCentralWidget(root)
        hbox = QHBoxLayout(root); hbox.setContentsMargins(0,0,0,0); hbox.setSpacing(0)

        # ── Sidebar ───────────────────────────────────────────
        sidebar_scroll = QScrollArea(); sidebar_scroll.setWidgetResizable(True)
        sidebar_scroll.setFrameShape(QFrame.Shape.NoFrame); sidebar_scroll.setFixedWidth(206)
        sidebar_scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)
        sidebar_scroll.setStyleSheet("QScrollArea{background:transparent;border:none;}")

        sidebar = QWidget(); sidebar.setObjectName("sidebar")
        sv = QVBoxLayout(sidebar); sv.setContentsMargins(12,20,12,16); sv.setSpacing(1)

        logo = QLabel("SCROBBOX")
        lf = QFont(); lf.setPointSize(14); lf.setWeight(QFont.Weight.Black)
        lf.setLetterSpacing(QFont.SpacingType.AbsoluteSpacing, 2.5); logo.setFont(lf)
        logo.setStyleSheet(f"color:{_current_theme['accent']};background:transparent;")
        sv.addWidget(logo)
        tagline = QLabel("rockbox companion"); tagline.setObjectName("muted")
        sv.addWidget(tagline); sv.addSpacing(18)

        sv.addWidget(SectionLabel("Scrobble to")); sv.addSpacing(5)
        self._plat_combo = QComboBox()
        self._plat_combo.addItems(ALL_PLATFORMS)
        self._plat_combo.setCurrentText(self._current_platform)
        self._plat_combo.currentTextChanged.connect(self._on_platform_change)
        sv.addWidget(self._plat_combo); sv.addSpacing(18)

        sv.addWidget(SectionLabel("Status")); sv.addSpacing(5)
        self._plat_status_widgets = {}
        for plat in ALL_PLATFORMS:
            row = QHBoxLayout(); row.setSpacing(7)
            dot = StatusDot(); lbl = QLabel(plat); lbl.setObjectName("muted")
            row.addWidget(dot); row.addWidget(lbl); row.addStretch()
            sv.addLayout(row); self._plat_status_widgets[plat] = dot
        self._refresh_platform_dots()
        sv.addSpacing(22); sv.addWidget(HDivider()); sv.addSpacing(14)

        # Nav pages — grouped logically, Settings always last
        pages = [
            ("Scrobble",        "♪"),
            ("Statistics",      "◉"),
            ("History",         "≡"),
            ("TIDAL DL",        "↓"),
            ("Spectrogram",     "◈"),
            ("Tag Editor",      "✎"),
            ("Cover Extractor", "⊞"),
            ("File Converter",  "⇌"),
            ("Rockbox Tools",   "⚙"),
            ("Rsync",           "⇄"),
            ("Settings",        "⊕"),
        ]
        _SECTION_BEFORE = {
            0: "Scrobbling",
            3: "Music Tools",
            8: "Device",
            10: "App",
        }
        self._nav_btns = []
        for i, (name, icon) in enumerate(pages):
            if i in _SECTION_BEFORE:
                sv.addSpacing(8)
                sv.addWidget(SectionLabel(_SECTION_BEFORE[i]))
                sv.addSpacing(4)
            btn = NavButton(name, icon)
            btn.setToolTip(f"Ctrl+{i+1}" if i < 9 else "")
            btn.clicked.connect(lambda _, idx=i: self._nav_to(idx))
            sv.addWidget(btn)
            self._nav_btns.append(btn)

        sv.addStretch()
        ver = QLabel("Made by Roy"); ver.setObjectName("muted"); sv.addWidget(ver)

        sidebar_scroll.setWidget(sidebar)
        hbox.addWidget(sidebar_scroll)

        # ── Content stack ─────────────────────────────────────
        self._stack = QStackedWidget()
        self._page_scrobble    = ScrobblePage(self._conf, self._get_platform)
        self._page_stats       = StatsPage(self._get_tracks, self._get_log_paths, self._conf)
        self._page_history     = HistoryPage()
        self._page_tidal       = TidalDownloaderPage(self._conf)
        self._page_spec        = SpectrogramPage()
        self._page_tags        = MusicTagEditorPage()
        self._page_covers      = AlbumCoverExtractorPage()
        self._page_converter   = FileConverterPage()
        self._page_rockbox     = RockboxToolsPage(self._conf)
        self._page_rsync       = RsyncPage(self._conf)
        self._page_settings    = SettingsPage(self._conf)

        self._page_scrobble.status_changed.connect(self._refresh_platform_dots)
        self._page_scrobble.art_ready.connect(self._page_history.set_bg_art)
        self._page_settings.auth_changed.connect(self._refresh_platform_dots)
        self._page_settings.auth_changed.connect(self._page_scrobble.refresh_for_platform)
        self._page_settings.theme_changed.connect(self._apply_theme)

        for page in [self._page_scrobble, self._page_stats,
                     self._page_history, self._page_tidal,
                     self._page_spec, self._page_tags,
                     self._page_covers, self._page_converter,
                     self._page_rockbox, self._page_rsync,
                     self._page_settings]:
            self._stack.addWidget(page)

        hbox.addWidget(self._stack, stretch=1)

        # Keyboard shortcuts: Ctrl+1 through Ctrl+9
        for i in range(9):
            sc = QShortcut(QKeySequence(f"Ctrl+{i+1}"), self)
            sc.activated.connect(lambda _, idx=i: self._nav_to(idx))

    def _on_update_available(self, latest: str, url: str):
        dlg = QDialog(self)
        dlg.setWindowTitle("Update Available")
        dlg.setMinimumWidth(400)
        dlg.setStyleSheet(self.styleSheet())
        v = QVBoxLayout(dlg); v.setContentsMargins(24, 20, 24, 20); v.setSpacing(14)

        icon_lbl = QLabel("🎉"); icon_lbl.setAlignment(Qt.AlignmentFlag.AlignCenter)
        icon_lbl.setStyleSheet("font-size:36px; background:transparent;")
        v.addWidget(icon_lbl)

        title = QLabel(f"Scrobbox {latest} is available")
        title.setObjectName("heading"); title.setAlignment(Qt.AlignmentFlag.AlignCenter)
        v.addWidget(title)

        current_lbl = QLabel(f"You are running version {APP_VERSION}.")
        current_lbl.setObjectName("secondary"); current_lbl.setAlignment(Qt.AlignmentFlag.AlignCenter)
        v.addWidget(current_lbl)

        v.addSpacing(4)
        btns = QHBoxLayout(); btns.setSpacing(10)
        later_btn = QPushButton("Later"); later_btn.setObjectName("ghost"); later_btn.setFixedHeight(38)
        download_btn = QPushButton("Download Update"); download_btn.setObjectName("primary"); download_btn.setFixedHeight(38)
        later_btn.clicked.connect(dlg.reject)
        download_btn.clicked.connect(lambda: (open_url(url), dlg.accept()))
        btns.addWidget(later_btn); btns.addWidget(download_btn)
        v.addLayout(btns)

        dlg.exec()

    def _nav_to(self, idx: int):
        self._stack.setCurrentIndex(idx)
        for i, btn in enumerate(self._nav_btns):
            btn.set_active(i == idx)
        if idx == 1:
            self._page_stats.refresh(self._get_tracks())
        elif idx == 2 and not self._page_history._all_rows:
            self._page_history.refresh()
    def _get_platform(self) -> str:
        return self._plat_combo.currentText()

    def _get_tracks(self) -> list[Track]:
        return self._page_scrobble.tracks

    def _get_log_paths(self) -> list[Path]:
        return self._page_scrobble.log_paths

    def _on_platform_change(self, plat: str):
        self._current_platform = plat
        c = self._conf[0]; c["last_platform"] = plat; save_conf(c)
        self._page_scrobble.refresh_for_platform()
        self._page_scrobble._refresh_queue_label()

    def _refresh_platform_dots(self):
        t = _current_theme
        for plat, dot in self._plat_status_widgets.items():
            if plat == P_LISTENBRAINZ:
                connected = bool(self._conf[0].get("lbz_token"))
            else:
                connected = bool(load_session(plat))
            dot.set_color(t["success"] if connected else t["txt2"])

    def _apply_theme(self, theme: dict):
        """Persist theme choice and ask for restart (no live apply).

        Live theme switching in large Qt apps with many per-widget stylesheets is fragile and can
        leave deleted widgets being referenced by background workers. For stability, we store the
        theme selection and prompt the user to restart the app.
        """
        try:
            c = self._conf[0] if isinstance(self._conf, list) else self._conf
            # Persist stable theme name
            c["theme"] = "dark"
            # Force restart-apply mode
            c["theme_apply"] = "restart"
            save_conf(c)
        except Exception:
            pass

        QMessageBox.information(
            self,
            "Restart required",
            "Theme changes take effect after restarting Scrobbox.\n\n"
            "Close and reopen the app to apply the new theme."
        )
        return

    def _get_running_tasks(self) -> list[str]:
        """Return a list of human-readable names for anything currently running."""
        running = []
        try:
            p = getattr(self._page_rsync, "_process", None)
            if p and p.state() != QProcess.ProcessState.NotRunning:
                running.append("Rsync transfer")
        except Exception:
            pass
        try:
            w = getattr(self._page_rsync, "_sanitize_worker", None)
            if w and w.isRunning():
                running.append("Filename sanitizer")
        except Exception:
            pass
        try:
            w = getattr(self._page_rockbox, "_db_worker", None)
            if w and w.isRunning():
                running.append("Database builder")
        except Exception:
            pass
        try:
            w = getattr(self._page_scrobble, "_worker", None)
            if w and w.isRunning():
                running.append("Scrobble submission")
        except Exception:
            pass
        try:
            if any(w for w in list(self._page_tidal._dl_wkrs.values())):
                running.append("TIDAL downloads")
        except Exception:
            pass
        try:
            if any(w for w in list(self._page_tidal._workers) if w and w.isRunning()):
                running.append("TIDAL search/metadata")
        except Exception:
            pass
        try:
            w = getattr(self._page_converter, "_worker", None)
            if w and w.isRunning():
                running.append("File conversion")
        except Exception:
            pass
        try:
            w = getattr(self._page_converter, "_scan_worker", None)
            if w and w.isRunning():
                running.append("File scan")
        except Exception:
            pass
        try:
            w = getattr(self._page_tags, "_rg_worker", None)
            if w and w.isRunning():
                running.append("ReplayGain strip")
        except Exception:
            pass
        try:
            w = getattr(self._page_tags, "_bulk_worker", None)
            if w and w.isRunning():
                running.append("Bulk cover resize")
        except Exception:
            pass
        try:
            w = getattr(self._page_tags, "_verify_worker", None)
            if w and w.isRunning():
                running.append("Integrity check")
        except Exception:
            pass
        try:
            w = getattr(self._page_tags, "_cluster_worker", None)
            if w and w.isRunning():
                running.append("Album clustering")
        except Exception:
            pass
        try:
            w = getattr(self._page_tags, "_rename_worker", None)
            if w and w.isRunning():
                running.append("File rename")
        except Exception:
            pass
        try:
            w = getattr(self._page_spec, "_worker", None)
            if w and w.isRunning():
                running.append("Spectrogram analysis")
        except Exception:
            pass
        try:
            w = getattr(self._page_covers, "_worker", None)
            if w and w.isRunning():
                running.append("Cover extraction")
        except Exception:
            pass
        return running

    def _kill_all_workers(self):
        """Forcefully stop all background workers and processes."""
        # Kill rsync — SIGKILL, cannot be ignored
        try:
            p = getattr(self._page_rsync, "_process", None)
            if p and p.state() != QProcess.ProcessState.NotRunning:
                p.kill()
                p.waitForFinished(2000)
        except Exception:
            pass
        # Stop sanitize worker
        try:
            w = getattr(self._page_rsync, "_sanitize_worker", None)
            if w and w.isRunning():
                w.requestInterruption()
                if not w.wait(1000):
                    w.terminate()
                    w.wait(500)
        except Exception:
            pass
        # Stop DB rebuild worker
        try:
            w = getattr(self._page_rockbox, "_db_worker", None)
            if w and w.isRunning():
                w.requestInterruption()
                if not w.wait(3000):
                    w.terminate()
                    w.wait(500)
        except Exception:
            pass
        # Stop scrobble worker
        try:
            w = getattr(self._page_scrobble, "_worker", None)
            if w and w.isRunning():
                w.requestInterruption()
                if not w.wait(2000):
                    w.terminate()
                    w.wait(500)
        except Exception:
            pass
        # Cancel all Tidal download workers
        try:
            for w in list(self._page_tidal._dl_wkrs.values()):
                try: w.cancel()
                except Exception: pass
        except Exception:
            pass
        # Stop all Tidal search/metadata workers
        try:
            for w in list(self._page_tidal._workers):
                try: w.quit(); w.wait(500)
                except Exception: pass
        except Exception:
            pass
        # Stop file converter worker
        try:
            w = getattr(self._page_converter, "_worker", None)
            if w and w.isRunning():
                w.cancel()
                if not w.wait(3000):
                    w.terminate()
                    w.wait(500)
        except Exception:
            pass
        # Stop file scan worker
        try:
            w = getattr(self._page_converter, "_scan_worker", None)
            if w and w.isRunning():
                w.requestInterruption()
                if not w.wait(2000):
                    w.terminate()
                    w.wait(500)
        except Exception:
            pass
        # Stop tag editor workers (RG strip, bulk cover resize, integrity check,
        #   cluster builder, file rename)
        for attr in ("_rg_worker", "_bulk_worker", "_verify_worker",
                     "_cluster_worker", "_rename_worker"):
            try:
                w = getattr(self._page_tags, attr, None)
                if w and w.isRunning():
                    w.requestInterruption()
                    if not w.wait(2000):
                        w.terminate()
                        w.wait(500)
            except Exception:
                pass
        # Stop spectrogram worker
        try:
            w = getattr(self._page_spec, "_worker", None)
            if w and w.isRunning():
                w.requestInterruption()
                w.quit()
                if not w.wait(1500):
                    w.terminate()
                    w.wait(500)
        except Exception:
            pass
        # Stop album cover extractor worker
        try:
            w = getattr(self._page_covers, "_worker", None)
            if w and w.isRunning():
                if hasattr(w, 'cancel'):
                    w.cancel()
                w.requestInterruption()
                if not w.wait(2000):
                    w.terminate()
                    w.wait(500)
        except Exception:
            pass
        # Stop eject worker (short-lived but let it finish gracefully)
        try:
            w = getattr(self._page_rockbox, "_eject_worker", None)
            if w and w.isRunning():
                w.requestInterruption()
                if not w.wait(500):
                    w.terminate()
                    w.wait(200)
        except Exception:
            pass
        # Stop library sanitize thread on rockbox page
        try:
            w = getattr(self._page_rockbox, "_sanitize_thread", None)
            if w and w.isRunning():
                w.requestInterruption()
                if not w.wait(1000):
                    w.terminate()
                    w.wait(500)
        except Exception:
            pass
        # Kill any subprocess still held by the DB worker
        try:
            w = getattr(self._page_rockbox, "_db_worker", None)
            proc = getattr(w, "_proc", None) if w else None
            if proc:
                try: proc.kill()
                except Exception: pass
        except Exception:
            pass
        # Kill any subprocess still held by the verify worker
        try:
            w = getattr(self._page_tags, "_verify_worker", None)
            proc = getattr(w, "_proc", None) if w else None
            if proc:
                try: proc.kill()
                except Exception: pass
        except Exception:
            pass

    def closeEvent(self, e):
        """Prompt if tasks are running, then kill everything and close."""
        running = self._get_running_tasks()
        if running:
            task_list = "\n".join(f"  • {t}" for t in running)
            ans = QMessageBox.warning(
                self,
                "Tasks still running",
                f"The following tasks are still running:\n\n{task_list}\n\n"
                "Closing now will stop them immediately. Continue?",
                QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.Cancel,
            )
            if ans != QMessageBox.StandardButton.Yes:
                e.ignore()
                return

        self._kill_all_workers()

        # Flush DB
        try:
            with _db_lock:
                _db.commit()
        except Exception:
            pass
        super().closeEvent(e)
        # Hard exit — kills any threads, subprocesses or Qt loops still alive
        os._exit(0)


if __name__ == "__main__":
    import multiprocessing
    multiprocessing.freeze_support()
    app = QApplication(sys.argv)
    app.setApplicationName("Scrobbox")
    _base = Path(getattr(sys, '_MEIPASS', Path(__file__).parent))
    _icon_path = _base / 'scrobbox.png'
    if _icon_path.exists():
        app.setWindowIcon(QIcon(str(_icon_path)))
    conf = load_conf()
    font = QFont(); font.setPointSize(conf.get("font_size", 13)); app.setFont(font)
    win = MainWindow(); win.show()
    sys.exit(app.exec())
