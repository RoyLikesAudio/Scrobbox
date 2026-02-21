# Scrobbox

By Roy

A desktop companion app for Rockbox players and music libraries. Scrobbles your listening history, manages your Rockbox device, and handles your music collection — tag editing, format conversion, cover art, spectrograms, and more.


---

## Download

Grab the AppImage from the [Releases](../../releases) page. No install required.

```bash
chmod +x scrobbox-*.AppImage
./Scrobbox-x86_64.AppImage
```

**Requires ffmpeg** on your system for conversion, spectrogram, and integrity checking:
```bash
sudo apt install ffmpeg     # Debian/Ubuntu
sudo pacman -S ffmpeg       # Arch
sudo dnf install ffmpeg     # Fedora
```

---

## What it does

### Scrobbling
Submit your Rockbox `.scrobbler.log` to **Last.fm**, **Libre.fm**, or **ListenBrainz**. Handles all Rockbox timezone modes correctly (UTC, UNKNOWN, and explicit offsets). Also works with foobar2000 scrobble logs. Tracks what's already been submitted so you never double-scrobble. Dry run mode to preview before sending.

### Statistics
Local stats from your submission history — total tracks, play time, sessions, top artists, albums, and tracks with album art. Configurable session gap detection.

### Submission History
Full searchable, paginated log of everything you've submitted across all platforms, with timestamps.

### Tag Editor
Bulk tag editor for MP3, FLAC, M4A, OGG, and Opus. Edit title, artist, album, year, track number, disc, genre, and comment. Cover art viewer with resize and revert, bulk cover resize across all loaded files with bulk revert. Strip ReplayGain tags. Verify file integrity. Everything runs in background threads so the UI stays responsive.

### File Converter
FFmpeg-powered converter between FLAC, MP3, AAC/M4A, OGG Vorbis, Opus, WAV, and AIFF. Remembers which files have already been converted per preset, so re-scanning only queues new or changed files. Optional EBU R128 loudness normalization. Configurable bitrate and sample rate. Saveable presets.

### Album Cover Extractor
Scan a library folder and extract embedded cover art to folder images alongside each album. Optional BMP output sized for Rockbox displays.

### Spectrogram
Drag-and-drop audio file inspection. Visualizes frequency content so you can check whether a "lossless" file is genuine or an upsampled transcode.

### TIDAL Downloader
Search TIDAL by track, album, or artist and download. Quality falls back automatically from Hi-Res Lossless → CD Lossless → 320k depending on availability. Embeds full tags and cover art.

### Rockbox Tools
- **Database Rebuilder** — rebuild Rockbox tagcache `.tcd` files on your PC without booting into Rockbox. Detects new and changed files. Requires [DAP-DB-Manager](https://github.com/vakintosh/DAP-DB-Manager) — see installation note below.
- **config.cfg Editor** — editor for every Rockbox setting with descriptions and validation.
- **tagnavi.config Editor** — visual tree editor for Rockbox database navigation menus. Generates valid chained syntax automatically.

### Rsync
GUI for rsync with saved profiles. Presets for mirror, backup, and SSH remote sync. Safe revert using timestamped backup dirs. Filename sanitizer to strip characters that cause issues on FAT32.

---

## Running from source

```bash
git clone https://github.com/RoyLikesAudio/Scrobbox
cd scrobbox
pip install -r requirements.txt
python scrobbox.py
```

**System packages needed:**
- `ffmpeg` — file conversion, spectrogram, integrity check
- `rsync` — only if you use the Rsync page

**Optional Python packages** (app runs without them, but some features are limited or disabled):
- `numpy` — faster spectrogram rendering
- `PyQt6-WebEngine` — TIDAL web login flow

**DAP-DB-Manager** (required for Rockbox database rebuilder, not on PyPI):
```bash
git clone https://github.com/vakintosh/DAP-DB-Manager
cd DAP-DB-Manager
pip install .
```
Without this, the database rebuilder will not function. All other features work fine without it.

---

## Credits

Made by Roy.

### Third-party libraries



- **[hifi-api](https://github.com/binimum/hifi-api)** by sachin senal — TIDAL download functionality — MIT License
- **[DAP-DB-Manager](https://github.com/vakintosh/DAP-DB-Manager)** by vakintosh — Rockbox tagcache database building — GPL v2+
- **[rsync](https://github.com/rsyncproject/rsync)** — file sync engine used by the Rsync page — GPL v3
- **[mutagen](https://github.com/quodlibet/mutagen)** — audio tag reading and writing — GPL v2
- **[ffmpeg](https://ffmpeg.org)** — audio conversion, spectrogram rendering, integrity checking — LGPL v2.1+
- **[Pillow](https://python-pillow.org)** — image processing for cover art — HPND License
- **[numpy](https://numpy.org)** — spectrogram computation — BSD License

---

## License

MIT
# Scrobbox
