import os
import uuid
import glob
import json
import shlex
import subprocess
import threading
import time
from collections import defaultdict
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

from flask import Flask, request, jsonify, send_file, render_template
from werkzeug.exceptions import HTTPException
from werkzeug.middleware.proxy_fix import ProxyFix

app = Flask(__name__)
app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1)

DOWNLOAD_DIR = os.path.join(os.path.dirname(__file__), "downloads")
os.makedirs(DOWNLOAD_DIR, exist_ok=True)
COOKIES_FILE = os.path.join(os.path.dirname(__file__), "cookies.txt")
YTDLP_EXTRA_ARGS = shlex.split(os.environ.get("YTDLP_EXTRA_ARGS", ""))
YOUTUBE_FALLBACK_ARGS = shlex.split(
    os.environ.get("YOUTUBE_FALLBACK_ARGS", "--extractor-args youtube:player_client=web_embedded")
)

FILE_TTL_SECONDS = int(os.environ.get("FILE_TTL_SECONDS", 1800))
CLEANUP_INTERVAL_SECONDS = int(os.environ.get("CLEANUP_INTERVAL_SECONDS", 300))
RATE_LIMIT = int(os.environ.get("RATE_LIMIT", 30))
RATE_WINDOW = int(os.environ.get("RATE_WINDOW", 60))
MAX_CONCURRENT_DOWNLOADS = int(os.environ.get("MAX_CONCURRENT_DOWNLOADS", 4))

jobs = {}
jobs_lock = threading.Lock()
_rate_store = defaultdict(list)
rate_lock = threading.Lock()
_download_semaphore = threading.Semaphore(MAX_CONCURRENT_DOWNLOADS)


def cleanup_old_files():
    """Delete downloaded files and expired job entries periodically."""
    while True:
        try:
            now = time.time()
            for filename in os.listdir(DOWNLOAD_DIR):
                filepath = os.path.join(DOWNLOAD_DIR, filename)
                if os.path.isfile(filepath):
                    file_age = now - os.path.getmtime(filepath)
                    if file_age > FILE_TTL_SECONDS:
                        try:
                            os.remove(filepath)
                        except OSError:
                            pass

            with jobs_lock:
                expired_jobs = [
                    jid for jid, job in jobs.items()
                    if now - job.get("created_at", now) > FILE_TTL_SECONDS
                    and job.get("status") != "downloading"
                ]
                for jid in expired_jobs:
                    jobs.pop(jid, None)
        except Exception:
            pass
        time.sleep(CLEANUP_INTERVAL_SECONDS)


_cleanup_thread = threading.Thread(target=cleanup_old_files, daemon=True)
_cleanup_thread.start()


def clean_url(url):
    """Strip tracking parameters from YouTube URLs."""
    parsed = urlparse(url)
    if parsed.hostname in ("youtu.be", "www.youtube.com", "youtube.com", "m.youtube.com"):
        params = parse_qs(parsed.query)
        tracking = {"si", "feature", "utm_source", "utm_medium", "utm_campaign",
                    "utm_content", "utm_term", "pp", "cbrd", "ucbcb"}
        cleaned = {k: v for k, v in params.items() if k not in tracking}
        new_query = urlencode(cleaned, doseq=True)
        return urlunparse(parsed._replace(query=new_query))
    return url


def _ytdlp_base():
    """Build base yt-dlp command, with cookies if available."""
    cmd = ["yt-dlp", "--no-playlist"]
    if os.path.isfile(COOKIES_FILE):
        cmd += ["--cookies", COOKIES_FILE]
    cmd += YTDLP_EXTRA_ARGS
    return cmd


def _should_retry_with_fallback(stderr):
    text = (stderr or "").lower()
    return any(
        phrase in text
        for phrase in (
            "sign in to confirm you're not a bot",
            "sign in to confirm you’re not a bot",
            "could not fetch video",
            "the string did not match the expected pattern",
            "login required",
        )
    )


def _run_ytdlp(args, timeout):
    cmd = _ytdlp_base() + list(args)
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    if result.returncode != 0 and YOUTUBE_FALLBACK_ARGS and _should_retry_with_fallback(result.stderr):
        fallback_cmd = _ytdlp_base() + YOUTUBE_FALLBACK_ARGS + list(args)
        result = subprocess.run(fallback_cmd, capture_output=True, text=True, timeout=timeout)
    return result


def check_rate_limit():
    """Returns True if request should be allowed, False if rate limited."""
    ip = request.remote_addr or "unknown"
    now = time.time()
    with rate_lock:
        _rate_store[ip] = [t for t in _rate_store[ip] if now - t < RATE_WINDOW]
        if len(_rate_store[ip]) >= RATE_LIMIT:
            return False
        _rate_store[ip].append(now)
    return True


def run_download(job_id, url, format_choice, format_id):
    acquired = _download_semaphore.acquire(blocking=False)
    if not acquired:
        with jobs_lock:
            jobs[job_id]["status"] = "error"
            jobs[job_id]["error"] = "Server is busy. Please try again later."
        return

    try:
        url = clean_url(url)
        out_template = os.path.join(DOWNLOAD_DIR, f"{job_id}.%(ext)s")

        cmd = ["-o", out_template]

        if format_choice == "audio":
            cmd += ["-x", "--audio-format", "mp3"]
        elif format_id:
            cmd += ["-f", f"{format_id}+bestaudio/best", "--merge-output-format", "mp4"]
        else:
            cmd += ["-f", "bestvideo+bestaudio/best", "--merge-output-format", "mp4"]

        cmd.append(url)

        try:
            result = _run_ytdlp(cmd, timeout=300)
            if result.returncode != 0:
                with jobs_lock:
                    jobs[job_id]["status"] = "error"
                    jobs[job_id]["error"] = result.stderr.strip().split("\n")[-1]
                return

            files = glob.glob(os.path.join(DOWNLOAD_DIR, f"{job_id}.*"))
            if not files:
                with jobs_lock:
                    jobs[job_id]["status"] = "error"
                    jobs[job_id]["error"] = "Download completed but no file was found"
                return

            if format_choice == "audio":
                target = [f for f in files if f.endswith(".mp3")]
                chosen = target[0] if target else files[0]
            else:
                target = [f for f in files if f.endswith(".mp4")]
                chosen = target[0] if target else files[0]

            for f in files:
                if f != chosen:
                    try:
                        os.remove(f)
                    except OSError:
                        pass

            with jobs_lock:
                job = jobs[job_id]
                job["status"] = "done"
                job["file"] = chosen
                ext = os.path.splitext(chosen)[1]
                title = job.get("title", "").strip()
                if title:
                    safe_title = "".join(c for c in title if c not in r'\/:*?"<>|').strip()[:20].strip()
                    job["filename"] = f"{safe_title}{ext}" if safe_title else os.path.basename(chosen)
                else:
                    job["filename"] = os.path.basename(chosen)
        except subprocess.TimeoutExpired:
            with jobs_lock:
                jobs[job_id]["status"] = "error"
                jobs[job_id]["error"] = "Download timed out (5 min limit)"
        except Exception as e:
            with jobs_lock:
                jobs[job_id]["status"] = "error"
                jobs[job_id]["error"] = str(e)
    finally:
        _download_semaphore.release()


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/health")
def health_check():
    return jsonify({"status": "ok"}), 200


@app.route("/api/info", methods=["POST"])
def get_info():
    if not check_rate_limit():
        return jsonify({"error": "Too many requests. Please slow down."}), 429

    data = request.get_json(silent=True) or {}
    url = data.get("url", "").strip()

    if not url:
        return jsonify({"error": "No URL provided"}), 400
    if not url.startswith(("http://", "https://")):
        return jsonify({"error": "Invalid URL — must start with http:// or https://"}), 400
    if len(url) > 2048:
        return jsonify({"error": "URL too long"}), 400

    url = clean_url(url)
    cmd = ["-j", url]
    try:
        result = _run_ytdlp(cmd, timeout=60)
        if result.returncode != 0:
            return jsonify({"error": result.stderr.strip().split("\n")[-1]}), 400

        info = json.loads(result.stdout)

        best_by_height = {}
        for f in info.get("formats", []):
            height = f.get("height")
            if height and f.get("vcodec", "none") != "none":
                tbr = f.get("tbr") or 0
                if height not in best_by_height or tbr > (best_by_height[height].get("tbr") or 0):
                    best_by_height[height] = f

        formats = []
        for height, f in best_by_height.items():
            formats.append({
                "id": f["format_id"],
                "label": f"{height}p",
                "height": height,
            })
        formats.sort(key=lambda x: x["height"], reverse=True)

        return jsonify({
            "title": info.get("title", ""),
            "thumbnail": info.get("thumbnail", ""),
            "duration": info.get("duration"),
            "uploader": info.get("uploader", ""),
            "formats": formats,
        })
    except subprocess.TimeoutExpired:
        return jsonify({"error": "Timed out fetching video info"}), 400
    except Exception as e:
        return jsonify({"error": str(e)}), 400


@app.route("/api/download", methods=["POST"])
def start_download():
    if not check_rate_limit():
        return jsonify({"error": "Too many requests. Please slow down."}), 429

    data = request.get_json(silent=True) or {}
    url = data.get("url", "").strip()
    format_choice = data.get("format", "video")
    format_id = data.get("format_id")
    title = data.get("title", "")

    if not url:
        return jsonify({"error": "No URL provided"}), 400
    if not url.startswith(("http://", "https://")):
        return jsonify({"error": "Invalid URL — must start with http:// or https://"}), 400
    if len(url) > 2048:
        return jsonify({"error": "URL too long"}), 400
    if format_choice not in ("video", "audio"):
        return jsonify({"error": "Invalid format — must be 'video' or 'audio'"}), 400

    job_id = uuid.uuid4().hex[:10]
    with jobs_lock:
        jobs[job_id] = {
            "status": "downloading",
            "url": url,
            "title": title,
            "created_at": time.time(),
        }

    thread = threading.Thread(target=run_download, args=(job_id, url, format_choice, format_id))
    thread.daemon = True
    thread.start()

    return jsonify({"job_id": job_id})


@app.route("/api/status/<job_id>")
def check_status(job_id):
    with jobs_lock:
        job = jobs.get(job_id)
        if not job:
            return jsonify({"error": "Job not found"}), 404
        return jsonify({
            "status": job["status"],
            "error": job.get("error"),
            "filename": job.get("filename"),
        })


@app.route("/api/file/<job_id>")
def download_file(job_id):
    with jobs_lock:
        job = jobs.get(job_id)
        if not job or job["status"] != "done":
            return jsonify({"error": "File not ready"}), 404
        filepath = job["file"]
        filename = job["filename"]
    return send_file(filepath, as_attachment=True, download_name=filename)


@app.errorhandler(Exception)
def handle_exception(e):
    if isinstance(e, HTTPException):
        return e
    app.logger.exception("Unhandled exception")
    return jsonify({"error": "Internal server error"}), 500


@app.errorhandler(404)
def not_found(e):
    return jsonify({"error": "Not found"}), 404


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8899))
    host = os.environ.get("HOST", "127.0.0.1")
    app.run(host=host, port=port)
