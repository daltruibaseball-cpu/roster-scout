#!/usr/bin/env python3
"""
Axis Baseball — Roster Scout Web App
Flask backend: accepts team URL + email, scrapes roster/stats,
creates a Google Sheet in Jake's account, shares it with the requester.

Run:
    python3 app.py

Then open http://localhost:5000
"""

import os
import re
import json
from datetime import datetime
from flask import Flask, render_template, request, jsonify
import gspread
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build as google_build

from roster_scraper import (
    get_roster_url,
    get_stats_url,
    get_team_label,
    scrape_roster,
    scrape_stats,
    merge,
    split_roster,
)

# ── Config ────────────────────────────────────────────────────────────────────

TOKEN_FILE      = os.environ.get("GOOGLE_TOKEN_FILE", "token.json")
DRIVE_FOLDER_ID = "1LE_lwaDLnoPD7ORvn3ha-ZUlB9dmJMiW"  # Jake's "Roster Sheets" folder

app = Flask(__name__)

# ── Routes ────────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return render_template("index.html")


@app.route("/scrape", methods=["POST"])
def scrape():
    body = request.get_json(silent=True) or {}
    url   = (body.get("url")   or "").strip()
    email = (body.get("email") or "").strip()

    if not url:
        return jsonify({"error": "Please enter a team URL."}), 400
    if not email or not re.match(r"[^@]+@[^@]+\.[^@]+", email):
        return jsonify({"error": "Please enter a valid email address."}), 400

    try:
        roster_url = get_roster_url(url)
        stats_url  = get_stats_url(roster_url)
        team_label = get_team_label(roster_url)

        # Scrape roster
        roster = scrape_roster(roster_url)
        if not roster:
            return jsonify({
                "error": (
                    "No players found on that page. "
                    "Try pasting the exact roster page URL "
                    "(e.g. .../sports/baseball/roster). "
                    "Some sites load players via JavaScript — contact Jake if this keeps happening."
                )
            }), 422

        # Scrape stats (optional — won't fail if unavailable)
        stats = scrape_stats(stats_url)

        # Merge + split into hitters / pitchers
        rows = merge(roster, stats)
        hitters, pitchers = split_roster(rows)

        # Write to Sheets + share
        sheet_url = _create_and_share(hitters, pitchers, team_label, email)

        return jsonify({
            "success":   True,
            "sheet_url": sheet_url,
            "team":      team_label,
            "players":   len(rows),
        })

    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


# ── Google auth helper ────────────────────────────────────────────────────────

def _get_creds() -> Credentials:
    """Load Jake's OAuth2 token, refreshing it if expired."""
    token_json = os.environ.get("GOOGLE_TOKEN_JSON")
    if token_json:
        creds = Credentials.from_authorized_user_info(json.loads(token_json))
    else:
        creds = Credentials.from_authorized_user_file(TOKEN_FILE)
    if creds.expired and creds.refresh_token:
        creds.refresh(Request())
        if not token_json:
            with open(TOKEN_FILE, "w") as f:
                f.write(creds.to_json())
    return creds


# ── Google Sheets helper ──────────────────────────────────────────────────────

def _write_tab(spreadsheet, title: str, data: list[dict]):
    """Write one tab into an existing spreadsheet."""
    if not data:
        return
    try:
        ws = spreadsheet.worksheet(title)
        ws.clear()
    except gspread.WorksheetNotFound:
        ws = spreadsheet.add_worksheet(title=title, rows=300, cols=30)

    col_headers = list(data[0].keys())
    rows = [[p.get(h, "") for h in col_headers] for p in data]
    ws.update([col_headers] + rows)

    ws.format("1:1", {
        "textFormat": {
            "bold": True,
            "foregroundColor": {"red": 1.0, "green": 1.0, "blue": 1.0},
        },
        "backgroundColor": {"red": 0.145, "green": 0.388, "blue": 0.922},
    })

    try:
        spreadsheet.batch_update({"requests": [{"autoResizeDimensions": {"dimensions": {
            "sheetId": ws.id, "dimension": "COLUMNS",
            "startIndex": 0, "endIndex": len(col_headers),
        }}}]})
    except Exception:
        pass


def _create_and_share(hitters: list[dict], pitchers: list[dict], team_label: str, email: str) -> str:
    """Create a new Google Sheet in Jake's Drive with two tabs, share with user."""
    creds = _get_creds()
    gc    = gspread.authorize(creds)

    month_year = datetime.now().strftime("%b %Y")
    sheet_name = f"{team_label} Baseball — {month_year}"

    # Create inside Jake's Drive folder
    drive = google_build("drive", "v3", credentials=creds)
    file_meta = {
        "name": sheet_name,
        "mimeType": "application/vnd.google-apps.spreadsheet",
        "parents": [DRIVE_FOLDER_ID],
    }
    created = drive.files().create(body=file_meta, fields="id").execute()
    spreadsheet = gc.open_by_key(created["id"])

    # Rename the default Sheet1 tab to Hitters, add Pitchers tab
    default_ws = spreadsheet.sheet1
    default_ws.update_title("Hitters")
    _write_tab(spreadsheet, "Hitters",  hitters)
    _write_tab(spreadsheet, "Pitchers", pitchers)

    total = len(hitters) + len(pitchers)
    spreadsheet.share(
        email,
        perm_type="user",
        role="writer",
        notify=True,
        email_message=(
            f"Your {team_label} baseball scouting sheet is ready — "
            f"{len(hitters)} hitters, {len(pitchers)} pitchers. Compiled by Axis Baseball."
        ),
    )

    return f"https://docs.google.com/spreadsheets/d/{spreadsheet.id}"


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(debug=False, port=port)
