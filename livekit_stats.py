#!/usr/bin/env -S uv run
# /// script
# requires-python = ">=3.11"
# dependencies = [
#   "rich",
# ]
# ///

"""LiveKit log statistics analyzer."""

import json
import re
import sys
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from rich.console import Console
from rich.table import Table
from rich import box
from rich.panel import Panel

TZ = ZoneInfo("Europe/Berlin")  # CET/CEST auto-switch

# Regex to parse log lines: timestamp TAB level TAB logger TAB source TAB message TAB json
LINE_RE = re.compile(
    r'^(?P<ts>\S+)\t(?P<level>\w+)\t(?P<logger>\S+)\t(?P<source>\S+)\t(?P<msg>[^\t]+)\t(?P<json>.+)$'
)


def parse_duration(s: str) -> float:
    """Parse Go duration string like '27m11.417s' or '664.837µs' into seconds."""
    total = 0.0
    for match in re.finditer(r'([\d.]+)(h|ms|µs|ns|m|s)', s):
        val, unit = float(match.group(1)), match.group(2)
        total += val * {'h': 3600, 'ms': 0.001, 'µs': 1e-6, 'ns': 1e-9, 'm': 60, 's': 1}[unit]
    return total


def fmt_duration(secs: float) -> str:
    if secs < 0:
        return "?"
    h, rem = divmod(int(secs), 3600)
    m, s = divmod(rem, 60)
    if h:
        return f"{h}h {m:02d}m {s:02d}s"
    elif m:
        return f"{m}m {s:02d}s"
    else:
        return f"{s}s"


def to_local(ts: str) -> datetime:
    return datetime.fromisoformat(ts.replace('Z', '+00:00')).astimezone(TZ)


def fmt_ts(ts: str) -> str:
    return to_local(ts).strftime('%H:%M:%S')


def short_identity(identity: str) -> str:
    m = re.match(r'^@?([^/]+)', identity)
    return m.group(1) if m else identity


def main():
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} <logfile>")
        sys.exit(1)
    log_file = Path(sys.argv[1])
    console = Console(width=200)

    if not log_file.exists():
        console.print(f"[red]Log file not found: {log_file}[/red]")
        sys.exit(1)

    rooms: dict[str, dict] = {}
    participants: dict[str, dict] = {}

    for line in log_file.read_text().splitlines():
        line = line.strip()
        if not line:
            continue
        m = LINE_RE.match(line)
        if not m:
            continue

        ts, level, msg, json_str = m.group('ts'), m.group('level'), m.group('msg'), m.group('json')
        try:
            data = json.loads(json_str)
        except json.JSONDecodeError:
            continue

        room_id = data.get('roomID')
        room_name = data.get('room', '')
        pid = data.get('pID')

        # ── RTC session start ─────────────────────────────────────────────────
        if 'starting RTC session' in msg and room_id:
            if room_id not in rooms:
                rooms[room_id] = {
                    'roomID': room_id, 'name': room_name,
                    'created_at': ts, 'closed_at': None, 'participants': [],
                    'active_identities': set(),
                }
            if pid and pid not in participants:
                client = data.get('participantInit', {}).get('Client', {})
                participants[pid] = {
                    'pID': pid, 'roomID': room_id,
                    'username': short_identity(data.get('participant', '')),
                    'join_ts': ts, 'close_ts': None,
                    'session_duration_s': None, 'leave_reason': None,
                    'os': client.get('os', '?'), 'device': client.get('deviceModel', '?'),
                }
                rooms[room_id]['participants'].append(pid)

        # ── Participant active (ICE connected, media flowing) ─────────────────
        if 'participant active' in msg and room_id and room_id in rooms:
            identity = data.get('participant', '')
            if identity:
                rooms[room_id]['active_identities'].add(short_identity(identity))

        # ── Participant closing (also handles participants first seen here) ───
        if 'participant closing' in msg and pid and room_id:
            if room_id not in rooms:
                rooms[room_id] = {
                    'roomID': room_id, 'name': room_name,
                    'created_at': ts, 'closed_at': None, 'participants': [],
                    'active_identities': set(),
                }
            if pid not in participants:
                client = data.get('clientInfo', {})
                participants[pid] = {
                    'pID': pid, 'roomID': room_id,
                    'username': short_identity(data.get('participant', '')),
                    'join_ts': None, 'close_ts': None,
                    'session_duration_s': None, 'leave_reason': None,
                    'os': client.get('os', '?'), 'device': client.get('deviceModel', '?'),
                }
                rooms[room_id]['participants'].append(pid)
            sd = data.get('sessionDuration', '')
            participants[pid]['close_ts'] = ts
            participants[pid]['session_duration_s'] = parse_duration(sd) if sd else None
            participants[pid]['leave_reason'] = data.get('reason', '?')

        # ── Room closed ───────────────────────────────────────────────────────
        if msg == 'room closed' and room_id and room_id in rooms:
            rooms[room_id]['closed_at'] = ts

    # ── Render ───────────────────────────────────────────────────────────────

    if not rooms:
        console.print("[yellow]No rooms found in log.[/yellow]")
        return

    all_pts = list(participants.values())
    all_sessions_s = [p['session_duration_s'] for p in all_pts if p['session_duration_s'] is not None]

    timestamps = [r['created_at'] for r in rooms.values()] + \
                 [r['closed_at'] for r in rooms.values() if r['closed_at']]
    log_start, log_end = min(timestamps), max(timestamps)

    # Determine timezone abbreviation from the first timestamp
    tz_abbr = to_local(log_start).strftime('%Z')

    success_calls = sum(1 for r in rooms.values() if r['closed_at'] and r['participants'])
    fail_calls = len(rooms) - success_calls

    date_str = to_local(log_start).strftime('%Y-%m-%d')
    unique_participants = len({(p['roomID'], p['username']) for p in all_pts})
    summary = f"LiveKit  ·  {date_str} {tz_abbr}  ·  {len(rooms)} calls  ·  {unique_participants} participants  ·  avg {fmt_duration(sum(all_sessions_s)/len(all_sessions_s)) if all_sessions_s else 'N/A'}"

    table = Table(box=box.ROUNDED, show_lines=False, header_style="bold magenta",
                  title=summary, title_style="bold cyan", title_justify="left")
    table.add_column("Status")
    table.add_column("Duration", justify="right", no_wrap=True)
    table.add_column("Start", no_wrap=True)
    table.add_column("End", no_wrap=True)
    table.add_column("User", style="bold")
    table.add_column("OS / Device", no_wrap=True, max_width=20)

    for i, (rid, room) in enumerate(rooms.items()):
        if i > 0:
            table.add_section()

        start = fmt_ts(room['created_at'])
        end = fmt_ts(room['closed_at']) if room['closed_at'] else "[yellow]open[/yellow]"

        if room['closed_at']:
            duration_s = (to_local(room['closed_at']) - to_local(room['created_at'])).total_seconds()
            duration = fmt_duration(duration_s)
        else:
            duration_s = -1
            duration = "?"

        room_pts = [participants[p] for p in room['participants'] if p in participants]

        if not room_pts:
            table.add_row("", duration, start, end, "[dim]—[/dim]", "")
            continue

        # Deduplicate by identity: keep last session per user (latest close_ts)
        by_user: dict[str, dict] = {}
        for p in room_pts:
            prev = by_user.get(p['username'])
            if prev is None or (p['close_ts'] or '') > (prev['close_ts'] or ''):
                by_user[p['username']] = p

        # Determine single call-level status
        any_error = any(
            p['leave_reason'] != 'CLIENT_REQUEST_LEAVE'
            for p in room_pts if p['leave_reason']
        )
        n_active = len(room.get('active_identities', set()))
        established = n_active >= 2  # both parties reached active (ICE connected)

        if any_error:
            call_status = "[red]error[/red]"
        elif not established:
            call_status = "[red]no conn[/red]"
        else:
            call_status = "[green]clean[/green]"

        for j, p in enumerate(by_user.values()):
            os_dev = f"{p['os']} / {p['device']}"
            if j == 0:
                table.add_row(call_status, duration, start, end, p['username'], os_dev)
            else:
                table.add_row("", "", "", "", p['username'], os_dev)

    console.print(table)


if __name__ == '__main__':
    main()
