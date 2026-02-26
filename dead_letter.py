"""Dead Letter Queue — File-based queue for failed SF creates.

Ensures no lead is ever lost, even if Salesforce is down.
Stores failed records in dead_letter.jsonl for replay via /api/retry-failed.
"""

import json
import os
import shutil
import threading
from datetime import datetime, timezone

DEAD_LETTER_FILE = 'dead_letter.jsonl'
_lock = threading.Lock()


def append(chat, record, error):
    """Append a failed record to the dead letter queue.

    Args:
        chat: Full chat payload from RetellAI
        record: The record we attempted to create
        error: Error message or details
    """
    entry = {
        'timestamp': datetime.now(timezone.utc).isoformat(),
        'chat_id': chat.get('chat_id', 'unknown'),
        'contact_id': record.get('contact_id', ''),
        'job_id': record.get('job_id', ''),
        'stage': record.get('stage', ''),
        'tier': record.get('tier', ''),
        'error': str(error),
        'chat_payload': chat,
    }

    with _lock:
        with open(DEAD_LETTER_FILE, 'a', encoding='utf-8') as f:
            f.write(json.dumps(entry, default=str) + '\n')


def read_all():
    """Read all entries from the dead letter queue.

    Returns:
        list of dicts, one per failed record
    """
    entries = []
    if not os.path.exists(DEAD_LETTER_FILE):
        return entries

    with _lock:
        with open(DEAD_LETTER_FILE, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        entries.append(json.loads(line))
                    except json.JSONDecodeError:
                        continue
    return entries


def count():
    """Return the number of entries in the dead letter queue."""
    if not os.path.exists(DEAD_LETTER_FILE):
        return 0

    n = 0
    with _lock:
        with open(DEAD_LETTER_FILE, 'r', encoding='utf-8') as f:
            for line in f:
                if line.strip():
                    n += 1
    return n


def clear():
    """Archive the current dead letter file and reset.

    Returns:
        (archived_path, entry_count) or (None, 0) if nothing to clear
    """
    if not os.path.exists(DEAD_LETTER_FILE):
        return None, 0

    with _lock:
        # Count entries
        n = 0
        with open(DEAD_LETTER_FILE, 'r', encoding='utf-8') as f:
            for line in f:
                if line.strip():
                    n += 1

        if n == 0:
            return None, 0

        # Archive with timestamp
        ts = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')
        archive_path = f'dead_letter_archive_{ts}.jsonl'
        shutil.move(DEAD_LETTER_FILE, archive_path)

    return archive_path, n
