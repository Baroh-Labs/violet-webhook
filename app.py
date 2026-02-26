"""Violet Webhook Service — Real-time RetellAI -> Salesforce candidate handoff.

Receives chat_analyzed webhooks from RetellAI, classifies candidates,
deduplicates against Salesforce, and creates Job Applicant records.

Routes:
  POST /webhook/retell      — Receive & process chat_analyzed events
  GET  /health              — Health check (SF connection, uptime)
  GET  /status              — HTML monitoring dashboard
  POST /api/retry-failed    — Replay dead letter queue
"""

import hashlib
import hmac
import json
import logging
import os
import sys
import time
import threading
from datetime import datetime, timezone

from flask import Flask, request, jsonify, render_template

import violet_core
import dead_letter
from notify import send_notification
from salesforce_client import get_salesforce_credentials

# ══════════════════════════════════════════════════════════════════════
# APP SETUP
# ══════════════════════════════════════════════════════════════════════
app = Flask(__name__)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s [%(name)s] %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('webhook.log', encoding='utf-8'),
    ],
)
log = logging.getLogger('app')

# ══════════════════════════════════════════════════════════════════════
# IN-MEMORY STATS (reset on restart — fine at this volume)
# ══════════════════════════════════════════════════════════════════════
_stats_lock = threading.Lock()
_stats = {
    'start_time': datetime.now(timezone.utc).isoformat(),
    'webhooks_received': 0,
    'created': 0,
    'duplicates': 0,
    'skipped': 0,
    'errors': 0,
    'last_webhook': None,
    'last_created': None,
    'recent_events': [],  # Last 50 events
}

RETELL_API_KEY = os.environ.get('RETELL_API_KEY', '')
MAX_RECENT_EVENTS = 50


def _record_event(event_type, chat_id, detail):
    """Thread-safe stats update."""
    with _stats_lock:
        _stats['webhooks_received'] += 1
        _stats['last_webhook'] = datetime.now(timezone.utc).isoformat()

        if event_type == 'created':
            _stats['created'] += 1
            _stats['last_created'] = datetime.now(timezone.utc).isoformat()
        elif event_type == 'duplicate':
            _stats['duplicates'] += 1
        elif event_type == 'skip':
            _stats['skipped'] += 1
        elif event_type == 'error':
            _stats['errors'] += 1

        _stats['recent_events'].append({
            'time': datetime.now(timezone.utc).strftime('%H:%M:%S'),
            'type': event_type,
            'chat_id': chat_id[:12] + '...' if len(chat_id) > 12 else chat_id,
            'detail': str(detail)[:100],
        })
        if len(_stats['recent_events']) > MAX_RECENT_EVENTS:
            _stats['recent_events'] = _stats['recent_events'][-MAX_RECENT_EVENTS:]


# ══════════════════════════════════════════════════════════════════════
# SIGNATURE VERIFICATION
# ══════════════════════════════════════════════════════════════════════
def verify_retell_signature(payload_body, signature):
    """Verify RetellAI webhook signature using HMAC-SHA256.

    Args:
        payload_body: Raw request body bytes
        signature: Value of x-retell-signature header

    Returns:
        True if signature is valid, False otherwise
    """
    if not RETELL_API_KEY:
        log.warning("RETELL_API_KEY not set — skipping signature verification")
        return True

    if not signature:
        return False

    expected = hmac.new(
        RETELL_API_KEY.encode('utf-8'),
        payload_body,
        hashlib.sha256,
    ).hexdigest()

    return hmac.compare_digest(expected, signature)


# ══════════════════════════════════════════════════════════════════════
# ROUTES
# ══════════════════════════════════════════════════════════════════════

@app.route('/webhook/retell', methods=['POST'])
def webhook_retell():
    """Receive and process RetellAI chat_analyzed webhooks."""
    # 1. Verify signature
    raw_body = request.get_data()
    signature = request.headers.get('x-retell-signature', '')

    if not verify_retell_signature(raw_body, signature):
        log.warning("Invalid webhook signature — rejected")
        return '', 401

    # 2. Parse payload
    try:
        payload = json.loads(raw_body)
    except json.JSONDecodeError:
        log.warning("Invalid JSON in webhook body")
        return '', 400

    # 3. Only process chat_analyzed events
    event = payload.get('event', '')
    if event != 'chat_analyzed':
        log.info(f"Ignoring event type: {event}")
        return '', 204

    chat = payload.get('data', payload.get('chat', payload))
    chat_id = chat.get('chat_id', 'unknown')

    log.info(f"[{chat_id[:12]}] Received chat_analyzed webhook")

    # 4. Process through core pipeline
    try:
        result = violet_core.process_chat_webhook(
            chat,
            notify_fn=send_notification,
        )

        action = result.get('action', 'unknown')
        detail = result.get('detail', '')

        _record_event(action, chat_id, detail)

        # If SF create failed, save to dead letter
        if action == 'error':
            dead_letter.append(chat, result, detail)

    except Exception as e:
        log.exception(f"[{chat_id[:12]}] Unhandled error processing webhook")
        _record_event('error', chat_id, str(e))
        dead_letter.append(chat, {'chat_id': chat_id}, str(e))

    # Always return 204 — never make RetellAI retry (we handle retries ourselves)
    return '', 204


@app.route('/health', methods=['GET'])
def health():
    """Health check — verifies SF connectivity and returns uptime."""
    sf_ok = False
    sf_detail = ''
    try:
        token, url = get_salesforce_credentials()
        sf_ok = bool(token and url)
        sf_detail = url if sf_ok else 'no credentials'
    except Exception as e:
        sf_detail = str(e)[:200]

    start = datetime.fromisoformat(_stats['start_time'])
    uptime_seconds = (datetime.now(timezone.utc) - start).total_seconds()

    return jsonify({
        'status': 'healthy' if sf_ok else 'degraded',
        'salesforce': {
            'connected': sf_ok,
            'instance': sf_detail if sf_ok else None,
            'error': sf_detail if not sf_ok else None,
        },
        'uptime_seconds': int(uptime_seconds),
        'dead_letter_count': dead_letter.count(),
        'stats': {
            'webhooks_received': _stats['webhooks_received'],
            'created': _stats['created'],
            'errors': _stats['errors'],
        },
    })


@app.route('/status', methods=['GET'])
def status():
    """HTML dashboard showing service stats."""
    start = datetime.fromisoformat(_stats['start_time'])
    uptime_seconds = (datetime.now(timezone.utc) - start).total_seconds()

    hours = int(uptime_seconds // 3600)
    minutes = int((uptime_seconds % 3600) // 60)
    uptime_str = f"{hours}h {minutes}m"

    sf_ok = False
    sf_instance = ''
    try:
        token, url = get_salesforce_credentials()
        sf_ok = bool(token and url)
        sf_instance = url
    except Exception:
        pass

    dl_count = dead_letter.count()

    return render_template('status.html',
        uptime=uptime_str,
        sf_connected=sf_ok,
        sf_instance=sf_instance,
        webhooks_received=_stats['webhooks_received'],
        created=_stats['created'],
        duplicates=_stats['duplicates'],
        skipped=_stats['skipped'],
        errors=_stats['errors'],
        dead_letter_count=dl_count,
        last_webhook=_stats['last_webhook'] or 'never',
        last_created=_stats['last_created'] or 'never',
        recent_events=list(reversed(_stats['recent_events'][-20:])),
    )


@app.route('/api/retry-failed', methods=['POST'])
def retry_failed():
    """Replay all entries in the dead letter queue."""
    entries = dead_letter.read_all()
    if not entries:
        return jsonify({'message': 'Dead letter queue is empty', 'retried': 0})

    results = []
    for entry in entries:
        chat = entry.get('chat_payload', {})
        chat_id = entry.get('chat_id', 'unknown')

        try:
            result = violet_core.process_chat_webhook(
                chat,
                notify_fn=send_notification,
            )
            results.append({
                'chat_id': chat_id,
                'action': result.get('action'),
                'detail': result.get('detail'),
            })
            _record_event(result.get('action', 'retry'), chat_id, result.get('detail', ''))
        except Exception as e:
            results.append({
                'chat_id': chat_id,
                'action': 'error',
                'detail': str(e)[:200],
            })

    # Clear the dead letter queue (successes and new failures will be handled fresh)
    archive_path, cleared = dead_letter.clear()

    created = sum(1 for r in results if r['action'] == 'created')
    failed = sum(1 for r in results if r['action'] == 'error')

    return jsonify({
        'retried': len(results),
        'created': created,
        'failed': failed,
        'archived': archive_path,
        'results': results,
    })


# ══════════════════════════════════════════════════════════════════════
# STARTUP
# ══════════════════════════════════════════════════════════════════════
if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    log.info(f"Starting Violet Webhook Service on port {port}")
    app.run(host='0.0.0.0', port=port, debug=False)
