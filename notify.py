"""Notification Dispatcher — Structured logging + optional Slack alerts.

Phase 1: Structured logging of all creates/errors.
Phase 2: Rich Slack messages when SLACK_WEBHOOK_URL is configured.
"""

import json
import logging
import os
import requests

log = logging.getLogger('notify')

SLACK_WEBHOOK_URL = os.environ.get('SLACK_WEBHOOK_URL', '')


def send_notification(event_type, details):
    """Dispatch a notification for a webhook event.

    Args:
        event_type: 'created', 'error', 'duplicate', 'skip'
        details: dict with record details (contact_id, job_id, stage, etc.)
    """
    # Always log
    _log_event(event_type, details)

    # Slack if configured
    if SLACK_WEBHOOK_URL and event_type in ('created', 'error'):
        _send_slack(event_type, details)


def _log_event(event_type, details):
    """Structured log entry for the event."""
    entry = {
        'event': event_type,
        'chat_id': details.get('chat_id', ''),
        'contact_id': details.get('contact_id', ''),
        'job_id': details.get('job_id', ''),
        'stage': details.get('stage', ''),
        'tier': details.get('tier', ''),
    }

    if event_type == 'created':
        entry['applicant_id'] = details.get('applicant_id', '')
        log.info(f"SF_CREATE | {json.dumps(entry)}")
    elif event_type == 'error':
        entry['error'] = details.get('error', '')
        log.error(f"SF_ERROR | {json.dumps(entry)}")
    else:
        log.info(f"EVENT | {json.dumps(entry)}")


def _send_slack(event_type, details):
    """Send a rich Slack notification."""
    instance_url = os.environ.get('SF_LOGIN_URL', 'https://surestaff.my.salesforce.com')

    if event_type == 'created':
        applicant_id = details.get('applicant_id', '')
        sf_link = f"{instance_url}/lightning/r/AVTRRT__Job_Applicant__c/{applicant_id}/view"
        job_desc = details.get('job_desc', 'Unknown position')
        tier = details.get('tier', '').upper()

        blocks = [
            {
                "type": "header",
                "text": {"type": "plain_text", "text": f"New {tier} Candidate"}
            },
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*Position:*\n{job_desc}"},
                    {"type": "mrkdwn", "text": f"*Stage:*\n{details.get('stage', '')}"},
                    {"type": "mrkdwn", "text": f"*Agent:*\n{details.get('agent', '')}"},
                    {"type": "mrkdwn", "text": f"*Chat ID:*\n{details.get('chat_id', '')[:12]}..."},
                ]
            },
            {
                "type": "actions",
                "elements": [
                    {
                        "type": "button",
                        "text": {"type": "plain_text", "text": "View in Salesforce"},
                        "url": sf_link,
                        "style": "primary"
                    }
                ]
            }
        ]
        payload = {"blocks": blocks}

    elif event_type == 'error':
        payload = {
            "text": f":warning: *SF Create Failed*\nChat: {details.get('chat_id', '')[:12]}...\nContact: {details.get('contact_id', '')}\nError: {details.get('error', '')[:200]}"
        }
    else:
        return

    try:
        resp = requests.post(SLACK_WEBHOOK_URL, json=payload, timeout=10)
        if resp.status_code != 200:
            log.warning(f"Slack notification failed: {resp.status_code}")
    except Exception as e:
        log.warning(f"Slack notification error: {e}")
