"""Violet Core — Business logic for chat classification, extraction, and SF sync.

Extracted from violet_sf_sync.py for use in the webhook service.
"""

import logging
import time
import requests
from salesforce_client import sf_query_all, get_salesforce_credentials

log = logging.getLogger('violet_core')

# Stage assignments in Salesforce
STAGE_QUALIFIED = 'New Application'
STAGE_INTERESTED = 'Candidate Interested'

# Minimum qualification levels to sync
SYNC_QUAL_RESULTS = ('fully_qualified', 'partially_qualified')
SYNC_INTEREST_LEVELS = ('very_interested', 'somewhat_interested')

# Agents to skip (no longer active or no job data)
SKIP_AGENTS = {
    'SMS Violet - EMR Trainer Outreach',
    'Violet - MedPro Inbound Lead Agent',
}


def classify_chat(chat):
    """Classify a chat into a sync action.

    Returns:
        ('qualified', stage) | ('interested', stage) | ('skip', reason)
    """
    agent = chat.get('agent_name', '')
    if agent in SKIP_AGENTS:
        return ('skip', f'agent skipped: {agent}')

    # For webhook mode, chat_analyzed events may not have chat_status='ended'
    # but they always have analysis data. Only skip if explicitly ongoing.
    status = chat.get('chat_status', '')
    if status == 'ongoing':
        return ('skip', 'chat still ongoing')

    ca = chat.get('chat_analysis') or {}
    custom = ca.get('custom_analysis_data') or {}

    if not custom:
        return ('skip', 'no analysis data')

    if custom.get('opted_out'):
        return ('skip', 'opted out')

    qual = custom.get('qualification_result', '')
    if qual in SYNC_QUAL_RESULTS:
        return ('qualified', STAGE_QUALIFIED)

    interest = custom.get('interest_level', '')
    if interest in SYNC_INTEREST_LEVELS:
        return ('interested', STAGE_INTERESTED)

    return ('skip', f'not qualified/interested (qual={qual}, interest={interest})')


def extract_contact_id(chat):
    """Extract Salesforce Contact ID from chat data."""
    dv = chat.get('retell_llm_dynamic_variables') or {}
    meta = chat.get('metadata') or {}

    cid = dv.get('candidate_id', meta.get('candidate_id', ''))
    if cid and cid.startswith('003') and len(cid) >= 15:
        return cid

    url = dv.get('candidate_salesforce_url', '')
    if url and '/Contact/' in url:
        extracted = url.split('/Contact/')[1].split('/')[0]
        if extracted.startswith('003') and len(extracted) >= 15:
            return extracted

    return ''


def extract_job_id(chat):
    """Extract Salesforce Job ID from chat data."""
    dv = chat.get('retell_llm_dynamic_variables') or {}

    url = dv.get('job_salesforce_url', '')
    if url and '/AVTRRT__Job__c/' in url:
        return url.split('/AVTRRT__Job__c/')[1].split('/')[0]

    j18 = dv.get('job_ID_18', '')
    if j18 and j18.startswith('a0F'):
        return j18

    return ''


def check_existing_applicants(contact_ids):
    """Check which contact IDs already have Job Applicant records.

    Returns:
        set of (contact_id_15, job_id_15) pairs that exist.
    """
    existing = set()
    unique_ids = list(set(contact_ids))

    for i in range(0, len(unique_ids), 25):
        batch = unique_ids[i:i + 25]
        ids = "','".join(batch)
        soql = f"SELECT AVTRRT__Contact_Candidate__c, AVTRRT__Job__c FROM AVTRRT__Job_Applicant__c WHERE AVTRRT__Contact_Candidate__c IN ('{ids}')"
        try:
            records = sf_query_all(soql)
            for r in records:
                cc = r.get('AVTRRT__Contact_Candidate__c', '')
                jj = r.get('AVTRRT__Job__c', '')
                if cc and jj:
                    existing.add((cc[:15], jj[:15]))
        except Exception as e:
            log.warning(f"Dedup query failed for batch: {e}")

    return existing


def create_job_applicant(record):
    """Create a single Job Applicant record in Salesforce.

    Args:
        record: dict with contact_id, job_id, stage

    Returns:
        (success: bool, result: dict)
    """
    access_token, instance_url = get_salesforce_credentials()

    payload = {
        'allOrNone': False,
        'records': [
            {
                'attributes': {'type': 'AVTRRT__Job_Applicant__c'},
                'AVTRRT__Contact_Candidate__c': record['contact_id'],
                'AVTRRT__Job__c': record['job_id'],
                'AVTRRT__Stage__c': record['stage'],
            }
        ],
    }

    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json',
    }

    for attempt in range(3):
        try:
            resp = requests.post(
                f'{instance_url}/services/data/v59.0/composite/sobjects',
                headers=headers,
                json=payload,
                timeout=120,
            )
            break
        except requests.exceptions.ReadTimeout:
            log.warning(f"SF timeout, attempt {attempt + 1}/3")
            if attempt < 2:
                access_token, instance_url = get_salesforce_credentials()
                headers['Authorization'] = f'Bearer {access_token}'
                time.sleep(2)
            else:
                return False, {'error': 'timeout after 3 attempts'}

    if resp.status_code == 200:
        api_results = resp.json()
        result = api_results[0]
        if result.get('success'):
            log.info(f"CREATED: {record['contact_id']} + {record['job_id']} -> {result['id']} ({record.get('tier', '?')})")
            return True, {'applicant_id': result['id']}
        else:
            err = str(result.get('errors', []))
            log.error(f"SF create failed: {record['contact_id']} + {record['job_id']}: {err}")
            return False, {'error': err}
    else:
        log.error(f"SF API error {resp.status_code}: {resp.text[:300]}")
        return False, {'error': f'HTTP {resp.status_code}: {resp.text[:300]}'}


def process_chat_webhook(chat, notify_fn=None):
    """Orchestrator: classify -> extract -> dedup -> create -> notify.

    Args:
        chat: Full chat object from RetellAI webhook payload
        notify_fn: Optional callback(event_type, details_dict) for notifications

    Returns:
        dict with keys: action, detail, contact_id, job_id, sf_result, chat_id
    """
    chat_id = chat.get('chat_id', 'unknown')
    result = {'chat_id': chat_id, 'action': None, 'detail': None}

    # 1. Classify
    action, detail = classify_chat(chat)
    result['action'] = action
    result['detail'] = detail

    if action == 'skip':
        log.info(f"[{chat_id[:12]}] SKIP: {detail}")
        return result

    # 2. Extract IDs
    contact_id = extract_contact_id(chat)
    job_id = extract_job_id(chat)
    result['contact_id'] = contact_id
    result['job_id'] = job_id

    if not contact_id:
        result['action'] = 'skip'
        result['detail'] = 'no contact ID in chat data'
        log.warning(f"[{chat_id[:12]}] SKIP: no contact ID")
        return result

    if not job_id:
        result['action'] = 'skip'
        result['detail'] = 'no job ID in chat data'
        log.warning(f"[{chat_id[:12]}] SKIP: no job ID")
        return result

    # 3. Dedup against Salesforce
    existing = check_existing_applicants([contact_id])
    pair = (contact_id[:15], job_id[:15])
    if pair in existing:
        result['action'] = 'duplicate'
        result['detail'] = 'job applicant already exists in SF'
        log.info(f"[{chat_id[:12]}] DEDUP: {contact_id} + {job_id} already exists")
        return result

    # 4. Build record and create
    dv = chat.get('retell_llm_dynamic_variables') or {}
    ca = chat.get('chat_analysis') or {}
    custom = ca.get('custom_analysis_data') or {}

    record = {
        'contact_id': contact_id,
        'job_id': job_id,
        'stage': detail,
        'tier': action,
        'chat_id': chat_id,
        'summary': (custom.get('conversation_summary') or ca.get('chat_summary', ''))[:500],
        'job_desc': f"{dv.get('job_title', '')} in {dv.get('job_city', '')}, {dv.get('job_state', '')}",
        'agent': chat.get('agent_name', ''),
    }

    success, sf_result = create_job_applicant(record)
    result['sf_result'] = sf_result

    if success:
        result['action'] = 'created'
        result['detail'] = f"Job Applicant {sf_result.get('applicant_id', '')} created"
        log.info(f"[{chat_id[:12]}] CREATED: {result['detail']}")

        if notify_fn:
            notify_fn('created', {**record, **sf_result})
    else:
        result['action'] = 'error'
        result['detail'] = sf_result.get('error', 'unknown SF error')
        log.error(f"[{chat_id[:12]}] ERROR: {result['detail']}")

        if notify_fn:
            notify_fn('error', {**record, 'error': result['detail']})

    return result
