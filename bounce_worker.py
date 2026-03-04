"""
Bounce Worker - Monitors bounce mailboxes via IMAP and processes bounced emails.
Runs as a background thread, checking each SMTP's bounce mailbox periodically.
"""

import imaplib
import email
import re
import time
import threading
from email import policy
from datetime import datetime

from database import (
    get_all_smtp,
    add_bounce_log,
    add_to_blacklist,
    get_db
)


class BounceWorker:
    """Background worker that checks bounce mailboxes via IMAP."""
    
    def __init__(self, check_interval=300):
        """
        Args:
            check_interval: Seconds between checks (default: 5 minutes)
        """
        self.check_interval = check_interval
        self.running = False
        self.thread = None
        self.last_check = {}
    
    def start(self):
        """Start the bounce worker thread."""
        if self.running:
            return
        self.running = True
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()
        print(f"[BounceWorker] Started (check every {self.check_interval}s)")
    
    def stop(self):
        """Stop the bounce worker."""
        self.running = False
        print("[BounceWorker] Stopped")
    
    def _run(self):
        """Main loop."""
        while self.running:
            try:
                self._check_all_mailboxes()
            except Exception as e:
                print(f"[BounceWorker] Error in main loop: {e}")
            time.sleep(self.check_interval)
    
    def _check_all_mailboxes(self):
        """Check all SMTP bounce mailboxes."""
        smtp_servers = get_all_smtp()
        
        for smtp in smtp_servers:
            bounce_email = smtp.get('bounce_email', '')
            bounce_password = smtp.get('bounce_password', '')
            
            if not bounce_email or not bounce_password:
                continue
            
            # Use same host as SMTP, port 993 for IMAP SSL
            imap_host = smtp.get('host', '')
            if not imap_host:
                continue
            
            try:
                self._check_mailbox(smtp, imap_host, 993, bounce_email, bounce_password)
            except Exception as e:
                print(f"[BounceWorker] Error checking {bounce_email}: {e}")
    
    def check_single(self, smtp_id):
        """Check a single SMTP's bounce mailbox (for manual trigger)."""
        from database import get_smtp_by_id
        smtp = get_smtp_by_id(smtp_id)
        if not smtp:
            return {"success": False, "error": "SMTP not found"}
        
        bounce_email = smtp.get('bounce_email', '')
        bounce_password = smtp.get('bounce_password', '')
        
        if not bounce_email or not bounce_password:
            return {"success": False, "error": "Bounce mailbox not configured"}
        
        imap_host = smtp.get('host', '')
        if not imap_host:
            return {"success": False, "error": "No SMTP host configured"}
        
        try:
            result = self._check_mailbox(smtp, imap_host, 993, bounce_email, bounce_password)
            return {"success": True, **result}
        except Exception as e:
            return {"success": False, "error": str(e)}
    
    def _check_mailbox(self, smtp, imap_host, imap_port, bounce_email, bounce_password):
        """Connect to IMAP and process bounce emails."""
        smtp_id = smtp['id']
        smtp_name = smtp.get('name', 'Unknown')
        
        print(f"[BounceWorker] Checking {bounce_email} on {imap_host}:{imap_port} (SMTP: {smtp_name})")
        
        # Connect via IMAP SSL
        mail = imaplib.IMAP4_SSL(imap_host, imap_port)
        mail.login(bounce_email, bounce_password)
        mail.select('INBOX')
        
        # Search for unseen emails
        status, data = mail.search(None, 'UNSEEN')
        
        if status != 'OK' or not data[0]:
            mail.logout()
            print(f"[BounceWorker] No new messages in {bounce_email}")
            return {"processed": 0, "hard": 0, "soft": 0, "unparsable": 0}
        
        msg_ids = data[0].split()
        processed = 0
        hard_count = 0
        soft_count = 0
        unparsable = 0
        
        for msg_id in msg_ids:
            try:
                status, msg_data = mail.fetch(msg_id, '(RFC822)')
                if status != 'OK':
                    continue
                
                raw_email = msg_data[0][1]
                msg = email.message_from_bytes(raw_email, policy=policy.default)
                
                # Try to parse as DSN bounce
                result = self._parse_bounce(msg, raw_email, smtp_id)
                
                if result:
                    bounce_type = result.get('type', 'hard')
                    bounced_addr = result.get('email', '')
                    bounce_code = result.get('code', '')
                    bounce_reason = result.get('reason', '')
                    original_subject = result.get('subject', '')
                    
                    if bounced_addr:
                        action = 'blacklisted' if bounce_type == 'hard' else 'soft_logged'
                        
                        # Hard bounce: blacklist the contact
                        if bounce_type == 'hard':
                            add_to_blacklist(bounced_addr, f"Bounce {bounce_code}: {bounce_reason}")
                            hard_count += 1
                        else:
                            soft_count += 1
                        
                        # Log the bounce
                        add_bounce_log(
                            smtp_id=smtp_id,
                            bounced_email=bounced_addr,
                            bounce_type=bounce_type,
                            bounce_code=bounce_code,
                            bounce_reason=bounce_reason,
                            original_subject=original_subject,
                            raw_content='',
                            action_taken=action
                        )
                        
                        # Mark as seen and delete
                        mail.store(msg_id, '+FLAGS', '\\Deleted')
                        processed += 1
                        print(f"[BounceWorker] {bounce_type.upper()} bounce: {bounced_addr} ({bounce_code})")
                    else:
                        unparsable += 1
                else:
                    # Non-parsable - log with raw content for manual review
                    subject = str(msg.get('Subject', 'No subject'))
                    add_bounce_log(
                        smtp_id=smtp_id,
                        bounced_email='unknown',
                        bounce_type='unknown',
                        bounce_code='',
                        bounce_reason='Could not parse bounce email',
                        original_subject=subject,
                        raw_content=raw_email.decode('utf-8', errors='replace')[:2000],
                        action_taken='unparsable'
                    )
                    unparsable += 1
                    
            except Exception as e:
                print(f"[BounceWorker] Error processing message: {e}")
                unparsable += 1
        
        # Expunge deleted messages
        mail.expunge()
        mail.logout()
        
        self.last_check[smtp_id] = datetime.now().isoformat()
        
        print(f"[BounceWorker] Done {bounce_email}: {processed} processed, {hard_count} hard, {soft_count} soft, {unparsable} unparsable")
        return {"processed": processed, "hard": hard_count, "soft": soft_count, "unparsable": unparsable}
    
    def _parse_bounce(self, msg, raw_email, smtp_id):
        """
        Parse a bounce email to extract the bounced address and error code.
        Supports DSN (RFC 3464) format and common bounce patterns.
        """
        bounced_email = None
        bounce_code = ''
        bounce_reason = ''
        bounce_type = 'hard'
        original_subject = ''
        
        content_type = msg.get_content_type()
        
        # Method 1: DSN format (multipart/report)
        if content_type == 'multipart/report':
            for part in msg.walk():
                ct = part.get_content_type()
                
                # The delivery-status part contains the bounce info
                if ct == 'message/delivery-status':
                    ds_text = part.get_payload()
                    if isinstance(ds_text, list):
                        for ds_part in ds_text:
                            ds_content = str(ds_part)
                            # Extract Final-Recipient
                            match = re.search(r'Final-Recipient:\s*(?:rfc822;)?\s*(.+)', ds_content, re.IGNORECASE)
                            if match:
                                bounced_email = match.group(1).strip().strip('<>').lower()
                            
                            # Extract Original-Recipient
                            if not bounced_email:
                                match = re.search(r'Original-Recipient:\s*(?:rfc822;)?\s*(.+)', ds_content, re.IGNORECASE)
                                if match:
                                    bounced_email = match.group(1).strip().strip('<>').lower()
                            
                            # Extract Status code
                            match = re.search(r'Status:\s*(\d+\.\d+\.\d+)', ds_content, re.IGNORECASE)
                            if match:
                                bounce_code = match.group(1)
                            
                            # Extract Diagnostic-Code
                            match = re.search(r'Diagnostic-Code:\s*(?:smtp;)?\s*(.+)', ds_content, re.IGNORECASE)
                            if match:
                                bounce_reason = match.group(1).strip()[:200]
                    else:
                        ds_content = str(ds_text)
                        match = re.search(r'Final-Recipient:\s*(?:rfc822;)?\s*(.+)', ds_content, re.IGNORECASE)
                        if match:
                            bounced_email = match.group(1).strip().strip('<>').lower()
                        match = re.search(r'Status:\s*(\d+\.\d+\.\d+)', ds_content, re.IGNORECASE)
                        if match:
                            bounce_code = match.group(1)
                        match = re.search(r'Diagnostic-Code:\s*(?:smtp;)?\s*(.+)', ds_content, re.IGNORECASE)
                        if match:
                            bounce_reason = match.group(1).strip()[:200]
                
                # The original message part may have the subject
                if ct == 'message/rfc822' or ct == 'text/rfc822-headers':
                    try:
                        orig_payload = part.get_payload()
                        if isinstance(orig_payload, list) and len(orig_payload) > 0:
                            original_subject = str(orig_payload[0].get('Subject', ''))
                        elif isinstance(orig_payload, str):
                            match = re.search(r'Subject:\s*(.+)', orig_payload, re.IGNORECASE)
                            if match:
                                original_subject = match.group(1).strip()
                    except Exception:
                        pass
        
        # Method 2: Plain text bounce (fallback)
        if not bounced_email:
            raw_text = raw_email.decode('utf-8', errors='replace')
            
            # Common patterns in bounce emails
            patterns = [
                r'delivery to (\S+@\S+) has failed',
                r'could not be delivered to:\s*<?(\S+@\S+)>?',
                r'failed to deliver to\s+<?(\S+@\S+)>?',
                r'undeliverable.*?<?(\S+@\S+)>?',
                r'The following address.*?had.*?errors.*?<(\S+@\S+)>',
                r'<(\S+@\S+)>.*?(?:550|551|552|553|554)',
                r'(?:550|551|552|553|554).*?<(\S+@\S+)>',
                r'Original-Recipient:\s*(?:rfc822;)?\s*(\S+@\S+)',
                r'Final-Recipient:\s*(?:rfc822;)?\s*(\S+@\S+)',
            ]
            
            for pattern in patterns:
                match = re.search(pattern, raw_text, re.IGNORECASE | re.DOTALL)
                if match:
                    bounced_email = match.group(1).strip().strip('<>').lower()
                    break
            
            # Try to extract SMTP code
            code_match = re.search(r'\b(5\d{2})\b', raw_text)
            if code_match:
                bounce_code = code_match.group(1)
            elif re.search(r'\b(4\d{2})\b', raw_text):
                code_match = re.search(r'\b(4\d{2})\b', raw_text)
                bounce_code = code_match.group(1)
            
            # Extract reason
            reason_match = re.search(r'(?:550|551|552|553|554|450|451|452)\s+(.{10,200})', raw_text, re.IGNORECASE)
            if reason_match:
                bounce_reason = reason_match.group(1).strip()[:200]
        
        # Determine bounce type from code
        if bounce_code:
            if bounce_code.startswith('5') or bounce_code.startswith('5.'):
                bounce_type = 'hard'
            elif bounce_code.startswith('4') or bounce_code.startswith('4.'):
                bounce_type = 'soft'
        
        if bounced_email:
            # Validate email format
            if not re.match(r'^[^@]+@[^@]+\.[^@]+$', bounced_email):
                return None
            
            return {
                'email': bounced_email,
                'type': bounce_type,
                'code': bounce_code,
                'reason': bounce_reason,
                'subject': original_subject
            }
        
        return None


# Global bounce worker instance
bounce_worker = BounceWorker(check_interval=300)
