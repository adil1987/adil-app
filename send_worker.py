"""
Send Worker - Email Sending Engine
===================================
Background worker that processes the send queue with:
- Round Robin SMTP rotation (multi-SMTP support)
- Respect daily_limit, rate_limit (hourly), and email_delay per campaign
- Error handling (421, 451, 550) with exponential backoff
- Auto-skip SMTP on rate limit errors, move to next
- Blacklist management on hard bounces
- Test inbox sending every X emails
"""

import threading
import time
import json
import smtplib
import random
import re
from datetime import datetime, timedelta
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.utils import formatdate
import uuid
import quopri
import concurrent.futures

from database import (
    get_db,
    get_campaign_by_id,
    get_all_campaigns,
    update_campaign_status,
    update_campaign_stats,
    get_campaign_job_stats,
    get_smtp_by_id,
    get_email_by_id,
    get_contact_by_id,
    add_to_blacklist,
    delete_pending_jobs_for_contact,
    update_contact_status,
    get_offer_by_id,
    get_pending_jobs,
    update_job_status,
    increment_job_retry,
    get_all_test_emails,
    add_campaign_log,
    increment_smtp_stats,
)


class SendWorker:
    """Background worker for processing email send queue."""
    
    def __init__(self):
        self.running = False
        self.thread = None
        self.current_campaign_id = None
        self.poll_interval = 5  # seconds between queue checks
        self.activity_log = []  # Last 50 activities
        
        # ThreadPool and Connection tracking
        self._lock = threading.Lock()
        self._active_connections = {}  # {smtp_id: count}
        
        # Per-SMTP tracking for rate limiting
        self._smtp_hour_counts = {}  # {smtp_id: {'count': N, 'reset_at': datetime}}
        self._smtp_backoff = {}  # {smtp_id: seconds_to_wait}
        
        # Round Robin index per campaign
        self._rr_index = {}  # {campaign_id: current_index}
        
        # Test inbox counter per campaign
        self._test_counters = {}  # {campaign_id: emails_since_last_test}
    
    def log(self, message, log_type='info'):
        """Add to activity log."""
        entry = {
            'time': datetime.now().strftime('%H:%M:%S'),
            'message': message,
            'type': log_type
        }
        self.activity_log.insert(0, entry)
        if len(self.activity_log) > 50:
            self.activity_log.pop()
        print(f"[SendWorker] {entry['time']} - {message}")
    
    def start(self, campaign_id=None):
        """Start the worker thread."""
        if self.running:
            return False
        
        self.running = True
        self.current_campaign_id = campaign_id
        self.thread = threading.Thread(target=self._run_loop, daemon=True)
        self.thread.start()
        self.log(f"Worker started for campaign {campaign_id}")
        return True
    
    def stop(self):
        """Stop the worker thread."""
        self.running = False
        self.log("Worker stopped")
    
    def _run_loop(self):
        """Main worker loop."""
        while self.running:
            try:
                print(f"[DEBUG] Polling for campaigns...", flush=True)
                self._process_campaigns()
            except Exception as e:
                self.log(f"Worker error: {e}", 'error')
                import traceback
                traceback.print_exc()
            
            time.sleep(self.poll_interval)
    
    def _process_campaigns(self):
        """Process all active campaigns (sending + pending with auto-resume)."""
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT id, status, pause_code FROM campaigns 
            WHERE status IN ('sending', 'pending') 
            ORDER BY created_at ASC
        """)
        campaigns = cursor.fetchall()
        conn.close()
        
        print(f"[DEBUG] Found {len(campaigns)} active campaigns", flush=True)
        
        for campaign_row in campaigns:
            campaign_id = campaign_row['id']
            status = campaign_row['status']
            print(f"[DEBUG] Campaign {campaign_id}: status={status}", flush=True)
            
            if status == 'pending':
                # Try to auto-resume: check if blocking condition cleared
                if self._try_auto_resume(campaign_id, campaign_row.get('pause_code', '')):
                    self._process_campaign(campaign_id)
            else:
                self._process_campaign(campaign_id)
    
    def _set_pending(self, campaign_id, campaign_name, pause_code, detail=''):
        """Transition campaign from sending to pending with a reason code."""
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute(
            "UPDATE campaigns SET status = 'pending', pause_code = ? WHERE id = ? AND status = 'sending'",
            (pause_code, campaign_id)
        )
        conn.commit()
        conn.close()
        add_campaign_log(campaign_id, campaign_name, 'pending', f"[{pause_code}] {detail}")
        self.log(f"Campaign {campaign_id}: PENDING ({pause_code}) - {detail}", 'warning')
    
    def _try_auto_resume(self, campaign_id, pause_code):
        """Check if a pending campaign can auto-resume. Returns True if resumed."""
        campaign = get_campaign_by_id(campaign_id)
        if not campaign:
            return False
        
        can_resume = False
        
        if pause_code == 'daily_limit':
            # Check if any SMTP has capacity today
            smtp_list = self._get_smtp_list(campaign)
            for smtp in smtp_list:
                fresh = get_smtp_by_id(smtp['id'])
                if fresh and fresh.get('sent_today', 0) < fresh.get('daily_limit', 500):
                    can_resume = True
                    break
        
        elif pause_code == 'hourly_limit':
            # Check if any SMTP hourly counter has reset
            smtp_list = self._get_smtp_list(campaign)
            for smtp in smtp_list:
                available, reason = self._check_smtp_available(smtp)
                if available:
                    can_resume = True
                    break
        
        elif pause_code in ('smtp_error', 'connection_error'):
            # Check if backoff has expired for any SMTP
            smtp_list = self._get_smtp_list(campaign)
            for smtp in smtp_list:
                if self._smtp_backoff.get(smtp['id'], 0) == 0:
                    can_resume = True
                    break
        
        elif pause_code == 'smtp_paused':
            # Check if any SMTP has been unpaused
            smtp_list = self._get_smtp_list(campaign)
            for smtp in smtp_list:
                fresh = get_smtp_by_id(smtp['id'])
                if fresh and not fresh.get('pause_reason'):
                    can_resume = True
                    break
        
        elif pause_code == 'bounce_rate':
            # bounce_rate requires manual resume — don't auto-resume
            can_resume = False
        
        elif pause_code == 'auth_error':
            # auth_error requires manual fix — don't auto-resume
            can_resume = False
        
        else:
            # Unknown code — try to resume
            smtp_list = self._get_smtp_list(campaign)
            for smtp in smtp_list:
                available, _ = self._check_smtp_available(smtp)
                if available:
                    can_resume = True
                    break
        
        if can_resume:
            conn = get_db()
            cursor = conn.cursor()
            cursor.execute(
                "UPDATE campaigns SET status = 'sending', pause_code = NULL WHERE id = ?",
                (campaign_id,)
            )
            conn.commit()
            conn.close()
            add_campaign_log(campaign_id, campaign.get('name', '?'), 'resume',
                             f"Auto-resumed from {pause_code}")
            self.log(f"Campaign {campaign_id}: AUTO-RESUMED from {pause_code}", 'success')
        
        return can_resume
    
    def _get_smtp_list(self, campaign):
        """Get the list of SMTP servers for a campaign (Round Robin support)."""
        if campaign.get('smtp_snapshot'):
            try:
                return json.loads(campaign['smtp_snapshot'])
            except Exception:
                pass
                
        smtp_ids_json = campaign.get('smtp_ids')
        if smtp_ids_json:
            try:
                smtp_ids = json.loads(smtp_ids_json)
                return [get_smtp_by_id(int(sid)) for sid in smtp_ids if get_smtp_by_id(int(sid))]
            except (json.JSONDecodeError, TypeError):
                pass
        # Fallback: single SMTP
        smtp = get_smtp_by_id(campaign.get('smtp_id', 0))
        return [smtp] if smtp else []
    def _check_smtp_available(self, smtp):
        """Check if SMTP is available (not paused, not over daily/hourly limits)."""
        smtp_id = smtp['id']
        
        # Check if paused
        if smtp.get('pause_reason'):
            return False, 'paused'
        
        # Check daily limit
        sent_today = smtp.get('sent_today', 0)
        daily_limit = smtp.get('daily_limit', 500)
        if sent_today >= daily_limit:
            return False, 'daily_limit'
        
        # Check hourly limit
        rate_limit = smtp.get('rate_limit', 100)
        now = datetime.now()
        hour_data = self._smtp_hour_counts.get(smtp_id)
        if hour_data:
            if now < hour_data['reset_at']:
                if hour_data['count'] >= rate_limit:
                    return False, 'hourly_limit'
            else:
                # Reset hour counter
                self._smtp_hour_counts[smtp_id] = {'count': 0, 'reset_at': now + timedelta(hours=1)}
        else:
            self._smtp_hour_counts[smtp_id] = {'count': 0, 'reset_at': now + timedelta(hours=1)}
        
        # Check backoff (from 421/451 errors)
        backoff = self._smtp_backoff.get(smtp_id, 0)
        if backoff > 0:
            return False, 'backoff'
        
        return True, 'ok'
    
    def _pick_next_smtp(self, campaign_id, smtp_list):
        """Round Robin: pick the next available SMTP, cycling through the list.
        Returns (smtp, 'ok') or (None, reason) where reason is the blocking cause."""
        if not smtp_list:
            return None, 'no_smtp'
        
        n = len(smtp_list)
        start_idx = self._rr_index.get(campaign_id, 0) % n
        last_reason = 'unknown'
        
        # Try each SMTP starting from current Round Robin position
        with self._lock:
            for i in range(n):
                idx = (start_idx + i) % n
                smtp = smtp_list[idx]
                smtp_id = smtp['id']
                
                # Check connection limits first
                max_conn = smtp.get('max_connections', 1)
                active_conn = self._active_connections.get(smtp_id, 0)
                if active_conn >= max_conn:
                    last_reason = 'max_connections'
                    continue
                
                available, reason = self._check_smtp_available(smtp)
                if available:
                    # Advance the index for next call
                    self._rr_index[campaign_id] = (idx + 1) % n
                    # Book the connection
                    self._active_connections[smtp_id] = active_conn + 1
                    return smtp, 'ok'
                last_reason = reason
        
        # All SMTPs exhausted or fully loaded
        return None, last_reason
    
    def _increment_smtp_hour_count(self, smtp_id):
        """Track hourly send count for a SMTP."""
        if smtp_id in self._smtp_hour_counts:
            self._smtp_hour_counts[smtp_id]['count'] += 1
    
    def _process_campaign(self, campaign_id):
        """Process a single campaign's pending jobs with Round Robin SMTP."""
        print(f"[DEBUG] _process_campaign({campaign_id}) called", flush=True)
        campaign = get_campaign_by_id(campaign_id)
        if not campaign or campaign['status'] != 'sending':
            print(f"[DEBUG] Campaign {campaign_id} skipped: not found or not sending (status={campaign.get('status') if campaign else 'None'})", flush=True)
            return
        
        # Get SMTP list and email template
        smtp_list = self._get_smtp_list(campaign)
        print(f"[DEBUG] SMTP list: {len(smtp_list)} servers -> {[s.get('name', s.get('host', '?')) for s in smtp_list]}", flush=True)
        
        email_template = None
        if campaign.get('email_snapshot'):
            try:
                email_template = json.loads(campaign['email_snapshot'])
            except Exception:
                pass
        if not email_template:
            email_template = get_email_by_id(campaign['email_id'])
        print(f"[DEBUG] Email template: {'found' if email_template else 'MISSING'}", flush=True)
        
        if not smtp_list or not email_template:
            update_campaign_status(campaign_id, 'error', 'SMTP or Email template missing')
            self.log(f"Campaign {campaign_id}: SMTP or template missing", 'error')
            return
        
        # Get campaign delay settings
        email_delay = campaign.get('email_delay', 3)
        test_inbox_interval = campaign.get('test_inbox_interval', 100)
        
        # Initialize test counter
        if campaign_id not in self._test_counters:
            self._test_counters[campaign_id] = 0
        
        # Fetch a larger batch for concurrent processing
        # We can fetch up to 50 jobs at a time to feed the threads
        jobs = get_pending_jobs(campaign_id, limit=50)
        print(f"[DEBUG] Fetched {len(jobs)} pending jobs for campaign {campaign_id}", flush=True)
        
        if not jobs:
            # Check if campaign is complete
            stats = get_campaign_job_stats(campaign_id)
            print(f"[DEBUG] No jobs left. Stats: {dict(stats)}", flush=True)
            if stats.get('queued', 0) == 0 and stats.get('sending', 0) == 0:
                update_campaign_status(campaign_id, 'completed')
                camp = get_campaign_by_id(campaign_id)
                cname = camp.get('name', '?') if camp else '?'
                add_campaign_log(campaign_id, cname, 'completed', f"Campaign completed ({stats.get('sent', 0)} sent, {stats.get('error', 0)} error)")
                self.log(f"Campaign {campaign_id}: Completed!", 'success')
            return
        
        # Check bounce rate before processing
        stats = get_campaign_job_stats(campaign_id)
        sent = stats.get('sent', 0)
        bounced = campaign.get('bounce_count', 0)
        if sent >= 20 and bounced > 0 and (bounced / sent) > 0.05:
            self._set_pending(campaign_id, campaign.get('name', '?'), 'bounce_rate',
                              f'High bounce rate: {bounced}/{sent} ({bounced/sent*100:.1f}%)')
            return

        def process_single_job(job):
            # Re-check campaign status
            camp_check = get_campaign_by_id(campaign_id)
            if not camp_check or camp_check['status'] != 'sending':
                return False, 'campaign_paused'
            
            # Spin/wait until an SMTP connection frees up (instead of failing immediately)
            max_wait_iterations = 60 # Try for 60 seconds
            wait_iterations = 0
            
            smtp = None
            block_reason = ''
            
            while wait_iterations < max_wait_iterations:
                smtp, block_reason = self._pick_next_smtp(campaign_id, smtp_list)
                
                if smtp:
                    break
                    
                if block_reason == 'max_connections':
                    # All SMTPs are maxed out, but they are healthy. Wait 1 second and retry.
                    time.sleep(1)
                    wait_iterations += 1
                else:
                    # An actual blocking error occurred (e.g. daily limit hit, paused, backoff)
                    break
                    
            if not smtp:
                if block_reason == 'max_connections':
                    return False, 'thread_timeout' # Waited 60s and no connection freed up
                else:
                    code_map = {
                        'daily_limit': 'daily_limit',
                        'hourly_limit': 'hourly_limit',
                        'backoff': 'smtp_error',
                        'paused': 'smtp_paused',
                        'no_smtp': 'no_smtp',
                    }
                    pause_code = code_map.get(block_reason, block_reason)
                    self._set_pending(campaign_id, camp_check.get('name', '?'), pause_code,
                                      f'All SMTPs blocked: {block_reason}')
                    return False, 'all_blocked'

            smtp_id = smtp['id']
            try:
                # Wait between emails BEFORE sending
                delay_max = campaign.get('delay_max', 0)
                if delay_max > email_delay:
                    actual_delay = random.randint(email_delay, delay_max)
                else:
                    actual_delay = email_delay
                    
                if actual_delay > 0:
                    time.sleep(actual_delay)
                
                # Send the email
                self._send_email(job, smtp, email_template, campaign_id)
                
                # Increment hourly and test counters
                with self._lock:
                    self._increment_smtp_hour_count(smtp_id)
                    increment_smtp_stats(smtp_id)
                    
                    self._test_counters[campaign_id] += 1
                    if test_inbox_interval > 0 and self._test_counters[campaign_id] >= test_inbox_interval:
                        self._test_counters[campaign_id] = 0
                        self._send_test_inbox_copy(smtp, email_template, campaign_id)
                
                return True, 'sent'
            finally:
                # CRITICAL: Always release the connection count under lock
                with self._lock:
                    current_count = self._active_connections.get(smtp_id, 0)
                    if current_count > 0:
                        self._active_connections[smtp_id] = current_count - 1

        # Use ThreadPoolExecutor to process jobs concurrently
        max_possible_threads = sum(s.get('max_connections', 1) for s in smtp_list)
        pool_size = min(len(jobs), max_possible_threads, 50)  # Cap at 50 threads max globally per worker
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=max(1, pool_size)) as executor:
            future_to_job = {executor.submit(process_single_job, job): job for job in jobs}
            
            for future in concurrent.futures.as_completed(future_to_job):
                try:
                    success, reason = future.result()
                    if reason == 'all_blocked' or reason == 'campaign_paused':
                        break
                except Exception as exc:
                    self.log(f"Job processing exception: {exc}", 'error')
    
    def _send_test_inbox_copy(self, smtp, email_template, campaign_id):
        """Send a copy to the first test email for inbox/spam checking."""
        try:
            from database import get_campaign_by_id
            campaign = get_campaign_by_id(campaign_id)
            
            to_email = None
            if campaign and campaign.get('test_email'):
                to_email = campaign.get('test_email')
            else:
                test_emails = get_all_test_emails()
                if not test_emails:
                    return
                to_email = test_emails[0]['email']
            subject = f"[TEST] {email_template.get('subject', 'No subject')}"
            body = email_template.get('body', '')
            
            # Use SMTP's from_email as sender
            sender_email = smtp.get('from_email') or smtp.get('username', '')
            sender_name = email_template.get('from_name', '')
            
            # Generate domain-aligned Message-ID
            smtp_domain = smtp.get('sending_domain') or smtp.get('domain') or (sender_email.split('@')[-1] if '@' in sender_email else 'localhost')
            msg_id = f"<{uuid.uuid4().hex[:16]}.{int(time.time())}@{smtp_domain}>"
            
            msg = MIMEMultipart('alternative')
            msg['Subject'] = subject
            msg['From'] = f"{sender_name} <{sender_email}>"
            msg['To'] = to_email
            msg['Date'] = formatdate(localtime=True)
            msg['Message-ID'] = msg_id
            
            html_part = MIMEText(body, 'html', _charset='utf-8')
            html_part.replace_header('Content-Transfer-Encoding', 'quoted-printable')
            html_part.set_payload(quopri.encodestring(body.encode('utf-8')).decode('ascii'))
            msg.attach(html_part)
            
            # Send via SMTP
            if smtp.get('use_tls', 1):
                server = smtplib.SMTP(smtp['host'], int(smtp.get('port', 587)), timeout=15)
                server.starttls()
            else:
                server = smtplib.SMTP_SSL(smtp['host'], int(smtp.get('port', 465)), timeout=15)
            
            server.login(smtp['username'], smtp['password'])
            server.sendmail(email_template.get('from_email'), to_email, msg.as_string())
            server.quit()
            
            self.log(f"Test inbox copy sent to {to_email} (campaign {campaign_id})", 'info')
        except Exception as e:
            self.log(f"Failed to send test inbox copy: {e}", 'warning')
    
    def _send_email(self, job, smtp, email_template, campaign_id):
        """Send a single email."""
        job_id = job['id']
        contact_email = job['email']
        contact_prenom = job.get('prenom', '')
        contact_nom = job.get('nom', '')
        
        try:
            # Personalize content
            subject = self._personalize(email_template.get('subject', ''), contact_prenom, contact_nom, contact_email)
            body = self._personalize(email_template.get('body', ''), contact_prenom, contact_nom, contact_email)
            
            # Replace {{tracking_link}} with CPA filter page URL
            offer_id = email_template.get('offer_id')
            if offer_id:
                offer = get_offer_by_id(int(offer_id))
                if offer:
                    cpa_url = offer.get('url', '')
                    if cpa_url:
                        # Store the CPA URL on the job for later retrieval by /go/<job_id>
                        try:
                            from database import get_db
                            conn = get_db()
                            conn.execute("UPDATE send_jobs SET offer_url = ? WHERE id = ?", (cpa_url, job_id))
                            conn.commit()
                            conn.close()
                        except Exception:
                            pass
                        
                        # Use dynamic tracking URL for the /go/ page
                        go_base = (smtp.get('tracking_url') or '').rstrip('/')
                        if not go_base:
                            go_base = "https://abc-connect.com"
                        go_page_url = f"{go_base}/go/{job_id}"
                        
                        subject = subject.replace('{{tracking_link}}', go_page_url)
                        body = body.replace('{{tracking_link}}', go_page_url)
            
            # Apply spintax
            subject = self._apply_spintax(subject)
            body = self._apply_spintax(body)
            
            # Generate unsubscribe URL
            unsub_url = self._generate_unsub_url(contact_email)
            
            # Append footer with unsubscribe link
            footer_html = email_template.get('footer', '')
            if unsub_url:
                unsub_html = f'<div style="text-align:center; margin-top:20px; padding-top:15px; border-top:1px solid #e5e7eb; font-size:12px; color:#9ca3af;"><a href="{unsub_url}" style="color:#9ca3af; text-decoration:underline;">Unsubscribe</a></div>'
                if footer_html:
                    body = body + footer_html + unsub_html
                else:
                    body = body + unsub_html
            elif footer_html:
                body = body + footer_html
            
            # ==========================================
            # CLICK TRACKING: Rewrite all links
            # ==========================================
            import re
            from urllib.parse import quote
            
            # Use SMTP's tracking URL if configured, otherwise fallback
            base_url = (smtp.get('tracking_url') or '').rstrip('/')
            if not base_url:
                base_url = "https://abc-connect.com"
            
            def rewrite_link(match):
                """Replace href with tracking redirect, skip unsubscribe links."""
                full_tag = match.group(0)
                url = match.group(1)
                # Skip unsubscribe links, empty links, anchors, mailto
                if not url or 'unsubscribe' in url.lower() or url.startswith('#') or url.startswith('mailto:'):
                    return full_tag
                # Skip already-rewritten links
                if '/track/click/' in url:
                    return full_tag
                tracked = f"{base_url}/track/click/{job_id}?url={quote(url, safe='')}"
                return full_tag.replace(url, tracked)
            
            body = re.sub(r'<a\s[^>]*href=["\']([^"\']*)["\']', rewrite_link, body, flags=re.IGNORECASE)
            
            # ==========================================
            # HONEYPOT: Invisible link (bot trap)
            # ==========================================
            honeypot_url = f"{base_url}/track/click/{job_id}?url=honeypot"
            honeypot_html = (
                f'<div style="overflow:hidden;height:0;width:0;max-height:0;max-width:0;opacity:0;mso-hide:all;">'
                f'<a href="{honeypot_url}" style="font-size:0;line-height:0;color:transparent;text-decoration:none;" tabindex="-1">.</a>'
                f'</div>'
            )
            body = body + honeypot_html
            
            # Inject tracking pixel (open tracking)
            tracking_pixel = f'<img src="{base_url}/track/open/{job_id}.gif" width="1" height="1" style="display:none;" alt="" />'
            body = body + tracking_pixel
            
            # Use SMTP's from_email as sender (not email template's)
            sender_email = smtp.get('from_email') or smtp.get('username', '')
            sender_name = email_template.get('from_name', '')
            
            # Generate domain-aligned Message-ID
            smtp_domain = smtp.get('sending_domain') or smtp.get('domain') or (sender_email.split('@')[-1] if '@' in sender_email else 'localhost')
            msg_id = f"<{uuid.uuid4().hex[:16]}.{int(time.time())}@{smtp_domain}>"
            
            # Build email
            msg = MIMEMultipart('alternative')
            msg['Subject'] = subject
            msg['From'] = f"{sender_name} <{sender_email}>"
            msg['To'] = contact_email
            msg['Date'] = formatdate(localtime=True)
            msg['Message-ID'] = msg_id
            
            # Add List-Unsubscribe headers (RFC 8058 - required by Gmail/Yahoo)
            if unsub_url:
                msg['List-Unsubscribe'] = f'<{unsub_url}>'
                msg['List-Unsubscribe-Post'] = 'List-Unsubscribe=One-Click'
            
            html_part = MIMEText(body, 'html', _charset='utf-8')
            html_part.replace_header('Content-Transfer-Encoding', 'quoted-printable')
            html_part.set_payload(quopri.encodestring(body.encode('utf-8')).decode('ascii'))
            msg.attach(html_part)
            
            # Send via SMTP (handle TLS vs SSL)
            if smtp.get('use_tls', 1):
                server = smtplib.SMTP(smtp['host'], int(smtp.get('port', 587)), timeout=30)
                server.ehlo()
                server.starttls()
                server.ehlo()
            else:
                server = smtplib.SMTP_SSL(smtp['host'], int(smtp.get('port', 465)), timeout=30)
            
            server.login(smtp['username'], smtp['password'])
            # Use bounce_email as envelope sender (Return-Path) if configured
            envelope_sender = smtp.get('bounce_email') or sender_email
            server.sendmail(envelope_sender, contact_email, msg.as_string())
            server.quit()
            
            # Mark as sent
            update_job_status(job_id, 'sent')
            self._update_campaign_counters(campaign_id)
            self.log(f"Sent to {contact_email} via {smtp.get('name', 'SMTP')}", 'success')
            
        except smtplib.SMTPResponseException as e:
            self._handle_smtp_error(e, job_id, contact_email, job['contact_id'], campaign_id, smtp)
        except Exception as e:
            update_job_status(job_id, 'error', str(e))
            self.log(f"Error sending to {contact_email}: {e}", 'error')
            self._update_campaign_counters(campaign_id)
    
    def _handle_smtp_error(self, error, job_id, contact_email, contact_id, campaign_id, smtp):
        """Handle SMTP errors with appropriate actions."""
        code = error.smtp_code
        message = str(error.smtp_error)
        smtp_id = smtp['id']
        
        if code in [421, 451]:
            # Temporary error - retry with exponential backoff
            increment_job_retry(job_id)
            
            # Set backoff on this specific SMTP so Round Robin skips it
            current_backoff = self._smtp_backoff.get(smtp_id, 30)
            self._smtp_backoff[smtp_id] = min(current_backoff * 2, 300)  # Max 5 min
            
            # Schedule backoff reset
            def reset_backoff():
                time.sleep(self._smtp_backoff.get(smtp_id, 30))
                self._smtp_backoff[smtp_id] = 0
            threading.Thread(target=reset_backoff, daemon=True).start()
            
            self.log(f"Temp error {code} for {contact_email} on {smtp.get('name')}: retrying on next SMTP", 'warning')
            
        elif code == 550:
            # Hard bounce - CRITICAL actions
            self.log(f"HARD BOUNCE for {contact_email}: {message}", 'error')
            
            # 1. Mark job as error
            update_job_status(job_id, 'error', f'Hard bounce: {message}')
            
            # 2. Add to global blacklist
            add_to_blacklist(contact_email, 'hard_bounce', campaign_id)
            
            # 3. Remove all pending jobs for this contact
            deleted = delete_pending_jobs_for_contact(contact_id)
            if deleted > 0:
                self.log(f"Removed {deleted} pending jobs for blacklisted contact", 'warning')
            
            # 4. Mark contact as bounced
            update_contact_status(contact_email, 'bounced')
            
            # 5. Update campaign bounce counter
            self._update_campaign_counters(campaign_id, is_bounce=True)
            
        else:
            # Other error
            update_job_status(job_id, 'error', f'SMTP {code}: {message}')
            self.log(f"SMTP error {code} for {contact_email}: {message}", 'error')
            self._update_campaign_counters(campaign_id)
    
    def _update_campaign_counters(self, campaign_id, is_bounce=False):
        """Update campaign statistics."""
        stats = get_campaign_job_stats(campaign_id)
        update_campaign_stats(
            campaign_id,
            sent_count=stats.get('sent', 0),
            error_count=stats.get('error', 0),
            bounce_count=stats.get('bounce', 0) if is_bounce else None
        )
    
    def _generate_unsub_url(self, contact_email):
        """Generate unsubscribe URL with security token."""
        import hashlib
        from database import get_setting
        app_url = get_setting("app_url", "").rstrip("/")
        if not app_url:
            return ""
        secret = "adil_app_unsub_secret_2024"
        token = hashlib.sha256(f"{contact_email}:{secret}".encode()).hexdigest()[:16]
        return f"{app_url}/unsubscribe?email={contact_email}&token={token}"
    
    def _personalize(self, content, prenom, nom, email):
        """Replace personalization variables."""
        content = content.replace('{{prenom}}', prenom or '')
        content = content.replace('{{nom}}', nom or '')
        content = content.replace('{{email}}', email or '')
        content = content.replace('{{prénom}}', prenom or '')
        return content
    
    def _apply_spintax(self, text):
        """Apply spintax (random variations)."""
        pattern = r'\{([^{}]+)\}'
        
        def replace_spin(match):
            options = match.group(1).split('|')
            return random.choice(options)
        
        # Apply up to 10 times for nested spintax
        for _ in range(10):
            new_text = re.sub(pattern, replace_spin, text)
            if new_text == text:
                break
            text = new_text
        
        return text
    
    def get_status(self):
        """Get worker status for API."""
        return {
            'running': self.running,
            'current_campaign': self.current_campaign_id,
            'logs': self.activity_log[:20]
        }


# Global worker instance
send_worker = SendWorker()


def start_send_worker():
    """Start the global send worker."""
    return send_worker.start()


def stop_send_worker():
    """Stop the global send worker."""
    send_worker.stop()


def get_worker_status():
    """Get status from global worker."""
    return send_worker.get_status()
