# ===========================
# EMAIL VALIDATION FILTERS
# ===========================

import re

# ===========================
# DISPOSABLE EMAIL DOMAINS
# ===========================
DISPOSABLE_DOMAINS = {
    # Temp mail services
    "mailinator.com", "guerrillamail.com", "tempmail.com", "throwaway.email",
    "temp-mail.org", "fakeinbox.com", "sharklasers.com", "guerrillamailblock.com",
    "10minutemail.com", "10minutemail.net", "minutemail.com", "emailondeck.com",
    "yopmail.com", "yopmail.fr", "cool.fr.nf", "jetable.fr.nf", "nospam.ze.tc",
    "nomail.xl.cx", "mega.zik.dj", "speed.1s.fr", "courriel.fr.nf", "moncourrier.fr.nf",
    "monemail.fr.nf", "monmail.fr.nf", "dispostable.com", "mailcatch.com",
    "maildrop.cc", "getairmail.com", "getnada.com", "tempail.com", "tempr.email",
    "discard.email", "discardmail.com", "spamgourmet.com", "trashmail.com",
    "trashmail.net", "mailnesia.com", "mailnull.com", "spamex.com", "spamfree24.org",
    "armyspy.com", "cuvox.de", "dayrep.com", "einrot.com", "fleckens.hu",
    "gustr.com", "jourrapide.com", "rhyta.com", "superrito.com", "teleworm.us",
    "mailforspam.com", "spam4.me", "grr.la", "guerrillamail.info", "pokemail.net",
    "imgof.com", "imgv.de", "mytrashmail.com", "mt2009.com", "thankyou2010.com",
    "trash2009.com", "mt2014.com", "bugmenot.com", "bumpymail.com", "dodgit.com",
    "e4ward.com", "emailwarden.com", "enterto.com", "gishpuppy.com", "kasmail.com"
}

# ===========================
# ROLE EMAIL PREFIXES
# ===========================
ROLE_PREFIXES = {
    "info", "contact", "support", "admin", "administrator", "noreply", "no-reply",
    "sales", "marketing", "help", "service", "webmaster", "postmaster", "hostmaster",
    "abuse", "security", "billing", "accounts", "office", "reception", "hr",
    "jobs", "careers", "press", "media", "legal", "compliance", "privacy",
    "feedback", "enquiries", "inquiries", "team", "hello", "bonjour", "accueil"
}

# ===========================
# FREE WEBMAIL DOMAINS
# ===========================
FREE_WEBMAIL_DOMAINS = {
    "gmail.com", "yahoo.com", "yahoo.fr", "hotmail.com", "hotmail.fr",
    "outlook.com", "outlook.fr", "live.com", "live.fr", "msn.com",
    "aol.com", "icloud.com", "me.com", "mac.com", "protonmail.com",
    "proton.me", "mail.com", "gmx.com", "gmx.fr", "zoho.com",
    "yandex.com", "laposte.net", "orange.fr", "free.fr", "sfr.fr",
    "wanadoo.fr", "bbox.fr", "neuf.fr", "numericable.fr", "alice.fr"
}

# ===========================
# COMMON TYPOS IN DOMAINS
# ===========================
TYPO_CORRECTIONS = {
    # Gmail typos
    "gmai.com": "gmail.com", "gmial.com": "gmail.com", "gmal.com": "gmail.com",
    "gamil.com": "gmail.com", "gnail.com": "gmail.com", "gmil.com": "gmail.com",
    "gmail.co": "gmail.com", "gmail.cm": "gmail.com", "gmail.om": "gmail.com",
    "gmail.con": "gmail.com", "gmail.cpm": "gmail.com", "gmaill.com": "gmail.com",
    
    # Hotmail typos
    "hotmai.com": "hotmail.com", "hotmal.com": "hotmail.com", "hotamil.com": "hotmail.com",
    "hotmial.com": "hotmail.com", "hotmail.co": "hotmail.com", "hotmail.cm": "hotmail.com",
    "hotmail.con": "hotmail.com", "homail.com": "hotmail.com", "htmail.com": "hotmail.com",
    
    # Yahoo typos
    "yaho.com": "yahoo.com", "yahooo.com": "yahoo.com", "yhoo.com": "yahoo.com",
    "yahoo.co": "yahoo.com", "yahoo.cm": "yahoo.com", "yahoo.con": "yahoo.com",
    "yaoo.com": "yahoo.com", "yhaoo.com": "yahoo.com",
    
    # Outlook typos
    "outlok.com": "outlook.com", "outloo.com": "outlook.com", "outlokk.com": "outlook.com",
    "outlook.co": "outlook.com", "outlook.cm": "outlook.com", "outllook.com": "outlook.com",
    
    # Other common typos
    "live.co": "live.com", "icloud.co": "icloud.com", "protonmail.co": "protonmail.com"
}


def validate_email_format(email):
    """
    Vérifie si l'email a un format valide.
    Returns: (bool, str) - (is_valid, error_message)
    """
    if not email or not isinstance(email, str):
        return False, "Email vide ou invalide"
    
    # Basic regex pattern
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    if not re.match(pattern, email):
        return False, "Format email invalide"
    
    return True, None


def clean_email(email):
    """
    Nettoie l'email: trim, lowercase, supprime caractères invisibles.
    Returns: str
    """
    if not email:
        return ""
    # Remove whitespace, invisible chars, convert to lowercase
    email = email.strip().lower()
    email = re.sub(r'[\x00-\x1f\x7f-\x9f]', '', email)  # Remove control chars
    return email


def check_email_length(email, min_length=6, max_length=254):
    """
    Vérifie la longueur de l'email (RFC 5321: max 254).
    Returns: (bool, str)
    """
    if len(email) < min_length:
        return False, f"Email trop court (min {min_length} caractères)"
    if len(email) > max_length:
        return False, f"Email trop long (max {max_length} caractères)"
    return True, None


def check_forbidden_chars(email):
    """
    Vérifie les caractères interdits (espaces, virgules, etc.).
    Returns: (bool, str)
    """
    forbidden = [' ', ',', ';', ':', '"', "'", '<', '>', '(', ')', '[', ']', '\\']
    for char in forbidden:
        if char in email:
            return False, f"Caractère interdit: '{char}'"
    return True, None


def is_disposable_email(email):
    """
    Vérifie si l'email utilise un domaine jetable/temporaire.
    Returns: bool
    """
    try:
        domain = email.split('@')[1].lower()
        return domain in DISPOSABLE_DOMAINS
    except:
        return False


def is_role_email(email):
    """
    Vérifie si l'email est un email de rôle (info@, contact@, etc.).
    Returns: bool
    """
    try:
        local_part = email.split('@')[0].lower()
        return local_part in ROLE_PREFIXES
    except:
        return False


def is_free_webmail(email):
    """
    Vérifie si l'email utilise un webmail gratuit (Gmail, Yahoo, etc.).
    Returns: bool
    """
    try:
        domain = email.split('@')[1].lower()
        return domain in FREE_WEBMAIL_DOMAINS
    except:
        return False


def has_valid_tld(email):
    """
    Vérifie si le domaine a une extension TLD valide.
    Returns: (bool, str)
    """
    try:
        domain = email.split('@')[1]
        if '.' not in domain:
            return False, "Domaine sans extension valide"
        tld = domain.split('.')[-1]
        if len(tld) < 2:
            return False, "Extension TLD trop courte"
        return True, None
    except:
        return False, "Domaine invalide"


def detect_typo(email):
    """
    Détecte et suggère une correction pour les typos courantes.
    Returns: (has_typo: bool, suggested_domain: str or None)
    """
    try:
        domain = email.split('@')[1].lower()
        if domain in TYPO_CORRECTIONS:
            return True, TYPO_CORRECTIONS[domain]
        return False, None
    except:
        return False, None


def apply_filters(contacts, filters):
    """
    Applique les filtres sélectionnés à une liste de contacts.
    
    Args:
        contacts: list of dict with 'email', 'prenom', 'nom'
        filters: dict with filter names as keys and bool as values
    
    Returns:
        tuple: (valid_contacts, rejected_contacts, stats)
    """
    valid = []
    rejected = []
    stats = {
        'total': len(contacts),
        'valid': 0,
        'format_invalid': 0,
        'too_short': 0,
        'too_long': 0,
        'forbidden_chars': 0,
        'disposable': 0,
        'role_email': 0,
        'free_webmail': 0,
        'no_tld': 0,
        'typo_detected': 0,
        'typo_corrected': 0
    }
    
    for contact in contacts:
        email = contact.get('email', '')
        
        # Always clean email first
        email = clean_email(email)
        contact['email'] = email
        
        if not email:
            rejected.append({'contact': contact, 'reason': 'Email vide'})
            stats['format_invalid'] += 1
            continue
        
        is_valid = True
        rejection_reason = None
        
        # Filter: Format validation
        if filters.get('validate_format', True):
            valid_format, error = validate_email_format(email)
            if not valid_format:
                is_valid = False
                rejection_reason = error
                stats['format_invalid'] += 1
        
        # Filter: Length check
        if is_valid and filters.get('check_length', True):
            valid_len, error = check_email_length(email)
            if not valid_len:
                is_valid = False
                rejection_reason = error
                if 'court' in error:
                    stats['too_short'] += 1
                else:
                    stats['too_long'] += 1
        
        # Filter: Forbidden characters
        if is_valid and filters.get('check_chars', True):
            valid_chars, error = check_forbidden_chars(email)
            if not valid_chars:
                is_valid = False
                rejection_reason = error
                stats['forbidden_chars'] += 1
        
        # Filter: Valid TLD
        if is_valid and filters.get('check_tld', True):
            valid_tld, error = has_valid_tld(email)
            if not valid_tld:
                is_valid = False
                rejection_reason = error
                stats['no_tld'] += 1
        
        # Filter: Disposable emails
        if is_valid and filters.get('block_disposable', False):
            if is_disposable_email(email):
                is_valid = False
                rejection_reason = "Email jetable/temporaire"
                stats['disposable'] += 1
        
        # Filter: Role emails
        if is_valid and filters.get('block_role', False):
            if is_role_email(email):
                is_valid = False
                rejection_reason = "Email de rôle (info@, contact@, etc.)"
                stats['role_email'] += 1
        
        # Filter: Block free webmail (B2B only)
        if is_valid and filters.get('block_free_webmail', False):
            if is_free_webmail(email):
                is_valid = False
                rejection_reason = "Webmail gratuit (B2B uniquement)"
                stats['free_webmail'] += 1
        
        # Filter: Keep only free webmail (B2C only)
        if is_valid and filters.get('only_free_webmail', False):
            if not is_free_webmail(email):
                is_valid = False
                rejection_reason = "Domaine professionnel (B2C uniquement)"
                stats['free_webmail'] += 1
        
        # Filter: Typo detection (correct or reject)
        if is_valid and filters.get('detect_typos', False):
            has_typo, correct_domain = detect_typo(email)
            if has_typo:
                stats['typo_detected'] += 1
                if filters.get('auto_correct_typos', False):
                    # Auto-correct the email
                    local_part = email.split('@')[0]
                    contact['email'] = f"{local_part}@{correct_domain}"
                    email = contact['email']
                    stats['typo_corrected'] += 1
                else:
                    is_valid = False
                    rejection_reason = f"Typo détectée: suggéré {correct_domain}"
        
        if is_valid:
            valid.append(contact)
            stats['valid'] += 1
        else:
            rejected.append({'contact': contact, 'reason': rejection_reason})
    
    return valid, rejected, stats


# Filter definitions with descriptions (for UI)
AVAILABLE_FILTERS = [
    {
        'id': 'validate_format',
        'name': 'Format email valide',
        'description': 'Rejette les emails mal formés (sans @, domaine invalide)',
        'default': True,
        'category': 'syntax'
    },
    {
        'id': 'check_length',
        'name': 'Longueur email',
        'description': 'Rejette les emails trop courts (<6) ou trop longs (>254 caractères)',
        'default': True,
        'category': 'syntax'
    },
    {
        'id': 'check_chars',
        'name': 'Caractères interdits',
        'description': 'Rejette les emails avec espaces, virgules, points-virgules',
        'default': True,
        'category': 'syntax'
    },
    {
        'id': 'check_tld',
        'name': 'Extension TLD valide',
        'description': 'Rejette les domaines sans extension (.com, .fr, etc.)',
        'default': True,
        'category': 'syntax'
    },
    {
        'id': 'block_disposable',
        'name': 'Bloquer emails jetables',
        'description': 'Rejette les emails temporaires (mailinator, tempmail, yopmail...)',
        'default': False,
        'category': 'domain'
    },
    {
        'id': 'block_role',
        'name': 'Bloquer emails de rôle',
        'description': 'Rejette info@, contact@, support@, admin@, noreply@...',
        'default': False,
        'category': 'domain'
    },
    {
        'id': 'block_free_webmail',
        'name': 'B2B uniquement',
        'description': 'Rejette Gmail, Yahoo, Hotmail... (garde seulement domaines pro)',
        'default': False,
        'category': 'type'
    },
    {
        'id': 'only_free_webmail',
        'name': 'B2C uniquement',
        'description': 'Garde seulement Gmail, Yahoo, Hotmail... (rejette domaines pro)',
        'default': False,
        'category': 'type'
    },
    {
        'id': 'detect_typos',
        'name': 'Détecter les typos',
        'description': 'Détecte gmai.com, hotmal.com, yaho.com et autres fautes courantes',
        'default': False,
        'category': 'quality'
    },
    {
        'id': 'auto_correct_typos',
        'name': 'Corriger automatiquement',
        'description': 'Corrige les typos détectées au lieu de rejeter (gmai.com → gmail.com)',
        'default': False,
        'category': 'quality'
    }
]
