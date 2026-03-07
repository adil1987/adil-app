import time
import imaplib
import traceback
from database import get_all_smtp

def run_dmarc_checks():
    """
    Parcourt tous les serveurs SMTP configurés avec une boîte DMARC
    et tente de s'y connecter par IMAP.
    À l'avenir, cette fonction téléchargera et parsagera les rapports XML.
    """
    print("🤖 Démarrage du cycle d'analyse DMARC/Health...")
    
    servers = get_all_smtp()
    checked_count = 0
    
    for smtp in servers:
        # On ne traite que les serveurs qui ont une config DMARC
        if not smtp.get("dmarc_email") or not smtp.get("dmarc_password"):
            continue
            
        print(f"🔄 Check santé (DMARC) du serveur : {smtp['name']} ({smtp['dmarc_email']})")
        checked_count += 1
        
        try:
            # Connexion IMAP (les boîtes sont hébergées sur le VPS lui-même)
            mail = imaplib.IMAP4_SSL("localhost", 993)
            mail.login(smtp["dmarc_email"], smtp["dmarc_password"])
            
            status, messages = mail.select("INBOX")
            if status == "OK":
                print(f"  ✅ Connexion réussie à la boîte INBOX.")
                # Futur TODO: Recherche des emails contenant les rapports DMARC (fichiers .zip / .gz)
                # status, email_ids = mail.search(None, '(SUBJECT "Report domain:")')
                # etc...
            else:
                print(f"  ⚠️ La boîte INBOX est introuvable pour {smtp['name']}.")
            
            mail.logout()
            
        except imaplib.IMAP4.error as e:
            print(f"  ❌ Identifiants invalides pour {smtp['dmarc_email']} sur {smtp['host']}.")
        except Exception as e:
            print(f"  ❌ Erreur inattendue pour {smtp['name']}: {str(e)}")
            
    print(f"✅ Cycle d'analyse terminé. {checked_count} serveurs vérifiés.\n")

if __name__ == "__main__":
    print("🚀 Worker d'analyse DMARC (Health Score) démarré !")
    while True:
        try:
            run_dmarc_checks()
        except Exception as e:
            print("❌ Erreur fatale dans le worker DMARC:")
            traceback.print_exc()
            
        # Attente de 60 minutes avant le prochain cycle (3600 secondes)
        print("⏳ Attente de 60 minutes...")
        time.sleep(3600)
