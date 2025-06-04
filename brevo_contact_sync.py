# brevo_contact_sync.py
import os
import requests
import time
import logging

# CONFIGURACIÓN DESDE VARIABLES DE ENTORNO
BREVO_A_API_KEY = os.getenv("BREVO_A_API_KEY")
BREVO_B_API_KEY = os.getenv("BREVO_B_API_KEY")
LIST_ID_ORIGEN = int(os.getenv("LIST_ID_ORIGEN"))
LIST_ID_DESTINO = int(os.getenv("LIST_ID_DESTINO"))

API_BASE_URL = "https://api.brevo.com/v3"
HEADERS_A = {"api-key": BREVO_A_API_KEY, "accept": "application/json"}
HEADERS_B = {"api-key": BREVO_B_API_KEY, "accept": "application/json", "Content-Type": "application/json"}

logging.basicConfig(filename='sync_log.txt', level=logging.INFO, format='%(asctime)s %(message)s')

def obtener_contactos(lista_id):
    contactos = []
    limit = 500
    offset = 0

    while True:
        url = f"{API_BASE_URL}/contacts?listId={lista_id}&limit={limit}&offset={offset}"
        r = requests.get(url, headers=HEADERS_A)
        r.raise_for_status()
        data = r.json()
        batch = data.get("contacts", [])
        contactos.extend(batch)
        if len(batch) < limit:
            break
        offset += limit

    return contactos

def agregar_contactos(lista_id, contactos):
    if not contactos:
        return

    for contacto in contactos:
        email = contacto.get("email")
        if not email:
            continue

        payload = {
            "email": email,
            "listIds": [lista_id]
        }
        try:
            r = requests.post(f"{API_BASE_URL}/contacts", headers=HEADERS_B, json=payload)
            if r.status_code == 400 and "already exists" in r.text:
                update_payload = {"listIds": [lista_id]}
                requests.put(f"{API_BASE_URL}/contacts/{email}", headers=HEADERS_B, json=update_payload)
            r.raise_for_status()
        except Exception as e:
            logging.error(f"Error con {email}: {str(e)}")
        time.sleep(0.2)

def sincronizar_listas():
    logging.info("Iniciando sincronización de contactos...")
    contactos = obtener_contactos(LIST_ID_ORIGEN)
    agregar_contactos(LIST_ID_DESTINO, contactos)
    logging.info(f"Sincronización completa. Total: {len(contactos)} contactos.")

if __name__ == "__main__":
    sincronizar_listas()
