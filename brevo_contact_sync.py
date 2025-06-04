import os
import requests
import time
import logging
from datetime import datetime

# CONFIGURACIÓN DESDE VARIABLES DE ENTORNO
BREVO_A_API_KEY = os.getenv("BREVO_A_API_KEY")
BREVO_B_API_KEY = os.getenv("BREVO_B_API_KEY")
LIST_ID_ORIGEN = int(os.getenv("LIST_ID_ORIGEN"))
LIST_ID_DESTINO = int(os.getenv("LIST_ID_DESTINO"))

API_BASE_URL = "https://api.brevo.com/v3"
HEADERS_A = {"api-key": BREVO_A_API_KEY, "accept": "application/json"}
HEADERS_B = {"api-key": BREVO_B_API_KEY, "accept": "application/json", "Content-Type": "application/json"}

logging.basicConfig(filename='sync_log.txt', level=logging.INFO, format='%(asctime)s %(message)s')

def obtener_contactos_modificados_hoy(lista_id, headers):
    contactos = []
    limit = 500
    offset = 0
    hoy = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0).isoformat() + "Z"

    while True:
        url = f"{API_BASE_URL}/contacts?listId={lista_id}&limit={limit}&offset={offset}&modifiedSince={hoy}"
        try:
            r = requests.get(url, headers=headers)
            r.raise_for_status()
            data = r.json()
            batch = data.get("contacts", [])
            contactos.extend(batch)
            if len(batch) < limit:
                break
            offset += limit
        except requests.exceptions.RequestException as e:
            logging.error(f"Error al obtener contactos de lista {lista_id}: {str(e)}")
            break

    return contactos

def agregar_contactos(lista_id, contactos, existentes):
    if not contactos:
        return 0

    agregados = 0
    for contacto in contactos:
        email = contacto.get("email")
        if not email or email in existentes:
            continue

        payload = {"email": email, "listIds": [lista_id]}

        try:
            r = requests.post(f"{API_BASE_URL}/contacts", headers=HEADERS_B, json=payload)
            if r.status_code == 400 and "already exists" in r.text:
                update_payload = {"listIds": [lista_id]}
                r = requests.put(f"{API_BASE_URL}/contacts/{email}", headers=HEADERS_B, json=update_payload)
            r.raise_for_status()
            agregados += 1
        except Exception as e:
            logging.error(f"Error con contacto {email}: {str(e)}")

    return agregados

def sincronizar_listas():
    logging.info("Iniciando sincronización de contactos modificados hoy...")

    origen = obtener_contactos_modificados_hoy(LIST_ID_ORIGEN, HEADERS_A)
    destino = obtener_contactos_modificados_hoy(LIST_ID_DESTINO, HEADERS_B)
    existentes = {c.get("email") for c in destino if c.get("email")}
    nuevos = [c for c in origen if c.get("email") and c.get("email") not in existentes]

    logging.info(f"Contactos nuevos a agregar: {len(nuevos)}")
    sincronizados = agregar_contactos(LIST_ID_DESTINO, nuevos, existentes)

    logging.info(f"Sincronización finalizada. Total sincronizados: {sincronizados}")

if __name__ == "__main__":
    sincronizar_listas()
