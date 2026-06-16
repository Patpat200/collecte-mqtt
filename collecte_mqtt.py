#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SAE 2.04 - Groupe 11
Script de collecte MQTT → MySQL
VM Debian → PC Windows (10.252.11.77)
"""

import paho.mqtt.client as mqtt
import mysql.connector
from mysql.connector import Error

# ============ CONFIGURATION ============

BROKER = "test.mosquitto.org"
PORT   = 1883
TOPICS = [
    "IUT/Colmar2026/SAE2.04/Maison1",
    "IUT/Colmar2026/SAE2.04/Maison2"
]

DB_HOST     = "10.252.11.77"
DB_PORT     = 3306
DB_USER     = "toto"
DB_PASSWORD = "toto"
DB_NAME     = "sae204"

# =======================================

# Si la base de données est coupée, on stocke les messages ici
cache = []


def connexion_db():
    """
    Essaie de se connecter à MySQL.
    Retourne la connexion si OK, sinon retourne None.
    """
    try:
        conn = mysql.connector.connect(
            host     = DB_HOST,
            port     = DB_PORT,
            user     = DB_USER,
            password = DB_PASSWORD,
            database = DB_NAME
        )
        print(f"[DB] ✅ Connecté à MySQL ({DB_HOST})")
        return conn
    except Error as e:
        print(f"[DB] ❌ Impossible de se connecter : {e}")
        return None


def parse_message(payload):
    """
    Transforme le message MQTT en dictionnaire Python.

    Exemple de message reçu :
    Id=12A6B8AF6CD3,piece=sejour,date=15/06/2026,heure=12:13:14,temp=26,35

    ⚠️  La température utilise une virgule comme séparateur décimal (26,35)
        ce qui complique le découpage — on gère ça manuellement.

    Retourne un dict ou None si erreur.
    """
    try:
        parties = payload.split(",")
        data    = {}
        i = 0

        while i < len(parties):
            if "=" in parties[i]:
                cle, valeur = parties[i].split("=", 1)
                cle    = cle.strip()
                valeur = valeur.strip()

                # Cas spécial : temp=26,35 → "26" et "35" sont sur deux éléments séparés
                # On vérifie si l'élément suivant n'a pas de "=" (c'est la partie décimale)
                if cle == "temp" and i + 1 < len(parties) and "=" not in parties[i + 1]:
                    valeur = valeur + "." + parties[i + 1].strip()
                    i += 1  # on saute l'élément suivant

                data[cle] = valeur
            i += 1

        # Convertir la date JJ/MM/AAAA → AAAA-MM-JJ (format MySQL)
        jour, mois, annee = data["date"].split("/")
        timestamp = f"{annee}-{mois}-{jour} {data['heure']}"

        return {
            "id"          : data["Id"],
            "piece"       : data["piece"],
            "timestamp"   : timestamp,
            "temperature" : float(data["temp"])
        }

    except Exception as e:
        print(f"[PARSE] ❌ Erreur sur le message '{payload}' : {e}")
        return None


def inserer_en_db(conn, data):
    """
    Insère un capteur (s'il n'existe pas déjà) puis sa mesure dans MySQL.
    Utilise INSERT IGNORE pour ne pas planter si le capteur existe déjà.
    """
    cursor = conn.cursor()

    # 1. Insérer le capteur si nouveau
    cursor.execute("""
        INSERT IGNORE INTO capteurs (id_capteur, nom_capteur, piece, emplacement)
        VALUES (%s, %s, %s, %s)
    """, (
        data["id"],
        f"Capteur_{data['id'][:6]}",   # nom par défaut : "Capteur_12A6B8"
        data["piece"],
        data["piece"]
    ))

    # 2. Insérer la mesure
    cursor.execute("""
        INSERT INTO mesures (id_capteur, timestamp_mesure, temperature)
        VALUES (%s, %s, %s)
    """, (
        data["id"],
        data["timestamp"],
        data["temperature"]
    ))

    conn.commit()
    cursor.close()

    print(f"[DB] ✅ Inséré — {data['id']} | {data['piece']} | {data['temperature']}°C | {data['timestamp']}")


def vider_cache(conn):
    """
    Après une reconnexion DB, réinsère tous les messages stockés dans le cache.
    """
    global cache

    if not cache:
        return

    print(f"[CACHE] 🔄 {len(cache)} message(s) en attente, réinsertion...")

    for data in cache[:]:   # on copie la liste pour pouvoir la modifier pendant la boucle
        try:
            inserer_en_db(conn, data)
            cache.remove(data)
        except Error as e:
            print(f"[CACHE] ❌ Échec réinsertion : {e}")
            break   # on arrête si ça échoue encore

    print("[CACHE] ✅ Cache vidé")


def traiter_message(data):
    """
    Essaie d'insérer en DB.
    Si la DB est indisponible → met le message en cache.
    """
    global cache

    conn = connexion_db()

    if conn:
        try:
            vider_cache(conn)       # vider le cache en premier si besoin
            inserer_en_db(conn, data)
            conn.close()
        except Error as e:
            print(f"[DB] ❌ Erreur insertion : {e} → mise en cache")
            cache.append(data)
            conn.close()
    else:
        print(f"[CACHE] ⚠️  DB indisponible → message mis en cache (total : {len(cache) + 1})")
        cache.append(data)


# ============ CALLBACKS MQTT ============

def on_connect(client, userdata, flags, rc):
    """Appelé quand le client se connecte au broker."""
    if rc == 0:
        print(f"[MQTT] ✅ Connecté au broker {BROKER}")
        # S'abonner à tous les topics
        for topic in TOPICS:
            client.subscribe(topic)
            print(f"[MQTT] 📡 Abonné au topic : {topic}")
    else:
        print(f"[MQTT] ❌ Erreur de connexion (code {rc})")


def on_message(client, userdata, msg):
    """Appelé à chaque message reçu."""
    payload = msg.payload.decode("utf-8").strip()
    print(f"\n[MQTT] 📨 Message reçu sur {msg.topic}")
    print(f"       Contenu : {payload}")

    data = parse_message(payload)
    if data:
        traiter_message(data)


def on_disconnect(client, userdata, rc):
    """Appelé quand le client se déconnecte du broker."""
    print(f"[MQTT] ⚠️  Déconnecté du broker (code {rc})")


# ============ PROGRAMME PRINCIPAL ============

if __name__ == "__main__":
    print("=" * 50)
    print("  SAE 2.04 - Collecte MQTT → MySQL")
    print(f"  Broker : {BROKER}:{PORT}")
    print(f"  MySQL  : {DB_HOST}:{DB_PORT} / {DB_NAME}")
    print("=" * 50)

    # Créer le client MQTT
    client = mqtt.Client()

    # Associer les fonctions callbacks
    client.on_connect    = on_connect
    client.on_message    = on_message
    client.on_disconnect = on_disconnect

    # Se connecter au broker
    print(f"\n[INFO] Connexion au broker MQTT...")
    client.connect(BROKER, PORT, keepalive=60)

    # Boucle infinie — écoute les messages en permanence
    # Ctrl+C pour arrêter
    print("[INFO] En écoute... (Ctrl+C pour arrêter)\n")
    client.loop_forever()
