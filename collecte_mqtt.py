#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import paho.mqtt.client as mqtt
import mysql.connector
from mysql.connector import Error

BROKER = "broker.hivemq.com"
PORT = 1883
TOPICS = [
    "IUT/Colmar2026/SAE2.04/Maison1",
    "IUT/Colmar2026/SAE2.04/Maison2"
]

DB_HOST = "10.252.11.79"
DB_PORT = 3306
DB_USER = "toto"
DB_PASSWORD = "toto"
DB_NAME = "sae204"

# Messages en attente si la base est indisponible
cache = []


def connexion_db():
    try:
        conn = mysql.connector.connect(
            host=DB_HOST, port=DB_PORT,
            user=DB_USER, password=DB_PASSWORD,
            database=DB_NAME
        )
        print(f"[DB] Connecté à MySQL ({DB_HOST})")
        return conn
    except Error as e:
        print(f"[DB] Impossible de se connecter : {e}")
        return None


def parse_message(payload):
    # "Id=12A6,piece=sejour,date=15/06/2026,heure=12:13:14,temp=26,35" -> dict
    try:
        data = {}
        for morceau in payload.split(","):
            if "=" in morceau:
                cle, valeur = morceau.split("=", 1)
                data[cle.strip()] = valeur.strip()

        jour, mois, annee = data["date"].split("/")
        timestamp = f"{annee}-{mois}-{jour} {data['time']}"

        return {
            "id": data["Id"],
            "piece": data["piece"],
            "timestamp": timestamp,
            "temperature": float(data["temp"])
        }
    except Exception as e:
        print(f"[PARSE] Erreur sur le message '{payload}' : {e}")
        return None


def inserer_en_db(conn, data):
    cursor = conn.cursor()

    nom_par_defaut = f"Capteur_{data['id'][:6]}"
    cursor.execute("""
        INSERT IGNORE INTO capteurs (id_capteur, nom_capteur, piece, emplacement)
        VALUES (%s, %s, %s, %s)
    """, (data["id"], nom_par_defaut, data["piece"], data["piece"]))

    cursor.execute("""
        INSERT INTO mesures (id_capteur, timestamp_mesure, temperature)
        VALUES (%s, %s, %s)
    """, (data["id"], data["timestamp"], data["temperature"]))

    conn.commit()
    cursor.close()
    print(f"[DB] Inséré — {data['id']} | {data['piece']} | {data['temperature']}°C | {data['timestamp']}")


def vider_cache(conn):
    global cache
    if len(cache) == 0:
        return

    print(f"[CACHE] {len(cache)} message(s) en attente, réinsertion...")
    messages_restants = []
    echec = False

    for data in cache:
        if echec:
            messages_restants.append(data)
            continue
        try:
            inserer_en_db(conn, data)
        except Error as e:
            print(f"[CACHE] Échec réinsertion : {e}")
            messages_restants.append(data)
            echec = True

    cache = messages_restants
    print("[CACHE] Cache vidé" if len(cache) == 0 else f"[CACHE] {len(cache)} message(s) toujours en attente")


def traiter_message(data):
    global cache
    conn = connexion_db()

    if conn is not None:
        try:
            vider_cache(conn)
            inserer_en_db(conn, data)
            conn.close()
        except Error as e:
            print(f"[DB] Erreur insertion : {e} → mise en cache")
            cache.append(data)
            conn.close()
    else:
        print(f"[CACHE] DB indisponible → message mis en cache (total : {len(cache) + 1})")
        cache.append(data)


def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print(f"[MQTT] Connecté au broker {BROKER}")
        for topic in TOPICS:
            client.subscribe(topic)
            print(f"[MQTT] Abonné au topic : {topic}")
    else:
        print(f"[MQTT] Erreur de connexion (code {rc})")


def on_message(client, userdata, msg):
    payload = msg.payload.decode("utf-8").strip()
    print(f"\n[MQTT] Message reçu sur {msg.topic}")
    print(f"       Contenu : {payload}")

    data = parse_message(payload)
    if data is not None:
        traiter_message(data)


def on_disconnect(client, userdata, rc):
    print(f"[MQTT] Déconnecté du broker (code {rc})")


if __name__ == "__main__":
    print("=" * 50)
    print("  SAE 2.04 - Collecte MQTT vers MySQL")
    print(f"  Broker MQTT : {BROKER}:{PORT}")
    print(f"  Base MySQL  : {DB_HOST}:{DB_PORT} / {DB_NAME}")
    print("=" * 50)

    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message
    client.on_disconnect = on_disconnect

    print("\n[INFO] Connexion au broker MQTT...")
    client.connect(BROKER, PORT, keepalive=60)

    print("[INFO] En écoute... (Ctrl+C pour arrêter)\n")
    client.loop_forever()
