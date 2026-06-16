**1 — Installer git et cloner le repo**
```bash
sudo apt install git -y
git clone https://github.com/Patpat200/collecte-mqtt
cd collecte-mqtt
```

**2 — Créer le venv et installer les dépendances**
```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

**3 — Lancer le script**
```bash
python3 collecte_mqtt.py
```
