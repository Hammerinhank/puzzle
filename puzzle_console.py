#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
puzzle_console.py - Serveur local de la console de pilotage du pipeline Puzzle.

A lancer depuis le repertoire du projet :
    python3.13 puzzle_console.py              demarre en tache de fond et ouvre le navigateur
    python3.13 puzzle_console.py --etat       dit si le serveur tourne
    python3.13 puzzle_console.py --arret      arrete le serveur
    python3.13 puzzle_console.py --premierplan   garde le serveur dans le Terminal (mise au point)

Une fois demarre en tache de fond, le serveur survit a la fermeture du Terminal
et de la fenetre du navigateur : un traitement long continue tout seul.
Adresse : http://127.0.0.1:8765

Ce serveur n'ecoute que sur 127.0.0.1 : rien n'est expose sur le reseau.
"""

import os
import re
import sys
import json
import time
import queue
import atexit
import signal
import threading
import subprocess
import webbrowser
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs

# ---------------------------------------------------------------------------
# VERSION
# ---------------------------------------------------------------------------
VERSION = "1.1"

HISTORIQUE = [
    ("1.0", "12/09/2026",
     "Version initiale. Execution des etapes une a une, en selection ou en "
     "sequence complete, journal en direct, inspection des entrees/sorties, "
     "decompte de progression, detection des echecs silencieux."),
    ("1.1", "12/09/2026",
     "Le serveur se detache du Terminal : un traitement long continue meme "
     "apres fermeture de la fenetre. Options --etat, --arret et --premierplan, "
     "fichier PID, journal du serveur, arret depuis la page, reprise de la "
     "session en cours quand on rouvre le navigateur."),
]

BASE = os.path.dirname(os.path.abspath(__file__))
PORT = 8765
LOG_DIR = os.path.join(BASE, "logs_console")
PID_FICHIER = os.path.join(LOG_DIR, "serveur.pid")
SERVEUR_LOG = os.path.join(LOG_DIR, "serveur.log")

# ---------------------------------------------------------------------------
# DEFINITION DU PIPELINE
# Chaque etape declare ce qu'elle lit et ce qu'elle ecrit : c'est ce qui
# permet a la console d'afficher les volumes et de detecter les sorties vides.
# ---------------------------------------------------------------------------
ETAPES = [
    # ---------------- Pipeline I ----------------
    {
        "id": "clear1", "pipeline": "I", "numero": "—",
        "nom": "Vider les sorties du pipeline I",
        "script": "MASTER_CLEAR.py", "args": [],
        "stdin": "oui\n",
        "description": "Vide out01 a out13 avant un traitement complet. "
                       "A ne pas lancer si vous voulez conserver les resultats en place.",
        "entrees": [], "sorties": [],
        "destructif": True,
    },
    {
        "id": "p01", "pipeline": "I", "numero": "01",
        "nom": "Decouper les planches",
        "script": "p01_decouper.py", "args": ["puzzle_planches", "out01_pieces_brutes"],
        "description": "Detoure chaque piece sur les photos de planches et en fait une image separee.",
        "entrees": ["puzzle_planches"], "sorties": ["out01_pieces_brutes"],
    },
    {
        "id": "p02", "pipeline": "I", "numero": "02",
        "nom": "Nettoyer les pieces",
        "script": "p02_nettoyer.py", "args": [],
        "description": "Uniformise le fond des pieces decoupees.",
        "entrees": ["out01_pieces_brutes"], "sorties": ["out02_pieces_nettoyees"],
    },
    {
        "id": "p03", "pipeline": "I", "numero": "03",
        "nom": "Extraire les contours",
        "script": "p03_contours.py", "args": ["ALL", "out02_pieces_nettoyees", "out03_contours"],
        "description": "Trace le contour de chaque piece et enregistre son centre.",
        "entrees": ["out02_pieces_nettoyees"], "sorties": ["out03_contours"],
    },
    {
        "id": "p04", "pipeline": "I", "numero": "04",
        "nom": "Reperer tenons et mortaises",
        "script": "p04_tenons_mortaises_batch.py",
        "args": ["p04_tenons_mortaises.py", "out03_contours", "out03_contours"],
        "description": "Detecte les bosses et les creux sur chaque contour. "
                       "Maillon critique : sans tenons detectes, aucun profil ne sera decoupe ensuite.",
        "entrees": ["out03_contours"], "sorties": ["out03_contours/tenons-mortaises_data"],
    },
    {
        "id": "p05", "pipeline": "I", "numero": "05",
        "nom": "Identifier les quatre coins",
        "script": "p05_coins_batch.py", "args": ["out03_contours", "out05_coins"],
        "description": "Place les quatre coins de chaque piece en evitant les zones de tenons.",
        "entrees": ["out03_contours"], "sorties": ["out05_coins"],
    },
    {
        "id": "p06", "pipeline": "I", "numero": "06",
        "nom": "Decouper les profils",
        "script": "p06_profils_batch.py",
        "args": ["out03_contours", "out03_contours", "out05_coins", "out05_coins", "out06_profils"],
        "description": "Coupe le contour en quatre profils, un par bord, et les redresse.",
        "entrees": ["out03_contours", "out05_coins"], "sorties": ["out06_profils/lesProfils"],
    },
    {
        "id": "p08", "pipeline": "I", "numero": "08",
        "nom": "Calculer les successeurs",
        "script": "p08_successeurs.py",
        "args": ["ALL", "out03_contours", "out05_coins",
                 "out05_coins/coinsDesTenonsMortaises", "out08_successeurs"],
        "description": "Etablit l'ordre des profils autour de chaque piece (sens horaire).",
        "entrees": ["out03_contours", "out05_coins"], "sorties": ["out08_successeurs"],
    },
    {
        "id": "p10a", "pipeline": "I", "numero": "10",
        "nom": "Graphiques de controle",
        "script": "p10_graphiques.py",
        "args": ["ALL", "out02_pieces_nettoyees", "out03_contours", "out05_coins",
                 "out05_coins/coinsDesTenonsMortaises", "out10_graphiques"],
        "description": "Superpose profils et photo pour verifier le decoupage.",
        "entrees": ["out03_contours", "out05_coins"], "sorties": ["out10_graphiques"],
        "note": "Bug connu : ce script attend 7 arguments (il manque le repertoire des profils) "
                "et sort sur son message d'usage sans rien produire.",
    },
    {
        "id": "p09", "pipeline": "I", "numero": "09",
        "nom": "Reordonner les profils",
        "script": "p09_profils_clean.py",
        "args": ["out03_contours", "out05_coins",
                 "out05_coins/coinsDesTenonsMortaises", "out09_profils_clean"],
        "description": "Remet les profils dans l'ordre naturel 1,2,3,4.",
        "entrees": ["out03_contours", "out05_coins"], "sorties": ["out09_profils_clean"],
    },
    {
        "id": "p10b", "pipeline": "I", "numero": "10'",
        "nom": "Graphiques sur profils reordonnes",
        "script": "p10_graphiques.py",
        "args": ["ALL", "out02_pieces_nettoyees", "out09_profils_clean/contours_clean",
                 "out09_profils_clean/coins_clean", "out09_profils_clean/corr_clean",
                 "out10_graphiques_clean"],
        "description": "Meme controle, sur les profils remis en ordre.",
        "entrees": ["out09_profils_clean"], "sorties": ["out10_graphiques_clean"],
        "note": "Meme bug d'argument manquant que l'etape 10.",
    },
    {
        "id": "p07", "pipeline": "I", "numero": "07",
        "nom": "Retourner les profils a 180°",
        "script": "p07_profils_180.py",
        "args": ["out06_profils/lesProfils", "out06_profils/lesProfils180degres"],
        "description": "Produit la version retournee de chaque profil, pour pouvoir "
                       "comparer un tenon a la mortaise qui l'accueille.",
        "entrees": ["out06_profils/lesProfils"], "sorties": ["out06_profils/lesProfils180degres"],
    },
    {
        "id": "p11", "pipeline": "I", "numero": "11",
        "nom": "Trier les pieces exploitables",
        "script": "p11_profils_comparables_batch.py",
        "args": ["out06_profils", "out11_profils_comparables"],
        "description": "Ecarte les pieces dont les quatre profils sont de longueurs trop disparates.",
        "entrees": ["out06_profils"], "sorties": ["out11_profils_comparables"],
    },
    {
        "id": "p12", "pipeline": "I", "numero": "12",
        "nom": "Comparer les profils entre eux",
        "script": "p12_matching.py",
        "args": ["out11_profils_comparables", "out05_coins/coinsDesTenonsMortaises",
                 "out06_profils/lesProfils", "out06_profils/lesProfils180degres",
                 "out06_profils/lesProfilsTenonOuMortaise", "out12_matchs"],
        "description": "Mesure l'ecart entre chaque paire de profils et retient les meilleurs. "
                       "C'est la sortie qui alimente tout le pipeline II.",
        "entrees": ["out11_profils_comparables"], "sorties": ["out12_matchs"],
    },
    {
        "id": "p13", "pipeline": "I", "numero": "13",
        "nom": "Annoter les pieces",
        "script": "p13_pieces_annotees.py",
        "args": ["out02_pieces_nettoyees", "out06_profils/lesProfilsAvantRotation",
                 "out05_coins/coinsDesTenonsMortaises", "out13_pieces_annotees"],
        "description": "Images de chaque piece avec ses coins et ses profils traces dessus. "
                       "Le meilleur outil pour comprendre d'un coup d'oeil ce qui a rate.",
        "entrees": ["out02_pieces_nettoyees"], "sorties": ["out13_pieces_annotees"],
    },

    # ---------------- Pipeline II ----------------
    {
        "id": "clear2", "pipeline": "II", "numero": "—",
        "nom": "Vider les sorties du pipeline II",
        "script": "MASTER_CLEAR_II.py", "args": [],
        "stdin": "oui\n",
        "description": "Vide out14 a out18 avant un assemblage complet.",
        "entrees": [], "sorties": [],
        "destructif": True,
    },
    {
        "id": "p14", "pipeline": "II", "numero": "14",
        "nom": "Retenir les profils apparies",
        "script": "p14_profils_match.py",
        "args": ["out12_matchs/bestMatch70.txt", "out14_profils_match"],
        "description": "Garde les appariements de profils au-dessus du seuil de ressemblance.",
        "entrees": ["out12_matchs"], "sorties": ["out14_profils_match"],
    },
    {
        "id": "p15", "pipeline": "II", "numero": "15",
        "nom": "Chercher les matchs consecutifs",
        "script": "p15_matchs_consecutifs.py",
        "args": ["out14_profils_match/lesPiècesProfilsMatchSeuls.txt",
                 "out15_matchs_consecutifs", "out08_successeurs/profilsNext_dataOnly.txt"],
        "description": "Repere les pieces qui s'accrochent a deux voisines par des bords voisins.",
        "entrees": ["out14_profils_match", "out08_successeurs"], "sorties": ["out15_matchs_consecutifs"],
    },
    {
        "id": "p16", "pipeline": "II", "numero": "16",
        "nom": "Dessiner les voisinages",
        "script": "p16_voisins_images.py",
        "args": ["out15_matchs_consecutifs/piecesAvec2matchConsecutif_dataOnly.txt",
                 "out16_voisins_images", "out08_successeurs/profilsNext_dataOnly.txt"],
        "description": "Images d'une piece centrale entouree de ses voisines retenues.",
        "entrees": ["out15_matchs_consecutifs"], "sorties": ["out16_voisins_images"],
    },
    {
        "id": "p17", "pipeline": "II", "numero": "17",
        "nom": "Former les carres",
        "script": "p17_carres.py",
        "args": ["out15_matchs_consecutifs/piecesAvec2matchConsecutif_dataOnly.txt",
                 "out14_profils_match/lesPiècesProfilsMatchSeuls.txt",
                 "out17_carres", "out08_successeurs/profilsNext_dataOnly.txt"],
        "description": "Cherche les groupes de quatre pieces qui bouclent en carre.",
        "entrees": ["out15_matchs_consecutifs", "out14_profils_match"], "sorties": ["out17_carres"],
    },
    {
        "id": "p18", "pipeline": "II", "numero": "18",
        "nom": "Dessiner les carres",
        "script": "p18_carres_images.py",
        "args": ["out17_carres/lesCarrés.txt", "out18_carres_images",
                 "out08_successeurs/profilsNext_dataOnly.txt", "out13_pieces_annotees"],
        "description": "Rend chaque carre en version theorique et en vraies images de pieces. "
                       "C'est le resultat final visible du projet.",
        "entrees": ["out17_carres"], "sorties": ["out18_carres_images"],
    },
]

OUTILS = [
    {
        "id": "o_draw", "nom": "Verifier un trace",
        "script": "puzzle_DRAW_test.py", "args": [],
        "description": "Controle visuel du trace des profils.",
    },
    {
        "id": "o_outdated", "nom": "Reperer les fichiers perimes",
        "script": "puzzle_FIND_outdatedFiles.py", "args": [],
        "description": "Liste les sorties plus anciennes que leurs entrees.",
    },
    {
        "id": "o_html", "nom": "Regenerer la galerie d'images",
        "script": "puzzle_GENERE_HTML_images.py", "args": [],
        "description": "Reconstruit puzzle_IMAGES_pieces1.html a partir des images produites.",
    },
    {
        "id": "o_scripts", "nom": "Analyser les appels entre scripts",
        "script": "SCRIPTS_appelés.py", "args": [],
        "description": "Montre quel script en appelle un autre.",
    },
]

# Motifs qui trahissent un echec alors que le script rend un code 0.
# C'est exactement le piege rencontre avec p03 : message d'usage, puis "[OK]".
MOTIFS_ALERTE = [
    (re.compile(r"^Usage\s*:", re.M), "le script est sorti sur son message d'usage : arguments incorrects"),
    (re.compile(r"Traceback \(most recent call last\)"), "une exception Python a ete levee"),
    (re.compile(r"introuvable|No such file|not found", re.I), "un fichier attendu est absent"),
    (re.compile(r"^Erreur\b", re.M | re.I), "le script a signale une erreur"),
]

# ---------------------------------------------------------------------------
# ETAT PARTAGE
# ---------------------------------------------------------------------------
VERROU = threading.Lock()
ETAT = {}           # id -> dict de statut
JOURNAUX = {}       # id -> liste de lignes
FILE_ATTENTE = queue.Queue()
EN_COURS = {"id": None, "proc": None, "arret": False, "attente": []}
SERVEUR = {"objet": None}

TOUTES = {e["id"]: e for e in ETAPES}
for _o in OUTILS:
    TOUTES[_o["id"]] = _o


def etat_initial(eid):
    return {
        "id": eid, "statut": "jamais", "code": None, "duree": None,
        "debut": None, "fin": None, "alertes": [], "lignes": 0,
    }


for _e in TOUTES:
    ETAT[_e] = etat_initial(_e)
    JOURNAUX[_e] = []


# ---------------------------------------------------------------------------
# INSPECTION DU DISQUE
# ---------------------------------------------------------------------------
def stats_chemin(rel):
    """Compte les fichiers et le volume d'un repertoire (ou d'un fichier)."""
    chemin = os.path.join(BASE, rel)
    if not os.path.exists(chemin):
        return {"chemin": rel, "existe": False, "fichiers": 0, "octets": 0, "modifie": None}

    if os.path.isfile(chemin):
        st = os.stat(chemin)
        return {"chemin": rel, "existe": True, "fichiers": 1,
                "octets": st.st_size, "modifie": st.st_mtime}

    total, octets, dernier = 0, 0, 0
    for racine, dossiers, fichiers in os.walk(chemin):
        dossiers[:] = [d for d in dossiers if not d.startswith(".")]
        for f in fichiers:
            if f.startswith("."):
                continue
            total += 1
            try:
                st = os.stat(os.path.join(racine, f))
                octets += st.st_size
                dernier = max(dernier, st.st_mtime)
            except OSError:
                pass
            if total > 20000:
                break
    return {"chemin": rel, "existe": True, "fichiers": total,
            "octets": octets, "modifie": dernier or None}


# ---------------------------------------------------------------------------
# EXECUTION
# ---------------------------------------------------------------------------
def journaliser(eid, ligne):
    with VERROU:
        JOURNAUX[eid].append(ligne)
        ETAT[eid]["lignes"] = len(JOURNAUX[eid])


def executer(eid):
    etape = TOUTES[eid]
    script = etape["script"]
    chemin_script = os.path.join(BASE, script)

    with VERROU:
        JOURNAUX[eid] = []
        ETAT[eid] = etat_initial(eid)
        ETAT[eid]["statut"] = "encours"
        ETAT[eid]["debut"] = time.time()
        EN_COURS["id"] = eid

    if not os.path.exists(chemin_script):
        journaliser(eid, f"Script introuvable : {script}")
        with VERROU:
            ETAT[eid]["statut"] = "echec"
            ETAT[eid]["code"] = -1
            ETAT[eid]["fin"] = time.time()
            ETAT[eid]["duree"] = 0
            EN_COURS["id"] = None
        return

    commande = [sys.executable, script] + list(etape.get("args", []))
    journaliser(eid, "$ " + " ".join(commande))
    journaliser(eid, "")

    env = dict(os.environ)
    env["PYTHONUNBUFFERED"] = "1"
    env["PYTHONIOENCODING"] = "utf-8"
    env["MPLBACKEND"] = "Agg"   # matplotlib sans fenetre : indispensable en service

    debut = time.time()
    try:
        proc = subprocess.Popen(
            commande, cwd=BASE, env=env,
            stdin=subprocess.PIPE if etape.get("stdin") else subprocess.DEVNULL,
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True, encoding="utf-8", errors="replace", bufsize=1,
        )
    except Exception as e:
        journaliser(eid, f"Lancement impossible : {e}")
        with VERROU:
            ETAT[eid]["statut"] = "echec"
            ETAT[eid]["code"] = -1
            ETAT[eid]["fin"] = time.time()
            ETAT[eid]["duree"] = time.time() - debut
            EN_COURS["id"] = None
        return

    with VERROU:
        EN_COURS["proc"] = proc

    if etape.get("stdin"):
        try:
            proc.stdin.write(etape["stdin"])
            proc.stdin.flush()
            proc.stdin.close()
        except Exception:
            pass

    for ligne in proc.stdout:
        journaliser(eid, ligne.rstrip("\n"))

    code = proc.wait()
    duree = time.time() - debut

    with VERROU:
        texte = "\n".join(JOURNAUX[eid])
        alertes = [msg for motif, msg in MOTIFS_ALERTE if motif.search(texte)]
        arrete = EN_COURS["arret"]
        if arrete:
            statut = "arrete"
        elif code != 0:
            statut = "echec"
        elif alertes:
            statut = "suspect"
        else:
            statut = "ok"
        ETAT[eid].update({
            "statut": statut, "code": code, "duree": duree,
            "fin": time.time(), "alertes": alertes,
        })
        EN_COURS["id"] = None
        EN_COURS["proc"] = None

    os.makedirs(LOG_DIR, exist_ok=True)
    horodate = datetime.now().strftime("%Y%m%d_%H%M%S")
    try:
        with open(os.path.join(LOG_DIR, f"{eid}_{horodate}.log"), "w", encoding="utf-8") as f:
            f.write("\n".join(JOURNAUX[eid]))
    except OSError:
        pass


def boucle_travail():
    while True:
        eid = FILE_ATTENTE.get()
        if eid is None:
            continue
        with VERROU:
            if EN_COURS["arret"]:
                EN_COURS["attente"] = []
                EN_COURS["arret"] = False
                continue
            if eid in EN_COURS["attente"]:
                EN_COURS["attente"].remove(eid)
        executer(eid)
        with VERROU:
            if ETAT[eid]["statut"] in ("echec", "arrete"):
                # Une etape ratee rend les suivantes sans objet : on vide la file.
                EN_COURS["attente"] = []
                while not FILE_ATTENTE.empty():
                    try:
                        FILE_ATTENTE.get_nowait()
                    except queue.Empty:
                        break
                EN_COURS["arret"] = False


def demander(ids):
    with VERROU:
        if EN_COURS["id"] is not None or EN_COURS["attente"]:
            return False, "Une execution est deja en cours."
        EN_COURS["arret"] = False
        EN_COURS["attente"] = list(ids)
        for eid in ids:
            ETAT[eid] = etat_initial(eid)
            ETAT[eid]["statut"] = "attente"
    for eid in ids:
        FILE_ATTENTE.put(eid)
    return True, f"{len(ids)} etape(s) lancee(s)."


def arreter():
    with VERROU:
        EN_COURS["arret"] = True
        EN_COURS["attente"] = []
        proc = EN_COURS["proc"]
        while not FILE_ATTENTE.empty():
            try:
                FILE_ATTENTE.get_nowait()
            except queue.Empty:
                break
    if proc and proc.poll() is None:
        try:
            proc.terminate()
            time.sleep(0.7)
            if proc.poll() is None:
                proc.kill()
        except Exception:
            pass
    return True, "Arret demande."


# ---------------------------------------------------------------------------
# SERVEUR HTTP
# ---------------------------------------------------------------------------
class Console(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *a):
        pass

    def _envoyer(self, contenu, type_mime="application/json; charset=utf-8", code=200):
        if isinstance(contenu, (dict, list)):
            contenu = json.dumps(contenu, ensure_ascii=False)
        donnees = contenu.encode("utf-8") if isinstance(contenu, str) else contenu
        self.send_response(code)
        self.send_header("Content-Type", type_mime)
        self.send_header("Content-Length", str(len(donnees)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(donnees)

    def do_GET(self):
        u = urlparse(self.path)
        q = parse_qs(u.query)

        if u.path in ("/", "/index.html", "/puzzle_console.html"):
            page = os.path.join(BASE, "puzzle_console.html")
            if not os.path.exists(page):
                self._envoyer("puzzle_console.html est absent du repertoire du projet.",
                              "text/plain; charset=utf-8", 404)
                return
            with open(page, "rb") as f:
                self._envoyer(f.read(), "text/html; charset=utf-8")
            return

        if u.path == "/api/config":
            self._envoyer({
                "version": VERSION,
                "historique": [{"num": n, "date": d, "resume": r} for n, d, r in HISTORIQUE],
                "projet": BASE,
                "interpreteur": sys.executable,
                "python": sys.version.split()[0],
                "pid": os.getpid(),
                "etapes": ETAPES,
                "outils": OUTILS,
            })
            return

        if u.path == "/api/state":
            with VERROU:
                etats = {k: dict(v) for k, v in ETAT.items()}
                courant = EN_COURS["id"]
                attente = list(EN_COURS["attente"])
            chemins = {}
            for e in ETAPES:
                for rel in e.get("entrees", []) + e.get("sorties", []):
                    if rel not in chemins:
                        chemins[rel] = stats_chemin(rel)
            self._envoyer({"etats": etats, "courant": courant,
                           "attente": attente, "chemins": chemins})
            return

        if u.path == "/api/log":
            eid = q.get("step", [""])[0]
            depuis = int(q.get("from", ["0"])[0])
            with VERROU:
                lignes = JOURNAUX.get(eid, [])[depuis:]
                total = len(JOURNAUX.get(eid, []))
                encours = EN_COURS["id"] == eid
            self._envoyer({"lignes": lignes, "total": total, "encours": encours})
            return

        if u.path == "/api/inspect":
            rel = q.get("path", [""])[0].strip("/")
            cible = os.path.normpath(os.path.join(BASE, rel))
            if not cible.startswith(BASE):
                self._envoyer({"erreur": "chemin hors projet"}, code=400)
                return
            elements = []
            if os.path.isdir(cible):
                for nom in sorted(os.listdir(cible))[:400]:
                    if nom.startswith("."):
                        continue
                    p = os.path.join(cible, nom)
                    try:
                        st = os.stat(p)
                    except OSError:
                        continue
                    elements.append({"nom": nom, "dossier": os.path.isdir(p),
                                     "octets": st.st_size, "modifie": st.st_mtime})
            self._envoyer({"chemin": rel, "elements": elements})
            return

        self._envoyer({"erreur": "inconnu"}, code=404)

    def do_POST(self):
        u = urlparse(self.path)
        longueur = int(self.headers.get("Content-Length", 0))
        corps = self.rfile.read(longueur).decode("utf-8") if longueur else "{}"
        try:
            donnees = json.loads(corps)
        except ValueError:
            donnees = {}

        if u.path == "/api/run":
            ids = [i for i in donnees.get("steps", []) if i in TOUTES]
            if not ids:
                self._envoyer({"ok": False, "message": "Aucune etape valide."})
                return
            ok, message = demander(ids)
            self._envoyer({"ok": ok, "message": message})
            return

        if u.path == "/api/stop":
            ok, message = arreter()
            self._envoyer({"ok": ok, "message": message})
            return

        if u.path == "/api/quit":
            self._envoyer({"ok": True, "message": "Serveur en cours d'arret."})
            threading.Thread(target=fin_de_service, daemon=True).start()
            return

        self._envoyer({"erreur": "inconnu"}, code=404)


# ---------------------------------------------------------------------------
# CYCLE DE VIE DU SERVEUR
# ---------------------------------------------------------------------------
def lire_pid():
    try:
        with open(PID_FICHIER, encoding="utf-8") as f:
            return int(f.read().strip())
    except (OSError, ValueError):
        return None


def vivant(pid):
    if not pid:
        return False
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


def effacer_pid():
    try:
        if lire_pid() == os.getpid():
            os.remove(PID_FICHIER)
    except OSError:
        pass


def fin_de_service():
    time.sleep(0.4)
    arreter()
    effacer_pid()
    srv = SERVEUR["objet"]
    if srv:
        srv.shutdown()


def servir():
    os.makedirs(LOG_DIR, exist_ok=True)
    with open(PID_FICHIER, "w", encoding="utf-8") as f:
        f.write(str(os.getpid()))
    atexit.register(effacer_pid)

    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, lambda *_: threading.Thread(target=fin_de_service, daemon=True).start())

    threading.Thread(target=boucle_travail, daemon=True).start()

    serveur = ThreadingHTTPServer(("127.0.0.1", PORT), Console)
    SERVEUR["objet"] = serveur
    print(f"[{datetime.now():%d/%m %H:%M:%S}] Console Puzzle v{VERSION} demarree "
          f"sur http://127.0.0.1:{PORT} (pid {os.getpid()})", flush=True)
    try:
        serveur.serve_forever()
    finally:
        arreter()
        effacer_pid()
        print(f"[{datetime.now():%d/%m %H:%M:%S}] Console arretee.", flush=True)


def detacher():
    """Relance le serveur dans une session independante du Terminal."""
    os.makedirs(LOG_DIR, exist_ok=True)
    sortie = open(SERVEUR_LOG, "a", encoding="utf-8")
    subprocess.Popen(
        [sys.executable, os.path.abspath(__file__), "--servir"],
        cwd=BASE, stdin=subprocess.DEVNULL, stdout=sortie, stderr=subprocess.STDOUT,
        start_new_session=True,
    )
    for _ in range(40):
        time.sleep(0.25)
        if vivant(lire_pid()):
            return True
    return False


def main():
    args = sys.argv[1:]
    pid = lire_pid()

    if "--etat" in args:
        if vivant(pid):
            print(f"Console Puzzle en service (pid {pid}) sur http://127.0.0.1:{PORT}")
        else:
            print("Console Puzzle arretee.")
        return

    if "--arret" in args:
        if not vivant(pid):
            print("Console Puzzle n'est pas en service.")
            effacer_pid()
            return
        os.kill(pid, signal.SIGTERM)
        for _ in range(20):
            time.sleep(0.25)
            if not vivant(pid):
                print("Console Puzzle arretee.")
                return
        os.kill(pid, signal.SIGKILL)
        print("Console Puzzle arretee (de force).")
        return

    if "--servir" in args:       # appele par --detache, ne pas lancer a la main
        servir()
        return

    manquants = [e["script"] for e in ETAPES
                 if not os.path.exists(os.path.join(BASE, e["script"]))]
    if manquants:
        print(f"Attention : scripts introuvables -> {', '.join(sorted(set(manquants)))}")

    if "--premierplan" in args:
        print(f"Console Puzzle v{VERSION} - premier plan, Ctrl-C pour arreter.")
        print(f"Projet       : {BASE}")
        print(f"Interpreteur : {sys.executable} ({sys.version.split()[0]})")
        threading.Timer(1.0, lambda: webbrowser.open(f"http://127.0.0.1:{PORT}")).start()
        servir()
        return

    if vivant(pid):
        print(f"Console Puzzle tourne deja (pid {pid}). Ouverture du navigateur.")
        webbrowser.open(f"http://127.0.0.1:{PORT}")
        return

    print(f"Console Puzzle v{VERSION}")
    print(f"Projet       : {BASE}")
    print(f"Interpreteur : {sys.executable} ({sys.version.split()[0]})")
    if detacher():
        print(f"Serveur demarre en tache de fond (pid {lire_pid()}).")
        print(f"Adresse      : http://127.0.0.1:{PORT}")
        print("Vous pouvez fermer ce Terminal : les traitements continuent.")
        print(f"Pour l'arreter : python3.13 {os.path.basename(__file__)} --arret")
        webbrowser.open(f"http://127.0.0.1:{PORT}")
    else:
        print("Le serveur n'a pas demarre. Details dans logs_console/serveur.log")


if __name__ == "__main__":
    main()
