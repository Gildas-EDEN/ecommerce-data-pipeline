"""
mock_api/server.py : API REST de commandes SIMULÉE, pour le développement.

L'URL du cours (https://api.example.com/orders) n'existe pas. Ce petit serveur
la remplace et reproduit le format de réponse du fichier Sample_orders.json :
    {"orders": [...], "has_more": true, "next_page": 2}

Endpoints
    GET /health                                          -> {"status": "ok"} (sans authentification)
    GET /orders?date_from=AAAA-MM-JJ&page=1&page_size=100   (en-tête Authorization: Bearer <API_KEY>)

Variables d'environnement (pour simuler des pannes et tester la robustesse du pipeline)
    API_KEY                clé attendue                                    (défaut : dev-api-key)
    MOCK_API_DATA_FILE     fichier JSON des commandes                      (défaut : ../data/api/orders.json)
    MOCK_API_PORT          port d'écoute                                   (défaut : 8000)
    MOCK_API_FAIL_FIRST    les N premières requêtes /orders renvoient HTTP 500
    MOCK_API_FAILURE_RATE  probabilité (0 à 1) qu'une requête /orders renvoie HTTP 500
    MOCK_API_MAX_RPS       requêtes par seconde au-delà desquelles on répond HTTP 429

Uniquement la bibliothèque standard de Python : rien à installer.
Le fichier de données est relu à chaque requête : on peut y ajouter des
commandes pendant que le serveur tourne (voir scripts/simulate_changes.py).
"""

from __future__ import annotations

import json
import os
import random
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Dict, Tuple
from urllib.parse import parse_qs, urlparse

API_KEY = os.getenv("API_KEY", "dev-api-key")
DATA_FILE = Path(os.getenv("MOCK_API_DATA_FILE", Path(__file__).resolve().parents[1] / "data" / "api" / "orders.json"))
PORT = int(os.getenv("MOCK_API_PORT", "8000"))
FAIL_FIRST = int(os.getenv("MOCK_API_FAIL_FIRST", "0"))
FAILURE_RATE = float(os.getenv("MOCK_API_FAILURE_RATE", "0"))
MAX_RPS = float(os.getenv("MOCK_API_MAX_RPS", "20"))

_state = {"orders_requests": 0, "window_start": time.monotonic(), "window_count": 0}
_lock = threading.Lock()


def load_orders() -> list:
    with DATA_FILE.open(encoding="utf-8") as handle:
        return json.load(handle).get("orders", [])


class OrdersHandler(BaseHTTPRequestHandler):
    server_version = "MockOrdersAPI/1.0"

    def _send(self, status: int, body: Dict[str, Any], headers: Dict[str, str] | None = None) -> None:
        payload = json.dumps(body).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        for name, value in (headers or {}).items():
            self.send_header(name, value)
        self.end_headers()
        self.wfile.write(payload)

    def do_GET(self) -> None:  # noqa: N802 - nom imposé par http.server
        url = urlparse(self.path)
        if url.path in ("/health", "/orders/health"):
            return self._send(200, {"status": "ok"})
        if url.path != "/orders":
            return self._send(404, {"error": "not found"})

        if self.headers.get("Authorization") != f"Bearer {API_KEY}":
            return self._send(401, {"error": "clé API invalide ou absente"})

        failure, retry_after = self._simulated_failure()
        if failure == 429:
            return self._send(429, {"error": "trop de requêtes"}, {"Retry-After": retry_after})
        if failure == 500:
            return self._send(500, {"error": "erreur interne simulée"})

        params = parse_qs(url.query)
        try:
            page = max(1, int(params.get("page", ["1"])[0]))
            page_size = min(500, max(1, int(params.get("page_size", ["100"])[0])))
        except ValueError:
            return self._send(400, {"error": "page et page_size doivent être des entiers"})
        date_from = params.get("date_from", [None])[0]

        orders = load_orders()
        if date_from:
            orders = [order for order in orders if str(order.get("order_date", "")) >= date_from]
        orders.sort(key=lambda order: (str(order.get("order_date", "")), str(order.get("order_id", ""))))
        start = (page - 1) * page_size
        chunk = orders[start:start + page_size]
        has_more = start + page_size < len(orders)
        return self._send(200, {
            "orders": chunk,
            "page": page,
            "page_size": page_size,
            "total": len(orders),
            "has_more": has_more,
            "next_page": page + 1 if has_more else None,
        })

    def _simulated_failure(self) -> Tuple[int, str]:
        with _lock:
            _state["orders_requests"] += 1
            now = time.monotonic()
            if now - _state["window_start"] >= 1.0:
                _state["window_start"], _state["window_count"] = now, 0
            _state["window_count"] += 1
            if MAX_RPS > 0 and _state["window_count"] > MAX_RPS:
                return 429, "1"
            if _state["orders_requests"] <= FAIL_FIRST:
                return 500, ""
        if FAILURE_RATE > 0 and random.random() < FAILURE_RATE:
            return 500, ""
        return 0, ""

    def log_message(self, fmt: str, *args: Any) -> None:
        print(f"[mock-api] {self.address_string()} {fmt % args}", flush=True)


def main() -> None:
    print(f"[mock-api] Démarrage sur le port {PORT} (données : {DATA_FILE})", flush=True)
    ThreadingHTTPServer(("0.0.0.0", PORT), OrdersHandler).serve_forever()


if __name__ == "__main__":
    main()
