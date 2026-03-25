"""
TP3 — Structured JSON Logging
==============================
Create a reusable logger that outputs structured JSON logs.

Each log line should be a JSON object like:
    {"timestamp": "2026-03-18T10:30:00Z", "level": "INFO", "module": "extract", "function": "extract_products", "message": "..."}
"""

import json
import logging
from datetime import datetime, timezone


class JSONFormatter(logging.Formatter):
    """
    Custom log formatter that outputs JSON.

    Override the format() method to return a JSON string instead of plain text.
    """

    def format(self, record: logging.LogRecord) -> str:
        # Construction du dictionnaire de log
        log_entry = {
            "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "level": record.levelname,
            "module": record.module,
            "function": record.funcName,
            "message": record.getMessage(),
        }

        # ON ajoute des informations d'exception si présentes
        if record.exc_info and record.exc_info[0] is not None:
            log_entry["exception"] = self.formatException(record.exc_info)

        # Retour fonction
        return json.dumps(log_entry, ensure_ascii=False)


def get_logger(name: str) -> logging.Logger:
    """
    Create and return a logger with JSON-formatted output.

    Args:
        name: Logger name (usually __name__ of the calling module)

    Returns:
        A configured logging.Logger instance
    """
    logger = logging.getLogger(name)

    if not logger.handlers:
        logger.setLevel(logging.DEBUG) # on affiche tous les détails.

        # Handler qui écrit sur stdout
        handler = logging.StreamHandler()
        handler.setFormatter(JSONFormatter())

        logger.addHandler(handler)

        # Important : on empêche la propagation vers le root logger
        logger.propagate = False # pas de duplication

    return logger