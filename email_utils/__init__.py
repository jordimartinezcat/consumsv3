"""
Utilidades para envío de correos electrónicos con OAuth2.
"""
from .oauth2 import get_smtp_oauth2_string
from .sender import send_validation_report

__all__ = ["get_smtp_oauth2_string", "send_validation_report"]
