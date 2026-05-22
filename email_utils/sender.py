"""
Módulo para envío de reportes de validación por correo electrónico.
"""
import logging
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
import os
from datetime import datetime

logger = logging.getLogger("consums")


def _smtp_connect(config_email: dict, logger: logging.Logger) -> smtplib.SMTP:
    """
    Abre y autentica una conexión SMTP.
    Usa OAuth2 XOAUTH2 para Outlook/Hotmail.
    """
    smtp_server = config_email["smtp_server"]
    smtp_port = config_email["smtp_port"]
    smtp_user = config_email["smtp_user"]
    use_tls = config_email.get("smtp_tls", True)
    
    if use_tls:
        smtp = smtplib.SMTP(smtp_server, smtp_port, timeout=30)
        smtp.ehlo()
        smtp.starttls()
        smtp.ehlo()
    else:
        smtp = smtplib.SMTP_SSL(smtp_server, smtp_port, timeout=30)

    # Autenticación OAuth2
    if config_email.get("oauth2_client_id"):
        from email_utils.oauth2 import get_smtp_oauth2_string
        xoauth2 = get_smtp_oauth2_string(
            username=smtp_user,
            client_id=config_email["oauth2_client_id"],
            token_cache_path=config_email.get("oauth2_token_cache", "token_cache.json"),
        )
        code, _ = smtp.docmd("AUTH", f"XOAUTH2 {xoauth2}")
        if code != 235:
            raise smtplib.SMTPAuthenticationError(code, b"XOAUTH2 failed")
        logger.info("SMTP autenticado con OAuth2 XOAUTH2")
    elif smtp_user and config_email.get("smtp_password"):
        smtp.login(smtp_user, config_email["smtp_password"])
        logger.info("SMTP autenticado con usuario/contraseña")
    
    return smtp


def send_validation_report(
    pdf_path: str,
    csv_path: str,
    period_start: str,
    period_end: str,
    config_email: dict,
    summary_stats: dict,
    logger: logging.Logger
) -> None:
    """
    Envía el reporte de validación por correo electrónico.
    
    Args:
        pdf_path: Ruta al archivo PDF del reporte
        csv_path: Ruta al archivo CSV de validación
        period_start: Fecha inicio del período (formato: YYYY-MM-DD HH:MM:SS)
        period_end: Fecha fin del período (formato: YYYY-MM-DD HH:MM:SS)
        config_email: Diccionario con configuración de email
        summary_stats: Diccionario con estadísticas del resumen
        logger: Logger para registro
    """
    if not config_email.get("enabled", False):
        logger.info("Envío de email deshabilitado en configuración")
        return
    
    if not config_email.get("recipients"):
        logger.warning("No hay destinatarios configurados para el reporte")
        return
    
    # Parsear fechas para el asunto
    try:
        from datetime import datetime
        start_dt = datetime.fromisoformat(period_start.replace(" ", "T"))
        end_dt = datetime.fromisoformat(period_end.replace(" ", "T"))
        date_str = start_dt.strftime("%Y-%m-%d")
    except:
        date_str = period_start.split(" ")[0]
    
    # Leer firma HTML
    firma_path = os.path.join(os.path.dirname(__file__), "..", "validacions", "firma.html")
    firma_html = ""
    if os.path.exists(firma_path):
        with open(firma_path, "r", encoding="utf-8") as f:
            firma_html = f.read()
    
    # Construir el cuerpo del correo en catalán
    total = summary_stats.get("total", 0)
    ok_perfect = summary_stats.get("ok_perfect", 0)
    ok_reset = summary_stats.get("ok_reset", 0)
    discrepancies = summary_stats.get("discrepancies", 0)
    errors = summary_stats.get("errors", 0)
    
    # Texto plano para clientes que no soporten HTML
    body_text_lines = [
        f"Informe de Validació de Consums - {date_str}",
        "",
        f"Període: {period_start} -> {period_end}",
        "",
        "=" * 70,
        "RESUM DE VALIDACIÓ",
        "=" * 70,
        f"Total senyals processades: {total}",
        f"  [OK] OK (perfectes):         {ok_perfect} ({ok_perfect/total*100:.1f}%)" if total > 0 else "",
        f"  [OK] OK (amb reset 16-bit):  {ok_reset} ({ok_reset/total*100:.1f}%)" if total > 0 else "",
        f"  [WARN] Discrepàncies:        {discrepancies} ({discrepancies/total*100:.1f}%)" if total > 0 else "",
        f"  [ERROR] Errors (sense dades): {errors} ({errors/total*100:.1f}%)" if total > 0 else "",
        "",
        "L'informe detallat s'adjunta en format PDF.",
        "Les dades completes estan disponibles a l'arxiu CSV adjunt.",
        "",
        "---",
        "Sistema automàtic de validació de consums",
        f"Generat: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
    ]
    body_text = "\n".join(body_text_lines)
    
    # HTML con formato y firma
    body_html = f"""
    <html>
    <head>
        <meta charset="UTF-8">
        <style>
            body {{ font-family: Arial, sans-serif; line-height: 1.6; }}
            .header {{ background-color: #1f4788; color: white; padding: 10px; text-align: center; }}
            .content {{ padding: 20px; }}
            .stats {{ background-color: #f5f5f5; padding: 15px; border-left: 4px solid #1f4788; margin: 20px 0; }}
            .footer {{ margin-top: 30px; padding-top: 20px; border-top: 1px solid #ddd; }}
        </style>
    </head>
    <body>
        <div class="header">
            <h2>Informe de Validació de Consums</h2>
            <p>{date_str}</p>
        </div>
        <div class="content">
            <p><strong>Període:</strong> {period_start} -> {period_end}</p>
            
            <div class="stats">
                <h3>RESUM DE VALIDACIÓ</h3>
                <p><strong>Total senyals processades:</strong> {total}</p>
                <p><span style="color: green;">[OK]</span> OK (perfectes): {ok_perfect} ({ok_perfect/total*100:.1f}%)</p>
                <p><span style="color: green;">[OK]</span> OK (amb reset 16-bit): {ok_reset} ({ok_reset/total*100:.1f}%)</p>
                <p><span style="color: orange;">[WARN]</span> Discrepàncies: {discrepancies} ({discrepancies/total*100:.1f}%)</p>
                <p><span style="color: red;">[ERROR]</span> Errors (sense dades): {errors} ({errors/total*100:.1f}%)</p>
            </div>
            
            <p>L'informe detallat s'adjunta en format PDF.</p>
            <p>Les dades completes estan disponibles a l'arxiu CSV adjunt.</p>
            
            <div class="footer">
                <p><em>Sistema automàtic de validació de consums</em><br>
                Generat: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
            </div>
        </div>
        <br><br>
        {firma_html}
    </body>
    </html>
    """
    
    # Construir mensaje MIME multipart/alternative
    msg = MIMEMultipart("mixed")
    msg["Subject"] = f"[Consums] Informe de Validació - {date_str}"
    msg["From"] = config_email["from_addr"]
    msg["To"] = ", ".join(config_email["recipients"])
    
    # Crear parte alternativa (texto plano + HTML)
    msg_alternative = MIMEMultipart("alternative")
    msg_alternative.attach(MIMEText(body_text, "plain", "utf-8"))
    msg_alternative.attach(MIMEText(body_html, "html", "utf-8"))
    msg.attach(msg_alternative)
    
    # Adjuntar PDF
    if os.path.exists(pdf_path):
        with open(pdf_path, "rb") as f:
            part = MIMEBase("application", "pdf")
            part.set_payload(f.read())
            encoders.encode_base64(part)
            part.add_header(
                "Content-Disposition",
                f"attachment; filename={os.path.basename(pdf_path)}"
            )
            msg.attach(part)
        logger.info(f"PDF adjuntado: {os.path.basename(pdf_path)}")
    else:
        logger.warning(f"PDF no encontrado: {pdf_path}")
    
    # Adjuntar CSV
    if os.path.exists(csv_path):
        with open(csv_path, "rb") as f:
            part = MIMEBase("text", "csv")
            part.set_payload(f.read())
            encoders.encode_base64(part)
            part.add_header(
                "Content-Disposition",
                f"attachment; filename={os.path.basename(csv_path)}"
            )
            msg.attach(part)
        logger.info(f"CSV adjuntado: {os.path.basename(csv_path)}")
    else:
        logger.warning(f"CSV no encontrado: {csv_path}")
    
    # Enviar
    try:
        with _smtp_connect(config_email, logger) as smtp:
            smtp.sendmail(
                config_email["from_addr"],
                config_email["recipients"],
                msg.as_string()
            )
        
        logger.info(
            f"Reporte enviado exitosamente a: {', '.join(config_email['recipients'])}"
        )
        print(f"\n{'='*80}")
        print(f"[OK] Email enviado a: {', '.join(config_email['recipients'])}")
        print(f"{'='*80}\n")
        
    except Exception as exc:
        logger.error(f"Error enviando email: {exc}")
        print(f"\n{'='*80}")
        print(f"[ERROR] Error enviando email: {exc}")
        print(f"{'='*80}\n")
        raise
