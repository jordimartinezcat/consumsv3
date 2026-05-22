"""
Script para generar PDF con reporte de señales con discrepancias o errores
en la validación de consumos.

Autor: Sistema de Validación Consums v3
"""

import os
import sys
import glob
import pandas as pd
from datetime import datetime
from reportlab.lib.pagesizes import A4, landscape
from reportlab.lib.units import cm
from reportlab.lib import colors
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, PageBreak, Image
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_CENTER, TA_LEFT

# Configurar path para importar módulos
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, ROOT)


def find_latest_validation_csv():
    """Busca el CSV de validación más reciente."""
    validations_dir = os.path.join(ROOT, "validacions")
    pattern = os.path.join(validations_dir, "validation_report_*.csv")
    files = glob.glob(pattern)
    
    if not files:
        raise FileNotFoundError("No se encontraron archivos de validación")
    
    # Ordenar por fecha de modificación (más reciente primero)
    files.sort(key=os.path.getmtime, reverse=True)
    return files[0]


def load_validation_data(csv_path):
    """Carga el CSV de validación y procesa los datos."""
    # Leer CSV con separador punto y coma y coma decimal
    df = pd.read_csv(
        csv_path, 
        sep=';', 
        decimal=',',
        encoding='utf-8'
    )
    
    return df


def categorize_issues(df):
    """Categoriza las discrepancias por tipo de problema."""
    categories = {
        'errors': [],
        'resets_65536': [],
        'other_discrepancies': []
    }
    
    for _, row in df.iterrows():
        if row['status'] == 'ERROR':
            categories['errors'].append(row)
        elif row['status'] == 'DISCREPANCIA':
            # Detectar si es un reset de 65536L
            diff = row.get('difference', 0)
            if abs(diff - 65536) < 10:  # Tolerancia de ±10L
                categories['resets_65536'].append(row)
            else:
                categories['other_discrepancies'].append(row)
    
    return categories


def create_pdf_report(csv_path, output_path):
    """Genera el PDF con el reporte de validación."""
    
    # Cargar datos
    df = load_validation_data(csv_path)
    
    # Filtrar solo señales con problemas
    df_issues = df[df['status'].isin(['ERROR', 'DISCREPANCIA'])].copy()
    
    # Categorizar
    categories = categorize_issues(df_issues)
    
    # Calcular estadísticas
    total_signals = len(df)
    total_ok = len(df[df['status'] == 'OK'])
    total_errors = len(categories['errors'])
    total_resets = len(categories['resets_65536'])
    total_other = len(categories['other_discrepancies'])
    
    # Crear PDF (landscape para más espacio)
    doc = SimpleDocTemplate(
        output_path,
        pagesize=landscape(A4),
        leftMargin=1*cm,
        rightMargin=1*cm,
        topMargin=1.5*cm,
        bottomMargin=1.5*cm
    )
    
    # Estilos
    styles = getSampleStyleSheet()
    title_style = ParagraphStyle(
        'CustomTitle',
        parent=styles['Heading1'],
        fontSize=16,
        textColor=colors.HexColor('#1f4788'),
        spaceAfter=12,
        alignment=TA_CENTER
    )
    heading_style = ParagraphStyle(
        'CustomHeading',
        parent=styles['Heading2'],
        fontSize=12,
        textColor=colors.HexColor('#1f4788'),
        spaceAfter=6
    )
    
    # Elementos del documento
    elements = []
    
    # Logo de la empresa
    logo_path = os.path.join(ROOT, "assets", "logo.jpg")
    if os.path.exists(logo_path):
        try:
            logo = Image(logo_path, width=6*cm, height=2*cm, kind='proportional')
            logo.hAlign = 'CENTER'
            elements.append(logo)
            elements.append(Spacer(1, 0.5*cm))
        except Exception as e:
            print(f"Avís: No s'ha pogut carregar el logo: {e}")
    
    # Título
    title = Paragraph("INFORME DE VALIDACIÓ DE CONSUMS", title_style)
    elements.append(title)
    elements.append(Spacer(1, 0.3*cm))
    
    # Información general
    info_text = f"""
    <b>Arxiu validat:</b> {os.path.basename(csv_path)}<br/>
    <b>Data de generació:</b> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}<br/>
    """
    elements.append(Paragraph(info_text, styles['Normal']))
    elements.append(Spacer(1, 0.5*cm))
    
    # Tabla de resumen
    summary_data = [
        ['RESUM DE VALIDACIÓ', 'Quantitat', '%'],
        ['Total senyals processades', str(total_signals), '100%'],
        ['Senyals correctes (OK)', str(total_ok), f'{total_ok/total_signals*100:.1f}%'],
        ['Errors (sense dades API)', str(total_errors), f'{total_errors/total_signals*100:.1f}%'],
        ['Discrepàncies - Resets 65536L', str(total_resets), f'{total_resets/total_signals*100:.1f}%'],
        ['Discrepàncies - Altres', str(total_other), f'{total_other/total_signals*100:.1f}%'],
        ['TOTAL PROBLEMES', str(total_errors + total_resets + total_other), 
         f'{(total_errors + total_resets + total_other)/total_signals*100:.1f}%']
    ]
    
    summary_table = Table(summary_data, colWidths=[12*cm, 3*cm, 3*cm])
    summary_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#1f4788')),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
        ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
        ('ALIGN', (1, 0), (-1, -1), 'CENTER'),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, 0), 11),
        ('BOTTOMPADDING', (0, 0), (-1, 0), 8),
        ('BACKGROUND', (0, 1), (-1, -2), colors.beige),
        ('BACKGROUND', (0, -1), (-1, -1), colors.HexColor('#ffcccc')),
        ('FONTNAME', (0, -1), (-1, -1), 'Helvetica-Bold'),
        ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
    ]))
    
    elements.append(summary_table)
    elements.append(Spacer(1, 0.8*cm))
    
    # Sección 1: ERRORES (sin datos API)
    if categories['errors']:
        elements.append(Paragraph(f"1. ERRORS - Sense dades a l'API ({len(categories['errors'])} senyals)", heading_style))
        elements.append(Spacer(1, 0.2*cm))
        
        error_data = [['Senyal', 'Missatge']]
        for row in categories['errors']:
            error_data.append([
                row['signal'],
                row.get('message', 'N/A')
            ])
        
        error_table = Table(error_data, colWidths=[10*cm, 15*cm])
        error_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#d9534f')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, -1), 8),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 6),
            ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
            ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.lightgrey])
        ]))
        
        elements.append(error_table)
        elements.append(Spacer(1, 0.5*cm))
    
    # Sección 2: RESETS de 65536L
    if categories['resets_65536']:
        elements.append(PageBreak())
        elements.append(Paragraph(f"2. DISCREPÀNCIES - Resets de comptador 65536L ({len(categories['resets_65536'])} senyals)", heading_style))
        elements.append(Spacer(1, 0.2*cm))
        
        reset_explanation = """
        <b>Causa:</b> Reset del comptador LOW (16 bits = 65.536L) detectat i corregit en el càlcul horari.<br/>
        <b>Estat:</b> L'anomalia ja ha estat tinguda en compte en els càlculs horaris mitjançant el sistema de detecció automàtica.<br/>
        <b>Nota:</b> La diferència amb l'API és esperada i confirma que la correcció s'ha aplicat correctament.
        """
        elements.append(Paragraph(reset_explanation, styles['Normal']))
        elements.append(Spacer(1, 0.3*cm))
        
        reset_data = [['Senyal', 'Tot. Inicial', 'Tot. Final', 'Diff API', 'Sum Consum', 'Diferència', 'Error %']]
        for row in categories['resets_65536']:
            reset_data.append([
                row['signal'],
                f"{row['tot_initial']:,.1f}",
                f"{row['tot_final']:,.1f}",
                f"{row['diff_totalizer']:,.1f}",
                f"{row['sum_consumption']:,.1f}",
                f"{row['difference']:,.1f}",
                f"{row['relative_error_pct']:.2f}%"
            ])
        
        reset_table = Table(reset_data, colWidths=[6*cm, 3*cm, 3*cm, 3*cm, 3*cm, 3*cm, 3*cm])
        reset_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#f0ad4e')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (0, -1), 'LEFT'),
            ('ALIGN', (1, 0), (-1, -1), 'RIGHT'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, -1), 7),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 6),
            ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
            ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.lightgrey])
        ]))
        
        elements.append(reset_table)
        elements.append(Spacer(1, 0.5*cm))
    
    # Sección 3: OTRAS DISCREPANCIAS
    if categories['other_discrepancies']:
        elements.append(PageBreak())
        elements.append(Paragraph(f"3. ALTRES DISCREPÀNCIES ({len(categories['other_discrepancies'])} senyals)", heading_style))
        elements.append(Spacer(1, 0.2*cm))
        
        other_explanation = """
        <b>Causa:</b> Diferències que no corresponen a resets de 65536L.<br/>
        <b>Acció requerida:</b> Anàlisi cas per cas per determinar l'origen de la discrepància.
        """
        elements.append(Paragraph(other_explanation, styles['Normal']))
        elements.append(Spacer(1, 0.3*cm))
        
        other_data = [['Senyal', 'Tot. Inicial', 'Tot. Final', 'Diff API', 'Sum Consum', 'Diferència', 'Error %']]
        for row in categories['other_discrepancies']:
            other_data.append([
                row['signal'],
                f"{row['tot_initial']:,.1f}",
                f"{row['tot_final']:,.1f}",
                f"{row['diff_totalizer']:,.1f}",
                f"{row['sum_consumption']:,.1f}",
                f"{row['difference']:,.1f}",
                f"{row['relative_error_pct']:.2f}%"
            ])
        
        other_table = Table(other_data, colWidths=[6*cm, 3*cm, 3*cm, 3*cm, 3*cm, 3*cm, 3*cm])
        other_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#5bc0de')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (0, -1), 'LEFT'),
            ('ALIGN', (1, 0), (-1, -1), 'RIGHT'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, -1), 7),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 6),
            ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
            ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.lightgrey])
        ]))
        
        elements.append(other_table)
    
    # Generar PDF
    doc.build(elements)
    print(f"\n{'='*80}")
    print(f"PDF generado exitosamente: {output_path}")
    print(f"{'='*80}\n")


def main():
    """Función principal."""
    try:
        # Buscar el CSV más reciente
        csv_path = find_latest_validation_csv()
        print(f"Procesando: {csv_path}")
        
        # Generar nombre del PDF
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_path = os.path.join(ROOT, "validacions", f"validation_report_{timestamp}.pdf")
        
        # Crear PDF
        create_pdf_report(csv_path, output_path)
        
    except Exception as e:
        print(f"ERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
