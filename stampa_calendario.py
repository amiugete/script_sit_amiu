#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2026
# Roberto Marzocchi, Roberta Fagandini

from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas
from reportlab.lib.colors import black, red
from reportlab.lib.units import cm
import calendar
from datetime import date


import locale

# Imposta la lingua italiana
locale.setlocale(locale.LC_TIME, 'it_IT.UTF-8')  # su Windows potrebbe essere 'Italian_Italy'


def genera_calendario_annuale(anno):
    file_pdf = f'calendario_{anno}.pdf'
    c = canvas.Canvas(file_pdf, pagesize=A4)
    width, height = A4

    # Margini ampi
    margin_x = 2 * cm
    margin_y = 2 * cm

    print(width, height)
    
    # Griglia: 4 righe x 3 colonne
    cols = 3
    rows = 4

    # Spaziatura extra tra mesi
    padding_x = 1 * cm
    padding_y = 1.5 * cm
    
    # spaziatura giorni
    padding_giorni_x = 0.6 * cm
    padding_giorni_y = 0.6 * cm
    
    
    cell_width = (width - 2 * margin_x - (cols-1) * padding_x) / cols
    cell_height = (height - 2 * margin_y- (rows-1) *padding_y) / rows

    print(cell_width, cell_height)  


    c.setFont("Helvetica-Bold", 18)
    c.drawCentredString(width / 2, height - 1 * cm, f"Calendario {anno} (ISOweek)")

    c.setFont("Helvetica", 9)

    mesi = list(calendar.month_name)[1:]

    for i, mese in enumerate(mesi):
        col = i % cols
        row = i // cols

        x0 = margin_x + col * cell_width + col * padding_x
        y0 = height - margin_y - (row + 1) * cell_height - row * padding_y

        print(i, mese, x0, y0)
        # Titolo mese
        c.setFont("Helvetica-Bold", 11)
        c.drawString(x0, y0 + cell_height - 1.2 * cm, mese.capitalize())

        c.setFont("Helvetica", 8)

        # Intestazione giorni
        giorni = ["Lu", "Ma", "Me", "Gi", "Ve", "Sa", "Do"]
        for d, g in enumerate(giorni):
            c.drawString(x0 + (d + 1.5) * padding_giorni_x, y0 + cell_height -  2 * cm, g)

        # Calendario mese
        cal = calendar.Calendar(firstweekday=calendar.MONDAY)
        settimane = cal.monthdayscalendar(anno, i + 1)

        y = y0 + cell_height - 2.8 * cm

        for settimana in settimane:
            # Numero settimana in rosso
            giorno_valido = next((g for g in settimana if g != 0), None)
            if giorno_valido:
                settimana_num = date(anno, i + 1, giorno_valido).isocalendar()[1]
                c.setFillColor(red)
                c.drawString(x0, y, f"{settimana_num:02d}")
                c.setFillColor(black)

            # Giorni del mese
            for d, giorno in enumerate(settimana):
                if giorno != 0:
                    c.drawRightString(
                        x0 + (d + 2) * padding_giorni_x,
                        y,
                        str(giorno)
                    )

            y -= 0.55 * cm

    c.showPage()
    c.save()

if __name__ == "__main__":
    genera_calendario_annuale(2026)

