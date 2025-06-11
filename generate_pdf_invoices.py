import json
import os
from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas

INPUT_DIR = "invoices"
OUTPUT_DIR = "invoices_pdf"
os.makedirs(OUTPUT_DIR, exist_ok=True)

def create_pdf(invoice_json):
    invoice = json.loads(invoice_json)
    customer_id = invoice["customer_id"]
    billing_month = invoice["billing_month"]
    total_cost = invoice["total_cost"]

    pdf_path = os.path.join(OUTPUT_DIR, f"invoice_{customer_id}_{billing_month}.pdf")
    c = canvas.Canvas(pdf_path, pagesize=A4)
    
    c.setFont("Helvetica-Bold", 14)
    c.drawString(50, 800, "Telecom Invoice")
    
    c.setFont("Helvetica", 12)
    c.drawString(50, 770, f"Customer ID: {customer_id}")
    c.drawString(50, 750, f"Billing Month: {billing_month}")
    c.drawString(50, 730, f"Total Cost: {total_cost} MAD")

    c.showPage()
    c.save()
    print(f"✅ PDF generated: {pdf_path}")

def main():
    for file_name in os.listdir(INPUT_DIR):
        if file_name.endswith(".json"):
            with open(os.path.join(INPUT_DIR, file_name), "r") as f:
                create_pdf(f.read())

if __name__ == "__main__":
    main()

