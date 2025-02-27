from pdf2image import convert_from_path
from pathlib import Path
import os

def create_folder_structure():
    base_dir = Path("gate_images")
    base_dir.mkdir(exist_ok=True)

    for year in range(2007, 2026):  # adjust range as needed
        year_dir = base_dir / str(year)
        year_dir.mkdir(exist_ok=True)

def extract_pages_from_pdf(pdf_path):
    # Get year from filename (e.g., "EE2008.pdf" -> "2008")
    year = pdf_path.stem[2:]

    # Standard A4 size in pixels at 200 DPI
    # A4 = 210mm × 297mm
    # At 200 DPI this is approximately 1654 × 2339 pixels

    # Convert PDF to images
    images = convert_from_path(
        pdf_path,
        fmt="png",
    )

    # Save each page
    output_dir = Path(f"gate_images/{year}")
    for i, image in enumerate(images, start=1):
        output_path = output_dir / f"{year}_EE_{i:02d}.png"
        image.save(output_path, "PNG")
        print(f"Saved {output_path}")

def process_all_pdfs():
    # Create folder structure
    create_folder_structure()

    # Process each PDF
    pdf_dir = Path(".")  # current directory, adjust if needed
    for pdf_file in pdf_dir.glob("EE*.pdf"):
        print(f"Processing {pdf_file}...")
        extract_pages_from_pdf(pdf_file)

if __name__ == "__main__":
    process_all_pdfs()
