import os
import csv
import requests
import time
import random
import argparse
import logging
import io
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from PIL import Image, ImageOps

# ==============================================================================
# CONFIGURAZIONE LOGGING
# ==============================================================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("image_processing_log.txt"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ==============================================================================
# FUNZIONE MIGLIORATA PER RENDERE LE IMMAGINI QUADRATE
# ==============================================================================

def make_image_square(image_path):
    """
    Controlla se un'immagine è quadrata. Se non lo è, aggiunge bordi trasparenti
    per renderla quadrata, centrando l'immagine originale.
    Preserva la trasparenza se presente.
    Sovrascrive il file di immagine originale.
    """
    try:
        with Image.open(image_path) as img:
            width, height = img.size
            original_mode = img.mode

            # Se l'immagine è già quadrata, non è necessario fare nulla.
            if width == height:
                return

            logger.info(f"L'immagine non è quadrata ({width}x{height}). Aggiunta di bordi a: {image_path}")

            # Trova la dimensione più grande che diventerà la dimensione del nostro quadrato.
            max_dim = max(width, height)

            # Determina il formato di output basato sulla presenza di trasparenza
            has_transparency = (
                original_mode in ('RGBA', 'LA') or 
                (original_mode == 'P' and 'transparency' in img.info)
            )

            if has_transparency:
                # Mantieni la trasparenza
                if original_mode != 'RGBA':
                    img = img.convert('RGBA')
                
                # Crea una tela quadrata trasparente
                square_canvas = Image.new('RGBA', (max_dim, max_dim), (0, 0, 0, 0))
                
                # Calcola le coordinate per centrare l'immagine
                paste_x = (max_dim - width) // 2
                paste_y = (max_dim - height) // 2
                
                # Incolla l'immagine usando se stessa come maschera alpha
                square_canvas.paste(img, (paste_x, paste_y), img)
                
                # Salva come PNG per preservare la trasparenza
                png_path = image_path.replace('.webp', '.png')
                square_canvas.save(png_path, 'PNG', optimize=True)
                
                # Rimuovi il file WebP se diverso dal PNG
                if png_path != image_path and os.path.exists(image_path):
                    os.remove(image_path)
                
                logger.info(f"Immagine con trasparenza salvata come PNG: {png_path}")
                
            else:
                # Immagine senza trasparenza - usa sfondo bianco
                if original_mode != 'RGB':
                    img = img.convert('RGB')
                
                square_canvas = Image.new('RGB', (max_dim, max_dim), (255, 255, 255))
                
                paste_x = (max_dim - width) // 2
                paste_y = (max_dim - height) // 2
                
                square_canvas.paste(img, (paste_x, paste_y))
                
                # Salva come WebP
                square_canvas.save(image_path, 'WEBP', quality=85)

    except Exception as e:
        logger.error(f"Errore durante la conversione in quadrato di {image_path}: {e}")


def detect_image_format(image_content):
    """
    Rileva il formato dell'immagine e se ha trasparenza.
    Ritorna (formato, ha_trasparenza)
    """
    try:
        with Image.open(io.BytesIO(image_content)) as img:
            has_transparency = (
                img.mode in ('RGBA', 'LA') or 
                (img.mode == 'P' and 'transparency' in img.info)
            )
            return img.format, has_transparency
    except Exception:
        return None, False


# ==============================================================================
# FUNZIONI DI DOWNLOAD E GESTIONE CSV (Aggiornate)
# ==============================================================================

def clean_filename(filename):
    """Pulisce il nome del file da caratteri non validi."""
    return "".join(c for c in filename if c.isalnum() or c in ('_', '-')).rstrip()

def download_process_image(url, save_path, name, index, total):
    """
    Scarica un'immagine, la converte nel formato appropriato e la rende quadrata.
    Mantiene PNG per immagini con trasparenza, WebP per le altre.
    """
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    }
    
    time.sleep(random.uniform(0.5, 1.5))
    
    try:
        response = requests.get(url, headers=headers, stream=True, timeout=30)
        response.raise_for_status()
        
        image_content = response.content
        original_format, has_transparency = detect_image_format(image_content)
        
        # Determina l'estensione del file basata sulla trasparenza
        if has_transparency:
            file_extension = ".png"
            save_format = "PNG"
            save_options = {"optimize": True}
        else:
            file_extension = ".webp"
            save_format = "WEBP"
            save_options = {"quality": 85}
        
        safe_filename = f"{clean_filename(name)}{file_extension}"
        final_path = os.path.join(save_path, safe_filename)
        
        if os.path.exists(final_path):
            logger.info(f"[{index}/{total}] File già esistente, saltato: {final_path}")
            return safe_filename
        
        # Salva l'immagine nel formato appropriato
        with Image.open(io.BytesIO(image_content)) as img:
            img.save(final_path, save_format, **save_options)
        
        logger.info(f"[{index}/{total}] Scaricato e convertito: {url} -> {final_path}")

        # Rendi l'immagine quadrata
        make_image_square(final_path)

        return safe_filename

    except requests.exceptions.RequestException as e:
        logger.error(f"[{index}/{total}] ERRORE HTTP scaricando {url}: {e}")
    except Exception as e:
        logger.error(f"[{index}/{total}] ERRORE generico processando {url}: {e}")
    
    return None

def create_updated_csv(original_csv_path, images_folder_name, download_results):
    """Crea una copia del CSV con i percorsi locali aggiornati."""
    local_csv_folder = Path("local_csv")
    local_csv_folder.mkdir(exist_ok=True)
    
    new_csv_name = f"{Path(original_csv_path).stem}_local.csv"
    new_csv_path = local_csv_folder / new_csv_name
    
    try:
        with open(original_csv_path, 'r', encoding='utf-8') as infile, \
             open(new_csv_path, 'w', encoding='utf-8', newline='') as outfile:
            
            reader = csv.DictReader(infile)
            if not reader.fieldnames:
                logger.error(f"Il file CSV {original_csv_path} è vuoto o malformattato.")
                return None
            
            writer = csv.DictWriter(outfile, fieldnames=reader.fieldnames)
            writer.writeheader()
            
            for row in reader:
                name = clean_filename(row.get('name', ''))
                if name in download_results and download_results[name]:
                    relative_path = f"/images/{images_folder_name}/{download_results[name]}"
                    row['image_url'] = relative_path
                writer.writerow(row)

        logger.info(f"Nuovo CSV creato: {new_csv_path}")
        return new_csv_path
    except Exception as e:
        logger.error(f"Impossibile creare il nuovo file CSV: {e}")
        return None

def process_csv(csv_file_path, max_workers):
    """Funzione principale per processare un singolo file CSV."""
    logger.info(f"\n--- Inizio processamento per: {csv_file_path} ---")
    
    folder_name = Path(csv_file_path).stem
    save_path = Path(folder_name)
    save_path.mkdir(exist_ok=True)
    
    tasks = []
    try:
        with open(csv_file_path, 'r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                raw_image_url = row.get('image_url', '')
                name = row.get('name', '')

                if raw_image_url and name:
                    http_pos = raw_image_url.find('http')
                    if http_pos != -1:
                        extracted_url = raw_image_url[http_pos:]
                        tasks.append({'name': clean_filename(name), 'url': extracted_url.strip()})
                    else:
                        logger.warning(f"Nessun URL 'http' trovato nella riga per il prodotto: {name}")

    except FileNotFoundError:
        logger.error(f"File non trovato: {csv_file_path}")
        return

    total_images = len(tasks)
    if total_images == 0:
        logger.warning(f"Nessuna immagine valida trovata in {csv_file_path}.")
        return
        
    logger.info(f"Trovate {total_images} immagini valide da processare.")
    
    successful_downloads = 0
    download_results = {}

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(download_process_image, task['url'], save_path, task['name'], i + 1, total_images): task
            for i, task in enumerate(tasks)
        }
        
        for future in futures:
            task = futures[future]
            try:
                result = future.result()
                if result:
                    successful_downloads += 1
                    download_results[task['name']] = result
            except Exception as e:
                logger.error(f"Errore critico nel task per {task['name']}: {e}")

    logger.info(f"\n--- Report per {csv_file_path} ---")
    logger.info(f"Immagini processate con successo: {successful_downloads}/{total_images}")
    
    create_updated_csv(csv_file_path, folder_name, download_results)
    logger.info(f"--- Fine processamento per: {csv_file_path} ---")

# ==============================================================================
# ESECUZIONE PRINCIPALE
# ==============================================================================
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Scarica immagini da file CSV, le converte nel formato appropriato e le rende quadrate preservando la trasparenza."
    )
    parser.add_argument(
        "csv_files", 
        nargs='+',
        help="Percorso/i del/i file CSV da processare."
    )
    parser.add_argument(
        "--workers", 
        type=int, 
        default=5, 
        help="Numero di thread concorrenti per il download."
    )
    
    args = parser.parse_args()
    
    start_time = time.time()
    for csv_file in args.csv_files:
        process_csv(csv_file, args.workers)
    
    end_time = time.time()
    logger.info(f"\nProcesso completato in {end_time - start_time:.2f} secondi.")