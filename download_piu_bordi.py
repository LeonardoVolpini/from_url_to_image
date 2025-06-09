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
# NUOVA FUNZIONE PER RENDERE LE IMMAGINI QUADRATE
# ==============================================================================

def make_image_square(image_path):
    """
    Controlla se un'immagine è quadrata. Se non lo è, aggiunge bordi bianchi
    per renderla quadrata, centrando l'immagine originale.
    Sovrascrive il file di immagine originale.
    """
    try:
        with Image.open(image_path).convert("RGB") as img:
            width, height = img.size

            # Se l'immagine è già quadrata, non è necessario fare nulla.
            if width == height:
                return

            logger.info(f"L'immagine non è quadrata ({width}x{height}). Aggiunta di bordi a: {image_path}")

            # Trova la dimensione più grande che diventerà la dimensione del nostro quadrato.
            max_dim = max(width, height)

            # Crea una nuova immagine di sfondo (tela) che sia un quadrato bianco.
            with Image.new("RGB", (max_dim, max_dim), (255, 255, 255)) as square_canvas:
                # Calcola le coordinate (x, y) per incollare l'immagine originale al centro.
                paste_x = (max_dim - width) // 2
                paste_y = (max_dim - height) // 2

                # Incolla l'immagine originale sulla tela quadrata.
                square_canvas.paste(img, (paste_x, paste_y))

                # Salva l'immagine quadrata sovrascrivendo quella originale, mantenendo il formato WebP.
                square_canvas.save(image_path, 'WEBP', quality=85)

    except Exception as e:
        logger.error(f"Errore durante la conversione in quadrato di {image_path}: {e}")

# ==============================================================================
# FUNZIONI DI DOWNLOAD E GESTIONE CSV (Aggiornate)
# ==============================================================================

def clean_filename(filename):
    """Pulisce il nome del file da caratteri non validi."""
    return "".join(c for c in filename if c.isalnum() or c in ('_', '-')).rstrip()

def download_process_image(url, save_path, name, index, total):
    """
    Scarica un'immagine, la converte in WebP e la rende quadrata.
    """
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    }
    
    safe_filename = f"{clean_filename(name)}.webp"
    webp_path = os.path.join(save_path, safe_filename)
    
    if os.path.exists(webp_path):
        logger.info(f"[{index}/{total}] File già esistente, saltato: {webp_path}")
        return safe_filename
    
    time.sleep(random.uniform(0.5, 1.5))
    
    try:
        response = requests.get(url, headers=headers, stream=True, timeout=30)
        response.raise_for_status()

        with Image.open(io.BytesIO(response.content)) as img:
            img.save(webp_path, 'WEBP', quality=85)
        
        logger.info(f"[{index}/{total}] Scaricato e convertito: {url} -> {webp_path}")

        # **NUOVO STEP**: Rendi l'immagine quadrata.
        make_image_square(webp_path)

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
        description="Scarica immagini da file CSV, le converte in WebP e le rende quadrate aggiungendo bordi bianchi."
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