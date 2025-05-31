import os
import csv
import requests
import time
import random
from pathlib import Path
from urllib.parse import urlparse, unquote
import argparse
from concurrent.futures import ThreadPoolExecutor
from PIL import Image
import io
import logging

# Configurazione del logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("download_log.txt"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def clean_filename(filename):
    """Pulisce il nome del file rimuovendo caratteri non validi."""
    invalid_chars = '<>:"/\\|?*'
    for char in invalid_chars:
        filename = filename.replace(char, '_')
    return filename

def download_and_convert_image(url, save_path, name, index, total, retry_delay=5, max_retries=3):
    """Scarica un'immagine dall'URL e la converte in WebP con gestione dei tentativi."""
    # Configuriamo uno User-Agent realistico
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'image/avif,image/webp,image/apng,image/svg+xml,image/*,*/*;q=0.8',
        'Accept-Language': 'it-IT,it;q=0.9,en-US;q=0.8,en;q=0.7'
    }
    
    # Puliamo il nome del file
    safe_filename = clean_filename(name)
    
    # Il nuovo percorso del file con estensione WebP
    webp_filename = f"{safe_filename}.webp"
    webp_path = os.path.join(save_path, webp_filename)
    
    # Se il file esiste già, aggiungiamo un indice
    if os.path.exists(webp_path):
        webp_filename = f"{safe_filename}_{index}.webp"
        webp_path = os.path.join(save_path, webp_filename)
    
    # Se il file convertito esiste già, lo saltiamo
    if os.path.exists(webp_path):
        logger.info(f"[{index}/{total}] Il file esiste già: {webp_path}")
        return webp_filename  
    
    # Aggiungiamo un ritardo casuale prima di ogni richiesta per evitare di essere bloccati
    time.sleep(random.uniform(0.5, 2.0))
    
    for attempt in range(1, max_retries + 1):
        try:
            # Facciamo la richiesta con gli header personalizzati
            response = requests.get(url, headers=headers, stream=True, timeout=30)
            
            if response.status_code == 200:
                # Convertiamo l'immagine in WebP
                img = Image.open(io.BytesIO(response.content))
                
                # Salviamo come WebP con qualità 85%
                img.save(webp_path, 'WEBP', quality=85)
                
                logger.info(f"[{index}/{total}] Scaricata e convertita: {url} -> {webp_path}")
                return webp_filename  
            elif response.status_code == 429:  # Too Many Requests
                wait_time = retry_delay * (2 ** (attempt - 1))  # Backoff esponenziale
                logger.warning(f"[{index}/{total}] Rate limit raggiunto (429). Tentativo {attempt}/{max_retries}. Attesa di {wait_time} secondi...")
                time.sleep(wait_time)
            else:
                logger.error(f"[{index}/{total}] ERRORE: Impossibile scaricare {url}, status code: {response.status_code}")
                if attempt < max_retries:
                    wait_time = retry_delay * attempt
                    logger.info(f"Tentativo {attempt}/{max_retries}. Attesa di {wait_time} secondi...")
                    time.sleep(wait_time)
                else:
                    return None
        except Exception as e:
            logger.error(f"[{index}/{total}] ERRORE durante il download/conversione di {url}: {str(e)}")
            if attempt < max_retries:
                wait_time = retry_delay * attempt
                logger.info(f"Tentativo {attempt}/{max_retries}. Attesa di {wait_time} secondi...")
                time.sleep(wait_time)
            else:
                return None
    
    return None

def create_updated_csv(original_csv_path, images_folder_name, download_results):
    """Crea una copia del CSV originale sostituendo gli URL con i path locali."""
    # Creiamo la cartella local_csv se non esiste
    local_csv_folder = Path("local_csv")
    local_csv_folder.mkdir(exist_ok=True)
    
    # Nome del nuovo file CSV
    csv_filename = os.path.basename(original_csv_path)
    new_csv_name = f"{os.path.splitext(csv_filename)[0]}_local.csv"
    new_csv_path = os.path.join(local_csv_folder, new_csv_name)
    
    # Leggiamo il CSV originale e creiamo quello nuovo
    with open(original_csv_path, 'r', encoding='utf-8') as input_file, \
         open(new_csv_path, 'w', encoding='utf-8', newline='') as output_file:
        
        reader = csv.DictReader(input_file)
        fieldnames = reader.fieldnames
        writer = csv.DictWriter(output_file, fieldnames=fieldnames)
        writer.writeheader()
        
        for row in reader:
            if 'image_url' in row and 'name' in row and row['name']:
                name = row['name'].strip().replace(' ', '_')
                # Se abbiamo scaricato con successo l'immagine, sostituiamo l'URL
                if name in download_results and download_results[name] is not None:
                    # Path assoluto alla cartella delle immagini
                    abs_path = os.path.abspath(os.path.join(images_folder_name, download_results[name]))
                    row['image_url'] = abs_path
                # Se il download è fallito, manteniamo l'URL originale
            writer.writerow(row)
    
    logger.info(f"Creato nuovo CSV con path locali (assoluti): {new_csv_path}")
    return new_csv_path

def process_csv(csv_file_path, max_workers=3, continue_from=None):
    """Processa il file CSV e scarica/converte tutte le immagini."""
    # Otteniamo il nome del file senza estensione
    csv_filename = os.path.basename(csv_file_path)
    folder_name = os.path.splitext(csv_filename)[0]
    
    # Creiamo la cartella di destinazione per le immagini
    save_path = Path(folder_name)
    save_path.mkdir(exist_ok=True)
    
    logger.info(f"File CSV: {csv_file_path}")
    logger.info(f"Cartella di output immagini: {save_path}")
    logger.info(f"Cartella di output CSV: local_csv/")
    
    # Leggiamo il file CSV
    image_urls = {}
    with open(csv_file_path, 'r', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            if 'image_url' in row and row['image_url'] and 'name' in row and row['name']:
                name = row['name'].strip().replace(' ', '_')
                image_urls[name] = row['image_url']
    
    total_images = len(image_urls)
    logger.info(f"Trovate {total_images} URL di immagini nel file CSV.")

    # Se è specificato un punto di ripresa, filtriamo gli URL
    start_index = 0
    # Trasformiamo il dict in lista di tuple per poter fare slicing
    items = list(image_urls.items())  # [(name1, url1), (name2, url2), ...]

    if continue_from is not None:
        try:
            start_index = int(continue_from)
            logger.info(f"Riprendendo dal download numero {start_index}")
            # slice a partire da start_index-1
            items = items[start_index-1:]
        except ValueError:
            logger.warning(f"Valore non valido per continue_from: {continue_from}. Verranno scaricate tutte le immagini.")

    # Scarichiamo e convertiamo tutte le immagini con un ThreadPoolExecutor
    successful_downloads = 0
    failed_downloads = []
    download_results = {}  # Dizionario per tracciare i risultati dei download
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        # items è lista di (name, url); i parte da start_index+1 per avere numerazione giusta
        for i, (name, url) in enumerate(items, start_index + 1):
            futures.append((i, name, url, executor.submit(
                download_and_convert_image, 
                url, save_path, name, i, total_images
            )))
        
        # Raccogliamo i risultati
        for i, name, url, future in futures:
            try:
                result = future.result()
                if result is not None:
                    successful_downloads += 1
                    download_results[name] = result  # Salviamo il nome del file scaricato
                else:
                    failed_downloads.append((i, name, url))
                    download_results[name] = None  # Segniamo il fallimento
            except Exception as e:
                logger.error(f"Errore nell'esecuzione del download {i} ({name}): {e}")
                failed_downloads.append((i, name, url))
                download_results[name] = None
    
    # Creiamo il nuovo CSV con i path locali nella cartella local_csv/
    new_csv_path = create_updated_csv(csv_file_path, folder_name, download_results)
    
    logger.info(f"\nOperazione completata!")
    logger.info(f"Immagini scaricate e convertite con successo: {successful_downloads}/{len(image_urls)}")
    logger.info(f"CSV aggiornato creato: {new_csv_path}")
    
    # Salva gli URL falliti in un file per un eventuale retry
    if failed_downloads:
        logger.warning(f"Download falliti: {len(failed_downloads)}")
        with open("failed_downloads.txt", "w", encoding="utf-8") as f:
            for i, name, url in failed_downloads:
                f.write(f"{i},{name},{url}\n")
        logger.info("Gli URL dei download falliti sono stati salvati in 'failed_downloads.txt'")
    
    return successful_downloads

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scarica e converti in WebP le immagini da un file CSV")
    parser.add_argument("csv_file", help="Percorso del file CSV contenente gli URL delle immagini")
    parser.add_argument("--workers", type=int, default=3, help="Numero massimo di thread concorrenti (default: 3)")
    parser.add_argument("--continue-from", type=int, help="Indice da cui riprendere il download (opzionale)")
    
    args = parser.parse_args()
    
    process_csv(args.csv_file, args.workers, args.continue_from)
    
    #Script:
    # python download_images.py nome_csv.csv