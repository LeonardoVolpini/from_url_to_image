import os
import csv
# import requests # Rimosso
import httpx      # Aggiunto
from httpx import ConnectError as HttpxConnectError, HTTPStatusError, RequestError as HttpxRequestError # Aggiunte eccezioni specifiche
import time
import random
from pathlib import Path
from urllib.parse import urlparse, unquote # unquote non è usato qui, ma potrebbe servire altrove
import argparse
from concurrent.futures import ThreadPoolExecutor
from PIL import Image
import io
import logging

# Configurazione del logging (invariata)
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
    """Pulisce il nome del file rimuovendo caratteri non validi, spazi, a capo e tabulazioni."""
    invalid_chars = '<>:"/\\|?*\n\r\t '
    for char in invalid_chars:
        filename = filename.replace(char, '_')
    return filename

def download_and_convert_image(url, save_path, name, index, total, retry_delay=5, max_retries=3):
    """Scarica un'immagine dall'URL e la converte in WebP con gestione dei tentativi usando httpx."""
    
    headers = { # Gli header possono essere definiti una volta
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36',
        'Accept': 'image/avif,image/webp,image/apng,image/*,*/*;q=0.8', # Mantenuto più specifico per le immagini
        'Accept-Language': 'it-IT,it;q=0.9,en-US;q=0.8,en;q=0.7',
        'Referer': f"{urlparse(url).scheme}://{urlparse(url).netloc}/"
    }
    
    safe_filename = clean_filename(name)
    webp_filename = f"{safe_filename}.webp"
    webp_path = os.path.join(save_path, webp_filename)
    
    # Logica per gestire i file esistenti (semplificata per chiarezza, adatta alla tua logica originale)
    # La tua logica originale per `safe_filename_{index}.webp` dovrebbe essere integrata qui se necessario.
    # Questo controllo base è per saltare se il file finale previsto esiste.
    # Considera la tua logica di gestione dei duplicati con `_{index}` globale.
    # Qui un esempio base:
    if os.path.exists(webp_path):
         logger.info(f"[{index}/{total}] Il file esiste già (controllo base): {webp_path}")
         # Se usi la logica con _{index} globale, dovrai controllare anche quel nome file.
         # Per ora, assumiamo che se webp_path (calcolato con {index} o meno) esiste, lo saltiamo.
         # Potresti dover rendere webp_filename e webp_path unici prima di questo blocco.
         # La tua logica:
         # if os.path.exists(webp_path): # Se 'nome.webp' esiste
         #     webp_filename = f"{safe_filename}_{index}.webp" # Crea 'nome_NUMEROGLOBALE.webp'
         #     webp_path = os.path.join(save_path, webp_filename)
         # Questo va fatto prima del controllo finale di esistenza:

    # Applica la logica di indicizzazione se il file base esiste
    # (come nel tuo codice originale, l'index è il contatore globale)
    if os.path.exists(os.path.join(save_path, f"{safe_filename}.webp")):
        potential_indexed_filename = f"{safe_filename}_{index}.webp"
        # Verifica se il file con l'indice globale esiste già
        if os.path.exists(os.path.join(save_path, potential_indexed_filename)):
            logger.info(f"[{index}/{total}] Il file (con indice globale) esiste già: {os.path.join(save_path, potential_indexed_filename)}")
            return potential_indexed_filename # Restituisci il nome del file esistente
        else: # Il file base esiste, ma quello con l'indice globale no, quindi usiamo quello con l'indice
            webp_filename = potential_indexed_filename
            webp_path = os.path.join(save_path, webp_filename)
    # Se neanche il file base esiste, webp_filename e webp_path sono già corretti per 'nome.webp'

    # Controllo finale se il file (con o senza indice) esiste già
    if os.path.exists(webp_path):
        logger.info(f"[{index}/{total}] Il file esiste già: {webp_path}")
        return webp_filename

    time.sleep(random.uniform(1.0, 3.0)) # Ritardo casuale
    
    # httpx.Client va usato preferibilmente con un context manager per chiamata se non si passano client ai thread
    # Oppure un client per thread. Qui ne creiamo uno per ogni chiamata a download_and_convert_image.
    try:
        with httpx.Client(http2=True, headers=headers, follow_redirects=True, timeout=30.0) as client:
            for attempt in range(1, max_retries + 1):
                try:
                    logger.debug(f"[{index}/{total}] Tentativo {attempt}/{max_retries} per {url}")
                    response = client.get(url) # Gli header sono già nel client
                    
                    if response.status_code == 200:
                        img = Image.open(io.BytesIO(response.content)) # response.content funziona come in requests
                        img.save(webp_path, 'WEBP', quality=85)
                        logger.info(f"[{index}/{total}] Scaricata e convertita (HTTP/2): {url} -> {webp_path}")
                        return webp_filename  
                    elif response.status_code == 429: # Too Many Requests
                        wait_time = retry_delay * (2 ** (attempt - 1)) 
                        logger.warning(f"[{index}/{total}] Rate limit raggiunto (429) per {url}. Tentativo {attempt}/{max_retries}. Attesa di {wait_time} secondi...")
                        time.sleep(wait_time)
                    else:
                        # Gestisce altri errori HTTP usando HTTPStatusError
                        logger.error(f"[{index}/{total}] ERRORE HTTP {response.status_code}: Impossibile scaricare {url}")
                        if attempt < max_retries:
                            wait_time = retry_delay * attempt 
                            logger.info(f"Tentativo {attempt}/{max_retries}. Attesa di {wait_time} secondi...")
                            time.sleep(wait_time)
                        else:
                            logger.error(f"[{index}/{total}] Download fallito per {url} dopo {max_retries} tentativi (status code: {response.status_code}).")
                            return None
                
                except HttpxConnectError as e: # Errore di connessione specifico di httpx
                    logger.warning(f"[{index}/{total}] ERRORE DI CONNESSIONE (httpx) per {url} (tentativo {attempt}/{max_retries}): {str(e)}")
                    if attempt < max_retries:
                        wait_time = retry_delay * (2 ** (attempt - 1)) 
                        logger.info(f"Attesa di {wait_time} secondi...")
                        time.sleep(wait_time)
                    else:
                        logger.error(f"[{index}/{total}] Download fallito per {url} dopo {max_retries} tentativi (errore di connessione persistente).")
                        return None # Esce dal loop dei tentativi per questa immagine
                except HTTPStatusError as e: # Cattura errori 4xx/5xx se raise_for_status() fosse usato, o per info
                     logger.error(f"[{index}/{total}] ERRORE HTTP STATUS (httpx) per {url} (tentativo {attempt}/{max_retries}): {e.response.status_code} - {str(e)}")
                     # La logica di retry per status code è già sopra, questo è più per errori imprevisti
                     # o se si usasse response.raise_for_status()
                     if attempt < max_retries:
                        wait_time = retry_delay * attempt
                        logger.info(f"Attesa di {wait_time} secondi...")
                        time.sleep(wait_time)
                     else:
                        return None
                except HttpxRequestError as e: # Altri errori di richiesta specifici di httpx (es. ReadTimeout)
                    logger.error(f"[{index}/{total}] ERRORE RICHIESTA (httpx) per {url} (tentativo {attempt}/{max_retries}): {str(e)}")
                    if attempt < max_retries:
                        wait_time = retry_delay * attempt
                        logger.info(f"Attesa di {wait_time} secondi...")
                        time.sleep(wait_time)
                    else:
                        logger.error(f"[{index}/{total}] Download fallito per {url} dopo {max_retries} tentativi (errore richiesta).")
                        return None
                except Exception as e: # Altre eccezioni generiche (es. problemi con PIL)
                    logger.error(f"[{index}/{total}] ERRORE INASPETTATO (non-httpx) durante il download/conversione di {url} (tentativo {attempt}/{max_retries}): {type(e).__name__} - {str(e)}")
                    if attempt < max_retries:
                        wait_time = retry_delay * attempt
                        logger.info(f"Attesa di {wait_time} secondi...")
                        time.sleep(wait_time)
                    else:
                        logger.error(f"[{index}/{total}] Download fallito per {url} dopo {max_retries} tentativi (errore inaspettato).")
                        return None
            
            # Se il loop finisce senza successo o return
            logger.error(f"[{index}/{total}] Download fallito per {url} dopo tutti i tentativi nel loop.")
            return None

    except Exception as e: # Eccezione nella creazione del client httpx o fuori dal loop
        logger.error(f"[{index}/{total}] ERRORE CRITICO con httpx.Client per {url}: {str(e)}")
        return None

# Le funzioni create_updated_csv, process_csv e il blocco if __name__ == "__main__":
# possono rimanere sostanzialmente invariate. L'unica cosa è che `download_and_convert_image`
# ora usa `httpx`.
# (Il resto del tuo script: create_updated_csv, process_csv, if __name__ ...)
# COPIA IL RESTO DEL TUO SCRIPT DA QUI IN POI
# Assicurati che la logica di gestione dei nomi file duplicati in `download_and_convert_image`
# sia quella che preferisci. Ho provato a integrare la tua logica di `safe_filename_{index}.webp`.

def create_updated_csv(original_csv_path, images_folder_name, download_results):
    """Crea una copia del CSV originale sostituendo gli URL con i path locali."""
    local_csv_folder = Path("local_csv")
    local_csv_folder.mkdir(exist_ok=True)
    
    csv_filename = os.path.basename(original_csv_path)
    new_csv_name = f"{os.path.splitext(csv_filename)[0]}_local.csv"
    new_csv_path = os.path.join(local_csv_folder, new_csv_name)
    
    with open(original_csv_path, 'r', encoding='utf-8') as input_file, \
         open(new_csv_path, 'w', encoding='utf-8', newline='') as output_file:
        
        reader = csv.DictReader(input_file)
        fieldnames = reader.fieldnames
        if not fieldnames: 
            logger.error(f"Il file CSV {original_csv_path} è vuoto o non ha header.")
            return None
        writer = csv.DictWriter(output_file, fieldnames=fieldnames)
        writer.writeheader()
        
        for row in reader:
            if 'image_url' in row and 'name' in row and row.get('name'): 
                name_original = row['name'].strip()
                name_cleaned_for_dict_key = name_original.replace(' ', '_') 

                if name_cleaned_for_dict_key in download_results and download_results[name_cleaned_for_dict_key] is not None:
                    relative_path = f"/images/{images_folder_name}/{download_results[name_cleaned_for_dict_key]}"
                    row['image_url'] = relative_path
            writer.writerow(row)
    
    logger.info(f"Creato nuovo CSV con path locali relativi: {new_csv_path}")
    return new_csv_path

def process_csv(csv_file_path, max_workers=3, continue_from=None):
    """Processa il file CSV e scarica/converte tutte le immagini."""
    csv_filename = os.path.basename(csv_file_path)
    folder_name = os.path.splitext(csv_filename)[0]
    
    save_path = Path(folder_name)
    save_path.mkdir(exist_ok=True)
    
    logger.info(f"File CSV: {csv_file_path}")
    logger.info(f"Cartella di output immagini: {save_path}")
    logger.info(f"Cartella di output CSV: local_csv/")
    
    image_urls_to_process = [] 
    with open(csv_file_path, 'r', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for i, row in enumerate(reader):
            if 'image_url' in row and row['image_url'] and 'name' in row and row.get('name'):
                original_name = row['name'].strip()
                image_url = row['image_url'].strip()
                cleaned_name_key = original_name.replace(' ', '_')
                image_urls_to_process.append({'original_name': original_name, 'cleaned_name': cleaned_name_key, 'url': image_url, 'original_index': i})

    total_images_to_process = len(image_urls_to_process)
    logger.info(f"Trovate {total_images_to_process} voci immagine da processare nel file CSV.")

    items_to_download = []
    current_start_index_for_loop = 0 # Indice 0-based per lo slicing
    if continue_from is not None:
        try:
            start_index_val = int(continue_from) 
            if 1 <= start_index_val <= total_images_to_process:
                current_start_index_for_loop = start_index_val -1
                items_to_download = image_urls_to_process[current_start_index_for_loop:]
                logger.info(f"Riprendendo il download dalla voce CSV numero {start_index_val} (indice {current_start_index_for_loop}). Immagini rimanenti da processare: {len(items_to_download)}")
            else:
                items_to_download = image_urls_to_process
                logger.warning(f"Valore continue_from ({start_index_val}) fuori range. Verranno processate tutte le {total_images_to_process} immagini.")
        except ValueError:
            items_to_download = image_urls_to_process
            logger.warning(f"Valore non valido per continue_from: {continue_from}. Verranno processate tutte le {total_images_to_process} immagini.")
    else:
        items_to_download = image_urls_to_process
        
    successful_downloads_session = 0
    failed_downloads_info = [] 
    download_results = {} 
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures_map = {} 
        
        for item_data in items_to_download:
            # L' 'index' passato a download_and_convert_image è il numero di riga CSV (1-based)
            csv_row_num_for_function = item_data['original_index'] + 1

            future = executor.submit(
                download_and_convert_image,  
                item_data['url'], 
                save_path, 
                item_data['cleaned_name'], 
                csv_row_num_for_function, 
                total_images_to_process 
            )
            futures_map[future] = {'cleaned_name': item_data['cleaned_name'], 'url': item_data['url'], 'csv_row_num': csv_row_num_for_function}
        
        for future in futures_map: # Era concurrent.futures.as_completed(futures_map)
            info = futures_map[future]
            cleaned_name = info['cleaned_name']
            url = info['url']
            csv_row_num = info['csv_row_num']
            try:
                result = future.result() 
                download_results[cleaned_name] = result 
                if result:
                    successful_downloads_session += 1
                else:
                    failed_downloads_info.append((csv_row_num, cleaned_name, url))
            except Exception as e:
                logger.error(f"Errore nell'esecuzione del future per l'immagine {cleaned_name} (riga CSV {csv_row_num}): {type(e).__name__} - {e}")
                download_results[cleaned_name] = None 
                failed_downloads_info.append((csv_row_num, cleaned_name, url))
                
    new_csv_path = create_updated_csv(csv_file_path, folder_name, download_results)
    
    logger.info(f"\nOperazione completata!")
    logger.info(f"Immagini tentate in questa sessione: {len(items_to_download)}")
    logger.info(f"Download riusciti in questa sessione: {successful_downloads_session}")
    if new_csv_path:
        logger.info(f"CSV aggiornato creato: {new_csv_path}")
    else:
        logger.error("Creazione del CSV aggiornato fallita.")
        
    if failed_downloads_info:
        logger.warning(f"Download falliti o errori durante il processo: {len(failed_downloads_info)}")
        with open("failed_downloads.txt", "w", encoding="utf-8") as f:
            f.write("CSV_Row_Num,Name,URL\n") 
            for csv_idx, name_val, url_val in failed_downloads_info:
                f.write(f"{csv_idx},{name_val},{url_val}\n")
        logger.info("I dettagli dei download falliti sono stati salvati in 'failed_downloads.txt'")
    
    return successful_downloads_session

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scarica e converti in WebP le immagini da un file CSV")
    parser.add_argument("csv_file", help="Percorso del file CSV contenente gli URL delle immagini")
    parser.add_argument("--workers", type=int, default=3, help="Numero massimo di thread concorrenti (default: 3)")
    parser.add_argument("--continue-from", type=int, help="Numero della riga (1-based) da cui riprendere il download (opzionale)")
    
    args = parser.parse_args()
    
    process_csv(args.csv_file, args.workers, args.continue_from)