import os
import argparse
import requests
import time
from urllib.parse import urlparse, unquote
from PIL import Image
import io

def clean_filename(filename):
    """Pulisce il nome del file rimuovendo caratteri non validi."""
    invalid_chars = '<>:"/\\|?*'
    for char in invalid_chars:
        filename = filename.replace(char, '_')
    return filename

def download_single_image(url, output_folder, custom_filename=None, quality=85, retry_attempts=3):
    """
    Scarica e converte in WebP una singola immagine.
    
    Args:
        url: L'URL dell'immagine da scaricare
        output_folder: La cartella in cui salvare l'immagine
        custom_filename: Nome file personalizzato (opzionale)
        quality: Qualità della compressione WebP (default: 85)
        retry_attempts: Numero di tentativi in caso di errore (default: 3)
    
    Returns:
        Il percorso del file salvato o None in caso di errore
    """
    # Configura gli headers per sembrare un browser normale
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'image/avif,image/webp,image/apng,image/svg+xml,image/*,*/*;q=0.8',
        'Accept-Language': 'it-IT,it;q=0.9,en-US;q=0.8,en;q=0.7',
        'Referer': 'https://products.kerakoll.com/',
    }
    
    # Assicurati che la cartella di output esista
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
        print(f"Creata la cartella: {output_folder}")
    
    # Determina il nome del file
    if custom_filename:
        # Usa il nome personalizzato fornito dall'utente
        filename = f"{clean_filename(custom_filename)}.webp"
    else:
        # Estrai il nome del file dall'URL
        parsed_url = urlparse(url)
        original_filename = os.path.basename(unquote(parsed_url.path))
        # Rimuovi parametri dalla query string
        original_filename = original_filename.split('?')[0]
        # Ottieni il nome del file senza estensione
        filename_without_ext = os.path.splitext(original_filename)[0]
        # Pulisci e aggiungi l'estensione webp
        filename = f"{clean_filename(filename_without_ext)}.webp"
    
    # Percorso completo del file di output
    output_path = os.path.join(output_folder, filename)
    
    # Se il file esiste già, chiedi all'utente se vuole sovrascriverlo
    if os.path.exists(output_path):
        response = input(f"Il file {output_path} esiste già. Sovrascrivere? (s/n): ")
        if response.lower() != 's':
            print("Download annullato.")
            return None
    
    print(f"Scaricamento di {url}")
    print(f"Destinazione: {output_path}")
    
    # Tenta il download con retry
    for attempt in range(1, retry_attempts + 1):
        try:
            response = requests.get(url, headers=headers, stream=True, timeout=30)
            
            if response.status_code == 200:
                # Apri e converti l'immagine
                img = Image.open(io.BytesIO(response.content))
                
                # Salva come WebP
                img.save(output_path, 'WEBP', quality=quality)
                
                print(f"Immagine scaricata e convertita con successo!")
                print(f"Dimensioni: {img.width}x{img.height} pixel")
                print(f"Salvata in: {output_path}")
                return output_path
            elif response.status_code == 429:  # Too Many Requests
                wait_time = 10 * attempt
                print(f"Rate limit raggiunto (429). Tentativo {attempt}/{retry_attempts}.")
                print(f"Attesa di {wait_time} secondi prima del prossimo tentativo...")
                time.sleep(wait_time)
            else:
                print(f"ERRORE: Impossibile scaricare l'immagine. Status code: {response.status_code}")
                if attempt < retry_attempts:
                    wait_time = 5 * attempt
                    print(f"Tentativo {attempt}/{retry_attempts}. Attesa di {wait_time} secondi...")
                    time.sleep(wait_time)
                else:
                    print("Tutti i tentativi falliti.")
                    return None
        except Exception as e:
            print(f"ERRORE: {str(e)}")
            if attempt < retry_attempts:
                wait_time = 5 * attempt
                print(f"Tentativo {attempt}/{retry_attempts}. Attesa di {wait_time} secondi...")
                time.sleep(wait_time)
            else:
                print("Tutti i tentativi falliti.")
                return None
    
    return None

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scarica e converti in WebP una singola immagine")
    parser.add_argument("url", help="URL dell'immagine da scaricare")
    parser.add_argument("output_folder", help="Cartella in cui salvare l'immagine")
    parser.add_argument("--filename", help="Nome file personalizzato (opzionale, senza estensione)")
    parser.add_argument("--quality", type=int, default=85, help="Qualità della compressione WebP (1-100, default: 85)")
    
    args = parser.parse_args()
    
    download_single_image(args.url, args.output_folder, args.filename, args.quality)
    
    #Script:
    # python download_single_image.py "https://link_image" folder_name