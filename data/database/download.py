import os
import sys
from datetime import date, datetime
from pathlib import Path

import requests


def download_file_from_google_drive(file_id: str, destination: str):
    """
    Baixa um arquivo público do Google Drive, seguindo o token de confirmação caso seja grande.
    """
    URL = "https://docs.google.com/uc?export=download"
    session = requests.Session()

    # primeira requisição para obter token de confirmação (se houver)
    response = session.get(URL, params={"id": file_id}, stream=True)
    token = _get_confirm_token(response)

    if token:
        # refaz requisição com o token
        response = session.get(
            URL, params={"id": file_id, "confirm": token}, stream=True
        )

    _save_response_content(response, destination)


def _get_confirm_token(response):
    for key, value in response.cookies.items():
        if key.startswith("download_warning"):
            return value
    return None


def _save_response_content(response, destination, chunk_size=32768):
    with open(destination, "wb") as f:
        for chunk in response.iter_content(chunk_size):
            if chunk:
                f.write(chunk)


if __name__ == "__main__":
    FILE_ID = "1v6LRphp2kYciU4SXp0PCjEMuev1bDejc"

    # Salva na mesma pasta onde o script está
    SCRIPT_DIR = Path(__file__).parent
    DEST_PATH = SCRIPT_DIR / "database.csv"

    print("Baixando o CSV do Google Drive…")
    download_file_from_google_drive(FILE_ID, str(DEST_PATH))
    print(f"[OK] Download concluído em: {DEST_PATH}")
