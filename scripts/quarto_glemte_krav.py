import os
import requests
import subprocess
import sys

from lib import utils

def update_quarto():
    utils.set_secrets_as_env(split_on=':', secret_name="projects/knada-gcp/secrets/vebjorn-rekkebo/versions/latest")
    ENV = "datamarkedsplassen.intern.nav.no"
    QUARTO_ID = "2cc73eb9-36b4-47d4-a719-918236de37e6"
    TEAM_TOKEN = os.environ["PENSAK_NADA_TOKEN"]

    render_quarto("../quarto/glemte_krav.qmd")

    # A list of file paths to be uploaded
    files_to_upload=["../quarto/glemte_krav.html"]
    upload_quarto(files_to_upload)


def render_quarto(file_to_render: str):
    subprocess.run(f"quarto render {file_to_render}")


def upload_quarto(files_to_upload: list[str]):
    multipart_form_data = {}
    for file_path in files_to_upload:
        file_name = os.path.basename(file_path)
        with open(file_path, 'rb') as file:
            # Read the file contents and store them in the dictionary
            file_contents = file.read()
            multipart_form_data[file_path] = (file_name, file_contents)

    # Send the request with all files in the dictionary
    response = requests.put( f"https://{ENV}/quarto/update/{QUARTO_ID}", 
                            headers={"Authorization": f"Bearer {TEAM_TOKEN}"},
                            files=multipart_form_data)
        
    print("Request to {env} with status code", response.status_code)

update_quarto()