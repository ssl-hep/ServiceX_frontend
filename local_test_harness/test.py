"""
Step 0: Start the codegen container
Step 1: Send the request with the query
Step 3: Set up the x509 proxy at /tmp -- do this manually because you have to provide passphrase
Step 4: Run the docker compose up with the relevant root file

Input: Query, Root file and output format
"""
import requests
import subprocess
import os
import json
from requests_toolbelt.multipart import decoder
from io import BytesIO
from zipfile import ZipFile
import argparse
import time

def start_codegen_container():
    subprocess.run(["docker", "run", "-d", "-p", "8000:5000", "--name","code_gen", "sslhep/servicex_code_gen_raw_uproot:develop"])

def send_request(query):
    post_url = "http://localhost:8000"
    query = json.dumps(query)
    postObj = {"code": query}
    result = requests.post(post_url + "/servicex/generated-code", json=postObj)
    return result

def generate_zipfile(result, output_folder):
    decoder_parts = decoder.MultipartDecoder.from_response(result)
    transformer_image = (decoder_parts.parts[0].text).strip()
    transformer_language = (decoder_parts.parts[1].text).strip()
    transformer_command = (decoder_parts.parts[2].text).strip()
    zipfile = decoder_parts.parts[3].content
    zipfile = ZipFile(BytesIO(zipfile))

    if not os.path.exists(output_folder):
        os.mkdir(output_folder)
    zipfile.extractall(output_folder)

def run_docker_compose_for_science():
    subprocess.run(["docker", "compose", "up", "-d", "--remove-orphans"])

def send_root_file_to_science(root_file, output_file, output_format):
    subprocess.run(["docker","compose","run","science","python",\
                    "/generated/transform_single_file.py",\
                    root_file, output_file, output_format])

                    # "root://xrootd.aglt2.org:1094//pnfs/aglt2.org/atlasdatadisk/rucio/mc20_13TeV/d4/32/DAOD_PHYS.37620644._000804.pool.root.1",\
                    # "/generated/out.parquet",  "root-file"])

if __name__ == "__main__":
    
    #start codegen
    start_codegen_container()
    time.sleep(10)
    # #send request and extract the zip file to folder
    query = [{'treename': {'nominal': 'modified'}, 'filter_name': ['lbn']}]
    result = send_request(query = query)
    generate_zipfile(result, output_folder="temp1")

    #setup the x509 proxy at your /tmp
    """
    Make sure you have the usercert.pem and userkey.pem in your ~/.globus folder
    Run this command:
    docker run -it --mount type=bind,source=$HOME/.globus,readonly,target=/globus -v /tmp:/tmp --rm sslhep/x509-secrets:develop voms-proxy-init -voms atlas -cert /globus/usercert.pem -key /globus/userkey.pem -out /tmp/x509up
    """

    # #run the docker compose for the science container
    run_docker_compose_for_science()
    time.sleep(10)
    
    # #send the root file to the science container
    send_root_file_to_science(
        root_file="root://xrootd.aglt2.org:1094//pnfs/aglt2.org/atlasdatadisk/rucio/mc20_13TeV/d4/32/DAOD_PHYS.37620644._000804.pool.root.1",
        output_file="/generated/out.parquet",
        output_format="root-file"
    )

