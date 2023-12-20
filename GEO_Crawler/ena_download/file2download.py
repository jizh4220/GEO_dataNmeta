# reference: https://www.ebi.ac.uk/bioimage-archive/help-download/
# Import necessary libraries
import os
import pandas as pd
import sys

# header = "$HOME/Applications/AsperaConnect.app/Contents/Resources/ascp -QT -l 300m -P33001 -i $HOME/Applications/AsperaConnect.app/Contents/Resources/asperaweb_id_dsa.openssh"
# Function to generate ascp_url and write them into a shell script
def ascp_collection_queue(asp_download_queue, collection_name, download_dir,
    header = "$HOME/Applications/AsperaConnect.app/Contents/Resources/ascp -QT -l 300m -P33001 -i $HOME/Applications/AsperaConnect.app/Contents/Resources/asperaweb_id_dsa.openssh"):
    # Add prefix to each download queue url
    asp_download_queue = ["era-fasp@" + url for url in asp_download_queue]
    # Combine header, url and download directory
    all_urls = [f"{header} {url} {os.path.join(download_dir, collection_name)}" for url in asp_download_queue]
    print("Successfully generate ascp_url")
    # Write all urls into a shell script
    with open(os.path.join(download_dir, collection_name, "ascp_download.sh"), 'w') as f:
        f.write('\n'.join(all_urls))

# Function to prepare ascp links from a csv file
def prepare_ascp_links(f_path):
    if 'accession_ena_info' not in f_path:
        raise ValueError("Make sure it is an accession_ena_info file! The function will not run.")
    # Read csv file into a pandas DataFrame
    download_url = pd.read_csv(f_path, sep="\t")
    # Get the unique study accession name
    collection_name = download_url['study_accession'].unique()[0]

    # Split the fastq_aspera column by ';' and flatten the result into a list
    asp_download_queue = download_url['fastq_aspera'].str.split(';', expand=True).values.flatten().tolist()

    # Get the directory of the csv file
    collection_name = os.path.dirname(f_path)
    # Get the current working directory
    download_dir = os.getcwd() + "/"
    # Replace 'fasp' with 'ftp://ftp' in each url
    fastq_list = [url.replace("fasp", "ftp://ftp") for url in asp_download_queue]
    # Write all fastq urls into a txt file
    with open(os.path.join(download_dir, collection_name, "fastq_urls.txt"), 'w') as f:
        f.write('\n'.join(fastq_list))
    # Call the function to generate ascp_url and write them into a shell script
    ascp_collection_queue(asp_download_queue, collection_name, download_dir)

# gds_result-ashtma_single-cell_0714_accession_ena_info
input_ena = sys.argv[1]
prepare_ascp_links(input_ena)
