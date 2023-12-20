import requests
import pandas as pd
import sys
import json
import csv
import os
import glob
import io

import logging
logging.getLogger().setLevel(logging.WARNING)


def ena_metadetails(fpath):
    accessioninfo = fpath.replace('.txt', '_detail_info.csv')
    file_dir = os.path.dirname(fpath)
    test = pd.read_csv(accessioninfo)
    non_humanlung = 'media|cell line|organoid|derived|MRC5|NT2-D1|2102EP|NCCIT|TCAM2|HUVEC|H441|BOEC|HUDEP2|A20+K562|H1299|Calu3|MDA-|culture|A549|brain|SW1573|hESC|h358|LM2|organotypic|Transplants|CKDCI|IMR90|IR Senescence|Nkx2.1+ RUES2|AALE|HCT116|BEAS-2B|RT4|LNCaP|H1|HBEC1|HBEC2|Lib_CGA|A549|MDCK|A427|Calu3|LNCaP/AR|AT2|iAT2|ALT-43T|HuT102|MJ|MT-4C16DMS53H524H841H69H82H1048CORL279DMS454H1299NCI-H3122 H3122NCI-H358H358SW480H1299Calu-3LC2PROCL1-5PC9CL1-0Cyt49LC-2/adH2009del-VSMC16HBE14o-prostateH7ECAD9'

    f_test = test[~test['summary'].str.contains(non_humanlung, case=False)]
    f_test = f_test[~f_test['title'].str.contains(non_humanlung, case=False)]
    df = f_test

    accession_ids = set(df.bioproject.to_list())

    # Create an empty DataFrame to store all data
    ena_df = pd.DataFrame()

    print("Fethcing all available ENA information for current data list")
    for accession_id in accession_ids:
        url = f'https://www.ebi.ac.uk/ena/portal/api/filereport?accession={accession_id}&result=read_run&fields=study_accession,sample_accession,secondary_sample_accession,experiment_accession,run_accession,tax_id,scientific_name,experiment_alias,fastq_ftp,fastq_aspera,submitted_ftp,submitted_aspera,sra_ftp,sra_aspera,sample_alias,sample_title&format=json&download=true&limit=0'
        response = requests.get(url)
        data = response.json()
        if len(data) == 0:
            continue
        ena_df = all_data.append(pd.DataFrame(data), ignore_index=True)
        # with open(f'{file_dir}/{accession_id}.tsv', 'w') as f:
        #     writer = csv.DictWriter(f, fieldnames=data[0].keys(), delimiter='\t')
        #     writer.writeheader()
        #     writer.writerows(data)
    # combining every PRJN ENA information
    print("Combining all fecthed PRJN for current data list")
    # Write the combined DataFrame to a single TSV file
    ena_df.to_csv(fpath.replace('.txt', '_accession_ena_info.csv'), sep='\t', index=False)
    
    # all_f = glob.glob(f'{file_dir}/PRJN*')
    # export_accession_ena(all_f, fpath)



def export_accession_ena(all_f, fpath):
    df_list = list()
    for meta in all_f:
        # print(meta)
        df = ena_cleanmeta(meta)
        df_list.append(df)
    print("Export combined accession_ena_info")
    df = pd.concat(df_list)
    df.to_csv(fpath.replace('.txt', '_accession_ena_info.csv'))


def study_crawler(study_accession):
    print("Fetching all available ENA information for current data list")
    all_data = pd.DataFrame()
    url = f'https://www.ebi.ac.uk/ena/portal/api/filereport?accession={study_accession}&result=read_run&fields=all&format=tsv&download=true'
    data = requests.get(url).text
    # data = response.json()
    df = pd.read_csv(io.StringIO(data), sep='\t')
    df.to_csv(f'{study_accession}_accession_ena_info.csv', sep='\t', index=False)


def ascp_collection_queue(asp_download_queue, collection_name, download_dir,
    header = "$HOME/Applications/AsperaConnect.app/Contents/Resources/ascp -QT -l 300m -P33001 -i $HOME/Applications/AsperaConnect.app/Contents/Resources/asperaweb_id_dsa.openssh"):
    # Add prefix to each download queue url
    # asp_download_queue = ["era-fasp@" + url for url in asp_download_queue]
    # Combine header, url and download directory
    all_urls = [f"{header} era-fasp@{url} {os.path.join(download_dir, collection_name)}" for url in asp_download_queue]
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
    # print(download_url['fastq_aspera'])
    # Split the fastq_aspera column by ';' and flatten the result into a list
    asp_download_queue = download_url['fastq_aspera'].str.split(';', expand=True).values.flatten().tolist()
    # Get the directory of the csv file
    collection_name = os.path.dirname(f_path)
    # Get the current working directory
    download_dir = os.getcwd() + "/"
    # Call the function to generate ascp_url and write them into a shell script
    ascp_collection_queue(asp_download_queue, collection_name, download_dir)

# ena_metadetails(fpath)
study_accession = sys.argv[1]
filename = f'{study_accession}_accession_ena_info.csv'
if os.path.isfile(filename):
    print("ENA information of current accession has already been collected")
else :
    # accessioninfo = input_gse.replace('.txt', '_detail_info.csv')
    study_crawler(study_accession)
print("Generate ENA download bash scripts now")
prepare_ascp_links(filename)