import requests
import pandas as pd
import sys
import csv
import os
import glob
import io


def ena_metadetails(fpath):
    accessioninfo = fpath.replace('.txt', '_detail_info.csv')
    file_dir = os.path.dirname(fpath)
    test = pd.read_csv(accessioninfo)
    non_humanlung = ['media', 'cell line', 'organoid', 'derived', 'MRC5', 'NT2-D1', '2102EP', 'NCCIT', 'TCAM2', 'HUVEC', 'H441', 'BOEC', 'HUDEP2',
        'A20+K562', 'H1299', 'Calu3', 'MDA-', 'culture', 'A549', 'brain', 'SW1573', 'hESC', 'h358', 'LM2', 'organotypic', 'Transplants', 'CKDCI', 'IMR90',
        'IR Senescence', 'Nkx2.1+ RUES2', 'AALE', 'HCT116', 'BEAS-2B', 'RT4', 'LNCaP', 'H1', 'HBEC1', 'HBEC2', 'Lib_CGA']

    f_test = test[~test['summary'].apply(lambda x: any([k in x for k in non_humanlung]))]
    f_test = f_test[~f_test['title'].apply(lambda x: any([k in x for k in non_humanlung]))]
    df = f_test

    accession_ids = set(df.bioproject.to_list())

    ena_df = pd.DataFrame()
    print("Fethcing all available ENA information for current data list")
    for accession_id in accession_ids:
        url = f'https://www.ebi.ac.uk/ena/portal/api/filereport?accession={accession_id}&result=read_run&fields=all&format=tsv&download=true'
        response = requests.get(url)
        data = pd.read_csv(io.StringIO(response.text), sep='\t')
        if data.empty:
            continue
        ena_df = ena_df.append(data, ignore_index=True)

    print("Combining all fecthed PRJN for current data list")
    # Write the combined DataFrame to a single TSV file
    ena_df.to_csv(fpath.replace('.txt', '_accession_ena_info.csv'), sep='\t', index=False)


def ena_cleanmeta(ena_info):
    # clean ena table format
    try:
        df = pd.read_table(ena_info, sep="\t")
    except:
        return
    num_rows = df.shape[0]
    if num_rows == 0:
        return
    df.drop(['experiment_accession', 'tax_id', 'fastq_ftp', 'fastq_aspera', 'sra_aspera', 'submitted_aspera', 'submitted_ftp', 'sra_ftp'], axis=1, inplace=True)
    df = df[(df['scientific_name'] == 'Homo sapiens')]
    return(df)


def export_accession_ena(all_f, fpath):
    df_list = list()
    for meta in all_f:
        # print(meta)
        df = ena_cleanmeta(meta)
        df_list.append(df)
    print("Export combined accession_ena_info")
    df = pd.concat(df_list)
    df.to_csv(fpath.replace('.txt', '_accession_ena_info.csv'))

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
input_gse = sys.argv[1]
# accessioninfo = input_gse.replace('.txt', '_detail_info.csv')
filename = input_gse.replace('.txt', '_accession_ena_info.csv')
if os.path.isfile(filename):
    print("ENA information of current accession has already been collected")
else :
    # accessioninfo = input_gse.replace('.txt', '_detail_info.csv')
    ena_metadetails(input_gse)
print("Generate ENA download bash scripts now")
prepare_ascp_links(filename)
