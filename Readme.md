# Overview 

A github repo targets GEO on how to fetch all necessary data from GEO databse in GEO query format. This entire workflow is designed to conquer the challenge of multi-omics integration(meta data harmonization, matrix allignment). You will receive a list of download url with accession ID and the corresponding metadata

Last update: 12/20/2023, Justin Zhang(justinxuan1230@gmail.com)

## Table of Contents
* [Initialization](#Initialization)
* [GSE_Crawler](#GSE_Crawler)
* [GSM_Crawler](#GSM_Crawler)
* [GEO_download](#GEO_download)
* [ENA_download](#ENA_download)
* [bulk_seurat/scanpy](#bulk_seurat/scanpy)

```bash
python3 gse_crawler.py YOUR_GDS_IDENTIFIER
python3 ena_crawler.py YOUR_GDS_IDENTIFIER
python3 gsm_crawler.py YOUR_GDS_IDENTIFIER
```

You can also check `GEO_Crawler/GEO_Crawler_demo.ipynb` to run a sample query fetch.