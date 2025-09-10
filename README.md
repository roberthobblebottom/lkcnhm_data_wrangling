# LKCNHM Data Wrangling for centralised portal development

Biodiversity of Singapore to gbif matching data wrangling.

The following commands is for `bash` cli and `uv` python environment.   
  
## Running the marimo notebook:  
Create the python environement by going to this folder in Command Line Interface and do 

```
uv init
source .venv/bin/activate  
uv pip install -r requirements.txt`
```
  
Type `marimo edit bos_gbif_match_marimo_notebook.py` to run the wrangling. Requires `gbif/Taxon.tsv` (not in repo) and `bos.csv`  
  
[You can download gbif from here](https://www.gbif.org/dataset/d7dddbf4-2cf0-4f39-9b2a-bb099caae36)

## Viewing the static version of the notebook
This version is available in '__marimo__/bos_gbif_match_marimo_notebook.html'

# CSVs
`updated_to_matching.csv` = `first_matches_set_from_wrangling.csv` + `second_matches_set_from_wrangling.csv`	   
  
`no_match.csv` are the remainders that does not have any `matched_taxonID` and does not match with any of `gbif` datapoints when used with the algorithms in the script. 
