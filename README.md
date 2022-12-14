# NLP-Retriever
This repository contains code used to generate the paper `paper_name_here.pdf`. 

## Paper Abstract


## Description of Contents
- `./colab_notebooks/`: Directory containing Google Colab notebooks used in the analysis
- `./database_info/`: Directory containing information about the BigQuery database used to store passage documents
- `./question_generator/`: Model declaration for the question generator with an adjusted regex parser. Modified from an initial repository by [Adam Montgomerie](https://github.com/AMontgomerie/question_generator)
- `helpers.py`: Code containing function declarations used in `main.py`
- `main.py`: Entry point for question generation on a local GPU for passages stored in BigQuery

## Overview
The repo is split into several directories, each with relevant components and a specific `readme`. The main utility is to
provide all code used in the analysis. Much of the work was completed in Google Colab using notebook files.

`main.py` defines the question generation pipeline, which interacts with a Google BigQuery database. The procedure
fetches passages in batches from a source table and inserts results to a target table: both are specified in the 
function call. We also ran question generation on Colab and include a notebook, `./colab_notebooks/QG_Notebook.ipynb`, 
with an example.

## Acknowledgements
This project was a joint effort with [Jose Pilego San Martin](https://github.com/josePliego) and completed as an open-ended
term project for _COMPSCI 572 (Natural Language Processsing)_ at Duke, taught by Professor Bhuwan Dhingra.


