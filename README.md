# NLP-Retriever
This repository contains code used to generate the paper `Amore_Pliego_CS572_01_F22.pdf`. 

## Paper Abstract
Modern Question Answering (QA) systems consist of two components: readers
and retrievers. Retrievers reduce the passage search space for answer extraction
and limit the overall accuracy of QA methods. Conventional retrievers consume
large amounts of resources, reducing their viability to large corporations or well
funded institutions. In addition, some retrieval methods are prone to overfitting,
requiring an expensive retraining process to understand new document sources.
In this paper, we outline a methodology for building information retrieval systems
on a limited budget and perform feature enhancement using transfer learning.
Through several ablation studies we demonstrate that existing DPR approaches
are very sensitive to small changes in the problem domain, and introduce an
approach to potentially improve generalizability which outperforms the existing
DPR framework under one ablation. We also highlight a potential data quality
issue from a well-cited paper, which may call into question published accuracy
metrics and warrant additional review.

## Description of Contents
- `./colab_notebooks/`: Directory containing Google Colab notebooks used in the analysis
- `./database_info/`: Directory containing processing information for raw passage documents
- `./question_generator/`: Model declaration for the modified question generator adapted from an initial repository by [Adam Montgomerie](https://github.com/AMontgomerie/question_generator)
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


