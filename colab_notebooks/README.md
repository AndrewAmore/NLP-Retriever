# Colab Notebooks

Contains information on the Google Colab notebooks used in the analysis.

## Description of Contents
- `./QG_Notebook.ipynb`: Data augmentation notebook for question generation using the Colab GPU
- `./add_wiki_passage_encodings/`: Contains the notebooks needed to append the Wikipedia noise passage encodings to the indexes created in `./nq_train_passage_encodings/`
- `./evaluate_retrieval/`: Contains the notebooks used to create the results table in the writeup, evaluates accuracy for each retrieval configuration
- `./nq_train_passage_encodings/`: Contains the notebooks used to obtain passage encodings from the NQ Train dataset and creates indexes using FAISS.
