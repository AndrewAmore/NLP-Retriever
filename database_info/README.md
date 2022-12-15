# Database Information
Contains information on the Google BigQuery database used to store passage documents.
Two sources of textual data were used in this analysis: [Google Natural Questions](https://github.com/beir-cellar/beir) and 
a [2018 Wikipedia archive](https://github.com/facebookresearch/DPR).
Raw data files were downloaded from linked sources and manually
uploaded to cloud storage buckets. From there we ingested files into 
appropriate BigQuery databases and used logic in the following files
for curation.

## Description of Contents
- `Duplicate_Passage_Analysis.sql`: Logic for finding and removing duplicate passages
- `Lookup_Tbl_Batch_Construction.sql`: Defines the parallel batching data architecture
- `Missing_Article_Analysis.sql`: Logic for assessing missing articles between corpora
- `Sample_Corpus_Construction.sql`: Defines random sampling logic for the final
retrieval corpus used in the analysis
- `Table_Curation.sql`: Logic for collapsing passages together