from question_generator.questiongenerator import QuestionGenerator
import torch

import helpers

import pandas as p
from google.cloud import bigquery
import numpy as np

## set configs
import google.cloud.bigquery as bq
client = bq.Client.from_service_account_json("/home/nimbus/Downloads/calcium-vial-368801-989160cca376.json")
project_id = 'calcium-vial-368801'
device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')


## query data only focused on NQ-train
def fetch_data_fromBQ(lower_bnd, upper_bnd):
  df = client.query(''' select distinct A.query_id, doc_text, title, doc_id from (
  SELECT array_to_string(corpus_id_array, ',') as answer_doc_block_id, query_id, A.doc_text, A.title, A.doc_id
  FROM `prod_datasets.nq_train_documents_3` A inner join ( select * from `beir_nq_train.train_answer_lookup`
      where cast(substr(query_id, 6) as INT64) >= %d AND cast(substr(query_id, 6) as INT64) < %d ) B ON B.corpus_id in UNNEST(corpus_id_array)
    ) A inner join `beir_nq_train.train_query_lookup` B on A.query_id = B.query_id''' % (lower_bnd, upper_bnd)).to_dataframe()
  return df

qg = QuestionGenerator()

## batch process
# batches = [[100, 110], [110, 150]]
# batches = [[150, 300], [300, 500], [500, 1000], [1000, 2000], [2000, 3000], [3000,4000], [4000,5000]]
batches = [[3000,4000], [4000,5000]]
for i in batches:
  print("batch: ", i)
  df = fetch_data_fromBQ(i[0], i[1])
  df["questions"] = ""
  for index, row in df.iterrows():
    print(index)
    article = df.at[index, "doc_text"]
    qa_list = qg.generate(article, num_questions=50)
    questions = [q['question'].replace('?', ' ') for q in qa_list]
    questions = ''.join(questions)
    df.at[index, "questions"] = questions
  ## save to database
  df.to_gbq('staging.nq_train_documents_3_qg_50',
                 'calcium-vial-368801',
                 chunksize=None,
                 if_exists='append')