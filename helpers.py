import numpy as np
import google.cloud.bigquery as bq
from tqdm import tqdm
import time, os

def connect_bigquery(isColab, project_id):
    '''
    :param isColab: boolean indicating env
    :param project_id: google project id
    :return: bigquery connection object
    '''
    print("building connection to bigquery...")
    if isColab:
        client = bq.Client(project=project_id)
    else:
        if os.name == "nt":
            client = bq.Client.from_service_account_json(
                "C:\\Users\Andrew Amore\Downloads\calcium-vial-368801-989160cca376.json")
        else:
            client = bq.Client.from_service_account_json("/home/nimbus/Downloads/calcium-vial-368801-989160cca376.json")
    return client

def delete_db_records(dataset_name, df, client):
    ''' delete records from target table in bigquery
    :param dataset_name: name of target table
    :param df: contains entries to delete
    :param client: bigquery connection obj
    :return: None
    '''
    print("deleting records from staging db...")
    client.query('''
            DELETE FROM
            `calcium-vial-368801.%s` A
            WHERE doc_id in unnest(%s) and text in unnest(%s)
            ''' % (dataset_name, df.doc_id.tolist(), df.text.tolist()))

## TODO::update queries to use project_id parameter instead of hardcoding
def build_batches(client, dataset_name, batch_size, num_batches):
    ''' randomly sample passages for QG
    :param client: google BigQuery client connection object
    :param dataset_name: database.table_name of target table containing the passage records
    :param batch_size: number of records to query
    :param num_batches: number of batches to run
    :return: list of df batches
    '''
    print("computing row count for random sample...")
    row_count = client.query('''SELECT COUNT(*) as total FROM
        `calcium-vial-368801.%s`''' % (dataset_name)).to_dataframe().total[0]
    print("row count: ", row_count)
    num_samples = batch_size*num_batches
    print("generating random sample...")
    df = client.query('''
        SELECT A.* FROM
        `calcium-vial-368801.%s` A
        WHERE RAND() < %d/%d
        ''' % (dataset_name, num_samples, row_count)).to_dataframe()
    print("number of samples: ", len(df.index))
    time.sleep(0.1)
    df["questions"] = ""
    df_split = np.array_split(df, num_batches)
    return df_split

def process_batches(isColab, project_id, qg, num_questions, target_table, lookup_tbl,
                    num_batches, batch_size, use_qa_evaluator=True, doDelete=True):
    ''' processes a batch of records for question generation and adds them back to database
    :param isColab: boolean denoting execution env
    :param project_id: google project id
    :param qg: question generator model object
    :param num_questions: number of questions to generate for each passage
    :param target_table: name of the target table to append records (database.table_name)
    :param lookup_tbl: name of lookup table to delete records (database.table_name)
    :param num_batches: number of batches to process
    :param batch_size: size of each batch
    :param use_qa_evaluator: boolean denoting whether 2nd stage evaluation should take place
    :param doDelete: boolean denoting whether records should be deleted from the lookup_tbl
    :return: None
    '''
    client = connect_bigquery(isColab, project_id)
    df_split = build_batches(batch_size=batch_size, num_batches=num_batches, dataset_name=lookup_tbl, client=client)
    ## iterate over mini-batches
    cnter = 1
    for df_ in tqdm(df_split, total=len(df_split), desc="Overall Batch Progress", position=0, leave=True):
        ## TODO::adjust this to batch instead of row by row
        for index, row in tqdm(df_.iterrows(), total=len(df_.index), desc=f"Mini-Batch Prog (Batch = {cnter})", leave=False):
            article = df_.at[index, "text"]
            qa_list = qg.generate(isColab, article, num_questions=num_questions, use_evaluator=use_qa_evaluator)
            questions = [q['question'] for q in qa_list]
            questions = ''.join(questions)
            df_.at[index, "questions"] = questions
        df_.to_gbq(target_table, project_id, chunksize=None, if_exists='append')
        if doDelete:
            delete_db_records(lookup_tbl, df_, client)
        cnter += 1