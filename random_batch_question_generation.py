from question_generator.questiongenerator import QuestionGenerator
import torch, helpers

## set configs
project_id = "calcium-vial-368801"
device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')

## instantiate question generator from library
qg = QuestionGenerator()

## fetch batch
num_batches = 10
df_split = helpers.build_batches(batch_size=10, num_batches=2, dataset_name="staging.stg_wikipedia_1_batch_loading", project_id=project_id)

## iterate over mini-batches
for df_ in df_split:
    # generate questions
    for index, row in df_.iterrows():
        print(index)
        article = df_.at[index, "text"]
        qa_list = qg.generate(article, num_questions=15)
        questions = [q['question'].replace('?', ' ') for q in qa_list]
        questions = ''.join(questions)
        df_.at[index, "questions"] = questions

    print("finished mini-batch, saving updates to db and deleting old records...")
    df_.to_gbq('staging.wikipedia_documents_1_qg_15',
               'calcium-vial-368801',
               chunksize=None,
               if_exists='append')
    helpers.delete_db_records("staging.wikipedia_documents_1_qg_15", client)

