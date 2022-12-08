import argparse, sys
from question_generator.questiongenerator import QuestionGenerator
import helpers

## config options
project_id = 'calcium-vial-368801'
qg = QuestionGenerator()
isColab = 'google.colab' in sys.modules

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Process Data", usage='%(prog)s [options]')
    parser.add_argument('--record-obj', help="passage type for data processing", required=True, choices=['DPR', 'NQ', 'testing'])
    args = parser.parse_args()
    if args.record_obj == "DPR":
        ## proces some DPR data
        print("processing DPR data")
        helpers.process_batches(isColab=isColab, project_id=project_id, qg=qg, num_questions=15,
                        target_table="staging.wikipedia_documents_1_qg_15",
                        lookup_tbl="staging.stg_wikipedia_1_batch_loading",
                        num_batches=100, batch_size=100, use_qa_evaluator=True)
    elif args.record_obj == "NQ":
        # process NQ
        print("processing NQ data")
        helpers.process_batches(isColab=isColab, project_id=project_id, qg=qg, num_questions=25,
                        target_table="staging.nq_train_documents_3_qg_50",
                        lookup_tbl="staging.stg_nq_train_3_batch_loading",
                        num_batches=20, batch_size=250, use_qa_evaluator=True)
    elif args.record_obj == "testing":
        # test methods
        print("testing initiated")
        helpers.process_batches(isColab=isColab, project_id=project_id, qg=qg, num_questions=25,
                        target_table="testing.nq_train_documents_3_qg_25_beam",
                        lookup_tbl="testing.stg_nq_train_3_batch_loading",
                        num_batches=1, batch_size=5,
                        use_qa_evaluator=True, doDelete=False)



