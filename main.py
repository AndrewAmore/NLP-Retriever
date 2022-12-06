import argparse
from question_generator.questiongenerator import QuestionGenerator
import helpers

## config options
project_id = 'calcium-vial-368801'
qg = QuestionGenerator()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Process Data", usage='%(prog)s [options]')
    parser.add_argument('--record-obj', help="passage type for data processing", required=True, choices=['DPR', 'NQ'])
    args = parser.parse_args()
    if args.record_obj == "DPR":
        ## proces some DPR data
        print("processing DPR data")
        helpers.process_batches(isColab=False, project_id=project_id, qg=qg, num_questions=15,
                        target_table="staging.wikipedia_documents_1_qg_15",
                        lookup_tbl="staging.stg_wikipedia_1_batch_loading",
                        num_batches=3, batch_size=5)
    elif args.record_obj == "NQ":
        # process NQ
        print("processing NQ data")
        helpers.process_batches(isColab=False, project_id=project_id, qg=qg, num_questions=50,
                        target_table="staging.wikipedia_documents_1_qg_15",
                        lookup_tbl="staging.stg_wikipedia_1_batch_loading",
                        num_batches=5, batch_size=25)


