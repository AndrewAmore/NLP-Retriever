import en_core_web_sm
import json
import numpy as np
import random
import re
import torch
from transformers import AutoTokenizer, AutoModelForSeq2SeqLM, AutoModelForSequenceClassification
from typing import Any, List, Mapping, Tuple
import question_generator.sentence_splitter as sentence_splitter


class QuestionGenerator:
    """A transformer-based NLP system for generating reading comprehension-style questions from
    texts. It can generate full sentence questions, multiple choice questions, or a mix of the
    two styles.
    To filter out low quality questions, questions are assigned a score and ranked once they have
    been generated. Only the top k questions will be returned. This behaviour can be turned off
    by setting use_evaluator=False.
    """

    def __init__(self) -> None:
        QG_PRETRAINED = "iarfmoose/t5-base-question-generator"
        self.ANSWER_TOKEN = "<answer>"
        self.CONTEXT_TOKEN = "<context>"
        self.SEQ_LENGTH = 512
        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

        ## fast vs. slow won't make a difference if doing one passage at a time
            ## won't work with default protobuf package (pip install protobuf==3.20.*) fixes issues
        self.qg_tokenizer = AutoTokenizer.from_pretrained(QG_PRETRAINED, use_fast=True)
        self.qg_model = AutoModelForSeq2SeqLM.from_pretrained(QG_PRETRAINED)
        self.qg_model.to(self.device)
        self.qg_model.eval()
        self.qa_evaluator = QAEvaluator()

    ## TODO::have to change this to be batched
    ### right now all possible questions are generated...can alter the GeneratorConfig to output a smaller set to improve runtime
    def generate(self, isColab: bool,  article: str, use_evaluator: bool = True, num_questions: bool = None, answer_style: str = "all") -> List:
        """Takes an article and generates a set of question and answer pairs. If use_evaluator
        is True then QA pairs will be ranked and filtered based on their quality. answer_style
        should select from ["all", "sentences", "multiple_choice"].
        """
        # print("Generating questions...\n")
        ## can't completely batch across inputs (not enough memory)
        qg_inputs, qg_answers = self.generate_qg_inputs(article, answer_style)
        ## new with batching
        generated_questions = self.generate_questions_from_inputs_BATCH(qg_inputs, isColab)
        # generated_questions = self.generate_questions_from_inputs(qg_inputs)
        message = "{} questions doesn't match {} answers".format(len(generated_questions), len(qg_answers))
        assert len(generated_questions) == len(qg_answers), message
        ## the evaluator...it's what shrinks the question count
        if use_evaluator:
            # print("Evaluating QA pairs...\n")
            encoded_qa_pairs = self.qa_evaluator.encode_qa_pairs(generated_questions, qg_answers)
            # encoded_qa_pairs = self.qa_evaluator.encode_qa_pairs_BATCH(generated_questions, qg_answers)
            scores = self.qa_evaluator.get_scores(encoded_qa_pairs)
            if num_questions:
                qa_list = self._get_ranked_qa_pairs(generated_questions, qg_answers, scores, num_questions)
            else:
                qa_list = self._get_ranked_qa_pairs(generated_questions, qg_answers, scores)
        else:
            # print("Skipping evaluation step.\n")
            qa_list = self._get_all_qa_pairs(generated_questions, qg_answers)
        return qa_list

    ## TODO::will need to be batched (first task)
    def generate_qg_inputs(self, text: str, answer_style: str) -> Tuple[List[str], List[str]]:
        """Given a text, returns a list of model inputs and a list of corresponding answers.
        Model inputs take the form "answer_token <answer text> context_token <context text>" where
        the answer is a string extracted from the text, and the context is the wider text surrounding
        the context.
        """
        VALID_ANSWER_STYLES = ["all", "sentences", "multiple_choice"]
        if answer_style not in VALID_ANSWER_STYLES:
            raise ValueError("Invalid answer style {}. Please choose from {}".format(answer_style, VALID_ANSWER_STYLES))
        inputs = []
        answers = []
        if answer_style == "sentences" or answer_style == "all":
            segments = self._split_into_segments(text)
            for segment in segments:
                sentences = self._split_text(segment)
                prepped_inputs, prepped_answers = self._prepare_qg_inputs(sentences, segment)
                inputs.extend(prepped_inputs)
                answers.extend(prepped_answers)
        if answer_style == "multiple_choice" or answer_style == "all":
            sentences = self._split_text(text)
            prepped_inputs, prepped_answers = self._prepare_qg_inputs_MC(sentences)
            inputs.extend(prepped_inputs)
            answers.extend(prepped_answers)
        return inputs, answers

    def generate_questions_from_inputs(self, qg_inputs: List) -> List[str]:
        """Given a list of concatenated answers and contexts generates a list of questions with the form:
        "answer_token <answer text> context_token <context text>".
        """
        generated_questions = []
        for qg_input in qg_inputs:
            question = self._generate_question(qg_input)
            generated_questions.append(question)
        return generated_questions

################## NEW FUNCTION CODE ##################
    def chunks(self, lst, n):
        """Yield successive n-sized chunks from lst."""
        for i in range(0, len(lst), n):
            yield lst[i:i + n]

    def generate_questions_from_inputs_BATCH(self, qg_inputs: List, isColab: bool) -> List[str]:
        """Given a list of concatenated answers and contexts generates a list of questions with the form:
        "answer_token <answer text> context_token <context text>".
        """
        ## this fills up GPU memory locally with just 8 chunks...set based on env as Colab has 12 GB
        ## note this will become less of a factor if less passages are concatenated together
        obs_per_chunk = 7
        if isColab:
            obs_per_chunk = 39
        generated_questions = []
        ## split the qg_inputs into mini-batches
        chunked = list(self.chunks(qg_inputs, obs_per_chunk))
        for chunk in chunked:
            questions = self._generate_question_BATCH(chunk)
            generated_questions.extend(q for q in questions)
        return generated_questions

    @torch.no_grad()
    def _generate_question_BATCH(self, qg_input: list) -> str:
        """Takes qg_input which is the concatenated answer and context, and uses it to generate
        a question sentence. The generated question is decoded and then returned.
        """
        encoded_input = self._encode_qg_input_BATCH(qg_input)
        ## adjust this to output a fixed set
        ### there are a lot of options to set here (https://huggingface.co/docs/transformers/main_classes/text_generation)
        #### adjusting this will be key
        output = self.qg_model.generate(input_ids=encoded_input["input_ids"], max_length=20, num_beams=4, temperature=2)
        questions = self.qg_tokenizer.batch_decode(output, skip_special_tokens=True)
        return questions

    def _encode_qg_input_BATCH(self, qg_input: list) -> torch.tensor:
        """Tokenizes a string and returns a tensor of input ids corresponding to indices of tokens in the vocab.
        """
        return self.qg_tokenizer(qg_input, padding='max_length', max_length=self.SEQ_LENGTH, truncation=True,
            return_tensors="pt").to(self.device)

################## END NEW FUNCTIONS ##################

    def _split_text(self, text: str) -> List[str]:
        """Splits the text into sentences, and attempts to split or truncate long sentences."""
        MAX_SENTENCE_LEN = 128
        ## add additional check to verify abbreviations...sentence can't end with a single letter (i.e. 'John S.')
        ## also add some additional parsing to remover citations that are in the form [digit]
        # sentences = re.findall(".*?[.!\?]", text)
        sentences = sentence_splitter.split_into_sentences(text)
        cut_sentences = []
        for sentence in sentences:
            if len(sentence) > MAX_SENTENCE_LEN:
                cut_sentences.extend(re.split("[,;:)]", sentence))
        # remove useless post-quote sentence fragments
        cut_sentences = [s for s in sentences if len(s.split(" ")) > 5]
        sentences = sentences + cut_sentences
        return list(set([s.strip(" ") for s in sentences]))

    def _split_into_segments(self, text: str) -> List[str]:
        """Splits a long text into segments short enough to be input into the transformer network.
        Segments are used as context for question generation.
        """
        MAX_TOKENS = 490
        paragraphs = text.split("\n")
        tokenized_paragraphs = [
            self.qg_tokenizer(p)["input_ids"] for p in paragraphs if len(p) > 0
        ]
        segments = []
        while len(tokenized_paragraphs) > 0:
            segment = []
            while len(segment) < MAX_TOKENS and len(tokenized_paragraphs) > 0:
                paragraph = tokenized_paragraphs.pop(0)
                segment.extend(paragraph)
            segments.append(segment)
        return [self.qg_tokenizer.decode(s, skip_special_tokens=True) for s in segments]

    def _prepare_qg_inputs(self, sentences: List[str], text: str) -> Tuple[List[str], List[str]]:
        """Uses sentences as answers and the text as context. Returns a tuple of (model inputs, answers).
        Model inputs are "answer_token <answer text> context_token <context text>" 
        """
        inputs = []
        answers = []
        for sentence in sentences:
            qg_input = f"{self.ANSWER_TOKEN} {sentence} {self.CONTEXT_TOKEN} {text}"
            inputs.append(qg_input)
            answers.append(sentence)
        return inputs, answers

    def _prepare_qg_inputs_MC(self, sentences: List[str]) -> Tuple[List[str], List[str]]:
        """Performs NER on the text, and uses extracted entities are candidate answers for multiple-choice
        questions. Sentences are used as context, and entities as answers. Returns a tuple of (model inputs, answers). 
        Model inputs are "answer_token <answer text> context_token <context text>"
        """
        spacy_nlp = en_core_web_sm.load()
        docs = list(spacy_nlp.pipe(sentences, disable=["parser"]))
        inputs_from_text = []
        answers_from_text = []
        for doc, sentence in zip(docs, sentences):
            entities = doc.ents
            if entities:
                for entity in entities:
                    qg_input = f"{self.ANSWER_TOKEN} {entity} {self.CONTEXT_TOKEN} {sentence}"
                    answers = self._get_MC_answers(entity, docs)
                    inputs_from_text.append(qg_input)
                    answers_from_text.append(answers)
        return inputs_from_text, answers_from_text

    def _get_MC_answers(self, correct_answer: Any, docs: Any) -> List[Mapping[str, Any]]:
        """Finds a set of alternative answers for a multiple-choice question. Will attempt to find
        alternatives of the same entity type as correct_answer if possible.
        """
        entities = []
        for doc in docs:
            entities.extend([{"text": e.text, "label_": e.label_}
                            for e in doc.ents])
        # remove duplicate elements
        entities_json = [json.dumps(kv) for kv in entities]
        pool = set(entities_json)
        num_choices = (min(4, len(pool)) - 1)  # -1 because we already have the correct answer

        # add the correct answer
        final_choices = []
        correct_label = correct_answer.label_
        final_choices.append({"answer": correct_answer.text, "correct": True})
        pool.remove(
            json.dumps({"text": correct_answer.text,
                       "label_": correct_answer.label_})
        )
        # find answers with the same NER label
        matches = [e for e in pool if correct_label in e]
        # if we don't have enough then add some other random answers
        if len(matches) < num_choices:
            choices = matches
            pool = pool.difference(set(choices))
            choices.extend(random.sample(pool, num_choices - len(choices)))
        else:
            choices = random.sample(matches, num_choices)
        choices = [json.loads(s) for s in choices]
        for choice in choices:
            final_choices.append({"answer": choice["text"], "correct": False})
        random.shuffle(final_choices)
        return final_choices

    @torch.no_grad()
    def _generate_question(self, qg_input: str) -> str:
        """Takes qg_input which is the concatenated answer and context, and uses it to generate
        a question sentence. The generated question is decoded and then returned.
        """
        encoded_input = self._encode_qg_input(qg_input)
        # output_greedy = self.qg_model.generate(input_ids=encoded_input["input_ids"], max_length=20)
        ## changed from greedy to beam and added Temperature to avoid repetive question outputs
        output = self.qg_model.generate(input_ids=encoded_input["input_ids"], max_length=20, num_beams=4, temperature=2)
        question = self.qg_tokenizer.decode(output[0], skip_special_tokens=True)
        return question

    def _encode_qg_input(self, qg_input: str) -> torch.tensor:
        """Tokenizes a string and returns a tensor of input ids corresponding to indices of tokens in the vocab.
        """
        return self.qg_tokenizer(
            qg_input,
            padding='max_length',
            max_length=self.SEQ_LENGTH,
            truncation=True,
            return_tensors="pt"
        ).to(self.device)

    def _get_ranked_qa_pairs(self, generated_questions: List[str],
                             qg_answers: List[str], scores, num_questions: int = 10) -> List[Mapping[str, str]]:
        """Ranks generated questions according to scores, and returns the top num_questions examples.
        """
        if num_questions > len(scores):
            num_questions = len(scores)
            # print((f"\nWas only able to generate {num_questions} questions.", "For more questions, please input a longer text."))
        qa_list = []
        for i in range(num_questions):
            index = scores[i]
            qa = {
                "question": generated_questions[index].split("?")[0] + " [SEP] ",
                "answer": qg_answers[index]
            }
            qa_list.append(qa)
        return qa_list

    def _get_all_qa_pairs(self, generated_questions: List[str], qg_answers: List[str]):
        """Formats question and answer pairs without ranking or filtering."""
        qa_list = []
        for question, answer in zip(generated_questions, qg_answers):
            qa = {
                "question": question.split("?")[0] + " [SEP] ",
                "answer": answer
            }
            qa_list.append(qa)
        return qa_list


## TODO::there are batching opportunities here
class QAEvaluator:
    """Wrapper for a transformer model which evaluates the quality of question-answer pairs.
    Given a QA pair, the model will generate a score. Scores can be used to rank and filter
    QA pairs.
    """

    def __init__(self) -> None:
        QAE_PRETRAINED = "iarfmoose/bert-base-cased-qa-evaluator"
        self.SEQ_LENGTH = 512
        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        self.qae_tokenizer = AutoTokenizer.from_pretrained(QAE_PRETRAINED)
        self.qae_model = AutoModelForSequenceClassification.from_pretrained(QAE_PRETRAINED)
        self.qae_model.to(self.device)
        self.qae_model.eval()

    ## here is a batch benefit...may have to balance GPU memory between this model and the QG
    def encode_qa_pairs(self, questions: List[str], answers: List[str]) -> List[torch.tensor]:
        """Takes a list of questions and a list of answers and encodes them as a list of tensors."""
        encoded_pairs = []
        for question, answer in zip(questions, answers):
            encoded_qa = self._encode_qa(question, answer)
            encoded_pairs.append(encoded_qa.to(self.device))
        return encoded_pairs

    ########## NEW FUNCTIONS ##########
    def encode_qa_pairs_BATCH(self, questions: List[str], answers: List[str]) -> List[torch.tensor]:
        """Takes a list of questions and a list of answers and encodes them as a list of tensors."""
        encoded_pairs = []
        ## need to concatnate together better here...see below item
        encoded_qa = self._encode_qa_BATCH(questions, answers)
        encoded_pairs.append(encoded_qa.to(self.device))
        return encoded_pairs

    def _encode_qa_BATCH(self, question: List[str], answer: List[str]) -> torch.tensor:
        """Concatenates a question and answer, and then tokenizes them. Returns a tensor of
        input ids corresponding to indices in the vocab.
        """
        ## this evaluates multiple choice answers
        ## TODO::figure out a way to do this faster
        if type(answer) is list:
            for a in answer:
                if a["correct"]:
                    correct_answer = a["answer"]
        else:
            correct_answer = answer
        return self.qae_tokenizer(
            text=question, text_pair=answer,
            padding="max_length", max_length=self.SEQ_LENGTH,
            truncation=True, return_tensors="pt",
        )
    ########## END NEW FUNCTIONS ##########

    def get_scores(self, encoded_qa_pairs: List[torch.tensor]) -> List[float]:
        """Generates scores for a list of encoded QA pairs."""
        scores = {}
        for i in range(len(encoded_qa_pairs)):
            scores[i] = self._evaluate_qa(encoded_qa_pairs[i])
        return [
            k for k, v in sorted(scores.items(), key=lambda item: item[1], reverse=True)
        ]

    def _encode_qa(self, question: str, answer: str) -> torch.tensor:
        """Concatenates a question and answer, and then tokenizes them. Returns a tensor of 
        input ids corresponding to indices in the vocab.
        """
        if type(answer) is list:
            for a in answer:
                if a["correct"]:
                    correct_answer = a["answer"]
        else:
            correct_answer = answer
        return self.qae_tokenizer(
            text=question,
            text_pair=correct_answer,
            padding="max_length",
            max_length=self.SEQ_LENGTH,
            truncation=True,
            return_tensors="pt",
        )

    @torch.no_grad()
    def _evaluate_qa(self, encoded_qa_pair: torch.tensor) -> float:
        """Takes an encoded QA pair and returns a score."""
        output = self.qae_model(**encoded_qa_pair)
        return output[0][0][1]
