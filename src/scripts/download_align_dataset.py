import pandas as pd
import csv
import json
import regex
import uuid

from rdflib import Graph, Literal, RDF, URIRef, Namespace, BNode, XSD
from rdflib.namespace import RDFS


# 1. Define Namespaces based on DIDO.ttl
DIDO = Namespace("http://purl.org/twc/dido#")
SIO = Namespace("http://semanticscience.org/resource/")
PROV = Namespace("http://www.w3.org/ns/prov#")
TIME = Namespace("http://www.w3.org/2006/time#")
EX = Namespace("http://purl.org/twc/dido/individuals#")


def data_to_rdf(batch, graph, config):
    """Convert a batched dataset slice into RDF triples and add them to `graph`.

    The function expects `batch` in the format produced by the HuggingFace
    `datasets` library when using `batched=True` (i.e. a mapping from column
    names to lists). For each row it will create a Dialogue subject URI and
    apply mappings described in `config`.

    Parameters:
    - batch (Mapping[str, list]): Batched dataset with columns referenced by
        `config["subject_col"]` and `config["mappings"]` entries.
    - graph (rdflib.Graph): RDF graph to which triples will be added.
    - config (dict): Recipe describing how to map columns to RDF. Expected
        keys include:
            * "subject_col": column name for the dialogue identifier.
            * "mappings": list of mapping dicts with keys:
                    - "col": source column name
                    - "pred": rdflib predicate to use
                    - "type": one of "literal", "object", or "temporal"
                    - optionally "obj_prefix" for object mappings

    Returns:
    - batch: the same input `batch` object (keeps compatibility with
        `datasets.Dataset.map` usage where a mapped function returns the batch).
    """
#    for i in range(len(batch[config["subject_col"]])):
#        # Create the primary Dialogue Subject
#        dialogue_uri = DIDO[f"dialogue/{batch[config['subject_col']][i]}"]
#        graph.add((dialogue_uri, RDF.type, DIDO.Dialogue))
#        graph.add((dialogue_uri, RDF.type, SIO.SIO_000006))  # Process
#
#        for m in config["mappings"]:
#            val = batch[m["col"]][i]
#            if val is None:
#                continue
#
#            if m["type"] == "literal":
#                graph.add((dialogue_uri, m["pred"], Literal(val)))
#
#            elif m["type"] == "object":
#                obj_uri = DIDO[f"{m['obj_prefix']}{val}"]
#                # Link Participant to Dialogue as shown in diagram
#                graph.add((obj_uri, RDF.type, DIDO.Interlocutor))
#                graph.add((obj_uri, m["pred"], dialogue_uri))
#
#            elif m["type"] == "temporal":
#                # Create a blank node or URI for the temporal entity
#                time_node = BNode()
#                graph.add((dialogue_uri, DIDO.hasAttribute, time_node))
#                graph.add((time_node, RDF.type, TIME.TemporalDuration))
#                graph.add((time_node, m["pred"], Literal(val, datatype=XSD.float)))
#
#    return batch
#
    return None

def align_ami_jsonl_to_dido(jsonl_file):
    '''
    Transform data points from AMI JSONL format to RDF format, aligned with DIDO.
    '''
    g = Graph()
    
    # Bind namespaces
    g.bind("dido", DIDO)
    g.bind("sio", SIO)
    g.bind("time", TIME)

    with open(jsonl_file, 'r') as f:
        for line in f:
            data = json.loads(line)
            
            # --- Dialogue Structure ---
            # Using meeting_id as the primary identifier for the Dialogue instance
            dialogue_uri = EX[f"dialogue/{data['meeting_id']}"]
            g.add((dialogue_uri, RDF.type, DIDO.Dialogue))
            g.add((dialogue_uri, RDF.type, SIO.SIO_000006)) # sio:process

            # --- DIDO-core: Participant (SIO Agent) ---
            participant_uri = EX[f"participant/{data['speaker_id']}"]
            g.add((participant_uri, RDF.type, DIDO. Participant))
            g.add((participant_uri, RDF.type, SIO.SIO_000397)) # sio:agent
            g.add((participant_uri, DIDO.isParticipantIn, dialogue_uri))

            # --- DIDO-data: Utterance & Transcript (SIO Entity) ---
            # Each row is an Utterance within the Dialogue
            utterance_id = f"{data['meeting_id']}_{data['begin_time']}"
            utterance_uri = EX[f"utterance/{utterance_id}"]
            g.add((utterance_uri, RDF.type, DIDO.Utterance))
            g.add((utterance_uri, DIDO.hasText, Literal(data['text'], datatype=XSD.string)))
            
            # Linking Utterance to the Dialogue
            g.add((utterance_uri, SIO.SIO_000068, dialogue_uri)) # sio:is-part-of
            
            # --- Temporal duration (OWL-Time) ---
            temp_node = BNode()
            g.add((utterance_uri, DIDO.hasAttribute, temp_node))
            g.add((temp_node, RDF.type, TIME.TemporalDuration))
            g.add((temp_node, TIME.hasBeginning, Literal(data['begin_time'], datatype=XSD.float)))
            g.add((temp_node, TIME.hasEnd, Literal(data['end_time'], datatype=XSD.float)))

    return g


def align_daicwoz_csv_to_dido(csv_filename):
    '''
    Transform data points from DAIC-WOZ CSV format to RDF format, aligned with DIDO.
    '''
    g = Graph()
    
    # Bind namespaces
    g.bind("dido", DIDO)
    g.bind("sio", SIO)
    g.bind("time", TIME)

    dialogue_num_pattern = regex.compile(r"^(\d{3})_TRANSCRIPT\.csv$")
    dialogue_num = dialogue_num_pattern.match(csv_filename)

    reader = csv.DictReader(csv_filename)

    dialogue_uri = EX[f"dialogue/{dialogue_num}"]
    g.add((dialogue_uri, RDF.type, DIDO.Dialogue))

    # Transcript/Dataset metadata
    transcript_uri = EX[f"dialogueTranscript/{dialogue_num}"]
    g.add((transcript_uri, RDF.type, DIDO.DialogueTranscript))
    g.add((transcript_uri, SIO.SIO_000332, dialogue_uri)) # sio:is about

    for utterance_num, utterance in enumerate(reader):
        # --- Dialogue Structure ---
        # Using meeting_id as the primary identifier for the Dialogue instance
        utterance_id = f"{utterance['meeting_id']}_{utterance_num}"
        utterance_uri = EX[f"utterances/{utterance_id}"]
        g.add((utterance_uri, RDF.type, DIDO.Utterance))

        # Linking Utterance to the Dialogue
        g.add((utterance_uri, SIO.SIO_000068, dialogue_uri)) # sio:is part of

        # --- DIDO-core: Participant (SIO Agent) ---
        participant_uri = ''
        if utterance['speaker'] == 'Ellie':
            participant_uri = EX[f"interlocutors/{'ellie'}"]
        else:
            participant_uri = EX[f"interlocutors/{dialogue_num}"]

        g.add((participant_uri, RDF.type, DIDO.Interlocutor))
        g.add((participant_uri, SIO.SIO_000062, dialogue_uri)) # sio:is participant in

        # --- DIDO-data: Transcript (SIO Entity) ---
        utterance_text_uri = EX[f"utteranceTexts/{utterance_id}"]
        g.add((utterance_text_uri, RDF.type, DIDO.UtteranceText))
        g.add((utterance_uri, SIO.SIO_000232, utterance_text_uri))  # sio:has output
        g.add((utterance_text_uri, RDFS.label, Literal(utterance['value'], datatype=XSD.string))) # sio:
        
        # --- Temporal duration (OWL-Time) ---
        temp_node = BNode()
        g.add((utterance_uri, DIDO.hasAttribute, temp_node))
        g.add((temp_node, RDF.type, TIME.TemporalDuration))
        g.add((temp_node, TIME.hasBeginning, Literal(utterance['begin_time'], datatype=XSD.float)))
        g.add((temp_node, TIME.hasEnd, Literal(utterance['end_time'], datatype=XSD.float)))

    return g


def get_first_n_dialogues(dataset, n):
    """Collect the first `n` distinct dialogues (meetings) from `dataset`.

    Iterates over `dataset` (an iterable of example dicts or a streaming
    dataset) and groups examples by the `meeting_id` field. Stops once `n`
    distinct meeting IDs have been collected. Returns a list of
    `pandas.DataFrame` objects, one per collected meeting, preserving the
    encounter order.

    Parameters:
    - dataset: iterable of mapping-like examples (each must contain
        a `meeting_id` key).
    - n (int): number of distinct meetings to collect.

    Returns:
    - List[pandas.DataFrame]: list of DataFrames, one per meeting collected.
    """
    meeting_groups = {}
    meeting_order = []

    for item in dataset:
        print(item)
        meeting_id = item['meeting_id']
        
        # Check if we've encountered this meeting ID before
        if meeting_id not in meeting_groups:
            # If we already have n meetings, stop collecting new ones
            if len(meeting_order) >= n:
                # If data is contiguous, we can 'break' here to save time.
                break
            
            meeting_order.append(meeting_id)
            meeting_groups[meeting_id] = []
        
        # Append the instance data (converted to a dictionary) to the correct group
        # Using vars(item) assumes the instance attributes match your desired columns
        meeting_groups[meeting_id].append(item)

    # Convert each collected list of dictionaries into a DataFrame
    return [pd.DataFrame(meeting_groups[m_id]) for m_id in meeting_order]



if __name__ == "__main__":
    pass