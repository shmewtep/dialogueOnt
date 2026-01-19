import uuid
import os
from datasets import load_dataset
import pandas as pd
from rdflib import Graph, Literal, RDF, URIRef, Namespace, BNode, XSD
from rdflib.namespace import XSD

import json

# 1. Define Namespaces based on DIDO.ttl
DIDO = Namespace("http://purl.org/twc/dido#")
SIO = Namespace("http://semanticscience.org/resource/")
PROV = Namespace("http://www.w3.org/ns/prov#")
EX = Namespace("http://example.org/instance/")
TIME = Namespace("http://www.w3.org/2006/time#")


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
    for i in range(len(batch[config["subject_col"]])):
        # Create the primary Dialogue Subject
        dialogue_uri = DIDO[f"dialogue/{batch[config['subject_col']][i]}"]
        graph.add((dialogue_uri, RDF.type, DIDO.Dialogue))
        graph.add((dialogue_uri, RDF.type, SIO.SIO_000006))  # Process

        for m in config["mappings"]:
            val = batch[m["col"]][i]
            if val is None:
                continue

            if m["type"] == "literal":
                graph.add((dialogue_uri, m["pred"], Literal(val)))

            elif m["type"] == "object":
                obj_uri = DIDO[f"{m['obj_prefix']}{val}"]
                # Link Participant to Dialogue as shown in diagram
                graph.add((obj_uri, RDF.type, DIDO.Participant))
                graph.add((obj_uri, m["pred"], dialogue_uri))

            elif m["type"] == "temporal":
                # Create a blank node or URI for the temporal entity
                time_node = BNode()
                graph.add((dialogue_uri, DIDO.hasAttribute, time_node))
                graph.add((time_node, RDF.type, TIME.TemporalDuration))
                graph.add((time_node, m["pred"], Literal(val, datatype=XSD.float)))

    return batch



# 1. Namespace Setup
DIDO = Namespace("http://purl.org/twc/dido#")
SIO = Namespace("http://semanticscience.org/resource/")
TIME = Namespace("http://www.w3.org/2006/time#")
EX = Namespace("http://purl.org/twc/dido/individuals#")


def align_jsonl_to_dido(jsonl_file):
    g = Graph()
    
    # Bind namespaces for cleaner turtle output
    g.bind("dido", DIDO)
    g.bind("sio", SIO)
    g.bind("time", TIME)

    with open(jsonl_file, 'r') as f:
        for line in f:
            data = json.loads(line)
            
            # --- DIDO-core: Dialogue (SIO Process) ---
            # Using meeting_id as the primary identifier for the Dialogue instance
            dialogue_uri = EX[f"dialogue/{data['meeting_id']}"]
            g.add((dialogue_uri, RDF.type, DIDO.Dialogue))
            g.add((dialogue_uri, RDF.type, SIO.SIO_000006)) # sio:process

            # --- DIDO-core: Participant (SIO Agent) ---
            participant_uri = EX[f"participant/{data['speaker_id']}"]
            g.add((participant_uri, RDF.type, DIDO.Participant))
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

# Usage
# graph = align_jsonl_to_dido('ami_data.jsonl')
# print(graph.serialize(format="turtle"))


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


def save_individual_dialogues_as_json(dialogues, output_dir="./dialogues_json"):
    """Save each dialogue DataFrame as an individual JSON file."""
    os.makedirs(output_dir, exist_ok=True)
    
    for dialogue in dialogues:
        output_path = os.path.join(output_dir, f"{dialogue.iloc[0]['meeting_id']}.jsonl")
        print(f"Saving dialogue {dialogue.iloc[0]['meeting_id']} to {output_path}...")
        dialogue.to_json(output_path, orient="records", lines=True)


def download_dataset(num_conversations=5):

    """Download and prepare a small slice of a configured corpus.

    Currently configured to download the AMI Meeting Corpus via
    `datasets.load_dataset` with streaming enabled. The function removes
    unwanted audio columns and returns a list of DataFrames created by
    `get_first_n_dialogues` containing the first `num_conversations` meetings.

    Parameters:
    - num_conversations (int): number of meetings to download and return.

    Returns:
    - List[pandas.DataFrame]: list of dialogues as DataFrames.
    """

    # Define specific settings for each corpus
    corpus_configs = {
        "AMI Meeting Corpus": {
            "path": "edinburghcstr/ami",
            "name": "ihm",          # AMI requires a subset name (e.g., 'ihm' or 'sdm')
            "streaming": True,
            "split": "train"
        },
    }
    
    # Download corpus from HuggingFace
    corpus_name = "AMI Meeting Corpus"
    corpus_config = corpus_configs[corpus_name]

    print(f"Downloading {corpus_name} with settings: {corpus_config}")
    dataset = load_dataset(**corpus_config)
    dataset = dataset.remove_columns(['audio_id', 'audio']) 

    dialogues = get_first_n_dialogues(dataset, num_conversations)

    return dialogues


def align_data_with_dido(num_conversations, g, e, corpus_name, dataset):
    """Download a small slice of a dialogue corpus and align it to DIDO.

    This function loads the DIDO ontology from `../ontology/DIDO.ttl`, then
    downloads a configured corpus (currently the AMI Meeting Corpus via
    `datasets.load_dataset`) and converts a small number of conversations into
    RDF using `data_to_rdf`. The resulting RDF graph is serialized to
    Turtle.

    Parameters:
    - num_conversations (int): approximate number of conversations to ingest
        (passed to `dataset.take(...).map(...)`).

    Notes:
    - The mapping recipes used to convert dataset columns to RDF are defined
        in `df_to_rdf_recipes` inside this function.
    - Output file is currently set to ".ttl" in the implementation; update
        that variable if you want a different path/name.
    """
    
    # Initialize the graph and load your ontology
    ontology_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "ontology"))
    ontology_filepath = os.path.join(ontology_dir, "DIDO.ttl")
    g = Graph()
    try:
        g.parse(ontology_filepath, format="turtle")
    except FileNotFoundError as e:
        print(f"Warning: Could not load DIDO.ttl file. Creating new graph. Error: {e}")

    # Bind prefixes for cleaner output
    g.bind("dido", DIDO)
    g.bind("sio", SIO)
    g.bind("prov", PROV)
    g.bind("ex", EX)
    
    df_to_rdf_recipes = {
        "AMI Meeting Corpus": {
            "subject_prefix": "dialogue/",
            "subject_col": "meeting_id",
            "subject_type": DIDO.Dialogue,
            "mappings": [
                # Map Transcript/Text to Utterance
                {"col": "transcript", "pred": DIDO.hasText, "type": "literal"},
                # Map Speaker to Participant
                {"col": "speaker_id", "pred": DIDO.isParticipantIn, "type": "object", "obj_prefix": "participant/"},
                # Map OWL-Time attributes
                {"col": "begin_time", "pred": TIME.hasBeginning, "type": "temporal"},
                {"col": "end_time", "pred": TIME.hasEnd, "type": "temporal"}
            ]
        }
    }

    print(f"Aligning {num_conversations} conversations...")
    
    # Handle both regular and streaming/iterable datasets. If the dataset
    # supports `take`, use it; otherwise, collect items from the first split
    # using `itertools.islice` and convert them into a batched mapping
    # compatible with `data_to_rdf`.
    from itertools import islice

    print(f"Dataset type: {type(dataset)}")



    # Get an iterable of examples
    examples = None
    if hasattr(dataset, "take"):
        examples = list(dataset.take(num_conversations))
    else:
        # pick first split if dataset is a dict-like container
        if isinstance(dataset, dict):
            first_split = next(iter(dataset.keys()))
            ds0 = dataset[first_split]
        else:
            ds0 = dataset
        examples = list(islice(ds0, num_conversations))

    # Convert list of example dicts into a batch mapping {col: [vals...]}
    def examples_to_batch(exs):
        cols = set()
        for e in exs:
            if isinstance(e, dict):
                cols.update(e.keys())
        batch = {c: [] for c in cols}
        for e in exs:
            for c in cols:
                batch[c].append(e.get(c))
        return batch

    if examples:
        batch = examples_to_batch(examples)
        data_to_rdf(batch, g, df_to_rdf_recipes[corpus_name])

    # 4. Save the aligned data
    output_file = ".ttl"
    g.serialize(destination=output_file, format="turtle")
    print(f"Alignment complete. Results saved to {output_file}")

if __name__ == "__main__":
    #dataset = download_dataset(num_conversations=5)
    #save_individual_dialogues_as_json(dataset)
    g = align_jsonl_to_dido('./dialogues_json/EN2001a.jsonl')
    g.serialize(destination="ami_dialogue.ttl", format="turtle")
    print(f"Alignment complete. Results saved to ami_dialogue.ttl")