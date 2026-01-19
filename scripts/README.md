# download_align_meddialog.py

Quick notes:

- Install dependencies:

```bash
pip install -r requirements.txt
```

- Example run (download a sample of 50 utterances):

```bash
python scripts/download_align_meddialog.py --url https://example.org/meddialog-sample.jsonl --sample 50 --out aligned_meddialog.ttl
```

The script writes an RDF Turtle file aligned with the `http://purl.org/twc/dido#` ontology.
