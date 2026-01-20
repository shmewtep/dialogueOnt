## Terminology

#### Ontology prefixes

| Namespace | Ontology | IRI |
|:----------|:---------|:-------| 
| sio   | Semanticscience Ontology  | http://semanticscience.org/ontology/sio.owl   |
| time  | Time Ontology in OWL      | https://www.w3.org/TR/owl-time/               |

#### Term list

| Label | IRI | Definition | Source | Definition Source | Parent | Module / Layer | Version / Status |
|:---|:---|:---|:---|:---|:---|:---|:---|
| **dialogue** | `:Dialogue` | an interactive communication between two or more entities |  | https://www.oed.com/dictionary/dialogue_n, | [SIO (Process)](http://semanticscience.org/resource/SIO_000006) | Process | Draft |
| **utterance** | `:Utterance` | a single unit of speech in spoken language that serves some pragmatic function | | | [SIO (Process)](http://semanticscience.org/resource/SIO_000006) | Process | Draft |
| **interlocutor** | `:Interlocutor` | One who takes part in a dialogue or conversation |  |  | [SIO (Role)](http://semanticscience.org/resource/SIO_000016) | Agent | Draft |
| **utterance text** | `:UtteranceText` |  |  |  | [SIO (Information Content Entity)](http://semanticscience.org/resource/SIO_000015) | Data | Draft |
| **dialogue transcript** | `:DialogueTranscript` | A digital or physical information content entity that provides a comprehensive, structured textual record of a dialogue process |  |  | [SIO (Information Content Entity)](http://semanticscience.org/resource/SIO_000015) | Data | Draft |



---

### Column Legend
* **Label**: Human-readable name (`rdfs:label`).
* **IRI**: Unique identifier (Reused full IRI or local prefix).
* **Definition**: Formal natural language description (`skos:definition`).
* **Source**: The existing ontology or knowledge source the term is mapped from.
* **Module / Layer**: If the term is part of a distinct layer of DIDO, or corresponds with an existing ontology dedsign pattern
* **Version / Status**: Development state (e.g., Draft, Verified, Deprecated).
