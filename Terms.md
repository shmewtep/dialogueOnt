## Terminology

#### Ontology prefixes

| Namespace | Ontology | IRI |
|:----------|:---------|:-------| 
| sio   | Semanticscience Ontology  | http://semanticscience.org/ontology/sio.owl   |
| time  | Time Ontology in OWL      | https://www.w3.org/TR/owl-time/               |

#### Term list

| Label | IRI / Identifier | Definition | Source | Definition Source | Parent | Equivalent To | Version / Status |
|:---|:---|:---|:---|:---|:---|:---|:---|
| **dialogue** | `:Dialogue` | an interactive communication between two or more entities |  | https://www.oed.com/dictionary/dialogue_n, | [SIO (Process)](http://semanticscience.org/resource/SIO_000006) |  |  |
| **utterance** | `:Utterance` | a single unit of speech in spoken language that serves some pragmatic function | | SIO Core | [SIO (Process)](http://semanticscience.org/resource/SIO_000006) |  |  |
| **interlocutor** | `:Interlocutor` | The role played by an agent when producing an utterance. |  |  | [SIO (Role)](http://semanticscience.org/resource/SIO_000016) | dialogue participant |  |



---

### Column Legend
* **Label**: Human-readable name (`rdfs:label`).
* **IRI**: Unique identifier (Reused full IRI or local prefix).
* **Definition**: Formal natural language description (`skos:definition`).
* **Source / Derivation**: The linguistic theory or existing ontology the term is mapped from.
* **Author/Contributor**: Entity responsible for the term in this specific implementation.
* **Version / Status**: Development state (e.g., Draft, Verified, Deprecated).
