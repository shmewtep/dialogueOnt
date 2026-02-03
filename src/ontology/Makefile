# --------------------------------
# Makefile for dido
# --------------------------------

# --------------------------------
# Constants
# --------------------------------

URI := 					https://purl.org/dido
ONT := 					dido
EDIT_FORMAT := 			ttl
SRC := 					$(ONT)-edit.$(EDIT_FORMAT)
ROBOT := 				robot

SCRIPTSDIR := 			../scripts
SPARQLDIR :=			../sparql
TEMPDIR := 				../temp

TODAY ?= 				$(shell date +%Y-%m-%d)

RELEASE_ARTEFACTS = 	$(ONT) $(ONT)-base $(ONT)-full

SIO_URL := 				http://semanticscience.org/ontology/sio.owl
PREFIX := 				http://semanticscience.org/resource/
MODULE_DIR := 			modules
SEED_TERMS := 			SIO_000016 # The ID for 'role'


# --------------------------------
# Top-level targets
# --------------------------------

.PHONY: .FORCE


# ---------------------------------
# Import targets
# ---------------------------------

IMPORTS = sio
IMPORT_ROOTS = $(patsubst %, imports/%_import, $(IMPORTS))
IMPORT_OWL_FILES = $(foreach n,$(IMPORT_ROOTS), $(n).owl)
IMPORT_FILES = $(IMPORT_OWL_FILES)


# The task to generate the SIO module
$(MODULE_DIR)/sio_module.ttl: | $(MODULE_DIR)
	$(ROBOT) extract --input-iri $(SIO_URL) \
		--method BOT \
		--term-file terms_sio.txt \
		--output $@

# Shortcut command
modules: $(MODULE_DIR)/sio_module.ttl

.PHONY: modules