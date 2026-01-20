# Variables
ROBOT := robot
SIO_URL := http://semanticscience.org/ontology/sio.owl
PREFIX := http://semanticscience.org/resource/
MODULE_DIR := modules
SEED_TERMS := SIO_000016 # The ID for 'role'

# Create the modules directory if it doesn't exist
# $(MODULE_DIR):
# 	mkdir -p $(MODULE_DIR)

# The task to generate the SIO module
$(MODULE_DIR)/sio_module.ttl: | $(MODULE_DIR)
	$(ROBOT) extract --input-iri $(SIO_URL) \
		--method BOT \
		--term-file terms_sio.txt \
		--output $@

# Shortcut command
modules: $(MODULE_DIR)/sio_module.ttl

.PHONY: modules