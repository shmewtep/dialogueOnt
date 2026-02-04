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
TEMPDIR := 				tmp
RELEASEDIR := 			../..
MODULE_DIR := 			modules

REPORT_FAIL_ON =            None
REPORT_LABEL =              -l true
REPORT_PROFILE_OPTS =  

TODAY ?= 				$(shell date +%Y-%m-%d)
VERSION = 				$(TODAY)
ANNOTATE_ONTOLOGY_VERSION = annotate -V $(URI)/releases/$(VERSION)/$@ --annotation owl:versionInfo $(VERSION)
FORMATS = 				$(sort ttl owl)
RELEASE_ARTEFACTS = 	$(ONT) $(ONT)-base $(ONT)-full

SIO_URI := 				http://semanticscience.org/ontology/sio.owl


# --------------------------------
# Top-level targets
# --------------------------------

.PHONY: .FORCE

.PHONY: all
all: all_imports all_main all_subsets sparql_test all_reports all_assets

.PHONY: test
test: sparql_test all_reports 
	$(ROBOT) reason --input $(SRC) --reasoner ELK  --equivalent-classes-allowed all --exclude-tautologies structural --output test.ttl && rm test.ttl && echo "Success"

$(TMPDIR) $(REPORTDIR) :
	mkdir -p $@


## -- main targets --
##
## By default this is the cross-product of {ont, ont-base} x FORMATS

MAIN_PRODUCTS = $(sort $(foreach r,$(RELEASE_ARTEFACTS), $(r)) $(ONT))
MAIN_GZIPPED = 
MAIN_FILES = $(foreach n,$(MAIN_PRODUCTS), $(foreach f,$(FORMATS), $(n).$(f))) $(MAIN_GZIPPED)

.PHONY: all_main
all_main: $(MAIN_FILES)


## -- import targets --
##
## By default this is the cross-product of IMPORT_MODULES x FORMATS

IMPORTS = sio
IMPORT_ROOTS = $(patsubst %, imports/%_import, $(IMPORTS))
IMPORT_OWL_FILES = $(foreach n,$(IMPORT_ROOTS), $(n).owl)
IMPORT_FILES = $(IMPORT_OWL_FILES)

.PHONY: all_imports
all_imports: $(IMPORT_FILES)


# ---------------------------------
# Subset targets
# By default this is the cross-product of SUBSETS x FORMATS
# ---------------------------------

SUBSETS = 

SUBSET_ROOTS = $(patsubst %, subsets/%, $(SUBSETS))
SUBSET_FORMATS = $(sort ttl owl)
SUBSET_FILES = $(foreach n,$(SUBSET_ROOTS), $(foreach f,$(SUBSET_FORMATS), $(n).$(f)))


.PHONY: all_subsets
all_subsets: $(SUBSET_FILES)


REPORT =  $(SRC)-report
REPORTS = $(REPORT)
REPORT_FILES = $(patsubst %, $(REPORTDIR)/%.tsv, $(REPORTS))

.PHONY: robot_reports
robot_reports: $(REPORT_FILES)

.PHONY: all_reports
all_reports: all_reports_onestep $(REPORT_FILES)

$(REPORTDIR)/validate_profile_owl2dl_%.txt: % | $(REPORTDIR)
	$(ROBOT) validate-profile --profile DL -i $< -o $@
.PRECIOUS: $(REPORTDIR)/validate_profile_owl2dl_%.txt

.PHONY: validate_profile_%
validate_profile_%: $(REPORTDIR)/validate_profile_owl2dl_%.txt



## -- all files/assets --

ASSETS = \
  $(IMPORT_FILES) \
  $(MAIN_FILES) \
  $(REPORT_FILES) \
  $(SUBSET_FILES)

RELEASE_ASSETS = \
  $(MAIN_FILES) \
  $(SUBSET_FILES)

.PHONY: all_assets
all_assets: $(ASSETS)


.PHONY: show_assets
show_assets:
	echo $(ASSETS)
	du -sh $(ASSETS)


# ----------------------------------------
# Release Management
# ----------------------------------------

CLEANFILES=$(MAIN_FILES) $(SRCMERGED)
# This should be executed by the release manager whenever time comes to make a release.
# It will ensure that all assets/files are fresh, and will copy to release folder

.PHONY: prepare_release
prepare_release: $(ASSETS) $(PATTERN_RELEASE_FILES)
	rsync -R $(RELEASE_ASSETS) $(RELEASEDIR) &&\
  rm -f $(CLEANFILES) &&\
  echo "Release files are now in $(RELEASEDIR) - now you should commit, push and make a release on your git hosting site such as GitHub or GitLab"

.PHONY: prepare_initial_release
prepare_initial_release: prepare_release
	cd $(RELEASEDIR) && git add $(RELEASE_ASSETS)


# ----------------------------------------
# Import modules
# ----------------------------------------
# Most ontologies are modularly constructed using portions of other ontologies
# These live in the imports/ folder


# seed.txt contains all referenced entities
IMPORTSEED=$(TMPDIR)/seed.txt
SRCMERGED=$(TMPDIR)/merged-$(SRC)
PRESEED=$(TMPDIR)/pre_seed.txt

$(SRCMERGED): $(SRC)
	$(ROBOT) remove --input $< --select imports --trim false \
		merge  $(patsubst %, -i %, $(OTHER_SRC)) -o .$@

$(PRESEED): $(SRCMERGED)
	$(ROBOT) query -f csv -i .$< --query ../sparql/terms.sparql .$@.tmp &&\
	cat .$@.tmp | sort | uniq >  .$@


ALLSEED = $(PRESEED) \


$(IMPORTSEED):  $(ALLSEED) 
	if [ $(IMP) = true ]; then cat $(ALLSEED) | sort | uniq > .$@; fi


ANNOTATION_PROPERTIES=rdfs:label IAO:0000115 

# -- Generate Import Modules --
#
# This pattern uses ROBOT to generate an import module
# Generate terms.txt for each import
# Should be able to drop this if robot can just take a big messy list of terms as input.
imports/%_terms_combined.txt: $(IMPORTSEED) imports/%_terms.txt
	if [ $(IMP) = true ]; then cat $^ | grep -v ^# | sort | uniq >  $@; fi


imports/%_import.owl: mirror/%.owl imports/%_terms_combined.txt
	if [ $(IMP) = true ]; then $(ROBOT) query -i $< --update ../sparql/preprocess-module.ru \
		extract -T imports/$*_terms_combined.txt --force true --copy-ontology-annotations true --individuals include --method BOT \
		query --update ../sparql/inject-subset-declaration.ru --update ../sparql/postprocess-module.ru \
		annotate --ontology-iri $(URI)/$@ $(ANNOTATE_ONTOLOGY_VERSION) --output $@.tmp.owl && mv $@.tmp.owl $@; fi

.PRECIOUS: imports/%_import.owl

.PHONY: refresh-imports
refresh-imports:
	make IMP=true MIR=true PAT=false IMP_LARGE=true all_imports -B

.PHONY: refresh-imports-excluding-large
refresh-imports-excluding-large:
	make IMP=true MIR=true PAT=false IMP_LARGE=false all_imports -B

.PHONY: refresh-%
refresh-%:
	make IMP=true IMP_LARGE=true MIR=true PAT=false imports/$*_import.owl -B



# ----------------------------------------
# Mirroring upstream ontologies
# ----------------------------------------


IMP=true # Global parameter to bypass import generation
MIR=true # Global parameter to bypass mirror generation
IMP_LARGE=true # Global parameter to bypass handling of large imports

## ONTOLOGY: sio
## Copy of sio is re-downloaded whenever source changes
mirror/sio.trigger: $(SRC)

mirror/sio.owl: mirror/sio.trigger
	if [ $(MIR) = true ] && [ $(IMP) = true ]; then $(ROBOT) convert -I $(SIO_URI) -o $@.tmp.owl && mv $@.tmp.owl $@; fi
.PRECIOUS: mirror/%.owl



# ----------------------------------------
# Release
# ----------------------------------------
# copy from staging area (this directory) to top-level
.PHONY: release
release: $(ONT).owl $(ONT).ttl
	cp $^ $(RELEASEDIR) && cp imports/* $(RELEASEDIR)/imports


# ----------------------------------------
# Sparql queries: Q/C
# ----------------------------------------

# these live in the ../sparql directory, and have suffix -violation.sparql
# adding the name here will make the violation check live.
# NOTE: these will soon be phased out and replaced by robot-report

#  run all violation checks
SPARQL_VALIDATION_QUERIES = $(foreach V,$(SPARQL_VALIDATION_CHECKS),$(SPARQLDIR)/$(V)-violation.sparql)
sparql_test: $(SRC) catalog-v001.xml | $(REPORTDIR)
ifneq ($(SPARQL_VALIDATION_QUERIES),)
	$(ROBOT) verify  --catalog catalog-v001.xml -i $< --queries $(SPARQL_VALIDATION_QUERIES) -O $(REPORTDIR)
endif

# ----------------------------------------
# ROBOT report
# ----------------------------------------
$(REPORTDIR)/%-report.tsv: % | $(REPORTDIR)
	$(ROBOT) report -i $< $(REPORT_LABEL) $(REPORT_PROFILE_OPTS) --fail-on $(REPORT_FAIL_ON) --print 5 -o $@

# ----------------------------------------
# Sparql queries: Exports
# ----------------------------------------

SPARQL_EXPORTS_ARGS = $(foreach V,$(SPARQL_EXPORTS),-s $(SPARQLDIR)/$(V).sparql $(REPORTDIR)/$(V).tsv)
# This combines all into one single command

.PHONY: all_reports_onestep
all_reports_onestep: $(SRC)
ifneq ($(SPARQL_EXPORTS_ARGS),)
	$(ROBOT) query -f tsv -i $< $(SPARQL_EXPORTS_ARGS)
endif


# ----------------------------------------
# Release artefacts: export formats
# ----------------------------------------

$(ONT)-full.owl: $(ONT)-full.ttl
	$(ROBOT) convert --input $< --check false -f owl -o $@.tmp.owl && mv $@.tmp.owl $@
$(ONT)-base.owl: $(ONT)-base.ttl
	$(ROBOT) convert --input $< --check false -f owl -o $@.tmp.owl && mv $@.tmp.owl $@


# We always want a base - even if it is not explicitly configured..
# We always want a full release - even if it is not explicitly configured..
# ----------------------------------------
# Release artefacts: main release artefacts
# ----------------------------------------

$(ONT).owl: $(ONT)-full.owl
	$(ROBOT) annotate --input $< --ontology-iri $(URIBASE)/$@ $(ANNOTATE_ONTOLOGY_VERSION) \
		convert -o $@.tmp.owl && mv $@.tmp.owl $@

$(ONT).ttl: $(ONT).owl
	$(ROBOT) convert --input $< --check false -f ttl -o $@.tmp.ttl && grep -v ^owl-axioms $@.tmp.ttl > $@ && rm $@.tmp.ttl


# -----------------------------------------------------
# Release artefacts: variants (base, full, simple, etc)
# -----------------------------------------------------

$(ONTOLOGYTERMS): $(SRC) $(OTHER_SRC)
	touch $(ONTOLOGYTERMS) && \
	$(ROBOT) query --use-graphs true -f csv -i $< --query ../sparql/triffo_terms.sparql $@




# base: OTHER sources of interest, such as definitions owl
$(ONT)-base.ttl: $(SRC) $(OTHER_SRC)
	$(ROBOT) remove --input $< --select imports --trim false \
		merge $(patsubst %, -i %, $(OTHER_SRC)) \
		annotate --link-annotation http://purl.org/dc/elements/1.1/type http://purl.obolibrary.org/obo/IAO_8000001 \
		--ontology-iri $(URI)/$@ $(ANNOTATE_ONTOLOGY_VERSION) \
		--output $@.tmp.owl && mv $@.tmp.owl $@

# Full: The full artefacts with imports merged, reasoned
$(ONT)-full.ttl: $(SRC) $(OTHER_SRC)
	$(ROBOT) merge --input $< \
		reason --reasoner ELK --equivalent-classes-allowed all --exclude-tautologies structural \
		relax \
		reduce -r ELK \
		annotate --ontology-iri $(URI)/$@ $(ANNOTATE_ONTOLOGY_VERSION) --output $@.tmp.owl && mv $@.tmp.owl $@





.PHONY: validate_idranges
validate_idranges:
	amm $(SCRIPTSDIR)/validate_id_ranges.sc triffo-idranges.owl

.PHONY: update_repo
update_repo:
	sh $(SCRIPTSDIR)/update_repo.sh
