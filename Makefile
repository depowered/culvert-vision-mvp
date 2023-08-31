.PHONY: poetry_install create_env update_env remove_env activate_env deactivate_env

#################################################################################
# ENVIRONMENT MANAGEMENT                                                        #
#################################################################################

SHELL = /bin/zsh
CONDA_ENV = culvert-vision-mvp
CONDA_ENV_PATH = $$(conda info --base)/envs/$(CONDA_ENV)
CONDA_ACTIVATE = source $$(conda info --base)/etc/profile.d/conda.sh ; conda activate ; conda activate

# If direct_url.json files exist in the conda environment, poetry will intepret the
# dependencies installed by conda as being different from those available on PyPI.
# This causes poetry to 'upgrade' these dependencies to their PyPI versions, causing
# conflicts with C libraries shared between libgdal-arrow-parquet and grpcio.
# We remove the direct_url.json files to prevent this from happening.
remove_direct_url_json:
	find $(CONDA_ENV_PATH) -name direct_url.json -delete

poetry_install:
	$(CONDA_ACTIVATE) $(CONDA_ENV) && poetry lock && poetry install

mamba_env_create:
	conda config --set channel_priority strict
	mamba env create --name $(CONDA_ENV) --file environment.yml

mamba_env_update:
	mamba env update --name $(CONDA_ENV) --file environment.yml --prune

## Set up the conda environment
create_env: mamba_env_create remove_direct_url_json poetry_install activate_env

## Update the conda environment
update_env: mamba_env_update remove_direct_url_json poetry_install activate_env

## Remove the conda environment
remove_env:
	conda env remove --name $(CONDA_ENV)

## Rebuild the conda environment from scratch
rebuild_env: remove_env create_env

## Displays the command to activate the conda environment
activate_env:
	@echo "To activate this environment, use\n"
	@echo "\t$$ conda activate $(CONDA_ENV)\n"

## Displays the command to deactivate an active environment
deactivate_env:
	@echo "To deactivate an activate environment, use\n"
	@echo "\t$$ conda deactivate\n"

#################################################################################
# Self Documenting Commands                                                     #
#################################################################################

.DEFAULT_GOAL := help

# Inspired by <http://marmelab.com/blog/2016/02/29/auto-documented-makefile.html>
# sed script explained:
# /^##/:
# 	* save line in hold space
# 	* purge line
# 	* Loop:
# 		* append newline + line to hold space
# 		* go to next line
# 		* if line starts with doc comment, strip comment character off and loop
# 	* remove target prerequisites
# 	* append hold space (+ newline) to line
# 	* replace newline plus comments by `---`
# 	* print line
# Separate expressions are necessary because labels cannot be delimited by
# semicolon; see <http://stackoverflow.com/a/11799865/1968>
.PHONY: help
help:
	@echo "$$(tput bold)Available rules:$$(tput sgr0)"
	@echo
	@sed -n -e "/^## / { \
		h; \
		s/.*//; \
		:doc" \
		-e "H; \
		n; \
		s/^## //; \
		t doc" \
		-e "s/:.*//; \
		G; \
		s/\\n## /---/; \
		s/\\n/ /g; \
		p; \
	}" ${MAKEFILE_LIST} \
	| LC_ALL='C' sort --ignore-case \
	| awk -F '---' \
		-v ncol=$$(tput cols) \
		-v indent=19 \
		-v col_on="$$(tput setaf 6)" \
		-v col_off="$$(tput sgr0)" \
	'{ \
		printf "%s%*s%s ", col_on, -indent, $$1, col_off; \
		n = split($$2, words, " "); \
		line_length = ncol - indent; \
		for (i = 1; i <= n; i++) { \
			line_length -= length(words[i]) + 1; \
			if (line_length <= 0) { \
				line_length = ncol - indent - length(words[i]) - 1; \
				printf "\n%*s ", -indent, " "; \
			} \
			printf "%s ", words[i]; \
		} \
		printf "\n"; \
	}' \
	| more $(shell test $(shell uname) = Darwin && echo '--no-init --raw-control-chars')