.PHONY: help bootstrap init lfs status sync osn-clone test bench paper clean
.DEFAULT_GOAL := help

# Repos that are actually written to (the rest are read-only reference)
WORKING := opdi opdi-portal

help:  ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}'

bootstrap: init lfs  ## Full setup: submodules + git-lfs
	@echo "Ready. See CLAUDE.md for the working guide."

lfs:  ## Initialise git-lfs in opdi/ (where reference/*.parquet is tracked)
	@git lfs version >/dev/null 2>&1 || { \
		echo "ERROR: git-lfs is not installed."; \
		echo "  opdi/reference/*.parquet is lfs-tracked. Without lfs, committing a"; \
		echo "  parquet stores it as a normal blob and permanently bloats history."; \
		echo "  Install it before touching reference/:  apt-get install git-lfs"; \
		exit 1; }
	@cd opdi && git lfs install --local && git lfs track
	@echo "git-lfs initialised in opdi/"

init:  ## Initialise/update submodules to their pinned commits
	git submodule update --init --recursive

status:  ## Show commit and branch of every submodule
	@git submodule foreach --quiet \
		'printf "%-56s %s  %s\n" "$$sm_path" "$$(git rev-parse --short HEAD)" "$$(git branch --show-current || echo detached)"'

sync:  ## Pull each submodule to the tip of its tracked branch
	git submodule update --remote --merge
	@echo
	@echo "Submodule pointers moved. Review, then commit them here:"
	@echo "  git add <path> && git commit"

osn-clone:  ## Print the shallow-clone recipe for the OpenSky server
	@echo "On the OSN server, clone only what runs there:"
	@echo
	@echo "  git clone --depth 1 https://github.com/euctrl-pru/opdi"
	@echo "  cd opdi && git lfs pull --include='reference/**'"
	@echo
	@echo "Do NOT clone the meta-repo: traffic/ and the PRC repos are"
	@echo "reference material and only bloat the checkout."

test:  ## Run the opdi test suite
	cd opdi && python -m pytest -q

bench:  ## Run milestone benchmarks against EUROCONTROL ground truth
	cd opdi && python -m pytest benchmarks -q

paper:  ## Render the Quarto papers (offline; no DB access needed)
	cd opdi-portal/papers && quarto render

clean:  ## Remove build artefacts from working repos
	@for r in $(WORKING); do \
		find $$r -name '__pycache__' -type d -prune -exec rm -rf {} + 2>/dev/null || true; \
		find $$r -name '.pytest_cache' -type d -prune -exec rm -rf {} + 2>/dev/null || true; \
	done
	@echo "Cleaned: $(WORKING)"
