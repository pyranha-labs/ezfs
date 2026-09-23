# Additional recipes for Python based development.
-include python.mk

##### Project Overrides #####

PYTHON_BIN := python3.12
PYLINT_EXTRAS := benchmark

##### Initial Development Setups and Configurations #####

UPSTREAM := git@github.com:pyranha-labs/ezfs.git

# Set up initial environment for development.
.PHONY: setup
setup:
	ln -sfnv $(PY_PROJECT_ROOT)tools/pre-push $(PY_PROJECT_ROOT).git/hooks/pre-push
	-git remote add upstream $(UPSTREAM)
	-git fetch upstream
	@echo "🏆 Git set up complete!"
	curl https://raw.githubusercontent.com/pyranha-labs/build-tools/refs/heads/main/python.mk -o python.mk
	make clean-venv venv
	make default
	@echo "🏆 Full set up complete!"

##### Quality Assurance #####

# Override both custom scripts due to being a single file module.
docstrings: ;
order: ;
