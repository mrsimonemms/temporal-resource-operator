CHECKOUT ?= main
REPO ?= https://github.com/mrsimonemms/new

.create:
	@cruft create ${REPO} --skip=.git --directory ${DIRECTORY} --checkout ${CHECKOUT}
.PHONY: .create

cruft-update:
ifeq (,$(wildcard .cruft.json))
	@echo "Cruft not configured"
else
	@cruft check || cruft update --skip-apply-ask --refresh-private-variables
endif
.PHONY: cruft-update

.update-precommit:
	@cd "${DIRECTORY}" && pre-commit autoupdate
.PHONY: .update-precommit

blank:
	@$(MAKE) .create DIRECTORY=blank
.PHONY: blank

golang:
	@$(MAKE) .create DIRECTORY=golang
.PHONY: golang

golang-cobra:
	@$(MAKE) .create DIRECTORY=golang-cobra
.PHONY: golang-cobra

js:
	@$(MAKE) .create DIRECTORY=js
.PHONY: js

monorepo:
	@$(MAKE) .create DIRECTORY=monorepo
.PHONY: monorepo

precommit-autoupdate:
	@$(MAKE) .update-precommit DIRECTORY="blank/{{ cookiecutter.project_name }}"
	@$(MAKE) .update-precommit DIRECTORY="golang/{{ cookiecutter.project_name }}"
	@$(MAKE) .update-precommit DIRECTORY="golang-cobra/{{ cookiecutter.project_name }}"
	@$(MAKE) .update-precommit DIRECTORY="js/{{ cookiecutter.project_name }}"
	@$(MAKE) .update-precommit DIRECTORY="monorepo/{{ cookiecutter.project_name }}"
	@$(MAKE) .update-precommit DIRECTORY="terraform/{{ cookiecutter.project_name }}"
.PHONY: precommit-autoupdate

terraform:
	@$(MAKE) .create DIRECTORY=terraform
.PHONY: terraform
