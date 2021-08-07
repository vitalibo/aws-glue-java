environment := dev
profile := default

.PHONY: help
help:
	@echo "Usage: make <update|delete> environment=<string> profile=<string>"
	@echo " "
	@echo "Options:"
	@echo "  environment      The JSON file which contains environment configuration (Supported values: [$(ENVIRONMENTS)])."
	@echo "  profile          Use a specific AWS profile from your credential file (Default: '$(profile)')."
	@echo " "

.PHONY: update
update:
	@echo "Create/Update stack"

.PHONY: delete
delete:
	@echo "Delete stack"
