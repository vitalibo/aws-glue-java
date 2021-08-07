environment := dev
profile := default

configuration := yq eval-all 'select(fileIndex == 0) * select(fileIndex == 1)' \
	'src/main/resources/default-application.yaml' 'src/profile/$(environment)/application.yaml' 2> /dev/null

define param
$(shell $(configuration) | yq eval '.Parameters.$(1)' - )
endef

define params
$(shell $(configuration) | yq eval '.Parameters | to_entries | (.[] |= .key + "=" + .value) | join(" ")' - )
endef

bucket := $(call param,'Bucket')
name := $(call param,'Name')

environments := $(shell ls src/profile | xargs)

checksum := $(shell git status > /dev/null; git diff-index --quiet HEAD -- && \
	git rev-parse --short HEAD || \
	md5 $$(git diff-index HEAD --name-only) 2>&1 | md5 | cut -c1-7)

.PHONY: help
help:
	@echo "Usage: make <deploy|delete> environment=<string> profile=<string>"
	@echo " "
	@echo "Options:"
	@echo "  environment      The JSON file which contains environment configuration (Supported values: [$(environments)])."
	@echo "  profile          Use a specific AWS profile from your credential file (Default: '$(profile)')."
	@echo " "

.PHONY: verify
verify:
    ifeq ($(filter $(environment),$(environments)),)
		$(error Environment '$(environment)' is not supported)
    endif

.PHONY: package
package:
	mvn clean package -Dmaven.test.skip=true -P $(environment) -f pom.xml

	cp stack.yaml tempstack.yaml
	for ref in $(shell cat stack.yaml | grep -e '\('"'"'\|"\)s3uri(.*)\('"'"'\|"\)' -o) ; do \
		source=$$(echo $$ref | sed -E 's/s3uri\((.+)\)/\1/') ; \
		target="s3://$(bucket)/$(environment)/$(name)/$(checksum)/$$(basename $$source)" ; \
		aws s3 cp $$source $$target --profile $(profile) ; \
		sed -i '.bak' "s|$$ref|$$target|g" tempstack.yaml ; \
	done

.PHONY: deploy
deploy: verify package
	aws cloudformation deploy \
        --stack-name "$(name)-infrastructure" \
        --parameter-overrides $(call params) \
        --capabilities 'CAPABILITY_NAMED_IAM' \
        --template-file tempstack.yaml \
        --profile $(profile)
	$(MAKE) clean

.PHONY: delete
delete: verify
	aws cloudformation delete-stack \
        --stack-name "$(name)-infrastructure" \
        --profile $(profile)
	$(MAKE) clean

.PHONY: clean
clean:
	rm -rf tempstack.yaml*
