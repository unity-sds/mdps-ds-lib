export IMAGE_PREFIX ?= cae-artifactory.jpl.nasa.gov:16001/gov/nasa/jpl/unity
export NAME ?= uds_lib
export VERSION ?= latest


all:
local: build_1 upload_1
build_1:
	rm -rf dist/* && python -m poetry build
upload_1:
	python3 -m twine upload --repository pypi dist/*
