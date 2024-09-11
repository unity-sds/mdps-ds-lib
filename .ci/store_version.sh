#!/usr/bin/env bash
software_version=`python3 -m poetry version --short`
echo $software_version
echo "software_version=${software_version}" >> ${GITHUB_ENV}
