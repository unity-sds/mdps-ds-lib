#!/usr/bin/env bash

echo "setting email and name config"
git config --local user.email "wai.phyo@jpl.nasa.gov"
git config --local user.name ${GITHUB_TRIGGERING_ACTOR}


echo "creating PR"
result=`gh pr create --base "develop" --body "NA" --head "main" --title "chore: catchup from main"`
echo "PR result $result"
pr_number=`echo $result | grep -oE '[0-9]+$'`
echo "PR number ${pr_number}"
