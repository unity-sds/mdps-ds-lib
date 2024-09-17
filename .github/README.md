Artifact generation steps

- Do Nothing in when creating a PR.
- When merging a PR (if title is breaking / feat / bug)
    - Bump a version with postfix & create a PR 
- When merging a PR with title 'chore: update version'
    - Build & upload to PyPi
- Creating Release PR
    - Startiwht "Release" + update official version
- When merging a PR to master
    - Build & upload to Pypi
    - Create a Release
    - Add Link to PyPi
