# cava-data-indexer

This tool creates "index files" for use by CAVA.
It crawls / queries specific sections of CDAWeb and the IMAP data server, 
and downloads characteristic files to inspect which variables are available.

The index files are stored in this repository for CAVA to fetch:

- [index_imap.v1.json](index_imap.v1.json)
- [index_psp.v1.json](index_psp.v1.json)

### Automatic indexing

A GitHub action has been set up to regularly regenerate the index files and commit them to this repository. As of 2025-07-11, this runs hourly.

### Disabling and re-enabling automatic indexing

See [instructions](https://docs.github.com/en/actions/how-tos/managing-workflow-runs-and-deployments/managing-workflow-runs/disabling-and-enabling-a-workflow) from GitHub.

### Manual/local usage

`python main.py psp`

`python main.py imap`
