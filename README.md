# tap-abcfinancial

Singer tap to extract data from the ABC Financial API, conforming to the Singer
spec: https://github.com/singer-io/getting-started/blob/master/docs/SPEC.md

ABC Financial API: https://abcfinancial.3scale.net/docs

Currently capable of extracting 4 streams:

- *Clubs*:
  - Full overwrite stream, cannot be incrementally extracted
- *Members*:
  - Incremental stream
- *Check-ins*:
  - Incremental stream; this stream can only be extracted in 31 day increments, which
  requires custom handling when back-filling data over a large time range
- *Prospects*:
  - Incremental stream
- *Events*:
  - Incremental stream

## Setup

`python3 setup.py install`

## Running the tap

#### Discover mode:

`tap-abcfinancial -c config.json --discover > catalog.json`

#### Sync mode:

`tap-abcfinancial -c config.json -p catalog.json -s state.json`

*Note:* The `-s` parameter is optional
