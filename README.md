# tap-abcfinancial

Singer tap to extract data from the ABC Financial API, conforming to the Singer
spec: https://github.com/singer-io/getting-started/blob/master/docs/SPEC.md

ABC Financial API: https://abcfinancial.3scale.net/docs

## Setup

`python3 setup.py install`

## Running the tap

#### Discover mode:

`tap-abcfinancial --config config.json --discover > catalog.json`

#### Sync mode:

`tap-abcfinancial --config config.json -p catalog.json -s state.json`