# tap-hubspot

`tap-hubspot` is a Singer tap for hubspot.

Built with the [Meltano Tap SDK](https://sdk.meltano.com) for Singer Taps.

## Installation

- [ ] `Developer TODO:` Update the below as needed to correctly describe the install procedure. For instance, if you do not have a PyPi repo, or if you want users to directly install from your git repo, you can modify this step as appropriate.

```bash
pipx install tap-hubspot
```

## Configuration

### Accepted Config Options

- [ ] `Developer TODO:` Provide a list of config options accepted by the tap.

A full list of supported settings and capabilities for this
tap is available by running:

```bash
tap-hubspot --about
```

### Source Authentication and Authorization

- [ ] `Developer TODO:` If your tap requires special access on the source system, or any special authentication requirements, provide those here.

## Usage

You can easily run `tap-hubspot` by itself or in a pipeline using [Meltano](https://meltano.com/).

### Executing the Tap Directly

```bash
tap-hubspot --version
tap-hubspot --help
tap-hubspot --config CONFIG --discover > ./catalog.json
```

### Available and Selected Filters

The tap supports Hotglue filter discovery and runtime filter selection:

- `--get-available-filters`: outputs `available-filters.json` metadata used by the UI.
- `--selected-filters`: applies the selected filters for a sync run.

For `contact_events`, filters are supported on `eventType`.

Get available filters:

```bash
tap-hubspot-beta \
  --config config.json \
  --catalog catalog-selected.json \
  --get-available-filters > available-filters.json
```

Run a sync with selected filters:

```bash
tap-hubspot-beta \
  --config config.json \
  --catalog catalog-selected.json \
  --state state.json \
  --selected-filters selected-filters.json
```

Example `selected-filters.json`:

```json
{
  "filters_version": "1.0.0",
  "streams": {
    "contact_events": {
      "clause_1": {
        "field": "eventType",
        "operator": "IN",
        "value": [
          "e_visited_page",
          "e_submitted_form"
        ]
      }
    }
  }
}
```

## Developer Resources

- [ ] `Developer TODO:` As a first step, scan the entire project for the text "`TODO:`" and complete any recommended steps, deleting the "TODO" references once completed.

### Initialize your Development Environment

```bash
pipx install poetry
poetry install
```

### Create and Run Tests

Create tests within the `tap_hubspot/tests` subfolder and
  then run:

```bash
poetry run pytest
```

You can also test the `tap-hubspot` CLI interface directly using `poetry run`:

```bash
poetry run tap-hubspot --help
```

### Testing with [Meltano](https://www.meltano.com)

_**Note:** This tap will work in any Singer environment and does not require Meltano.
Examples here are for convenience and to streamline end-to-end orchestration scenarios._

Your project comes with a custom `meltano.yml` project file already created. Open the `meltano.yml` and follow any _"TODO"_ items listed in
the file.

Next, install Meltano (if you haven't already) and any needed plugins:

```bash
# Install meltano
pipx install meltano
# Initialize meltano within this directory
cd tap-hubspot
meltano install
```

Now you can test and orchestrate using Meltano:

```bash
# Test invocation:
meltano invoke tap-hubspot --version
# OR run a test `elt` pipeline:
meltano elt tap-hubspot target-jsonl
```

### SDK Dev Guide

See the [dev guide](https://sdk.meltano.com/en/latest/dev_guide.html) for more instructions on how to use the SDK to 
develop your own taps and targets.
