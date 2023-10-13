# tap-hubspot

## Run hubspot locally

Use poetry to create a virtual environment, for more details see [poetry](https://python-poetry.org/docs/basic-usage/)

```sh
poetry init
```

Follow the prompts and add dependencies interactively based on `install_requires` in `setup.py`
Install the dependencies

```sh
poetry install
```

Activating the virtual environment

```sh
poetry shell
```

Construct config.json like `config.sample.json` using [scripts-and-tools](https://github.com/dreamdata-io/scripts-and-tools)

```
  cp config.sample.json config.json
  dreamdatastore config --integration hubspot --slug dreamdata_io

```

run the tap

```sh
python3 main.py -c config.json > out.ndjson
```