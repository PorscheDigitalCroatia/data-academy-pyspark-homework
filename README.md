# Homework 3

Trip definition algorithm implementation in PySpark

## Local setup

Download and put sample data into `sample_data` directory. Rename it to `telemetry-data.parquet`.

Create and activate Python 3 virtual environment:

```bash
python3 -m venv venv
source venv/bin/activate
```
Install development Python requirements:

```bash
pip install -r requirements-dev.txt
```

Run application with sample data:

```bash
python app/trips.py --dev
```

## ML Workspace setup

Install requirement:

```bash
pip install -r requirements.txt
```

Run application from terminal:

```bash
spark-submit app/trips.py
```


## Tests

Run test using:

```bash
bash ci/test.sh
```

## Formatting the code

Automatically format the code with:

```bash
bash ci/format.sh
```
