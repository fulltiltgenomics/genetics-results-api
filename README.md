# Genetics Results API

API to serve human genetics association results and annotations

This is deployed as part of FinnGenie AI assistant (see [https://github.com/fulltiltgenomics/genetics-results-suite](https://github.com/fulltiltgenomics/genetics-results-suite))

A deployment with publicly available data is not yet available

## Requirements

- Python 3.13+
- [uv](https://docs.astral.sh/uv/) package manager

## Setup

Install requirements

```bash
uv sync
```

Update files under [config/](config/) to point to your data files

## Run the server

```bash
uv run python run_server.py 8081
```

## API docs

Once running, open http://localhost:8081/api/v1/docs to see available endpoints

## Run tests against running server

```bash
uv pip install --system -r requirements.txt
SERVER_URL=http://localhost:8081 tests/run_tests.sh
```

## License

MIT
