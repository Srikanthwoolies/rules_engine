# Development Guide

This document provides information for developers working on the GCS Rules Engine project.

## Project Structure

```
gcs_rules_engine/
├── docs/               # Documentation
├── src/                # Source code
│   └── rules_engine/   # Main package
│       ├── connectors/ # External system connectors (GCS, BigQuery)
│       ├── models/     # Data models and business logic
│       ├── utils/      # Utility functions and helpers
│       └── subpackage/ # Additional functionality
└── tests/              # Tests
    ├── unit/           # Unit tests
    └── integration/    # Integration tests
```

## Development Setup

1. Clone the repository
2. Set up a virtual environment:
   ```
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   pip install -r requirements.txt
   ```
3. Install development dependencies:
   ```
   pip install -e .  # Install in development mode
   ```

## Running Tests

Run the test suite with pytest:

```
pytest
```

For coverage report:

```
pytest --cov=rules_engine
```

## Local Development

For local development, you can use the Functions Framework to test your function:

```
functions-framework --target=process_gcs_file --signature-type=event
```

Then, send a test request:

```
curl localhost:8080 \
  -H "Content-Type: application/json" \
  -d '{
    "bucket": "test-bucket",
    "name": "test-file.csv"
  }'
```

## Coding Standards

This project follows PEP 8 style guidelines. Code formatting is done with Black:

```
black src/ tests/
```

Linting is done with flake8:

```
flake8 src/ tests/
```

## CI/CD Pipeline

The CI/CD pipeline runs on every push to the repository and:

1. Runs the test suite
2. Checks code formatting and linting
3. Deploys the Cloud Function (only on main branch)

For more details, see the GitHub Actions workflow files.

## Adding New Rules

Rules are stored in BigQuery in the `rules_definition` table. Each rule consists of:

- `rule_number`: A unique identifier for the rule
- `rule_description`: A human-readable description of what the rule checks
- `rule_query`: A query that identifies violations of the rule

To add a new rule, insert a row into the `rules_definition` table.
