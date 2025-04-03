# GCS Rules Engine

A Cloud Function that processes data from Google Cloud Storage (GCS), applies rules stored in BigQuery, and logs rule violations.

## Overview

This project implements a serverless rules engine that:
1. Triggers on GCS bucket events (file uploads/updates)
2. Reads data from the uploaded file 
3. Fetches rules definitions from BigQuery
4. Applies the rules to the data
5. Logs any rule violations back to BigQuery

## Architecture

```
┌───────────┐     ┌───────────────┐     ┌───────────────┐
│  GCS File │────▶│ Cloud Function │────▶│ BigQuery Rule │
│  Upload   │     │ (Rules Engine) │◀────│ Definitions   │
└───────────┘     └───────┬───────┘     └───────────────┘
                          │
                          ▼
                  ┌───────────────┐
                  │ BigQuery      │
                  │ Rule          │
                  │ Violations Log│
                  └───────────────┘
```

## Setup Instructions

### Prerequisites
- Google Cloud Platform (GCP) account
- gcloud CLI installed
- Python 3.8+

### Installation

1. Clone this repository:
   ```
   git clone https://github.com/your-username/gcs_rules_engine.git
   cd gcs_rules_engine
   ```

2. Set up a virtual environment:
   ```
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   pip install -r requirements.txt
   ```

3. Set up GCP resources:
   ```
   # Set your GCP project
   gcloud config set project YOUR_PROJECT_ID
   
   # Create a GCS bucket (if needed)
   gsutil mb gs://YOUR_BUCKET_NAME/
   
   # Create BigQuery dataset (if needed)
   bq mk rules_engine
   ```

4. Deploy the Cloud Function:
   ```
   gcloud functions deploy process_gcs_file \
     --runtime python38 \
     --trigger-resource YOUR_BUCKET_NAME \
     --trigger-event google.storage.object.finalize \
     --entry-point process_gcs_file \
     --source src/
   ```

### Configuration

1. Create the BigQuery tables for rule definitions and violations (see `docs/sql/setup.md`)
2. Add your rules to the `rules_definition` table
3. Upload a file to the GCS bucket to trigger the rules engine

## Usage

1. Upload a file to your configured GCS bucket
2. The Cloud Function will automatically process the file and apply rules
3. Check the BigQuery `rule_violations` table for any detected violations

## Development

See the [Development Guide](docs/development.md) for more information.
