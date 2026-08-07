#!/bin/bash
awslocal s3 mb s3://continuo || true

# Apply 30-day expiry lifecycle rules for candidate-sql/, logs/, and code-bundles/.
# This runs every time LocalStack starts; put-bucket-lifecycle-configuration
# is idempotent — it replaces the full rule set on each call.
awslocal s3api put-bucket-lifecycle-configuration \
  --bucket continuo \
  --lifecycle-configuration '{
    "Rules": [
      {
        "ID": "expire-candidate-sql-30d",
        "Filter": { "Prefix": "candidate-sql/" },
        "Status": "Enabled",
        "Expiration": { "Days": 30 }
      },
      {
        "ID": "expire-logs-30d",
        "Filter": { "Prefix": "logs/" },
        "Status": "Enabled",
        "Expiration": { "Days": 30 }
      },
      {
        "ID": "expire-code-bundles-30d",
        "Filter": { "Prefix": "code-bundles/" },
        "Status": "Enabled",
        "Expiration": { "Days": 30 }
      }
    ]
  }' || true
