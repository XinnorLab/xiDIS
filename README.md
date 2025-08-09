# xiDIS

Disaggregated storage for IBM Storage Scale.

## Deployment

The `deploy.py` script orchestrates storage exports and aggregator
configuration using a single JSON file as the source of truth.  The script is
idempotent and provides a phase based pipeline:

```
precheck → storage_export → aggregator_connect → opus_raid → reexport → verify
```

Usage example:

```
python deploy.py --config fabric.json --phase precheck
```

The configuration format and behaviour are described in `deploy.py`'s
documentation string.  The implementation currently focuses on configuration
validation and a pluggable framework; low level NVMe-oF and Opus integration is
left for future work.
