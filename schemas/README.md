# Dark Ecology Dataset Schemas

This folder has [Frictionless Table Schemas](https://specs.frictionlessdata.io//table-schema/) for Dark Ecology Dataset files. See the [documentation](https://darkecology.github.io/dataset/) to see the schemas in a human-friendly format.

Here are some examples of using the schemas to validate data files with [Frictionless CLI](https://framework.frictionlessdata.io/docs/console/overview.html) (after populating the data directory):

```bash
frictionless validate data/profiles/2000/10/01/KFWS/KFWS20001001_000440.csv --schema schemas/profile.schema.json
frictionless validate data/scans/2000/KBOX-2000.csv --schema schemas/scan.schema.json
frictionless validate data/5min/2000/KBOX-2000-5min.csv  --schema schemas/5min.schema.json
frictionless validate data/daily/2000-daily.csv --schema schemas/daily.schema.json
```
