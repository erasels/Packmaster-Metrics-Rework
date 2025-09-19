pip install -e .
metrics init
metrics load --metrics-root "full/path/to/metrics" --warehouse data/warehouse
metrics insight win_by_asc --warehouse data/warehouse
metrics insight pack_pick  --warehouse data/warehouse
metrics insight pack_win   --warehouse data/warehouse --min-support 200
metrics insight card_pick  --warehouse data/warehouse --min-support 500
metrics insight card_win   --warehouse data/warehouse --min-support 500

Rework of https://github.com/erasels/Packmaster-Metrics