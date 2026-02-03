

```
# txtのダウンロード
uv run python ./src/download_pmc.py \
  --html ./html/2026-02-02.html \
  --outdir ./data \
  --workers 16

# xmlのダウンロード
uv run python ./src/download_pmc.py \
  --html ./html/2026-02-02_xml.html \
  --outdir ./data \
  --format xml \
  --workers 16
```
