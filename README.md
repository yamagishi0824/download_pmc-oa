

```
# txtのダウンロード
uv run python ./src/download_pmc.py \
  --html ./html/2026-02-02.html \
  --outdir ./data \
  --workers 16

# txtのダウンロード
uv run python ./src/download_pmc.py \
  --html ./html/2026-02-02_xml.html \
  --outdir ./data \
  --workers 32
```
