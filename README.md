# NurtacCoreEngineGpt

Project:
NurtacCoreEngineGpt

GitHub repository name:
NurtacCoreEngineGpt

This repository is the ChatGPT Layer-0 implementation.

It reads real live Binance USD-M Futures BTCUSDT trade and public depth update streams, groups events by Binance event timestamp into UTC one-second windows, and appends:

- Candle DNA
- Footprint DNA
- Depth Mutation DNA
- Combined 1S DNA

Run:

```bash
cd NurtacCoreEngineGpt
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python3 main.py
```

Layer-1 Rolling Window Engine reads `data/one_second_combined_dna.jsonl` from the beginning and keeps following it live. It produces only sliding micro windows:

- `data/rolling_3s_dna.jsonl`
- `data/rolling_5s_dna.jsonl`
- `data/rolling_15s_dna.jsonl`

Layer-1 does not produce `rolling_60s_dna.jsonl`; 60S aligned candles belong to Layer-2 as 1M aligned candle output.

Run Layer-1 in a separate terminal while Layer-0 is running:

```bash
python rolling_window_engine.py
```

Layer-2 Aligned Candle Engine reads `data/one_second_combined_dna.jsonl` from the beginning and keeps following it live. It produces non-overlapping UTC-boundary candles:

- `data/aligned_1m_candle_dna.jsonl`
- `data/aligned_5m_candle_dna.jsonl`
- `data/aligned_15m_candle_dna.jsonl`
- `data/aligned_1h_candle_dna.jsonl`
- `data/aligned_4h_candle_dna.jsonl`
- `data/aligned_1d_candle_dna.jsonl`

Layer-2 runs independently from Layer-1 and builds candles hierarchically:

`1S -> 1M -> 5M -> 15M -> 1H -> 4H -> 1D`

Run Layer-2 in a separate terminal while Layer-0 is running:

```bash
python aligned_candle_engine.py
```

Layer-3 Historical Baseline + Context Metrics Engine reads only existing DNA JSONL files from Layer-0, Layer-1, and Layer-2. It does not use Binance API, WebSocket, or new raw data. It calculates ATR, VWAP, CVD, and historical baseline context metrics.

Batch:

```bash
python historical_baseline_engine.py --mode batch
```

Live:

```bash
python historical_baseline_engine.py --mode live
```
