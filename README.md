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
