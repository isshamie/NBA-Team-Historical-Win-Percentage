#!/usr/bin/env bash
set -euo pipefail
python build_data.py
streamlit run app.py
