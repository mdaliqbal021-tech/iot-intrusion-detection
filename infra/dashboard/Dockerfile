FROM python:3.12-slim-bookworm
WORKDIR /app
COPY requirements.txt .
RUN grep -E '^(streamlit|requests)==' requirements.txt > /tmp/dashboard-requirements.txt \
    && python -m pip install --no-cache-dir -r /tmp/dashboard-requirements.txt
COPY frontend/streamlit_app.py /app/frontend/streamlit_app.py
COPY frontend/results_views.py /app/frontend/results_views.py
COPY frontend/theme.toml /app/.streamlit/config.toml
COPY figures/ /app/figures/
CMD ["python", "-m", "streamlit", "run", "frontend/streamlit_app.py", "--server.address=0.0.0.0", "--server.port=8501", "--server.headless=true", "--browser.gatherUsageStats=false"]
