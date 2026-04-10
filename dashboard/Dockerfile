FROM python:3.11-slim
WORKDIR /app
RUN pip install --no-cache-dir streamlit==1.32.0 pandas==2.2.1 pyarrow==15.0.1 plotly==5.20.0 streamlit-autorefresh==1.0.1
COPY dashboard.py .
CMD ["streamlit", "run", "dashboard.py", "--server.port=8501", "--server.address=0.0.0.0"]