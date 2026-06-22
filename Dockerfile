FROM continuumio/miniconda3:latest

RUN apt-get update && apt-get install -y \
    vim \
    git \
    socat \
    conntrack \
    gcc \
    libgsl-dev \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY .pip /root/.pip
COPY environment.yml .

RUN conda env create -f environment.yml && \
    conda clean -afy

# Build inverter against system GSL (libgsl.so.28 from libgsl-dev)
RUN /bin/bash -lc "source /opt/conda/etc/profile.d/conda.sh && conda activate simple-isotonic && \
  pip install --no-build-isolation --no-binary=inverter inverter==2.0.1"

COPY . .

CMD ["bash", "-c", "source /opt/conda/etc/profile.d/conda.sh && conda activate simple-isotonic && exec python -u main.py"]
