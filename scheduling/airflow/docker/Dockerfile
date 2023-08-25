FROM ghcr.io/navikt/knada-images/airflow-custom-base:v1

COPY requirements.txt .

RUN pip install -r requirements.txt

# jq, wget, og tar må være installert for at kommandoen nedenfor skal fungere
RUN cd /app && \
    QUARTO_VERSION=$(curl https://api.github.com/repos/quarto-dev/quarto-cli/releases/latest | jq '.tag_name' | sed -e 's/[\"v]//g') && \
    wget https://github.com/quarto-dev/quarto-cli/releases/download/v${QUARTO_VERSION}/quarto-${QUARTO_VERSION}-linux-amd64.tar.gz && \
    tar -zxvf quarto-${QUARTO_VERSION}-linux-amd64.tar.gz \
ENV PATH="${PATH}:/app/quarto-${QUARTO_VERSION}/bi
