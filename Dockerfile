FROM python:3.9-slim

WORKDIR /airbyte/integration_code

# deps de runtime básicas
RUN apt-get update && apt-get install -y --no-install-recommends bash ca-certificates \
 && rm -rf /var/lib/apt/lists/*

# código
COPY source_btg ./source_btg
COPY main.py .
COPY setup.py .
COPY requirements.txt .

# base.sh
COPY docker/airbyte_base.sh /airbyte/base.sh

RUN chmod +x /airbyte/base.sh \
    && ln -s /airbyte/base.sh /airbyte/base \
    # Wrappers corretos que passam o nome do comando
    && printf '#!/usr/bin/env bash\nexec /airbyte/base.sh spec "$@"\n' > /usr/local/bin/spec \
    && printf '#!/usr/bin/env bash\nexec /airbyte/base.sh check "$@"\n' > /usr/local/bin/check \
    && printf '#!/usr/bin/env bash\nexec /airbyte/base.sh discover "$@"\n' > /usr/local/bin/discover \
    && printf '#!/usr/bin/env bash\nexec /airbyte/base.sh read "$@"\n' > /usr/local/bin/read \
    && chmod +x /usr/local/bin/spec /usr/local/bin/check /usr/local/bin/discover /usr/local/bin/read



# deps python
RUN pip install --no-cache-dir --upgrade pip \
 && pip install --no-cache-dir -r requirements.txt \
 && pip install --no-cache-dir .

# o base.sh chamará isso
ENV AIRBYTE_ENTRYPOINT="python /airbyte/integration_code/main.py"

# Labels
LABEL io.airbyte.name=airbyte/source-btg
LABEL io.airbyte.version=0.1.13

# MUITO IMPORTANTE: entrypoint usa o base.sh
ENTRYPOINT ["/airbyte/base.sh"]
