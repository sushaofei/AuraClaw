# Supply the verified Python 3.11 base digest and wheels from the pinned SDK.
ARG APPWORLD_BASE_IMAGE
FROM ${APPWORLD_BASE_IMAGE}
ARG APPWORLD_REVISION
LABEL org.opencontainers.image.source="https://github.com/StonyBrookNLP/appworld" \
      org.opencontainers.image.revision="$APPWORLD_REVISION"
COPY wheels /wheels
RUN python -m pip install --no-index --no-deps /wheels/*.whl && \
    appworld install && rm -rf /wheels
ENV APPWORLD_ROOT=/run PYTHONDONTWRITEBYTECODE=1 HOME=/tmp
WORKDIR /run
USER 10001:10001
# A host socket proxy provides the loopback HTTP endpoint. No container network.
CMD ["python", "-m", "uvicorn", "appworld.serve.environment:app", "--uds", "/socket/environment.sock", "--no-access-log"]
