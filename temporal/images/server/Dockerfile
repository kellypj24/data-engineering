FROM temporalio/server:1.25.0.0
# TODO: use a specific version for production

USER root

USER temporal

COPY start.sh start.sh

ENTRYPOINT ["sh", "./start.sh"]