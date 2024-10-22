FROM temporalio/auto-setup:1.25.0.0

# https://temporal.io/blog/auto-setup

ENV POSTGRES_SEEDS=''
ENV POSTGRES_USER=''
ENV POSTGRES_PWD=''

USER root

USER temporal

COPY setup.sh setup.sh

ENTRYPOINT ["sh", "./setup.sh"]