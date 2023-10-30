FROM public.ecr.aws/company/datascience-dagster-base:ds-dagster-cm-009

LABEL com.company.tag="$tag"
LABEL com.company.imagename="datascience-dagster"

COPY --chown=dagster:dagster ./ /home/dagster/
USER dagster
RUN /site/venv/bin/python -m compileall /home/dagster/

RUN cd /home/dagster/data-warehouse && /site/venv/bin/dbt deps
