"""Read-only by construction: no document is a mutation, and the client
refuses to send one."""

import pytest

from dagster_status import cli, queries


@pytest.mark.parametrize("name", sorted(queries.ALL))
def test_no_document_contains_a_mutation(name):
    document = queries.ALL[name]
    assert "mutation" not in document.lower()
    assert document.lstrip().startswith("query ")


def test_every_document_is_in_all():
    documents = {
        v
        for k, v in vars(queries).items()
        if k.isupper() and k != "ALL" and "query" in str(v)
    }
    assert documents == set(queries.ALL.values())


def test_client_refuses_a_mutation():
    with pytest.raises(cli.StatusError, match="only a query|but a query"):
        cli.execute(
            "http://127.0.0.1:9/graphql", None, "mutation { launchRun { __typename } }"
        )
