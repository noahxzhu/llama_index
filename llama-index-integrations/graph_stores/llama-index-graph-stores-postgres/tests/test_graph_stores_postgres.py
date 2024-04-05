from unittest.mock import MagicMock, patch

from llama_index.core.graph_stores.types import GraphStore

from llama_index.graph_stores.postgres import PostgresGraphStore


@patch("llama_index.graph_stores.postgres.PostgresGraphStore")
def test_kuzu_graph_store(MockPostgresGraphStore: MagicMock):
    instance: PostgresGraphStore = MockPostgresGraphStore.return_value()
    assert isinstance(instance, GraphStore)
