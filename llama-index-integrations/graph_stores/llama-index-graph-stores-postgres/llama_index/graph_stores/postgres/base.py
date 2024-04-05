"""Postgres graph store index."""

from typing import Any, Dict, List, Optional

import age
from llama_index.core.graph_stores.types import GraphStore


class PostgresGraphStore(GraphStore):
    """Postgres Graph Store.

    Examples:
        `pip install llama-index-graph-stores-postgres`

        ```python
        from llama_index.graph_stores.postgres import PostgresGraphStore

        # Setup PGVectoRs client
        dsn = "postgresql+psycopg://{user}:{password}@{host}:{port}/{db_name}".format(
            user=os.getenv("DB_USER", "postgres"),
            password=os.getenv("DB_PASS", "mysecretpassword"),
            host=os.getenv("DB_HOST", "localhost"),
            port=os.getenv("DB_PORT", "5432"),
            db_name=os.getenv("DB_NAME", "postgres"),
        )

        graph_store = PostgresGraphStore(
            dsn=dsn,
        )

        or

        graph_store = PostgresGraphStore(
            host=os.getenv("DB_HOST", "localhost"),
            port=os.getenv("DB_PORT", "5432"),
            db_name=os.getenv("DB_NAME", "postgres"),
            user=os.getenv("DB_USER", "postgres"),
            password=os.getenv("DB_PASS", "mysecretpassword"),
        )

        ```

    """

    def __init__(
        self,
        host: Optional[str] = None,
        port: Optional[str] = None,
        db_name: Optional[str] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
        dsn: Optional[str] = None,
        graph_name: str = "graph_store",
        node_label: str = "Entity",
        **kwargs,
    ):
        dsn = dsn or f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db_name}"

        try:
            self._age = age.connect(
                dsn=dsn,
                graph=graph_name,
                **kwargs,
            )
        except Exception:
            raise ValueError(
                "Could not connect to Postgres."
                "Please ensure that the configuration is correct."
            )

        self.node_label = node_label
        # TODO: Create constraint once AGE supports

    @property
    def client(self) -> Any:
        return self._age

    def get(self, subj: str) -> List[List[str]]:
        """Get triplets."""
        query = f"""
            MATCH (n1:`{self.node_label}`)-[r]->(n2:`{self.node_label}`)
            WHERE n1.id = %s
            RETURN type(r), n2.id
        """

        with self._age.execCypher(
            query,
            cols=["rel", "obj"],
            params=(subj,),
        ) as cursor:
            return [list(row) for row in cursor]

    def get_rel_map(
        self, subjs: Optional[List[str]] = None, depth: int = 2, limit: int = 30
    ) -> Dict[str, List[List[str]]]:
        """Get flat rel map."""
        # The flat means for multi-hop relation path, we could get
        # knowledge like: subj -> rel -> obj -> rel -> obj -> rel -> obj.
        # This type of knowledge is useful for some tasks.
        # +-------------+------------------------------------+
        # | subj        | flattened_rels                     |
        # +-------------+------------------------------------+
        # | "player101" | [95, "player125", 2002, "team204"] |
        # | "player100" | [1997, "team204"]                  |
        # ...
        # +-------------+------------------------------------+

        rel_map: Dict[Any, List[Any]] = {}
        if subjs is None or len(subjs) == 0:
            # unlike simple graph_store, we don't do get_all here
            return rel_map

        query = f"""
            MATCH p=(n1:`{self.node_label}`)-[*1..{depth}]->()
            WHERE toLower(n1.id) IN {[subj.lower() for subj in subjs]}
            UNWIND relationships(p) AS rel WITH n1.id AS subj, p,
            collect([type(rel), endNode(rel).id]) AS flattened_rels
            UNWIND flattened_rels as fr
            WITH DISTINCT fr, subj
            RETURN subj, collect(fr) AS flattened_rels LIMIT {limit}
        """

        with self._age.execCypher(query, cols=["subj", "rels"]) as cursor:
            for row in cursor:
                rel_map[row[0]] = row[1]

        return rel_map

    def upsert_triplet(self, subj: str, rel: str, obj: str) -> None:
        """Add triplet."""
        rel = rel.replace(" ", "_").upper()
        prepared_statement = f"""
            MERGE (n1:`{self.node_label}` {{id: %s}})
            MERGE (n2:`{self.node_label}` {{id: %s}})
            MERGE (n1)-[:`{rel}`]->(n2)
        """

        self._age.execCypher(prepared_statement, params=(subj, obj))
        self._age.commit()

    def delete(self, subj: str, rel: str, obj: str) -> None:
        """Delete triplet."""

        def delete_rel(subj: str, obj: str, rel: str) -> None:
            rel = rel.replace(" ", "_").upper()
            query = f"""
                MATCH (n1:`{self.node_label}`)-[r:`{rel}`]->(n2:`{self.node_label}`)
                WHERE n1.id = %s AND n2.id = %s DELETE r
            """

            self._age.execCypher(query, params=(subj, obj))
            self._age.commit()

        def delete_entity(entity: str) -> None:
            query = f"MATCH (n:`{self.node_label}`) WHERE n.id = %s DELETE n"

            self._age.execCypher(query, params=(entity,))
            self._age.commit()

        def check_edges(entity: str) -> bool:
            query = f"""
                MATCH (n1:`{self.node_label}`)--()
                WHERE n1.id = %s RETURN count(*)
            """

            with self._age.execCypher(query, params=(entity,)) as cursor:
                for row in cursor:
                    result = bool(row[0])
            return result

        delete_rel(subj, obj, rel)
        if not check_edges(subj):
            delete_entity(subj)
        if not check_edges(obj):
            delete_entity(obj)

    def delete_graph(self, graph_name) -> None:
        """Delete the graph, useful when you want to delete a graph index and related data."""
        age.deleteGraph(self._age.connection, graph_name)

    def get_schema(self, refresh: bool = False) -> str:
        """Get the schema of the graph store."""
        # TODO: To find an appropriate way to get the schema
        # Apache AGE currently does not provide a direct way to botain it
        # The schema in ag_catalog looks not so useful
        raise NotImplementedError

    def query(self, query: str, param_map: Optional[Dict[str, Any]] = {}) -> Any:
        """Query the graph store with statement and parameters."""
        # Use the client instead, don't forget to specify the "cols" once the query returns multiple values
        raise NotImplementedError
