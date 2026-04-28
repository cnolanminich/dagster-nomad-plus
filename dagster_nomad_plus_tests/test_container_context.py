from dagster_nomad_plus.container_context import NomadContainerContext


def test_merge_concatenates_sequences_and_prefers_rhs_scalars():
    a = NomadContainerContext(
        env_vars=["A=1"],
        constraints=[{"attribute": "x", "operator": "=", "value": "y"}],
        node_pool="pool-a",
    )
    b = NomadContainerContext(
        env_vars=["B=2"],
        constraints=[{"attribute": "z", "operator": "=", "value": "w"}],
        node_pool="pool-b",
    )
    merged = a.merge(b)
    assert merged.env_vars == ["A=1", "B=2"]
    assert len(merged.constraints) == 2
    assert merged.node_pool == "pool-b"


def test_merge_dict_rhs_wins_per_key():
    a = NomadContainerContext(meta={"k": "v1", "only_a": "a"})
    b = NomadContainerContext(meta={"k": "v2", "only_b": "b"})
    merged = a.merge(b)
    assert merged.meta == {"k": "v2", "only_a": "a", "only_b": "b"}


def test_get_environment_dict_parses_env_vars():
    ctx = NomadContainerContext(env_vars=["FOO=bar"])
    assert ctx.get_environment_dict() == {"FOO": "bar"}
