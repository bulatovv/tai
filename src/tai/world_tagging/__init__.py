"""World Tagging package."""

from .rules import rules_def
from .solver import InferenceEngine

# Instantiate and expose the engine
engine = InferenceEngine(rules_def)


def infer_tags(
    world_name: str, return_metadata: bool = False
) -> list[str] | tuple[list[str], dict]:
    """Infers tags for a given world name using the compiled InferenceEngine."""
    return engine.solve(world_name, return_metadata)
