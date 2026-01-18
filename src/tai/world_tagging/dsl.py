import re
from dataclasses import dataclass, field

from .utils import normalize_name


class Predicate:
    """Base class for DSL predicates."""

    def __or__(self, other: 'Predicate') -> 'Or':
        """Compose two predicates with logical OR."""
        return Or(self, other)

    def __and__(self, other: 'Predicate') -> 'And':
        """Compose two predicates with logical AND."""
        return And(self, other)

    def __invert__(self) -> 'Not':
        """Negate the predicate."""
        return Not(self)


@dataclass(frozen=True)
class Token(Predicate):
    """Matches a single token or a sequence of tokens in the normalized name."""

    value: str | tuple[str, ...]  # Tuple for immutability/hashing
    _pattern: re.Pattern = field(init=False, compare=False, repr=False)

    def __post_init__(self):
        """Compile the token pattern for efficient matching."""
        # Prepare the regex pattern
        if isinstance(self.value, tuple):
            parts = [normalize_name(p) for p in self.value]
            pattern_str = r'\s+'.join(map(re.escape, parts))
        else:
            norm = normalize_name(self.value)
            pattern_str = re.escape(norm)

        # Bypass frozen dataclass constraint for caching the compiled pattern
        pattern = (
            re.compile(r'(?!x)x')
            if not pattern_str
            else re.compile(rf'\b{pattern_str}\b', re.IGNORECASE)
        )
        object.__setattr__(self, '_pattern', pattern)

    def matches(self, normalized_name: str) -> str | None:
        """Check if the token pattern matches the normalized name."""
        if self._pattern.search(normalized_name):
            # Return the "source" string for metadata
            return ' '.join(self.value) if isinstance(self.value, tuple) else self.value
        return None


@dataclass(frozen=True)
class Regex(Predicate):
    """Matches a regex pattern against the ORIGINAL world name."""

    pattern: str
    _compiled: re.Pattern = field(init=False, compare=False, repr=False)

    def __post_init__(self):
        """Compile the regex pattern."""
        object.__setattr__(self, '_compiled', re.compile(self.pattern, re.IGNORECASE))

    def matches(self, raw_name: str) -> str | None:
        """Check if the regex pattern matches the raw name."""
        if self._compiled.search(raw_name):
            return self.pattern
        return None


@dataclass(frozen=True)
class Fuzzy(Predicate):
    """Matches words in the normalized name similar to the target value."""

    value: str
    threshold: float = 0.85  # Similarity threshold (0.0 to 1.0)

    def __post_init__(self):
        """Normalize the target value."""
        # Since we compare against normalized words, we must normalize the target too.
        # But we cannot mutate frozen dataclass, so we assume the user might pass raw.
        # We can store a normalized version in a private field if needed, but for simplicity
        # we'll normalize during matching or assume value is cleaner.
        # Better to normalize once.
        object.__setattr__(self, 'value', normalize_name(self.value))


@dataclass(frozen=True)
class HasTag(Predicate):
    """Dependency on another tag."""

    tag_name: str


@dataclass(frozen=True)
class Or(Predicate):
    """Logical OR of two predicates."""

    left: Predicate
    right: Predicate


@dataclass(frozen=True)
class And(Predicate):
    """Logical AND of two predicates."""

    left: Predicate
    right: Predicate


@dataclass(frozen=True)
class Not(Predicate):
    """Logical NOT of a predicate."""

    operand: Predicate


# --- DSL HELPERS ---


def token(val: str | list[str]) -> Token:
    """Create a Token predicate."""
    if isinstance(val, list):
        return Token(tuple(val))
    return Token(val)


def regex(pat: str) -> Regex:
    """Create a Regex predicate."""
    return Regex(pat)


def fuzzy(val: str, threshold: float = 0.85) -> Fuzzy:
    """Create a Fuzzy match predicate."""
    return Fuzzy(val, threshold)


def has_tag(tag: str) -> HasTag:
    """Create a HasTag predicate."""
    return HasTag(tag)


def one_of(*args: str | list[str] | Predicate) -> Predicate:
    """Helper to create a chain of Or predicates from a list of tokens/predicates."""
    if not args:
        raise ValueError('one_of requires at least one argument')

    preds = []
    for a in args:
        if isinstance(a, str | list):
            preds.append(token(a))
        elif isinstance(a, Predicate):
            preds.append(a)
        else:
            raise ValueError(f'Invalid argument type for one_of: {type(a)}')

    result = preds[0]
    for p in preds[1:]:
        result = result | p
    return result
