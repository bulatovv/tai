from collections import defaultdict
from dataclasses import dataclass, field
from difflib import SequenceMatcher

from .dsl import And, Fuzzy, HasTag, Not, Or, Predicate, Regex, Token
from .utils import normalize_name


@dataclass
class DatalogRule:
    """Represents a simplified Horn clause: Head :- Atom1, Atom2, ..., ~Neg1, ~Neg2..."""

    head_tag: str

    # Positive Atoms
    conditions: list[Token | Regex | Fuzzy] = field(default_factory=list)
    dependencies: list[str] = field(default_factory=list)

    # Negative Atoms
    neg_conditions: list[Token | Regex | Fuzzy] = field(default_factory=list)
    neg_dependencies: list[str] = field(default_factory=list)


class InferenceEngine:
    """Compiles DSL rules into Datalog-style rules and executes them."""

    def __init__(self, rules_map: dict[str, Predicate]):
        """Initialize the engine with a rule map."""
        self.rules: list[DatalogRule] = []
        self._compile(rules_map)
        self.stratified_rules = self._stratify_rules()

    def _to_dnf(self, pred: Predicate) -> list[list[Predicate | Not]]:
        """
        Converts a predicate AST into Disjunctive Normal Form.

        Handles negations by pushing them down (De Morgan's).
        """
        match pred:
            case Token() | Regex() | HasTag() | Fuzzy():
                return [[pred]]

            case Not(operand):
                match operand:
                    case Token() | Regex() | HasTag() | Fuzzy():
                        return [[Not(operand)]]
                    case Not(inner):
                        return self._to_dnf(inner)
                    case Or(left, right):
                        return self._distribute_and(
                            self._to_dnf(Not(left)), self._to_dnf(Not(right))
                        )
                    case And(left, right):
                        return self._to_dnf(Not(left)) + self._to_dnf(Not(right))
                    case _:
                        raise ValueError(f'Unknown negation operand: {type(operand)}')

            case Or(left, right):
                return self._to_dnf(left) + self._to_dnf(right)

            case And(left, right):
                return self._distribute_and(self._to_dnf(left), self._to_dnf(right))

            case _:
                raise ValueError(f'Unknown predicate type: {type(pred)}')

    def _distribute_and(
        self, left_dnf: list[list[Predicate]], right_dnf: list[list[Predicate]]
    ) -> list[list[Predicate]]:
        """Distributes AND over OR: (A|B) & (C|D) -> AC | AD | BC | BD."""
        combined = []
        for l_conj in left_dnf:
            for r_conj in right_dnf:
                combined.append(l_conj + r_conj)
        return combined

    def _compile(self, rules_map: dict[str, Predicate]):
        """Compiles the DSL rules into flat DatalogRules."""
        for head, predicate in rules_map.items():
            dnf = self._to_dnf(predicate)

            for conjunction in dnf:
                rule = DatalogRule(head_tag=head)

                for atom in conjunction:
                    if isinstance(atom, Not):
                        inner = atom.operand
                        if isinstance(inner, Token | Regex | Fuzzy):
                            rule.neg_conditions.append(inner)
                        elif isinstance(inner, HasTag):
                            rule.neg_dependencies.append(inner.tag_name)
                    else:
                        if isinstance(atom, Token | Regex | Fuzzy):
                            rule.conditions.append(atom)
                        elif isinstance(atom, HasTag):
                            rule.dependencies.append(atom.tag_name)

                self.rules.append(rule)

    def _stratify_rules(self) -> list[list[DatalogRule]]:
        """
        Organize rules into strata based on dependencies.

        Stratum 0: No dependencies or only positive dependencies on Stratum 0.
        Stratum N: Can depend negatively on Stratum < N.
        """
        # Calculate stratum for each tag
        strata = defaultdict(int)

        # Fixed-point iteration to propagate stratum levels
        changed = True
        while changed:
            changed = False
            for rule in self.rules:
                head = rule.head_tag
                current_level = strata[head]

                # Max stratum of positive dependencies (same layer or lower)
                for dep in rule.dependencies:
                    if strata[dep] > current_level:
                        strata[head] = strata[dep]
                        current_level = strata[head]
                        changed = True

                # Max stratum of negative dependencies (MUST be strictly lower -> so current must be higher)
                for neg_dep in rule.neg_dependencies:
                    if strata[neg_dep] >= current_level:
                        strata[head] = strata[neg_dep] + 1
                        current_level = strata[head]
                        changed = True

        # Group rules by stratum
        max_stratum = max(strata.values(), default=0)
        stratified = [[] for _ in range(max_stratum + 1)]

        for rule in self.rules:
            s = strata[rule.head_tag]
            stratified[s].append(rule)

        return stratified

    def solve(
        self, world_name: str, return_metadata: bool = False
    ) -> tuple[list[str], dict] | list[str]:
        """Execute the inference engine for a given world name."""
        if not world_name:
            return ([], {}) if return_metadata else []

        normalized = normalize_name(world_name)
        normalized_tokens = normalized.split()

        known_tags: set[str] = set()
        metadata: dict[str, dict] = {}
        condition_cache: dict[Token | Regex | Fuzzy, str | None] = {}

        def check_condition(cond: Token | Regex | Fuzzy) -> str | None:
            if cond in condition_cache:
                return condition_cache[cond]

            res = None
            if isinstance(cond, Token):
                res = cond.matches(normalized)
            elif isinstance(cond, Regex):
                res = cond.matches(world_name)
            elif isinstance(cond, Fuzzy):
                for word in normalized_tokens:
                    if SequenceMatcher(None, word, cond.value).ratio() >= cond.threshold:
                        res = word
                        break

            condition_cache[cond] = res
            return res

        # Solve stratum by stratum
        for stratum_rules in self.stratified_rules:
            # Fixed point for current stratum
            while True:
                new_info = False

                for rule in stratum_rules:
                    if rule.head_tag in known_tags:
                        continue

                    # 1. Check dependencies (Pos & Neg)
                    deps_met = True
                    inference_source = None

                    for dep in rule.dependencies:
                        if dep not in known_tags:
                            deps_met = False
                            break
                        if inference_source is None:
                            inference_source = dep
                    if not deps_met:
                        continue

                    for dep in rule.neg_dependencies:
                        if dep in known_tags:
                            deps_met = False
                            break
                    if not deps_met:
                        continue

                    # 2. Check conditions (Pos & Neg)
                    conds_met = True
                    match_source = None

                    for cond in rule.conditions:
                        res = check_condition(cond)
                        if res is None:
                            conds_met = False
                            break
                        if match_source is None:
                            match_source = res
                    if not conds_met:
                        continue

                    for cond in rule.neg_conditions:
                        if check_condition(cond) is not None:
                            conds_met = False
                            break
                    if not conds_met:
                        continue

                    # Rule fired!
                    known_tags.add(rule.head_tag)
                    new_info = True

                    if return_metadata:
                        meta_entry = {}
                        if match_source:
                            meta_entry['match'] = match_source
                        if inference_source:
                            meta_entry['inference'] = inference_source
                        metadata[rule.head_tag] = meta_entry

                if not new_info:
                    break

        sorted_tags = sorted(list(known_tags))
        return (sorted_tags, metadata) if return_metadata else sorted_tags
