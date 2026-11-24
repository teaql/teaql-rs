#!/usr/bin/env python3
"""Run a native generated-workspace adapter against the shared i18n scenarios."""

import argparse
import json
from pathlib import Path
import subprocess
import sys
from tempfile import TemporaryDirectory


PREFIX = "TEAQL_I18N_SHOWCASE "
HERE = Path(__file__).resolve().parent


def read_json(path):
    return json.loads(Path(path).read_text(encoding="utf-8"))


class Scenario:
    def __init__(self, model):
        self.model = model
        self.steps = []

    def select(self, code, effective, *, context="main", accepted=True):
        self.steps.append((
            {"operation": "select", "context": context, "locale": code},
            {"type": "locale", "context": context, "requestedLocale": code,
             "effectiveLocale": effective, "accepted": accepted},
        ))

    def validate(self, effective, *, context="main", phase="ordinary"):
        for case in self.model["cases"]:
            expected = {"type": "validation", "context": context, "case": case["id"],
                        **case["error"], "phase": phase}
            if effective is not None:
                expected["effectiveLocale"] = effective
            self.steps.append((
                {"operation": "validate", "context": context, "case": case["id"],
                 "input": case["input"]},
                expected,
            ))

    def edges(self, catalog_file):
        config = self.model["localeCases"]
        self.validate(self.model["defaultLocale"], phase="default")
        alias = config["alias"]
        self.select(alias["input"], alias["effectiveLocale"])
        self.validate(alias["effectiveLocale"], phase="alias")
        for invalid in config["unsupported"]:
            self.select(invalid, alias["effectiveLocale"], accepted=False)
        other = config["independentContextLocale"]
        self.select(other, other, context="independent")
        for index, locale in enumerate(config["switchSequence"]):
            self.select(locale, locale)
            self.validate(locale, phase=f"switch-{index + 1}")
            self.validate(other, context="independent", phase=f"isolation-{index + 1}")
        missing = config["missingTranslationLocale"]
        catalog = read_json(catalog_file)
        keys = [case["error"]["messageKey"] for case in self.model["cases"]]
        if any(key in catalog["locales"][missing]["messages"] for key in keys):
            raise ValueError(f"The application catalog must omit the example messages in {missing}")
        self.select(missing, missing)
        self.validate(missing, phase="builtin-before-install")
        self.steps.append((
            {"operation": "install-catalog", "context": "main", "path": str(catalog_file)},
            {"type": "catalog-installed", "context": "main", "effectiveLocale": missing},
        ))
        self.validate(missing, phase="missing-application-translation")


def compare(expected, observed):
    phase = expected.get("phase")
    if phase:
        observed["phase"] = phase
    for key, value in expected.items():
        if observed.get(key) != value:
            raise ValueError(f"Unexpected {key}: expected {expected}, received {observed}")
    if expected["type"] == "locale":
        if type(observed.get("accepted")) is not bool or not observed.get("effectiveLocale"):
            raise ValueError(f"Missing native locale selection outcome: {observed}")
        if not observed["accepted"] and not observed.get("nativeError"):
            raise ValueError(f"Missing native locale rejection: {observed}")
    if expected["type"] == "validation":
        if not observed.get("nativeRule") or not observed.get("message", "").strip():
            raise ValueError(f"Missing native rule or localized text: {observed}")


def execute(command, scenario, database, run_number):
    payload = {
        "database": str(database), "warehouse": scenario.model["warehouse"],
        "steps": [request for request, _ in scenario.steps],
    }
    result = subprocess.run(command, input=json.dumps(payload, ensure_ascii=False), encoding="utf-8",
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=90)
    if result.returncode:
        print(result.stderr, file=sys.stderr, end="")
        print(result.stdout, file=sys.stderr, end="")
        return result.returncode
    observations = [json.loads(line[len(PREFIX):]) for line in result.stdout.splitlines()
                    if line.startswith(PREFIX)]
    if len(observations) != len(scenario.steps):
        raise ValueError(f"Expected {len(scenario.steps)} native events, received {len(observations)}")
    current_locale = {}
    rejected_single_selection = False
    for (_, expected), observed in zip(scenario.steps, observations):
        if expected["type"] == "validation" and "effectiveLocale" not in expected:
            expected = {**expected, "effectiveLocale": current_locale[expected["context"]]}
        compare(expected, observed)
        if expected["type"] == "locale":
            current_locale[observed["context"]] = observed["effectiveLocale"]
            rejected_single_selection |= not observed["accepted"] and "accepted" not in expected
        observed["round"] = run_number
        print(json.dumps(observed, ensure_ascii=False, sort_keys=True))
    by_phase = {(row.get("phase"), row.get("case")): row for row in observations}
    for case in scenario.model["cases"]:
        before = by_phase.get(("builtin-before-install", case["id"]))
        after = by_phase.get(("missing-application-translation", case["id"]))
        if before and after and before["message"] != after["message"]:
            raise ValueError(f"Missing application translation changed the built-in result: {after}")
        for phases in (("switch-1", "switch-3"), ("isolation-1", "isolation-2", "isolation-3")):
            messages = {by_phase[phase, case["id"]]["message"] for phase in phases
                        if (phase, case["id"]) in by_phase}
            if len(messages) > 1:
                raise ValueError(f"Locale state leaked across {phases} for {case['id']}")
    return 2 if rejected_single_selection else 0


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--locale", help="set the locale through the native UserContext")
    mode.add_argument("--all", action="store_true", help="exercise all 15 locales in one native process")
    mode.add_argument("--edge-cases", action="store_true", help="verify default, rejection, isolation and fallback")
    parser.add_argument("--catalog", type=Path, help="generated teaql-i18n.json for --edge-cases")
    parser.add_argument("--repeat", type=int, choices=(1, 2), default=1,
                        help="run twice against the same SQLite file")
    parser.add_argument("command", nargs=argparse.REMAINDER, help="native command after --")
    args = parser.parse_args()
    command = args.command[1:] if args.command[:1] == ["--"] else args.command
    if not command:
        parser.error("a native workspace command is required after --")
    model = read_json(HERE / "semantics.json")
    scenario = Scenario(model)
    if args.edge_cases:
        if not args.catalog:
            parser.error("--edge-cases requires --catalog")
        scenario.edges(args.catalog.resolve(strict=True))
    elif args.all:
        for definition in model["locales"]:
            code = definition["code"]
            scenario.select(code, code)
            scenario.validate(code)
    elif args.locale is not None:
        # Single-language mode accepts aliases and reports the actual normalized locale.
        scenario.steps.append((
            {"operation": "select", "context": "main", "locale": args.locale},
            {"type": "locale", "context": "main", "requestedLocale": args.locale},
        ))
        scenario.validate(None)
    else:
        scenario.validate(model["defaultLocale"])
    scenario.steps.append((
        {"operation": "verify-unchanged", "context": "main"},
        {"type": "storage", "context": "main", "supplierCount": 0},
    ))
    with TemporaryDirectory(prefix="teaql-i18n-supplier-") as directory:
        database = Path(directory) / "registration.sqlite"
        for run_number in range(1, args.repeat + 1):
            status = execute(command, scenario, database, run_number)
            if status:
                return status
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
