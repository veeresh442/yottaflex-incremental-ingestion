from __future__ import annotations

from yottaflex_ingestion.generator import generate_events


def test_generator_is_reproducible_and_keeps_expected_labels_out_of_input(tmp_path):
    first_path = tmp_path / "first.csv"
    second_path = tmp_path / "second.csv"

    first_count = generate_events(first_path, record_count=20, seed=42)
    second_count = generate_events(second_path, record_count=20, seed=42)

    assert first_count == second_count == 26
    assert first_path.read_bytes() == second_path.read_bytes()
    assert b"edge-" not in first_path.read_bytes()
