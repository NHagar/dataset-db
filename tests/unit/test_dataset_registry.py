"""Tests for the persistent DatasetRegistry."""

from dataset_db.ingestion.dataset_registry import DatasetRegistry


def test_register_assigns_incrementing_ids(tmp_path):
    registry = DatasetRegistry(base_path=tmp_path)

    first = registry.register_dataset("dataset_one")
    second = registry.register_dataset("dataset_two")
    first_again = registry.register_dataset("dataset_one")

    assert first == 0
    assert second == 1
    assert first_again == first


def test_registry_persists_between_instances(tmp_path):
    registry = DatasetRegistry(base_path=tmp_path)
    registry.register_dataset("dataset_one")

    # Re-initialize to simulate a new ingestion run
    registry2 = DatasetRegistry(base_path=tmp_path)
    next_id = registry2.register_dataset("dataset_two")

    assert registry2.get_dataset_id("dataset_one") == 0
    assert next_id == 1


def test_to_dict_returns_copy(tmp_path):
    registry = DatasetRegistry(base_path=tmp_path)
    registry.register_dataset("dataset_one")
    data = registry.to_dict()

    assert data == {"dataset_one": 0}
    data["dataset_one"] = 42

    # Internal state should be unchanged
    assert registry.get_dataset_id("dataset_one") == 0
