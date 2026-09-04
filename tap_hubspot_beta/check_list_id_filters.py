"""Self-check for list id filter parsing. Run: python -m tap_hubspot_beta.check_list_id_filters"""

from tap_hubspot_beta.utils import ids_from_config, ids_from_filter_clause, parse_selected_id_label


def main() -> None:
    assert parse_selected_id_label("9") == "9"
    assert parse_selected_id_label("Hassan's List (9)") == "9"
    assert parse_selected_id_label(None) is None

    assert ids_from_filter_clause({"operator": "EQ", "value": "9"}) == frozenset({"9"})
    assert ids_from_filter_clause(
        {"operator": "IN", "value": ["9", "Special (11)"]}
    ) == frozenset({"9", "11"})
    assert ids_from_config(None) is None
    assert ids_from_config([]) is None
    assert ids_from_config([9, "10"]) == frozenset({"9", "10"})
    print("ok")


if __name__ == "__main__":
    main()
