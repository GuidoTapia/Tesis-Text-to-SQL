from core.verifier.static import verify_sql


SCHEMA = {
    "singer": ["singer_id", "name", "country", "song_name"],
    "concert": ["concert_id", "concert_name", "year"],
}


def test_valid_query() -> None:
    sql = "SELECT count(*) FROM singer"
    assert verify_sql(sql, SCHEMA) == []


def test_unknown_table() -> None:
    sql = "SELECT * FROM musicians"
    errors = verify_sql(sql, SCHEMA)
    assert any("musicians" in e for e in errors)


def test_unknown_column() -> None:
    sql = "SELECT age FROM singer"
    errors = verify_sql(sql, SCHEMA)
    assert any("age" in e for e in errors)
