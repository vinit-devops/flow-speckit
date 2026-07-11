from uuid import UUID, uuid4

from flow_speckit.artifacts.refs import parse_ref


def test_parse_ref_uuid() -> None:
    uid = uuid4()
    assert parse_ref(str(uid)) == uid
    assert isinstance(parse_ref(str(uid)), UUID)


def test_parse_ref_key_with_version() -> None:
    assert parse_ref("foo@5") == ("foo", 5)


def test_parse_ref_bare_key() -> None:
    assert parse_ref("foo") == ("foo", None)


def test_parse_ref_last_at_wins() -> None:
    assert parse_ref("a@b@3") == ("a@b", 3)


def test_parse_ref_empty_key_edge_case() -> None:
    # Locked-in current behavior: an empty key before "@" is returned verbatim.
    assert parse_ref("@5") == ("", 5)


def test_parse_ref_trailing_at_edge_case() -> None:
    # Locked-in current behavior: a non-numeric suffix means the whole input
    # (including the trailing "@") is treated as the key.
    assert parse_ref("foo@") == ("foo@", None)


def test_parse_ref_non_numeric_version() -> None:
    assert parse_ref("foo@bar") == ("foo@bar", None)
