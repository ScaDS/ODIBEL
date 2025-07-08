
def map_uri_in_triple(triple: tuple[str, str, str], uri_map: dict[str, str]) -> tuple[str, str, str]:
    s, p, o = triple
    if s in uri_map:
        s = uri_map[s]
    if o in uri_map:
        o = uri_map[o]
    return s, p, o


# use sed instead
def map_uri_ns_in_triple(triple: tuple[str, str, str], ns_map: dict[str, str]) -> tuple[str, str, str]:
    s, p, o = triple
    if s in ns_map:
        s = ns_map[s]
    if o in ns_map:
        o = ns_map[o]
    return s, p, o

def test_idns():
    pass