def _is_passthrough(expr_sql: str, src_col: str, src_alias: str | None = None) -> bool:
    bare = _unqualified(expr_sql).upper()
    if bare != src_col.upper():
        return False
    # If alias known, verify qualifier matches alias
    qualifier = re.match(r"^([A-Za-z_][A-Za-z0-9_]*)\.", expr_sql.strip(), re.I)
    if qualifier and src_alias:
        return qualifier.group(1).upper() == src_alias.upper()
    return True