def _extract_from_insert(
    parsed: exp.Expression,
    src_col: str,
    tgt_col: str,
) -> list[str] | str | None:
    insert = parsed.find(exp.Insert)
    if insert is None:
        return None

    tgt_columns: list[str] = []
    schema = insert.find(exp.Schema)
    if schema:
        for col in schema.find_all(exp.Column):
            tgt_columns.append(col.name.upper())

    sel = insert.find(exp.Select)
    if sel is None:
        return None

    select_exprs = sel.expressions

    if tgt_columns:
        if tgt_col not in tgt_columns:
            return None
        idx = tgt_columns.index(tgt_col)
        if idx >= len(select_exprs):
            return None
        logic = _expr_to_sql(select_exprs[idx])
        return logic
    else:
        return _extract_from_select(parsed, src_col, tgt_col)
    

def _extract_from_select(
    parsed: exp.Expression,
    src_col: str,
    tgt_col: str,
) -> list[str]:
    results: list[str] = []

    for sel in parsed.find_all(exp.Select):
        for projection in sel.expressions:
            alias: str | None = None
            if isinstance(projection, exp.Alias):
                alias = projection.alias.upper()
                inner = projection.this
            else:
                inner = projection
                if isinstance(inner, exp.Column):
                    alias = inner.name.upper() if inner.name else None
                elif isinstance(inner, exp.Star):
                    alias = "*"
                else:
                    alias = None

            if alias and alias == tgt_col:
                logic = _expr_to_sql(inner)
                results.append(logic)

    return list(dict.fromkeys(results))  # deduplicate preserving order


def _extract_from_merge(
    parsed: exp.Expression,
    src_col: str,
    tgt_col: str,
) -> list[str]:
    merge = parsed.find(exp.Merge)
    if merge is None:
        return []

    results: list[str] = []

    for when in parsed.find_all(exp.When):
        for eq in when.find_all(exp.EQ):
            left = eq.left
            if isinstance(left, exp.Column) and left.name.upper() == tgt_col:
                logic = _expr_to_sql(eq.right)
                results.append(logic)

        schema = when.find(exp.Schema)
        values_node = when.find(exp.Tuple)

        if schema and values_node:
            ins_cols = [c.name.upper() for c in schema.find_all(exp.Column)]
            ins_vals = list(values_node.expressions)
            if tgt_col in ins_cols:
                idx = ins_cols.index(tgt_col)
                if idx < len(ins_vals):
                    logic = _expr_to_sql(ins_vals[idx])
                    results.append(logic)

    return list(dict.fromkeys(results))


def _extract_from_update(
    parsed: exp.Expression,
    src_col: str,
    tgt_col: str,
) -> list[str]:
    update = parsed.find(exp.Update)
    if update is None:
        return []

    results: list[str] = []

    for eq in parsed.find_all(exp.EQ):
        left = eq.left
        if isinstance(left, exp.Column) and left.name.upper() == tgt_col:
            logic = _expr_to_sql(eq.right)
            results.append(logic)

    return list(dict.fromkeys(results))


