def _extract_static_target_logic(
    parsed: exp.Expression,
    tgt_col: str,
) -> list[str]:
    if not tgt_col:
        return []

    results = []

    # UPDATE SET
    for eq in parsed.find_all(exp.EQ):
        left = eq.left
        if isinstance(left, exp.Column) and left.name.upper() == tgt_col:
            results.append(_expr_to_sql(eq.right))

    # INSERT positional / named — SELECT body
    insert = parsed.find(exp.Insert)
    if insert:
        schema = insert.find(exp.Schema)
        if schema:
            ins_cols = [c.name.upper() for c in schema.find_all(exp.Column)]
            if tgt_col in ins_cols:
                idx = ins_cols.index(tgt_col)
                for sel in parsed.find_all(exp.Select):
                    exprs = sel.expressions
                    if idx < len(exprs):
                        results.append(_expr_to_sql(exprs[idx]))

    # SELECT alias — all UNION branches
    if not results:
        for sel in parsed.find_all(exp.Select):
            for proj in sel.expressions:
                if isinstance(proj, exp.Alias) and proj.alias.upper() == tgt_col:
                    results.append(_expr_to_sql(proj.this))

    return list(dict.fromkeys(r for r in results if r))



if is_static:
                    raw_logics = _extract_static_target_logic(stmt["parsed"], tgt_col)