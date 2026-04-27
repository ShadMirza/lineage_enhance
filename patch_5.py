def _extract_static_target_logic(
    parsed: exp.Expression,
    tgt_col: str,
) -> list[str]:
    if not tgt_col:
        return []

    results = []

    # UPDATE SET — collect all matching assignments
    for eq in parsed.find_all(exp.EQ):
        left = eq.left
        if isinstance(left, exp.Column) and left.name.upper() == tgt_col:
            results.append(_expr_to_sql(eq.right))

    # INSERT positional / named — Case A: SELECT body
    insert = parsed.find(exp.Insert)
    if insert:
        schema = insert.find(exp.Schema)
        if schema:
            ins_cols = [c.name.upper() for c in schema.find_all(exp.Column)]
            if tgt_col in ins_cols:
                idx = ins_cols.index(tgt_col)
                # Walk ALL Select nodes (covers UNION ALL branches)
                for sel in parsed.find_all(exp.Select):
                    exprs = sel.expressions
                    if idx < len(exprs):
                        results.append(_expr_to_sql(exprs[idx]))

            # Case B: INSERT INTO tbl (cols) VALUES (literals)
            values_node = insert.find(exp.Values) or insert.find(exp.Tuple)
            if values_node and tgt_col in ins_cols:
                idx = ins_cols.index(tgt_col)
                if isinstance(values_node, exp.Values):
                    rows = values_node.expressions
                    if rows and isinstance(rows[0], exp.Tuple):
                        row_exprs = rows[0].expressions
                        if idx < len(row_exprs):
                            results.append(_expr_to_sql(row_exprs[idx]))
                else:
                    row_exprs = values_node.expressions
                    if idx < len(row_exprs):
                        results.append(_expr_to_sql(row_exprs[idx]))

    # MERGE — check all WHEN branches
    for eq in parsed.find_all(exp.EQ):
        left = eq.left
        if isinstance(left, exp.Column) and left.name.upper() == tgt_col:
            val = _expr_to_sql(eq.right)
            results.append(val)

    # SELECT alias — all UNION branches
    for sel in parsed.find_all(exp.Select):
        for proj in sel.expressions:
            if isinstance(proj, exp.Alias) and proj.alias.upper() == tgt_col:
                results.append(_expr_to_sql(proj.this))

    return list(dict.fromkeys(r for r in results if r))



if is_static:
                    raw_logics = _extract_static_target_logic(stmt["parsed"], tgt_col)
