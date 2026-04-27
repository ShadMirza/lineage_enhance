def extract_transformation(
    parsed: exp.Expression,
    src_col: str,
    tgt_col: str,
    is_static: bool,
    src_alias: str | None = None,
) -> list[str]:
    """Returns list of transformation logic strings (empty list if not found)."""
    if is_static:
        result = _extract_static_target_logic(parsed, tgt_col)
        return [result] if result and result != STATIC_FALLBACK else []

    results: list[str] = []

    if parsed.find(exp.Merge):
        r = _extract_from_merge(parsed, src_col, tgt_col)
        if r:
            results = r if isinstance(r, list) else [r]

    if not results and parsed.find(exp.Update):
        r = _extract_from_update(parsed, src_col, tgt_col)
        if r:
            results = r if isinstance(r, list) else [r]

    if not results and parsed.find(exp.Insert):
        r = _extract_from_insert(parsed, src_col, tgt_col)
        if r:
            results = r if isinstance(r, list) else [r]

    if not results:
        r = _extract_from_select(parsed, src_col, tgt_col)
        if r:
            results = r if isinstance(r, list) else [r]

    # Apply pass-through check on each result
    final = []
    for logic in results:
        if _is_passthrough(logic, src_col, src_alias):
            final.append(DIRECT_PASSTHRU)
        else:
            final.append(logic)

    return list(dict.fromkeys(final))  # deduplicate preserving order










for stmt in parseable:
            if stmt["parsed"] is None:
                continue

            stmt_tgt = _extract_stmt_target_table(stmt["parsed"])
            if not tgt_tbl:
                tgt_match = True
            elif not stmt_tgt:
                tgt_match = True
            else:
                tgt_match = _table_matches(tgt_tbl, stmt_tgt)

            src_alias: str | None = None
            if src_tbl:
                try:
                    src_alias = _resolve_source_alias(stmt["parsed"], src_tbl)
                except Exception:
                    pass

            try:
                if is_static:
                    raw_logics = _extract_static_target_logic(stmt["parsed"], tgt_col)
                    raw_logics = [raw_logics] if raw_logics and raw_logics != STATIC_FALLBACK else []
                else:
                    raw_logics = extract_transformation(
                        stmt["parsed"], src_col, tgt_col, False, src_alias
                    )
                    if isinstance(raw_logics, str):
                        raw_logics = [raw_logics] if raw_logics not in (TRANSFORM_NF, "") else []
            except Exception as ex:
                errors.append(
                    f"[EXTRACT WARN] {Path(file_path).name} "
                    f"pair {src_col}->{tgt_col}: {ex}"
                )
                continue

            if not raw_logics:
                continue

            # Alias verification — reject expressions from wrong source table
            raw_logics_verified = []
            for logic_item in raw_logics:
                if logic_item in (TRANSFORM_NF, STATIC_FALLBACK, ""):
                    continue
                qualifier = re.match(r"^([A-Za-z_][A-Za-z0-9_]*)\.", logic_item.strip(), re.I)
                if qualifier:
                    expr_alias = qualifier.group(1).upper()
                    if src_alias and expr_alias == src_alias:
                        raw_logics_verified.append(logic_item)
                    elif src_tbl and expr_alias == src_tbl.upper():
                        raw_logics_verified.append(logic_item)
                    elif logic_item == DIRECT_PASSTHRU:
                        raw_logics_verified.append(logic_item)
                else:
                    raw_logics_verified.append(logic_item)  # bare column — accept

            raw_logics = raw_logics_verified
            if not raw_logics:
                continue

            src_confirmed = all(
                _expr_refs_source(l, src_tbl, src_alias, is_static)
                for l in raw_logics
            )

            if tgt_match and src_confirmed:
                score = 3
            elif tgt_match:
                score = 2
            else:
                score = 1

            for logic_item in raw_logics:
                all_candidates.append(_CandidateMatch(
                    logic      = logic_item,
                    uuid       = stmt["uuid"],
                    score      = score,
                    is_literal = _is_literal_expr(logic_item),
                ))
