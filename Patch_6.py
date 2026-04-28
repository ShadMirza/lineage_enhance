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

            # Resolve alias for source_table in THIS statement
            src_alias: str | None = None
            if src_tbl:
                try:
                    src_alias = _resolve_source_alias(stmt["parsed"], src_tbl)
                except Exception:
                    pass

            # Extract the expression for this (src_col, tgt_col) pair
            try:
                if is_static:
                    raw_logics = _extract_static_target_logic(stmt["parsed"], tgt_col)
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
            # Only runs AFTER extraction so we have actual logic to verify
            raw_logics_verified = []
            for logic_item in raw_logics:
                if logic_item in (TRANSFORM_NF, STATIC_FALLBACK, ""):
                    continue
                if logic_item == DIRECT_PASSTHRU:
                    raw_logics_verified.append(logic_item)
                    continue
                qualifier = re.match(r"^([A-Za-z_][A-Za-z0-9_]*)\.", logic_item.strip(), re.I)
                if qualifier and src_alias:
                    expr_alias = qualifier.group(1).upper()
                    if expr_alias == src_alias or (src_tbl and expr_alias == src_tbl.upper()):
                        raw_logics_verified.append(logic_item)
                    # else: known wrong alias — reject
                else:
                    # No qualifier OR src_alias unknown — accept
                    raw_logics_verified.append(logic_item)

            raw_logics = raw_logics_verified
            if not raw_logics:
                continue

            src_confirmed = all(
                _expr_refs_source(l, src_tbl, src_alias, is_static)
                for l in raw_logics
            ) if raw_logics else False

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
