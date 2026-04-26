all_candidates: list[_CandidateMatch] = []

        for stmt in parseable:
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
                    raw_logic = _extract_static_target_logic(stmt["parsed"], tgt_col)
                else:
                    raw_logic = extract_transformation(
                        stmt["parsed"], src_col, tgt_col, False, src_alias
                    )
            except Exception as ex:
                errors.append(f"[EXTRACT WARN] {Path(file_path).name} pair {src_col}->{tgt_col}: {ex}")
                continue

            if not raw_logic or raw_logic in (TRANSFORM_NF, STATIC_FALLBACK):
                continue

            src_confirmed = _expr_refs_source(raw_logic, src_tbl, src_alias, is_static)
            score = 3 if (tgt_match and src_confirmed) else (2 if tgt_match else 1)

            all_candidates.append(_CandidateMatch(
                logic=raw_logic, uuid=stmt["uuid"],
                score=score, is_literal=_is_literal_expr(raw_logic),
            ))

        # ── Select best candidate(s) ──────────────────────────────────────────
        found_logic = TRANSFORM_NF
        found_uuid  = default_uuid

        if all_candidates:
            best_score = max(c.score for c in all_candidates)
            top = [c for c in all_candidates if c.score == best_score]
            if is_static:
                lit_top = [c for c in top if c.is_literal]
                top = lit_top or top

            seen: dict[str, str] = {}
            for c in top:
                if c.logic not in seen:
                    seen[c.logic] = c.uuid

            if len(seen) == 1:
                found_logic, found_uuid = next(iter(seen.items()))
                results.append({**pair, "transformation_logic": found_logic, "query_uuid": found_uuid})
                continue
            else:
                for logic_val, uuid_val in seen.items():
                    results.append({**pair, "transformation_logic": logic_val, "query_uuid": uuid_val})
                continue

        # ── Post-scan outcome (only reached when all_candidates is empty) ─────
        if found_logic == TRANSFORM_NF:
            if is_static:
                static_lit_logic = ""
                static_lit_uuid  = default_uuid
                for stmt in parseable:
                    try:
                        expr = _extract_static_target_logic(stmt["parsed"], tgt_col)
                        if expr and expr != STATIC_FALLBACK:
                            static_lit_logic = expr
                            static_lit_uuid  = stmt["uuid"]
                            if _is_literal_expr(expr):
                                break
                    except Exception:
                        pass
                if static_lit_logic:
                    found_logic = static_lit_logic
                    found_uuid  = static_lit_uuid
                else:
                    found_logic = STATIC_FALLBACK
            elif failed_stmts:
                found_logic = PARSE_FAILED
                found_uuid  = failed_stmts[0]["uuid"]

        results.append({**pair, "transformation_logic": found_logic, "query_uuid": found_uuid})
