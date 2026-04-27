df = df.drop_duplicates(
        subset=["source_table", "source_column", "target_table", "target_column", "parent"],
        keep="first"
    ).reset_index(drop=True)



return list(seen.keys())[0] if len(seen) == 1 else " | ".join(seen.keys())
return list(seen.keys())



return " | ".join(dict.fromkeys(results)) if results else None
return list(dict.fromkeys(results)) if results else None



if not raw_logic or raw_logic in (TRANSFORM_NF, STATIC_FALLBACK):
                continue
if not raw_logic:
                continue
            raw_logics = raw_logic if isinstance(raw_logic, list) else [raw_logic]
            raw_logics = [l for l in raw_logics if l not in (TRANSFORM_NF, STATIC_FALLBACK)]
            if not raw_logics:
                continue


all_candidates.append(_CandidateMatch(
                logic      = raw_logic,
                uuid       = stmt["uuid"],
                score      = score,
                is_literal = _is_literal_expr(raw_logic),
            ))
for logic_item in raw_logics:
                all_candidates.append(_CandidateMatch(
                    logic      = logic_item,
                    uuid       = stmt["uuid"],
                    score      = score,
                    is_literal = _is_literal_expr(logic_item),
                ))



src_alias: str | None = None
            if src_tbl:
                try:
                    src_alias = _resolve_source_alias(stmt["parsed"], src_tbl)
                except Exception:
                    pass

src_alias: str | None = None
            if src_tbl:
                try:
                    src_alias = _resolve_source_alias(stmt["parsed"], src_tbl)
                except Exception:
                    pass

            # If expression references a qualified alias (T1.col, T2.col etc.)
            # verify that alias resolves to src_tbl. If src_tbl exists in the
            # statement but alias couldn't be resolved, skip this statement —
            # we cannot confirm the expression belongs to the correct source.
            if raw_logics:
                for logic_item in raw_logics:
                    qualifier = re.match(r"^([A-Za-z_][A-Za-z0-9_]*)\.", logic_item.strip())
                    if qualifier:
                        expr_alias = qualifier.group(1).upper()
                        if src_alias and expr_alias != src_alias:
                            # Expression references different table — skip
                            raw_logics = [l for l in raw_logics if not re.match(rf"^{expr_alias}\.", l.strip(), re.I)]
