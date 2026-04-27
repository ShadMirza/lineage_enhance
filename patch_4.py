src_alias: str | None = None
            if src_tbl:
                try:
                    src_alias = _resolve_source_alias(stmt["parsed"], src_tbl)
                except Exception:
                    pass

            raw_logics_verified = []
            for logic_item in raw_logics:
                qualifier = re.match(r"^([A-Za-z_][A-Za-z0-9_]*)\.", logic_item.strip(), re.I)
                if qualifier:
                    expr_alias = qualifier.group(1).upper()
                    # Expression has qualifier — verify it matches src_alias or src_tbl
                    if src_alias and expr_alias == src_alias:
                        raw_logics_verified.append(logic_item)  # confirmed ✓
                    elif src_tbl and expr_alias == src_tbl.upper():
                        raw_logics_verified.append(logic_item)  # confirmed ✓
                    else:
                        pass  # alias doesn't match src — reject
                else:
                    raw_logics_verified.append(logic_item)  # bare col — accept
            raw_logics = raw_logics_verified
            if not raw_logics:
                continue