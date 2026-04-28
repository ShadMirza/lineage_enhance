raw_logics_verified = []
            for logic_item in raw_logics:
                if logic_item in (TRANSFORM_NF, STATIC_FALLBACK, ""):
                    continue
                if logic_item == DIRECT_PASSTHRU:
                    raw_logics_verified.append(logic_item)
                    continue
                qualifier = re.match(r"^([A-Za-z_][A-Za-z0-9_]*)\.", logic_item.strip(), re.I)
                if qualifier:
                    expr_alias = qualifier.group(1).upper()
                    if src_alias and expr_alias == src_alias:
                        raw_logics_verified.append(logic_item)  # confirmed ✓
                    elif src_tbl and expr_alias == src_tbl.upper():
                        raw_logics_verified.append(logic_item)  # confirmed ✓
                    elif not src_alias:
                        # src_alias unknown — check if expr_alias belongs to
                        # a DIFFERENT table in this statement. If it does, reject.
                        try:
                            for tbl_node in stmt["parsed"].find_all(exp.Table):
                                tbl_alias = str(tbl_node.alias).strip().upper() if tbl_node.alias else ""
                                tbl_name  = _bare_table(tbl_node.name or "")
                                if (tbl_alias == expr_alias or tbl_name == expr_alias) \
                                        and tbl_name != src_tbl:
                                    break  # expr_alias belongs to different table — reject
                            else:
                                raw_logics_verified.append(logic_item)  # no conflicting table found
                        except Exception:
                            raw_logics_verified.append(logic_item)  # can't determine — accept
                    # else: src_alias known but doesn't match — reject
                else:
                    raw_logics_verified.append(logic_item)  # bare column — accept
