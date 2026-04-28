try:
                if is_static:
                    raw_logics = _extract_static_target_logic(stmt["parsed"], tgt_col)
                    if isinstance(raw_logics, str):
                        raw_logics = [raw_logics] if raw_logics not in (STATIC_FALLBACK, "") else []
                    elif not isinstance(raw_logics, list):
                        raw_logics = []
                else:
                    raw_logics = extract_transformation(
                        stmt["parsed"], src_col, tgt_col, False, src_alias
                    )
                    if isinstance(raw_logics, str):
                        raw_logics = [raw_logics] if raw_logics not in (TRANSFORM_NF, "") else []
                    elif not isinstance(raw_logics, list):
                        raw_logics = []
            except Exception as ex:
                errors.append(
                    f"[EXTRACT WARN] {Path(file_path).name} "
                    f"pair {src_col}->{tgt_col}: {ex}"
                )
                continue
