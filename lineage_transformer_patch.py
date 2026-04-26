# For CTAS and REPLACE VIEW statements, strip the Teradata-specific
        # prefix before passing to sqlglot. This lets sqlglot parse the SELECT
        # body cleanly without choking on VOLATILE / MULTISET / LOCKING etc.
        # The original full SQL is still stored in `restored` for Dim Queries.
        _TERADATA_PREFIX = re.compile(
            r"^\s*(?:"
            # CTAS variants: CREATE [MULTISET|VOLATILE|GLOBAL TEMPORARY] TABLE t AS SELECT
            r"CREATE\s+(?:MULTISET\s+|GLOBAL\s+TEMPORARY\s+(?:MULTISET\s+)?|VOLATILE\s+)?"
            r"TABLE\s+\S+\s+AS(?=\s+SELECT\b)"
            r"|"
            # REPLACE VIEW variants: REPLACE [RECURSIVE] VIEW v [LOCKING...] [WITH [NO] DATA] AS SELECT
            r"REPLACE\s+(?:RECURSIVE\s+)?VIEW\s+\S+\s+"
            r"(?:LOCKING\s+\S+\s+(?:ACCESS|READ|WRITE|EXCLUSIVE)(?:\s+FOR\s+LOAD)?\s+)??"
            r"(?:WITH\s+(?:NO\s+)?DATA\s+)?"
            r"AS(?=\s+SELECT\b)"
            r")\s*",
            re.IGNORECASE,
        )
        parse_stmt_text = (
            _TERADATA_PREFIX.sub("", stmt_text).strip()
            if _TERADATA_PREFIX.match(stmt_text)
            else stmt_text
        )

        if len(parse_stmt_text) > MAX_STMT_LENGTH_CHARS:
            parse_ok        = False
            parse_error_msg = (
                f"Statement skipped: {len(parse_stmt_text):,} chars exceeds "
                f"MAX_STMT_LENGTH_CHARS={MAX_STMT_LENGTH_CHARS:,}"
            )
            errors.append(f"[SIZE SKIP] {Path(file_path).name} stmt {stmt_uuid[:8]}: {parse_error_msg}")
        else:
            try:
                parsed_list = _safe_parse(parse_stmt_text)   # ← stripped version
                parsed = parsed_list[0] if parsed_list else None
                if parsed is None:
                    parse_ok        = False
                    parse_error_msg = "sqlglot returned empty AST (unsupported or DDL statement)"
                elif isinstance(parsed, (exp.Drop, exp.Alter, exp.Command)):
                    parse_ok        = False
                    parse_error_msg = (f"DDL statement ({type(parsed).__name__}) "
                                       f"— skipped, not DML")
                    parsed          = None
                elif isinstance(parsed, exp.Create) and parsed.expression is None:
                    parse_ok        = False
                    parse_error_msg = "DDL CREATE TABLE (no SELECT) — skipped, not DML"
                    parsed          = None
            except TimeoutError as te:
                parse_ok        = False
                parse_error_msg = str(te)
                errors.append(f"[TIMEOUT] {Path(file_path).name} stmt {stmt_uuid[:8]}: {parse_error_msg}")
            except Exception as parse_exc:
                parse_ok        = False
                parse_error_msg = str(parse_exc)
                errors.append(f"[PARSE WARN] {Path(file_path).name} stmt {stmt_uuid[:8]}: {parse_error_msg}")