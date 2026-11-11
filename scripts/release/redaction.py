"""Sanitize buffered release output before it reaches logs or the console.

secret_forms() derives literal and serialized spellings from credential variables.
redact() removes those forms and structured credential fields from complete byte
streams, including mixed escapes and overlapping matches. Literal case is retained;
encoding syntax accepts the spellings covered by test_capture.py.

Matching uses original byte offsets and bounded state to handle dense near matches.
This module does no I/O. The caller caps capture size and withholds output if
sanitization fails.
"""

import base64
import html
import json
import re
import shlex
import urllib.parse


def secret_forms(env):
    values = {
        v
        for k, v in env.items()
        if v and re.search(r"PASSWORD|PASSPHRASE|TOKEN|SECRET|PRIVATE_KEY|AUTHORIZATION", k, re.I)
    }
    user, password = env.get("SONATYPE_TOKEN_USERNAME"), env.get("SONATYPE_TOKEN_PASSWORD")
    if user and password:
        values.add(user + ":" + password)
    # Include multiline components, escaped newlines, URL and both Base64 alphabets.
    values |= {line for value in values for line in value.splitlines() if line}
    values |= {value.replace("\n", "\\n").replace("\r", "\\r") for value in values}
    # JSON/logging serializers also escape quotes, backslashes and Unicode secrets.
    values |= {json.dumps(value, ensure_ascii=ascii_only)[1:-1]
               for value in tuple(values) for ascii_only in (True, False)}
    # Maven XML serializers escape punctuation and may use named or numeric quotes.
    for value in tuple(values):
        escaped = html.escape(value, quote=False)
        for apostrophe in ("'", "&apos;", "&#39;", "&#x27;"):
            for quote in ('"', "&quot;", "&#34;", "&#x22;"):
                values.add(escaped.replace("'", apostrophe).replace('"', quote))
    forms = set(values)
    for value in values:
        raw = value.encode()
        forms.update(
            (
                base64.b64encode(raw).decode(),
                base64.urlsafe_b64encode(raw).decode(),
                urllib.parse.quote(value, safe=""),
                urllib.parse.quote(value),
                urllib.parse.quote_plus(value, safe=""),
            )
        )
    controls = {"\a": r"\a", "\b": r"\b", "\x1b": r"\E", "\f": r"\f",
                "\n": r"\n", "\r": r"\r", "\t": r"\t", "\v": r"\v"}
    for value in tuple(forms):
        forms.add(urllib.parse.quote(value, safe=""))
        forms.add(urllib.parse.quote(value))
        forms.add(value.rstrip("="))
        # POSIX quoting may split apostrophes, or escape special characters
        # inside double quotes. Retain these spellings for each derived value.
        if "'" in value:
            forms.add(shlex.quote(value))
        forms.add(re.sub(r'([\\"$`])', r'\\\1', value))
        # Ordinary shell words escape metacharacters with a backslash. Keep a
        # whole literal form so repeated backslashes cannot cause backtracking.
        shell = re.sub(r'''([ \\"'$`&;|<>(){}\[\]*?!,^])''', r'\\\1', value)
        forms.add("\\" + shell if shell.startswith(("#", "~")) else shell)
        # Bash uses ANSI-C spelling for controls, retaining printable Unicode.
        forms.add("".join(
            controls.get(char, rf"\{ord(char):03o}") if ord(char) < 32 or ord(char) == 127
            else "\\" + char if char in "\\'" else char
            for char in value
        ))
    return sorted((v.encode() for v in forms if v), key=len, reverse=True)


# Each encoding defines its scanner payload and matcher delimiters together.
# Keys use canonical payloads, with numeric leading zeros removed by the scanner.
ESCAPE_NAMES = {"&": b"amp", "<": b"lt", ">": b"gt", '"': b"quot", "'": b"apos"}
ESCAPE_ENCODINGS = {
    "percent": (rb"%", rb"[0-9a-f]{2}", b"", True),
    "decimal": (rb"&#0*", rb"[1-9][0-9]*|0", b";", False),
    "numeric_hex": (rb"&#x0*", rb"[1-9a-f][0-9a-f]*|0", b";", True),
    "octal": (rb"\\", rb"[0-7]{3}", b"", False),
    "hex": (rb"\\x", rb"[0-9a-f]{2}", b"", True),
    "unicode": (rb"\\u", rb"[0-9a-f]{4}", b"", True),
    "named": (b"&", b"|".join(ESCAPE_NAMES.values()), b";", False),
    "slash": (rb"\\", b"/", b"", False),
}


def escaped_tokens(value):
    # Share tokenization and accepted spellings between matching and filtering.
    # Read code points from credential forms only; child output remains raw bytes.
    for token in re.findall(r"\\u[0-9a-fA-F]{4}|[\s\S]", value.decode("utf-8", "surrogateescape")):
        if len(token) == 6:
            # JSON Unicode escapes, including surrogate pairs, allow either hex case.
            yield [(None, [("unicode", token[2:].lower().encode())])], []
            continue
        raw = token.encode("utf-8", "surrogateescape")
        parts = []
        for byte in raw:
            hexadecimal = f"{byte:02x}".encode()
            keys = [("percent", hexadecimal), ("numeric_hex", f"{byte:x}".encode()),
                    ("hex", hexadecimal), ("unicode", b"00" + hexadecimal),
                    ("decimal", str(byte).encode())]
            # Bash emits fixed-width octal for control and non-ASCII bytes.
            if byte < 32 or byte >= 127:
                keys.append(("octal", f"{byte:03o}".encode()))
            parts.append((bytes([byte]), keys))
        alternatives = []
        if len(raw) > 1:
            # XML numeric references represent Unicode code points, not UTF-8 bytes.
            alternatives.extend((("decimal", str(ord(token)).encode()),
                                 ("numeric_hex", f"{ord(token):x}".encode())))
        if token in ESCAPE_NAMES:
            alternatives.append(("named", ESCAPE_NAMES[token]))
        if token == "/":
            alternatives.append(("slash", b"/"))
        yield parts, alternatives


def escape_pattern(key, literal_prefix=False):
    kind, token = key
    prefix, _, suffix, ignore_case = ESCAPE_ENCODINGS[kind]
    pattern = prefix + re.escape(token) + suffix
    if ignore_case:
        # Already escaped JSON tokens keep their literal lowercase \u prefix.
        if literal_prefix:
            return prefix + b"(?i:" + re.escape(token) + b")" + suffix
        return b"(?i:" + pattern + b")"
    return pattern


def escaped_pattern(value):
    # Keeping original offsets lets redaction cover the encoded bytes themselves.
    patterns = []
    for parts, alternatives in escaped_tokens(value):
        pattern = b"".join(
            b"(?:" + b"|".join(([re.escape(raw)] if raw else [])
                               + [escape_pattern(key, raw is None) for key in keys]) + b")"
            for raw, keys in parts
        )
        if alternatives:
            pattern = b"(?:" + b"|".join([pattern] + [escape_pattern(key) for key in alternatives]) + b")"
        patterns.append(pattern)
    return b"".join(patterns)


def repetition_end(data, repeat, unit, start):
    # Keep the regex engine's repetition stack independent of stream length.
    # One complete unit may exceed a chunk because numeric entities allow zeros.
    while start < len(data):
        stop = repeat.match(data, start, min(len(data), start + 65536)).end()
        if stop == start:
            match = unit.match(data, start)
            if match is None:
                break
            stop = match.end()
        start = stop
    return start


def redact(data, forms):
    # Match every source against the original bytes so replacements cannot hide secrets.
    def spans(pattern, group, flags=0):
        return (match.span(group) for match in re.finditer(pattern, data, flags))

    escape_scanner = re.compile(b"|".join(
        prefix + b"(?P<" + kind.encode() + b">" + payload + b")" + suffix
        for kind, (prefix, payload, suffix, _) in ESCAPE_ENCODINGS.items()
    ), re.I)

    def escape_key(match):
        # Numeric references can have arbitrarily many digits. Matching code
        # points have at most seven decimal or six hex digits after leading zeros.
        begin, finish = match.span(match.lastgroup)
        if finish - begin <= 7:
            return match.lastgroup, match.group(match.lastgroup).lower()
        return None

    # Cache only sparse locations. All other escape metadata is bounded by the
    # credential vocabulary, not the output's length or vocabulary.
    cached_escapes = []
    escape_counts = {}
    for value in forms:
        for parts, alternatives in escaped_tokens(value):
            for key in alternatives:
                escape_counts.setdefault(key, 0)
            for raw, keys in parts:
                for kind, token in keys:
                    escape_counts.setdefault(("unicode_literal" if raw is None else kind, token), 0)
    strict_spellings = {}
    for kind, token in escape_counts:
        literal = kind == "unicode_literal"
        base_kind = "unicode" if literal else kind
        if literal or not ESCAPE_ENCODINGS[base_kind][3]:
            strict_spellings[kind, token] = re.compile(escape_pattern((base_kind, token), literal))

    def matching_keys(match):
        key = escape_key(match)
        if key is None:
            return ()
        candidates = (key, ("unicode_literal", key[1])) if key[0] == "unicode" else (key,)
        return tuple(candidate for candidate in candidates if candidate in escape_counts and (
            candidate not in strict_spellings
            or strict_spellings[candidate].fullmatch(data, match.start(), match.end()) is not None))

    if escape_counts:
        for match in escape_scanner.finditer(data):
            for key in matching_keys(match):
                escape_counts[key] += 1
            if cached_escapes is not None:
                if len(cached_escapes) == 128:
                    cached_escapes = None
                else:
                    cached_escapes.append(match)

    def escaped_regions(value, allowed):
        if cached_escapes is None:
            # Dense matching reads the source directly; do not first traverse
            # every escape separately for each derived credential form.
            yield 0, len(data)
            return
        # A nonliteral match touches an escape. Complete escape spans plus one
        # credential length of literal padding cover all sparse matches.
        left = right = 0
        for match in cached_escapes:
            if not any(key in allowed for key in matching_keys(match)):
                continue
            start, end = max(0, match.start() - len(value)), min(len(data), match.end() + len(value))
            if start > right:
                if right:
                    yield left, right
                left = start
            right = max(right, end)
        if right:
            yield left, right

    byte_counts = {}

    def repeated_token_spans(token, repetitions):
        parts, alternatives = token
        # Prefer a complete encoding over its literal opener. A literal suffix
        # of an encoded '3' can contain two 3s, handled separately below.
        pattern = b"|".join(escape_pattern(key) for key in alternatives)
        raw_pattern = b"".join(b"(?:" + b"|".join(
            [escape_pattern(key, raw is None) for key in keys]
            + ([re.escape(raw)] if raw else [])) + b")" for raw, keys in parts)
        pattern = b"(?:" + (pattern + b"|" if pattern else b"") + raw_pattern + b")"
        unit = re.compile(pattern)
        word = re.compile(pattern + b"{" + str(repetitions).encode() + b"}")
        repeat = re.compile(pattern + b"*")
        position = 0
        while (first := unit.search(data, position)) is not None:
            start = first.start()
            end = repetition_end(data, repeat, unit, start)
            match = word.match(data, start)
            if (match is None and len(parts) == 1 and parts[0][0] == b"3"
                    and first.end() - start >= 2 and data[first.end() - 2:first.end()] == b"33"):
                start = first.end() - 2
                match = word.match(data, start)
            if match is not None:
                yield start, end
            position = end

    def escaped_spans(value):
        if not value:
            return
        tokens = list(escaped_tokens(value))
        offsets = [0]
        allowed = set()
        scores = []
        for parts, alternatives in tokens:
            counts = []
            allowed.update(alternatives)
            for raw, keys in parts:
                if raw is not None and raw not in byte_counts:
                    byte_counts[raw] = data.count(raw)
                spellings = [("unicode_literal" if raw is None else kind, token)
                             for kind, token in keys]
                allowed.update(spellings)
                counts.append((byte_counts[raw] if raw else 0)
                              + sum(escape_counts.get(key, 0) for key in spellings))
            scores.append(min(counts) + sum(escape_counts.get(key, 0) for key in alternatives))
            offsets.append(offsets[-1] + (6 if parts[0][0] is None else len(parts)))
        if not any(escape_counts.get(key, 0) for key in allowed) or not all(scores):
            return
        if all(token == tokens[0] for token in tokens):
            yield from repeated_token_spans(tokens[0], len(tokens))
            return
        # A fixed-size required substring rejects unrelated logs in native regex
        # code before the state machine. Its length never grows with a secret.
        selected = min(range(len(tokens)), key=scores.__getitem__)
        first = max(0, min(selected - 3, len(tokens) - 8))
        last = min(len(tokens), first + 8)
        if re.search(escaped_pattern(value[offsets[first]:offsets[last]]), data) is None:
            return

        # One packed integer holds the earliest source start for every matching
        # credential prefix. Each field is a complemented source offset: a larger
        # tag means an earlier start. Retaining that tag covers all overlapping
        # paths without keeping an object for every source offset.
        bits = max(32, (len(data) + 1).bit_length() + 2)
        tag_mask = (1 << (bits - 1)) - 1
        lane_ones = ((1 << (bits * (len(value) + 1))) - 1) // ((1 << bits) - 1)
        guards = lane_ones << (bits - 1)
        payloads = guards - lane_ones
        literal_masks = {}
        transitions = {}
        position = 0
        for parts, alternatives in tokens:
            start = position
            for raw, keys in parts:
                width = 6 if raw is None else 1
                if raw is not None:
                    literal_masks[raw[0]] = literal_masks.get(raw[0], 0) | (
                        tag_mask << ((position + 1) * bits))
                for kind, token in keys:
                    key = ("unicode_literal" if raw is None else kind), token
                    shifts = transitions.setdefault(key, {})
                    shifts[width] = shifts.get(width, 0) | (tag_mask << (position * bits))
                position += width
            for key in alternatives:
                shifts = transitions.setdefault(key, {})
                width = position - start
                shifts[width] = shifts.get(width, 0) | (tag_mask << (start * bits))
        transitions = {key: tuple((mask, width * bits) for width, mask in shifts.items())
                       for key, shifts in transitions.items()}
        terminal = len(value) * bits

        def merge_starts(left, right):
            if not right:
                return left
            if left <= tag_mask and not right & tag_mask:
                return left | right
            # The guard bit prevents subtraction borrowing across fields. Expand
            # each comparison bit to select the larger tag independently.
            selected = (((left | guards) - right) & guards) >> (bits - 1)
            selected *= tag_mask
            return (left & selected) | (right & (payloads ^ selected))

        for left, right in escaped_regions(value, allowed):
            state = tag_mask - left
            matches = iter(escape_scanner.finditer(data, left, right))
            escape = next(matches, None)
            escape_start = escape.start() if escape is not None else right
            escape_end = escape.end() if escape is not None else right + 1
            jumps = ()
            snapshot = 0
            pending = None
            for offset, byte in enumerate(memoryview(data)[left:right], left):
                if offset == escape_start:
                    jumps = tuple(item for key in matching_keys(escape)
                                  for item in transitions.get(key, ()))
                    snapshot = state
                mask = literal_masks.get(byte, 0)
                state = (((state << bits) & mask) if mask else 0) | (tag_mask - offset - 1)
                end = offset + 1
                if end == escape_end:
                    for mask, shift in jumps:
                        state = merge_starts(state, (snapshot & mask) << shift)
                    jumps = ()
                    escape = next(matches, None)
                    escape_start = escape.start() if escape is not None else right
                    escape_end = escape.end() if escape is not None else right + 1
                tag = (state >> terminal) & tag_mask
                if tag:
                    begin = tag_mask - tag
                    if pending is not None:
                        if begin > pending[1]:
                            yield pending
                        else:
                            begin = min(begin, pending[0])
                    pending = begin, end
            if pending is not None:
                yield pending


    folded = None

    def literal_spans(value):
        nonlocal folded
        if not value:
            return
        source = data
        if b"%" in value:
            # Preserve the existing case-insensitive percent-form matching.
            if folded is None:
                folded = data.lower()
            source, value = folded, value.lower()
        failure = [0] * len(value)
        for index in range(1, len(value)):
            prefix = failure[index - 1]
            while prefix and value[index] != value[prefix]:
                prefix = failure[prefix - 1]
            if value[index] == value[prefix]:
                prefix += 1
            failure[index] = prefix
        period = len(value) - failure[-1]
        del failure
        # Coalesce periodic overlaps and adjacent hits instead of every offset.
        extension = unit = None
        start = 0
        while True:
            left = source.find(value, start)
            if left < 0:
                return
            if extension is None:
                unit = re.compile(re.escape(value[-period:]))
                extension = re.compile(b"(?:" + unit.pattern + b")*")
            right = repetition_end(source, extension, unit, left + len(value))
            yield left, right
            start = right - len(value) + period

    def credential_spans():
        # Consume each quoted value once, even when its closing quote is missing.
        consumed = 0
        property_name = re.compile(rb"[\w.-]*")
        for match in re.finditer(
            rb"(?i)([\"']?)(?:--?define[=\s]+|-D\s*)",
            data,
        ):
            if match.end() <= consumed:
                continue
            # A preceding value's closing quote may touch the next option.
            outer = match[1] if match.start() >= consumed else b""
            consumed = property_name.match(data, match.end()).end()
            if (data[consumed:consumed + 1] != b"=" or not re.search(
                    rb"passphrase|password|username|token|secret|authorization",
                    data[match.end():consumed], re.I)):
                continue
            left = right = consumed + 1
            quote = outer or data[left:left + 1]
            if quote in (b'"', b"'"):
                closed = False
                if not outer:
                    right += 1
                while right < len(data):
                    if data[right] == 92:
                        right = min(right + 2, len(data))
                    elif data[right] == quote[0]:
                        right += 1
                        closed = True
                        break
                    else:
                        right += 1
                consumed = right
                yield left, right - 1 if outer and closed else right
            else:
                while right < len(data) and data[right] not in b" \t\n\r\v\f":
                    right += 1
                consumed = right
                yield left, right

    # Consume one matcher at a time, so derived forms never retain simultaneous
    # state machines. A bit per source byte unions spans even when an encoded
    # path discovers an earlier start after a later span has already been yielded.
    redacted = bytearray((len(data) + 7) // 8)
    has_redactions = False

    def mark(regions):
        nonlocal has_redactions
        for left, right in regions:
            if left >= right:
                continue
            has_redactions = True
            first, begin = divmod(left, 8)
            last, finish = divmod(right, 8)
            if first == last:
                redacted[first] |= ((1 << (finish - begin)) - 1) << begin
            else:
                redacted[first] |= (255 << begin) & 255
                redacted[first + 1:last] = b"\xff" * (last - first - 1)
                if finish:
                    redacted[last] |= (1 << finish) - 1

    mark(credential_spans())
    mark(spans(rb"((?:proxy-)?" + escaped_pattern(b"authorization")
               + rb"[\"']?\s*[:=]\s*)([^\r\n]*)", 2, re.I))
    for value in forms:
        mark(literal_spans(value))
        if cached_escapes is None or cached_escapes:
            mark(escaped_spans(value))
    if not has_redactions:
        return bytes(data)

    partial_runs = {}
    non_full = re.compile(rb"[^\xff]")

    def marked_spans():
        for run in re.finditer(rb"[^\x00]+", redacted):
            position = run.start()
            while position < run.end():
                value = redacted[position]
                if value == 255:
                    partial = non_full.search(redacted, position, run.end())
                    stop = partial.start() if partial is not None else run.end()
                    yield position * 8, stop * 8
                    position = stop
                    continue
                if value not in partial_runs:
                    bits = value
                    pieces = []
                    while bits:
                        begin = (bits & -bits).bit_length() - 1
                        tail = bits >> begin
                        width = (tail ^ (tail + 1)).bit_length() - 1
                        pieces.append((begin, begin + width))
                        bits &= ~(((1 << width) - 1) << begin)
                    partial_runs[value] = pieces
                for begin, finish in partial_runs[value]:
                    yield position * 8 + begin, position * 8 + finish
                position += 1

    output = bytearray()
    start = end = 0
    for left, right in marked_spans():
        if left > end:
            if end > start:
                output.extend(b"[REDACTED]")
            output.extend(data[end:left])
            start = left
        end = max(end, right)
    if end > start:
        output.extend(b"[REDACTED]")
    output.extend(data[end:])
    return bytes(output)


