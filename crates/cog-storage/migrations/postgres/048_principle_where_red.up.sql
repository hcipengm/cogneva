-- Migration: 048_principle_where_red
-- Where a principle says it would go red, carried beside the two gate columns.
--
-- `host_gate` says whether the host's own registry runs a check for a principle,
-- and `platform_gate` says whether the platform's tree declares one. Neither
-- answers the question a person asking about a rule actually asks next: *which
-- file would turn red*. Until now that answer existed only as a sentence in the
-- memory's own body, written by hand and parsed by nothing, so a path in it could
-- move and the sentence would keep reading as an answer.
--
-- This column is the platform's carrier for that sentence. It holds what the
-- carrier wrote after its gate marker, as written, markers joined when a memory
-- states more than one; `-` means the carrier names no gate at all. It is a
-- reading, not a verdict: it says where the answer points, not whether the thing
-- pointed at still exists -- the host-side gate that reads the same sentence
-- judges existence, and folding that verdict into this column would make the
-- account claim a look nobody took.
--
-- NULL is therefore "no reading has said", the same as on `host_gate`: an older
-- reading that predates this column carries nothing, and a column that read NULL
-- as "no gate named" would report every one of those as a rule with no answer.
--
-- The sentence is unbounded TEXT rather than a path list: some carriers answer
-- with a test name or a command instead of a file, and requiring a path here
-- would either drop those answers or force the payload to invent one.

ALTER TABLE principle_ledger
    ADD COLUMN IF NOT EXISTS where_red TEXT;

ALTER TABLE principle_ledger_declared
    ADD COLUMN IF NOT EXISTS where_red TEXT;
