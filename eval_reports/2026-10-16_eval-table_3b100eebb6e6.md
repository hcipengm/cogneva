# eval-table

## pins
backbone	via-security-gateway@1
judge	via-security-gateway@1
tools	none
diagnostic	this run is the tools-off diagnostic arm; its numbers are not a table row
seeds	0,1,2
data-root	/data
checkpoint	/eval/3b100eebb6e6.md.cases.jsonl	run-id=a294c2d6282e
checkpoint-rule	only judged cases are recorded; a case that got no verdict (env or upstream failure) is run again on resume rather than remembered as failed, and a case with no record counts as not resolved in its cell
subset	/eval/seed20261011_n40.tsv	sha256:d0e74c3b2f8d6774bbbb4b3526801033cfe2ced736f8645199307fba4778b638
subset-cases	hle	40
subset-cases	swe-bench-pro	40
subset-cases	toolathlon	40
benchmark	hle	rev=1ec1f1f25ed4ad891e3a81d1cbc08f261f5e77c6	read-file=hle/test-00000-of-00001.jsonl	verified[read-file hle/test-00000-of-00001.jsonl sha256(8efbca01f176a40d4aa6139c4b99794da2cb7f797a818af2f2ddd84916d9519d); source hle/test-00000-of-00001.parquet sha256(6d0ee0602e8aea6b159509577e884f48ecac7b8e3f6822a35f51335a446c726a)]
benchmark	swe-bench-pro	rev=0c470e9cebb295f66caccb625dc7f40169771f79	read-file=swe-bench-pro/test-00000-of-00001.jsonl	verified[read-file swe-bench-pro/test-00000-of-00001.jsonl sha256(a1a67075e95009f25709b64792df2fcebef59884473f83c792e25886bd07e1bd); source swe-bench-pro/test-00000-of-00001.parquet sha256(c8cd7115496ad4e9a8b21d088cef576a65bf821bb542b24336f13f714cef13f8)]
benchmark	toolathlon	rev=ed735ba0af71d82688a55f952a6ea11479ae7206	read-dir=toolathlon-gym/tasks/finalpool	verified[read-dir toolathlon-gym/tasks/finalpool (a directory has no hash of its own); source toolathlon-gym.tar.gz sha256(fbe8af77414fad3d2a2741dccd2e3a73d86716aebdc4e878c3878e5957bad766)]

## wiring
missing	tools: this run used the tools-off diagnostic arm, so it is not a main-table row
missing	swe-bench-pro: no container backend is wired
missing	toolathlon: no compose backend is wired
missing	nql: no platform is wired

## gate
concurrency-slot	4
concurrency-rule	effective = min(column ceiling, gate bound, static slot): the three are not alternatives and the smallest one wins
concurrency-column	hle	ceiling=none: the cases in this column do not share state, so the column itself adds no ceiling
concurrency-column	swe-bench-pro	ceiling=none: the cases in this column do not share state, so the column itself adds no ceiling
concurrency-column	toolathlon	ceiling=1
gate	not in force: no --admission directory was given, so the bound on this run is the static slot and each column's own ceiling, and nothing looked at the host

## table
(withheld: this run is the tools-off diagnostic arm; its cells are not a table row. The per-seed readings below are kept for diagnosis.)

## per seed
cell	hle	agentflow	seed=0	resolved=0	total=40	errored=40	resumed=0	tokens=0	steps=0
cell	hle	agentflow	seed=1	resolved=0	total=40	errored=40	resumed=0	tokens=0	steps=0
cell	hle	agentflow	seed=2	resolved=0	total=40	errored=40	resumed=0	tokens=0	steps=0
cell	hle	codeact	seed=0	resolved=0	total=40	errored=40	resumed=0	tokens=0	steps=0
cell	hle	codeact	seed=1	resolved=0	total=40	errored=40	resumed=0	tokens=0	steps=0
cell	hle	codeact	seed=2	resolved=0	total=40	errored=40	resumed=0	tokens=0	steps=0
cell	hle	gepa	seed=0	resolved=0	total=40	errored=40	resumed=0	tokens=0	steps=0
cell	hle	gepa	seed=1	resolved=0	total=40	errored=40	resumed=0	tokens=0	steps=0
cell	hle	gepa	seed=2	resolved=0	total=40	errored=40	resumed=0	tokens=0	steps=0
cell	hle	nql	seed=0	resolved=0	total=40	errored=40	resumed=0	tokens=0	steps=0
cell	hle	nql	seed=1	resolved=0	total=40	errored=40	resumed=0	tokens=0	steps=0
cell	hle	nql	seed=2	resolved=0	total=40	errored=40	resumed=0	tokens=0	steps=0
cell	swe-bench-pro	agentflow	seed=0	resolved=0	total=40	errored=40	resumed=0	tokens=0	steps=0
cell	swe-bench-pro	agentflow	seed=1	resolved=0	total=40	errored=40	resumed=0	tokens=0	steps=0
cell	swe-bench-pro	agentflow	seed=2	resolved=0	total=40	errored=40	resumed=0	tokens=0	steps=0
cell	swe-bench-pro	codeact	seed=0	resolved=0	total=40	errored=40	resumed=0	tokens=0	steps=0
cell	swe-bench-pro	codeact	seed=1	resolved=0	total=40	errored=40	resumed=0	tokens=0	steps=0
cell	swe-bench-pro	codeact	seed=2	resolved=0	total=40	errored=40	resumed=0	tokens=0	steps=0
cell	swe-bench-pro	gepa	seed=0	resolved=0	total=40	errored=40	resumed=0	tokens=0	steps=0
cell	swe-bench-pro	gepa	seed=1	resolved=0	total=40	errored=40	resumed=0	tokens=0	steps=0
cell	swe-bench-pro	gepa	seed=2	resolved=0	total=40	errored=40	resumed=0	tokens=0	steps=0
cell	swe-bench-pro	nql	seed=0	resolved=0	total=40	errored=40	resumed=0	tokens=0	steps=0
cell	swe-bench-pro	nql	seed=1	resolved=0	total=40	errored=40	resumed=0	tokens=0	steps=0
cell	swe-bench-pro	nql	seed=2	resolved=0	total=40	errored=40	resumed=0	tokens=0	steps=0
cell	toolathlon	agentflow	seed=0	resolved=0	total=40	errored=40	resumed=0	tokens=0	steps=0
cell	toolathlon	agentflow	seed=1	resolved=0	total=40	errored=40	resumed=0	tokens=0	steps=0
cell	toolathlon	agentflow	seed=2	resolved=0	total=40	errored=40	resumed=0	tokens=0	steps=0
cell	toolathlon	codeact	seed=0	resolved=0	total=40	errored=40	resumed=0	tokens=0	steps=0
cell	toolathlon	codeact	seed=1	resolved=0	total=40	errored=40	resumed=0	tokens=0	steps=0
cell	toolathlon	codeact	seed=2	resolved=0	total=40	errored=40	resumed=0	tokens=0	steps=0
cell	toolathlon	gepa	seed=0	resolved=0	total=40	errored=40	resumed=0	tokens=0	steps=0
cell	toolathlon	gepa	seed=1	resolved=0	total=40	errored=40	resumed=0	tokens=0	steps=0
cell	toolathlon	gepa	seed=2	resolved=0	total=40	errored=40	resumed=0	tokens=0	steps=0
cell	toolathlon	nql	seed=0	resolved=0	total=40	errored=40	resumed=0	tokens=0	steps=0
cell	toolathlon	nql	seed=1	resolved=0	total=40	errored=40	resumed=0	tokens=0	steps=0
cell	toolathlon	nql	seed=2	resolved=0	total=40	errored=40	resumed=0	tokens=0	steps=0

## failures
failure	hle	agentflow	seed=0	hle-66e88728ba7d8bc0d5806f3a	scaffold failed: Agent execution error: OpenAIProvider: no HttpClient configured
failure	hle	agentflow	seed=0	hle-66f0e2bb707a9da209d68e32	scaffold failed: Agent execution error: OpenAIProvider: no HttpClient configured
failure	hle	agentflow	seed=0	hle-66f1a1fdccdf3d48cee44b17	scaffold failed: Agent execution error: OpenAIProvider: no HttpClient configured
failure	hle	agentflow	seed=0	…and 37 more
failure	hle	agentflow	seed=1	hle-66e88728ba7d8bc0d5806f3a	scaffold failed: Agent execution error: OpenAIProvider: no HttpClient configured
failure	hle	agentflow	seed=1	hle-66f0e2bb707a9da209d68e32	scaffold failed: Agent execution error: OpenAIProvider: no HttpClient configured
failure	hle	agentflow	seed=1	hle-66f1a1fdccdf3d48cee44b17	scaffold failed: Agent execution error: OpenAIProvider: no HttpClient configured
failure	hle	agentflow	seed=1	…and 37 more
failure	hle	agentflow	seed=2	hle-66e88728ba7d8bc0d5806f3a	scaffold failed: Agent execution error: OpenAIProvider: no HttpClient configured
failure	hle	agentflow	seed=2	hle-66f0e2bb707a9da209d68e32	scaffold failed: Agent execution error: OpenAIProvider: no HttpClient configured
failure	hle	agentflow	seed=2	hle-66f1a1fdccdf3d48cee44b17	scaffold failed: Agent execution error: OpenAIProvider: no HttpClient configured
failure	hle	agentflow	seed=2	…and 37 more
failure	hle	codeact	seed=0	hle-66e88728ba7d8bc0d5806f3a	scaffold failed: this benchmark offers no code tool (`python` or `bash`): the action space of this scaffold is code, and answering without one would be a different row
failure	hle	codeact	seed=0	hle-66f0e2bb707a9da209d68e32	scaffold failed: this benchmark offers no code tool (`python` or `bash`): the action space of this scaffold is code, and answering without one would be a different row
failure	hle	codeact	seed=0	hle-66f1a1fdccdf3d48cee44b17	scaffold failed: this benchmark offers no code tool (`python` or `bash`): the action space of this scaffold is code, and answering without one would be a different row
failure	hle	codeact	seed=0	…and 37 more
failure	hle	codeact	seed=1	hle-66e88728ba7d8bc0d5806f3a	scaffold failed: this benchmark offers no code tool (`python` or `bash`): the action space of this scaffold is code, and answering without one would be a different row
failure	hle	codeact	seed=1	hle-66f0e2bb707a9da209d68e32	scaffold failed: this benchmark offers no code tool (`python` or `bash`): the action space of this scaffold is code, and answering without one would be a different row
failure	hle	codeact	seed=1	hle-66f1a1fdccdf3d48cee44b17	scaffold failed: this benchmark offers no code tool (`python` or `bash`): the action space of this scaffold is code, and answering without one would be a different row
failure	hle	codeact	seed=1	…and 37 more
failure	hle	codeact	seed=2	hle-66e88728ba7d8bc0d5806f3a	scaffold failed: this benchmark offers no code tool (`python` or `bash`): the action space of this scaffold is code, and answering without one would be a different row
failure	hle	codeact	seed=2	hle-66f0e2bb707a9da209d68e32	scaffold failed: this benchmark offers no code tool (`python` or `bash`): the action space of this scaffold is code, and answering without one would be a different row
failure	hle	codeact	seed=2	hle-66f1a1fdccdf3d48cee44b17	scaffold failed: this benchmark offers no code tool (`python` or `bash`): the action space of this scaffold is code, and answering without one would be a different row
failure	hle	codeact	seed=2	…and 37 more
failure	hle	gepa	seed=0	hle-66e88728ba7d8bc0d5806f3a	scaffold failed: Agent execution error: OpenAIProvider: no HttpClient configured
failure	hle	gepa	seed=0	hle-66f0e2bb707a9da209d68e32	scaffold failed: Agent execution error: OpenAIProvider: no HttpClient configured
failure	hle	gepa	seed=0	hle-66f1a1fdccdf3d48cee44b17	scaffold failed: Agent execution error: OpenAIProvider: no HttpClient configured
failure	hle	gepa	seed=0	…and 37 more
failure	hle	gepa	seed=1	hle-66e88728ba7d8bc0d5806f3a	scaffold failed: Agent execution error: OpenAIProvider: no HttpClient configured
failure	hle	gepa	seed=1	hle-66f0e2bb707a9da209d68e32	scaffold failed: Agent execution error: OpenAIProvider: no HttpClient configured
failure	hle	gepa	seed=1	hle-66f1a1fdccdf3d48cee44b17	scaffold failed: Agent execution error: OpenAIProvider: no HttpClient configured
failure	hle	gepa	seed=1	…and 37 more
failure	hle	gepa	seed=2	hle-66e88728ba7d8bc0d5806f3a	scaffold failed: Agent execution error: OpenAIProvider: no HttpClient configured
failure	hle	gepa	seed=2	hle-66f0e2bb707a9da209d68e32	scaffold failed: Agent execution error: OpenAIProvider: no HttpClient configured
failure	hle	gepa	seed=2	hle-66f1a1fdccdf3d48cee44b17	scaffold failed: Agent execution error: OpenAIProvider: no HttpClient configured
failure	hle	gepa	seed=2	…and 37 more
failure	hle	nql	seed=0	hle-66e88728ba7d8bc0d5806f3a	scaffold failed: no platform endpoint is wired: the composition root wires no platform
failure	hle	nql	seed=0	hle-66f0e2bb707a9da209d68e32	scaffold failed: no platform endpoint is wired: the composition root wires no platform
failure	hle	nql	seed=0	hle-66f1a1fdccdf3d48cee44b17	scaffold failed: no platform endpoint is wired: the composition root wires no platform
failure	hle	nql	seed=0	…and 37 more
failure	hle	nql	seed=1	hle-66e88728ba7d8bc0d5806f3a	scaffold failed: no platform endpoint is wired: the composition root wires no platform
failure	hle	nql	seed=1	hle-66f0e2bb707a9da209d68e32	scaffold failed: no platform endpoint is wired: the composition root wires no platform
failure	hle	nql	seed=1	hle-66f1a1fdccdf3d48cee44b17	scaffold failed: no platform endpoint is wired: the composition root wires no platform
failure	hle	nql	seed=1	…and 37 more
failure	hle	nql	seed=2	hle-66e88728ba7d8bc0d5806f3a	scaffold failed: no platform endpoint is wired: the composition root wires no platform
failure	hle	nql	seed=2	hle-66f0e2bb707a9da209d68e32	scaffold failed: no platform endpoint is wired: the composition root wires no platform
failure	hle	nql	seed=2	hle-66f1a1fdccdf3d48cee44b17	scaffold failed: no platform endpoint is wired: the composition root wires no platform
failure	hle	nql	seed=2	…and 37 more
failure	swe-bench-pro	agentflow	seed=0	swe-pro-instance_NodeBB__NodeBB-da0211b1a001d45d73b4c84c6417a4f1b0312575-vf2cf3cbd463b7ad942381f1c6d077626485a1e9e	cannot acquire env: SWE-bench Pro runs each instance in its own container and no container backend is wired (case swe-pro-instance_NodeBB__NodeBB-da0211b1a001d45d73b4c84c6417a4f1b0312575-vf2cf3cbd463b7ad942381f1c6d077626485a1e9e); a case scored without one would read as the model failing the task
failure	swe-bench-pro	agentflow	seed=0	swe-pro-instance_ansible__ansible-0ea40e09d1b35bcb69ff4d9cecf3d0defa4b36e8-v30a923fb5c164d6cd18280c02422f75e611e8fb2	cannot acquire env: SWE-bench Pro runs each instance in its own container and no container backend is wired (case swe-pro-instance_ansible__ansible-0ea40e09d1b35bcb69ff4d9cecf3d0defa4b36e8-v30a923fb5c164d6cd18280c02422f75e611e8fb2); a case scored without one would read as the model failing the task
failure	swe-bench-pro	agentflow	seed=0	swe-pro-instance_ansible__ansible-12734fa21c08a0ce8c84e533abdc560db2eb1955-v7eee2454f617569fd6889f2211f75bc02a35f9f8	cannot acquire env: SWE-bench Pro runs each instance in its own container and no container backend is wired (case swe-pro-instance_ansible__ansible-12734fa21c08a0ce8c84e533abdc560db2eb1955-v7eee2454f617569fd6889f2211f75bc02a35f9f8); a case scored without one would read as the model failing the task
failure	swe-bench-pro	agentflow	seed=0	…and 37 more
failure	swe-bench-pro	agentflow	seed=1	swe-pro-instance_NodeBB__NodeBB-da0211b1a001d45d73b4c84c6417a4f1b0312575-vf2cf3cbd463b7ad942381f1c6d077626485a1e9e	cannot acquire env: SWE-bench Pro runs each instance in its own container and no container backend is wired (case swe-pro-instance_NodeBB__NodeBB-da0211b1a001d45d73b4c84c6417a4f1b0312575-vf2cf3cbd463b7ad942381f1c6d077626485a1e9e); a case scored without one would read as the model failing the task
failure	swe-bench-pro	agentflow	seed=1	swe-pro-instance_ansible__ansible-0ea40e09d1b35bcb69ff4d9cecf3d0defa4b36e8-v30a923fb5c164d6cd18280c02422f75e611e8fb2	cannot acquire env: SWE-bench Pro runs each instance in its own container and no container backend is wired (case swe-pro-instance_ansible__ansible-0ea40e09d1b35bcb69ff4d9cecf3d0defa4b36e8-v30a923fb5c164d6cd18280c02422f75e611e8fb2); a case scored without one would read as the model failing the task
failure	swe-bench-pro	agentflow	seed=1	swe-pro-instance_ansible__ansible-12734fa21c08a0ce8c84e533abdc560db2eb1955-v7eee2454f617569fd6889f2211f75bc02a35f9f8	cannot acquire env: SWE-bench Pro runs each instance in its own container and no container backend is wired (case swe-pro-instance_ansible__ansible-12734fa21c08a0ce8c84e533abdc560db2eb1955-v7eee2454f617569fd6889f2211f75bc02a35f9f8); a case scored without one would read as the model failing the task
failure	swe-bench-pro	agentflow	seed=1	…and 37 more
failure	swe-bench-pro	agentflow	seed=2	swe-pro-instance_NodeBB__NodeBB-da0211b1a001d45d73b4c84c6417a4f1b0312575-vf2cf3cbd463b7ad942381f1c6d077626485a1e9e	cannot acquire env: SWE-bench Pro runs each instance in its own container and no container backend is wired (case swe-pro-instance_NodeBB__NodeBB-da0211b1a001d45d73b4c84c6417a4f1b0312575-vf2cf3cbd463b7ad942381f1c6d077626485a1e9e); a case scored without one would read as the model failing the task
failure	swe-bench-pro	agentflow	seed=2	swe-pro-instance_ansible__ansible-0ea40e09d1b35bcb69ff4d9cecf3d0defa4b36e8-v30a923fb5c164d6cd18280c02422f75e611e8fb2	cannot acquire env: SWE-bench Pro runs each instance in its own container and no container backend is wired (case swe-pro-instance_ansible__ansible-0ea40e09d1b35bcb69ff4d9cecf3d0defa4b36e8-v30a923fb5c164d6cd18280c02422f75e611e8fb2); a case scored without one would read as the model failing the task
failure	swe-bench-pro	agentflow	seed=2	swe-pro-instance_ansible__ansible-12734fa21c08a0ce8c84e533abdc560db2eb1955-v7eee2454f617569fd6889f2211f75bc02a35f9f8	cannot acquire env: SWE-bench Pro runs each instance in its own container and no container backend is wired (case swe-pro-instance_ansible__ansible-12734fa21c08a0ce8c84e533abdc560db2eb1955-v7eee2454f617569fd6889f2211f75bc02a35f9f8); a case scored without one would read as the model failing the task
failure	swe-bench-pro	agentflow	seed=2	…and 37 more
failure	swe-bench-pro	codeact	seed=0	swe-pro-instance_NodeBB__NodeBB-da0211b1a001d45d73b4c84c6417a4f1b0312575-vf2cf3cbd463b7ad942381f1c6d077626485a1e9e	cannot acquire env: SWE-bench Pro runs each instance in its own container and no container backend is wired (case swe-pro-instance_NodeBB__NodeBB-da0211b1a001d45d73b4c84c6417a4f1b0312575-vf2cf3cbd463b7ad942381f1c6d077626485a1e9e); a case scored without one would read as the model failing the task
failure	swe-bench-pro	codeact	seed=0	swe-pro-instance_ansible__ansible-0ea40e09d1b35bcb69ff4d9cecf3d0defa4b36e8-v30a923fb5c164d6cd18280c02422f75e611e8fb2	cannot acquire env: SWE-bench Pro runs each instance in its own container and no container backend is wired (case swe-pro-instance_ansible__ansible-0ea40e09d1b35bcb69ff4d9cecf3d0defa4b36e8-v30a923fb5c164d6cd18280c02422f75e611e8fb2); a case scored without one would read as the model failing the task
failure	swe-bench-pro	codeact	seed=0	swe-pro-instance_ansible__ansible-12734fa21c08a0ce8c84e533abdc560db2eb1955-v7eee2454f617569fd6889f2211f75bc02a35f9f8	cannot acquire env: SWE-bench Pro runs each instance in its own container and no container backend is wired (case swe-pro-instance_ansible__ansible-12734fa21c08a0ce8c84e533abdc560db2eb1955-v7eee2454f617569fd6889f2211f75bc02a35f9f8); a case scored without one would read as the model failing the task
failure	swe-bench-pro	codeact	seed=0	…and 37 more
failure	swe-bench-pro	codeact	seed=1	swe-pro-instance_NodeBB__NodeBB-da0211b1a001d45d73b4c84c6417a4f1b0312575-vf2cf3cbd463b7ad942381f1c6d077626485a1e9e	cannot acquire env: SWE-bench Pro runs each instance in its own container and no container backend is wired (case swe-pro-instance_NodeBB__NodeBB-da0211b1a001d45d73b4c84c6417a4f1b0312575-vf2cf3cbd463b7ad942381f1c6d077626485a1e9e); a case scored without one would read as the model failing the task
failure	swe-bench-pro	codeact	seed=1	swe-pro-instance_ansible__ansible-0ea40e09d1b35bcb69ff4d9cecf3d0defa4b36e8-v30a923fb5c164d6cd18280c02422f75e611e8fb2	cannot acquire env: SWE-bench Pro runs each instance in its own container and no container backend is wired (case swe-pro-instance_ansible__ansible-0ea40e09d1b35bcb69ff4d9cecf3d0defa4b36e8-v30a923fb5c164d6cd18280c02422f75e611e8fb2); a case scored without one would read as the model failing the task
failure	swe-bench-pro	codeact	seed=1	swe-pro-instance_ansible__ansible-12734fa21c08a0ce8c84e533abdc560db2eb1955-v7eee2454f617569fd6889f2211f75bc02a35f9f8	cannot acquire env: SWE-bench Pro runs each instance in its own container and no container backend is wired (case swe-pro-instance_ansible__ansible-12734fa21c08a0ce8c84e533abdc560db2eb1955-v7eee2454f617569fd6889f2211f75bc02a35f9f8); a case scored without one would read as the model failing the task
failure	swe-bench-pro	codeact	seed=1	…and 37 more
failure	swe-bench-pro	codeact	seed=2	swe-pro-instance_NodeBB__NodeBB-da0211b1a001d45d73b4c84c6417a4f1b0312575-vf2cf3cbd463b7ad942381f1c6d077626485a1e9e	cannot acquire env: SWE-bench Pro runs each instance in its own container and no container backend is wired (case swe-pro-instance_NodeBB__NodeBB-da0211b1a001d45d73b4c84c6417a4f1b0312575-vf2cf3cbd463b7ad942381f1c6d077626485a1e9e); a case scored without one would read as the model failing the task
failure	swe-bench-pro	codeact	seed=2	swe-pro-instance_ansible__ansible-0ea40e09d1b35bcb69ff4d9cecf3d0defa4b36e8-v30a923fb5c164d6cd18280c02422f75e611e8fb2	cannot acquire env: SWE-bench Pro runs each instance in its own container and no container backend is wired (case swe-pro-instance_ansible__ansible-0ea40e09d1b35bcb69ff4d9cecf3d0defa4b36e8-v30a923fb5c164d6cd18280c02422f75e611e8fb2); a case scored without one would read as the model failing the task
failure	swe-bench-pro	codeact	seed=2	swe-pro-instance_ansible__ansible-12734fa21c08a0ce8c84e533abdc560db2eb1955-v7eee2454f617569fd6889f2211f75bc02a35f9f8	cannot acquire env: SWE-bench Pro runs each instance in its own container and no container backend is wired (case swe-pro-instance_ansible__ansible-12734fa21c08a0ce8c84e533abdc560db2eb1955-v7eee2454f617569fd6889f2211f75bc02a35f9f8); a case scored without one would read as the model failing the task
failure	swe-bench-pro	codeact	seed=2	…and 37 more
failure	swe-bench-pro	gepa	seed=0	swe-pro-instance_NodeBB__NodeBB-da0211b1a001d45d73b4c84c6417a4f1b0312575-vf2cf3cbd463b7ad942381f1c6d077626485a1e9e	cannot acquire env: SWE-bench Pro runs each instance in its own container and no container backend is wired (case swe-pro-instance_NodeBB__NodeBB-da0211b1a001d45d73b4c84c6417a4f1b0312575-vf2cf3cbd463b7ad942381f1c6d077626485a1e9e); a case scored without one would read as the model failing the task
failure	swe-bench-pro	gepa	seed=0	swe-pro-instance_ansible__ansible-0ea40e09d1b35bcb69ff4d9cecf3d0defa4b36e8-v30a923fb5c164d6cd18280c02422f75e611e8fb2	cannot acquire env: SWE-bench Pro runs each instance in its own container and no container backend is wired (case swe-pro-instance_ansible__ansible-0ea40e09d1b35bcb69ff4d9cecf3d0defa4b36e8-v30a923fb5c164d6cd18280c02422f75e611e8fb2); a case scored without one would read as the model failing the task
failure	swe-bench-pro	gepa	seed=0	swe-pro-instance_ansible__ansible-12734fa21c08a0ce8c84e533abdc560db2eb1955-v7eee2454f617569fd6889f2211f75bc02a35f9f8	cannot acquire env: SWE-bench Pro runs each instance in its own container and no container backend is wired (case swe-pro-instance_ansible__ansible-12734fa21c08a0ce8c84e533abdc560db2eb1955-v7eee2454f617569fd6889f2211f75bc02a35f9f8); a case scored without one would read as the model failing the task
failure	swe-bench-pro	gepa	seed=0	…and 37 more
failure	swe-bench-pro	gepa	seed=1	swe-pro-instance_NodeBB__NodeBB-da0211b1a001d45d73b4c84c6417a4f1b0312575-vf2cf3cbd463b7ad942381f1c6d077626485a1e9e	cannot acquire env: SWE-bench Pro runs each instance in its own container and no container backend is wired (case swe-pro-instance_NodeBB__NodeBB-da0211b1a001d45d73b4c84c6417a4f1b0312575-vf2cf3cbd463b7ad942381f1c6d077626485a1e9e); a case scored without one would read as the model failing the task
failure	swe-bench-pro	gepa	seed=1	swe-pro-instance_ansible__ansible-0ea40e09d1b35bcb69ff4d9cecf3d0defa4b36e8-v30a923fb5c164d6cd18280c02422f75e611e8fb2	cannot acquire env: SWE-bench Pro runs each instance in its own container and no container backend is wired (case swe-pro-instance_ansible__ansible-0ea40e09d1b35bcb69ff4d9cecf3d0defa4b36e8-v30a923fb5c164d6cd18280c02422f75e611e8fb2); a case scored without one would read as the model failing the task
failure	swe-bench-pro	gepa	seed=1	swe-pro-instance_ansible__ansible-12734fa21c08a0ce8c84e533abdc560db2eb1955-v7eee2454f617569fd6889f2211f75bc02a35f9f8	cannot acquire env: SWE-bench Pro runs each instance in its own container and no container backend is wired (case swe-pro-instance_ansible__ansible-12734fa21c08a0ce8c84e533abdc560db2eb1955-v7eee2454f617569fd6889f2211f75bc02a35f9f8); a case scored without one would read as the model failing the task
failure	swe-bench-pro	gepa	seed=1	…and 37 more
failure	swe-bench-pro	gepa	seed=2	swe-pro-instance_NodeBB__NodeBB-da0211b1a001d45d73b4c84c6417a4f1b0312575-vf2cf3cbd463b7ad942381f1c6d077626485a1e9e	cannot acquire env: SWE-bench Pro runs each instance in its own container and no container backend is wired (case swe-pro-instance_NodeBB__NodeBB-da0211b1a001d45d73b4c84c6417a4f1b0312575-vf2cf3cbd463b7ad942381f1c6d077626485a1e9e); a case scored without one would read as the model failing the task
failure	swe-bench-pro	gepa	seed=2	swe-pro-instance_ansible__ansible-0ea40e09d1b35bcb69ff4d9cecf3d0defa4b36e8-v30a923fb5c164d6cd18280c02422f75e611e8fb2	cannot acquire env: SWE-bench Pro runs each instance in its own container and no container backend is wired (case swe-pro-instance_ansible__ansible-0ea40e09d1b35bcb69ff4d9cecf3d0defa4b36e8-v30a923fb5c164d6cd18280c02422f75e611e8fb2); a case scored without one would read as the model failing the task
failure	swe-bench-pro	gepa	seed=2	swe-pro-instance_ansible__ansible-12734fa21c08a0ce8c84e533abdc560db2eb1955-v7eee2454f617569fd6889f2211f75bc02a35f9f8	cannot acquire env: SWE-bench Pro runs each instance in its own container and no container backend is wired (case swe-pro-instance_ansible__ansible-12734fa21c08a0ce8c84e533abdc560db2eb1955-v7eee2454f617569fd6889f2211f75bc02a35f9f8); a case scored without one would read as the model failing the task
failure	swe-bench-pro	gepa	seed=2	…and 37 more
failure	swe-bench-pro	nql	seed=0	swe-pro-instance_NodeBB__NodeBB-da0211b1a001d45d73b4c84c6417a4f1b0312575-vf2cf3cbd463b7ad942381f1c6d077626485a1e9e	cannot acquire env: SWE-bench Pro runs each instance in its own container and no container backend is wired (case swe-pro-instance_NodeBB__NodeBB-da0211b1a001d45d73b4c84c6417a4f1b0312575-vf2cf3cbd463b7ad942381f1c6d077626485a1e9e); a case scored without one would read as the model failing the task
failure	swe-bench-pro	nql	seed=0	swe-pro-instance_ansible__ansible-0ea40e09d1b35bcb69ff4d9cecf3d0defa4b36e8-v30a923fb5c164d6cd18280c02422f75e611e8fb2	cannot acquire env: SWE-bench Pro runs each instance in its own container and no container backend is wired (case swe-pro-instance_ansible__ansible-0ea40e09d1b35bcb69ff4d9cecf3d0defa4b36e8-v30a923fb5c164d6cd18280c02422f75e611e8fb2); a case scored without one would read as the model failing the task
failure	swe-bench-pro	nql	seed=0	swe-pro-instance_ansible__ansible-12734fa21c08a0ce8c84e533abdc560db2eb1955-v7eee2454f617569fd6889f2211f75bc02a35f9f8	cannot acquire env: SWE-bench Pro runs each instance in its own container and no container backend is wired (case swe-pro-instance_ansible__ansible-12734fa21c08a0ce8c84e533abdc560db2eb1955-v7eee2454f617569fd6889f2211f75bc02a35f9f8); a case scored without one would read as the model failing the task
failure	swe-bench-pro	nql	seed=0	…and 37 more
failure	swe-bench-pro	nql	seed=1	swe-pro-instance_NodeBB__NodeBB-da0211b1a001d45d73b4c84c6417a4f1b0312575-vf2cf3cbd463b7ad942381f1c6d077626485a1e9e	cannot acquire env: SWE-bench Pro runs each instance in its own container and no container backend is wired (case swe-pro-instance_NodeBB__NodeBB-da0211b1a001d45d73b4c84c6417a4f1b0312575-vf2cf3cbd463b7ad942381f1c6d077626485a1e9e); a case scored without one would read as the model failing the task
failure	swe-bench-pro	nql	seed=1	swe-pro-instance_ansible__ansible-0ea40e09d1b35bcb69ff4d9cecf3d0defa4b36e8-v30a923fb5c164d6cd18280c02422f75e611e8fb2	cannot acquire env: SWE-bench Pro runs each instance in its own container and no container backend is wired (case swe-pro-instance_ansible__ansible-0ea40e09d1b35bcb69ff4d9cecf3d0defa4b36e8-v30a923fb5c164d6cd18280c02422f75e611e8fb2); a case scored without one would read as the model failing the task
failure	swe-bench-pro	nql	seed=1	swe-pro-instance_ansible__ansible-12734fa21c08a0ce8c84e533abdc560db2eb1955-v7eee2454f617569fd6889f2211f75bc02a35f9f8	cannot acquire env: SWE-bench Pro runs each instance in its own container and no container backend is wired (case swe-pro-instance_ansible__ansible-12734fa21c08a0ce8c84e533abdc560db2eb1955-v7eee2454f617569fd6889f2211f75bc02a35f9f8); a case scored without one would read as the model failing the task
failure	swe-bench-pro	nql	seed=1	…and 37 more
failure	swe-bench-pro	nql	seed=2	swe-pro-instance_NodeBB__NodeBB-da0211b1a001d45d73b4c84c6417a4f1b0312575-vf2cf3cbd463b7ad942381f1c6d077626485a1e9e	cannot acquire env: SWE-bench Pro runs each instance in its own container and no container backend is wired (case swe-pro-instance_NodeBB__NodeBB-da0211b1a001d45d73b4c84c6417a4f1b0312575-vf2cf3cbd463b7ad942381f1c6d077626485a1e9e); a case scored without one would read as the model failing the task
failure	swe-bench-pro	nql	seed=2	swe-pro-instance_ansible__ansible-0ea40e09d1b35bcb69ff4d9cecf3d0defa4b36e8-v30a923fb5c164d6cd18280c02422f75e611e8fb2	cannot acquire env: SWE-bench Pro runs each instance in its own container and no container backend is wired (case swe-pro-instance_ansible__ansible-0ea40e09d1b35bcb69ff4d9cecf3d0defa4b36e8-v30a923fb5c164d6cd18280c02422f75e611e8fb2); a case scored without one would read as the model failing the task
failure	swe-bench-pro	nql	seed=2	swe-pro-instance_ansible__ansible-12734fa21c08a0ce8c84e533abdc560db2eb1955-v7eee2454f617569fd6889f2211f75bc02a35f9f8	cannot acquire env: SWE-bench Pro runs each instance in its own container and no container backend is wired (case swe-pro-instance_ansible__ansible-12734fa21c08a0ce8c84e533abdc560db2eb1955-v7eee2454f617569fd6889f2211f75bc02a35f9f8); a case scored without one would read as the model failing the task
failure	swe-bench-pro	nql	seed=2	…and 37 more
failure	toolathlon	agentflow	seed=0	toolathlon-arxiv-latex-analysis-notion-word	cannot acquire env: Toolathlon runs each task against a Docker Compose project and no compose backend is wired (case toolathlon-arxiv-latex-analysis-notion-word); a case scored without one would read as the model failing the task
failure	toolathlon	agentflow	seed=0	toolathlon-canvas-assignment-deadline-word-gcal	cannot acquire env: Toolathlon runs each task against a Docker Compose project and no compose backend is wired (case toolathlon-canvas-assignment-deadline-word-gcal); a case scored without one would read as the model failing the task
failure	toolathlon	agentflow	seed=0	toolathlon-canvas-assignment-word-notion	cannot acquire env: Toolathlon runs each task against a Docker Compose project and no compose backend is wired (case toolathlon-canvas-assignment-word-notion); a case scored without one would read as the model failing the task
failure	toolathlon	agentflow	seed=0	…and 37 more
failure	toolathlon	agentflow	seed=1	toolathlon-arxiv-latex-analysis-notion-word	cannot acquire env: Toolathlon runs each task against a Docker Compose project and no compose backend is wired (case toolathlon-arxiv-latex-analysis-notion-word); a case scored without one would read as the model failing the task
failure	toolathlon	agentflow	seed=1	toolathlon-canvas-assignment-deadline-word-gcal	cannot acquire env: Toolathlon runs each task against a Docker Compose project and no compose backend is wired (case toolathlon-canvas-assignment-deadline-word-gcal); a case scored without one would read as the model failing the task
failure	toolathlon	agentflow	seed=1	toolathlon-canvas-assignment-word-notion	cannot acquire env: Toolathlon runs each task against a Docker Compose project and no compose backend is wired (case toolathlon-canvas-assignment-word-notion); a case scored without one would read as the model failing the task
failure	toolathlon	agentflow	seed=1	…and 37 more
failure	toolathlon	agentflow	seed=2	toolathlon-arxiv-latex-analysis-notion-word	cannot acquire env: Toolathlon runs each task against a Docker Compose project and no compose backend is wired (case toolathlon-arxiv-latex-analysis-notion-word); a case scored without one would read as the model failing the task
failure	toolathlon	agentflow	seed=2	toolathlon-canvas-assignment-deadline-word-gcal	cannot acquire env: Toolathlon runs each task against a Docker Compose project and no compose backend is wired (case toolathlon-canvas-assignment-deadline-word-gcal); a case scored without one would read as the model failing the task
failure	toolathlon	agentflow	seed=2	toolathlon-canvas-assignment-word-notion	cannot acquire env: Toolathlon runs each task against a Docker Compose project and no compose backend is wired (case toolathlon-canvas-assignment-word-notion); a case scored without one would read as the model failing the task
failure	toolathlon	agentflow	seed=2	…and 37 more
failure	toolathlon	codeact	seed=0	toolathlon-arxiv-latex-analysis-notion-word	cannot acquire env: Toolathlon runs each task against a Docker Compose project and no compose backend is wired (case toolathlon-arxiv-latex-analysis-notion-word); a case scored without one would read as the model failing the task
failure	toolathlon	codeact	seed=0	toolathlon-canvas-assignment-deadline-word-gcal	cannot acquire env: Toolathlon runs each task against a Docker Compose project and no compose backend is wired (case toolathlon-canvas-assignment-deadline-word-gcal); a case scored without one would read as the model failing the task
failure	toolathlon	codeact	seed=0	toolathlon-canvas-assignment-word-notion	cannot acquire env: Toolathlon runs each task against a Docker Compose project and no compose backend is wired (case toolathlon-canvas-assignment-word-notion); a case scored without one would read as the model failing the task
failure	toolathlon	codeact	seed=0	…and 37 more
failure	toolathlon	codeact	seed=1	toolathlon-arxiv-latex-analysis-notion-word	cannot acquire env: Toolathlon runs each task against a Docker Compose project and no compose backend is wired (case toolathlon-arxiv-latex-analysis-notion-word); a case scored without one would read as the model failing the task
failure	toolathlon	codeact	seed=1	toolathlon-canvas-assignment-deadline-word-gcal	cannot acquire env: Toolathlon runs each task against a Docker Compose project and no compose backend is wired (case toolathlon-canvas-assignment-deadline-word-gcal); a case scored without one would read as the model failing the task
failure	toolathlon	codeact	seed=1	toolathlon-canvas-assignment-word-notion	cannot acquire env: Toolathlon runs each task against a Docker Compose project and no compose backend is wired (case toolathlon-canvas-assignment-word-notion); a case scored without one would read as the model failing the task
failure	toolathlon	codeact	seed=1	…and 37 more
failure	toolathlon	codeact	seed=2	toolathlon-arxiv-latex-analysis-notion-word	cannot acquire env: Toolathlon runs each task against a Docker Compose project and no compose backend is wired (case toolathlon-arxiv-latex-analysis-notion-word); a case scored without one would read as the model failing the task
failure	toolathlon	codeact	seed=2	toolathlon-canvas-assignment-deadline-word-gcal	cannot acquire env: Toolathlon runs each task against a Docker Compose project and no compose backend is wired (case toolathlon-canvas-assignment-deadline-word-gcal); a case scored without one would read as the model failing the task
failure	toolathlon	codeact	seed=2	toolathlon-canvas-assignment-word-notion	cannot acquire env: Toolathlon runs each task against a Docker Compose project and no compose backend is wired (case toolathlon-canvas-assignment-word-notion); a case scored without one would read as the model failing the task
failure	toolathlon	codeact	seed=2	…and 37 more
failure	toolathlon	gepa	seed=0	toolathlon-arxiv-latex-analysis-notion-word	cannot acquire env: Toolathlon runs each task against a Docker Compose project and no compose backend is wired (case toolathlon-arxiv-latex-analysis-notion-word); a case scored without one would read as the model failing the task
failure	toolathlon	gepa	seed=0	toolathlon-canvas-assignment-deadline-word-gcal	cannot acquire env: Toolathlon runs each task against a Docker Compose project and no compose backend is wired (case toolathlon-canvas-assignment-deadline-word-gcal); a case scored without one would read as the model failing the task
failure	toolathlon	gepa	seed=0	toolathlon-canvas-assignment-word-notion	cannot acquire env: Toolathlon runs each task against a Docker Compose project and no compose backend is wired (case toolathlon-canvas-assignment-word-notion); a case scored without one would read as the model failing the task
failure	toolathlon	gepa	seed=0	…and 37 more
failure	toolathlon	gepa	seed=1	toolathlon-arxiv-latex-analysis-notion-word	cannot acquire env: Toolathlon runs each task against a Docker Compose project and no compose backend is wired (case toolathlon-arxiv-latex-analysis-notion-word); a case scored without one would read as the model failing the task
failure	toolathlon	gepa	seed=1	toolathlon-canvas-assignment-deadline-word-gcal	cannot acquire env: Toolathlon runs each task against a Docker Compose project and no compose backend is wired (case toolathlon-canvas-assignment-deadline-word-gcal); a case scored without one would read as the model failing the task
failure	toolathlon	gepa	seed=1	toolathlon-canvas-assignment-word-notion	cannot acquire env: Toolathlon runs each task against a Docker Compose project and no compose backend is wired (case toolathlon-canvas-assignment-word-notion); a case scored without one would read as the model failing the task
failure	toolathlon	gepa	seed=1	…and 37 more
failure	toolathlon	gepa	seed=2	toolathlon-arxiv-latex-analysis-notion-word	cannot acquire env: Toolathlon runs each task against a Docker Compose project and no compose backend is wired (case toolathlon-arxiv-latex-analysis-notion-word); a case scored without one would read as the model failing the task
failure	toolathlon	gepa	seed=2	toolathlon-canvas-assignment-deadline-word-gcal	cannot acquire env: Toolathlon runs each task against a Docker Compose project and no compose backend is wired (case toolathlon-canvas-assignment-deadline-word-gcal); a case scored without one would read as the model failing the task
failure	toolathlon	gepa	seed=2	toolathlon-canvas-assignment-word-notion	cannot acquire env: Toolathlon runs each task against a Docker Compose project and no compose backend is wired (case toolathlon-canvas-assignment-word-notion); a case scored without one would read as the model failing the task
failure	toolathlon	gepa	seed=2	…and 37 more
failure	toolathlon	nql	seed=0	toolathlon-arxiv-latex-analysis-notion-word	cannot acquire env: Toolathlon runs each task against a Docker Compose project and no compose backend is wired (case toolathlon-arxiv-latex-analysis-notion-word); a case scored without one would read as the model failing the task
failure	toolathlon	nql	seed=0	toolathlon-canvas-assignment-deadline-word-gcal	cannot acquire env: Toolathlon runs each task against a Docker Compose project and no compose backend is wired (case toolathlon-canvas-assignment-deadline-word-gcal); a case scored without one would read as the model failing the task
failure	toolathlon	nql	seed=0	toolathlon-canvas-assignment-word-notion	cannot acquire env: Toolathlon runs each task against a Docker Compose project and no compose backend is wired (case toolathlon-canvas-assignment-word-notion); a case scored without one would read as the model failing the task
failure	toolathlon	nql	seed=0	…and 37 more
failure	toolathlon	nql	seed=1	toolathlon-arxiv-latex-analysis-notion-word	cannot acquire env: Toolathlon runs each task against a Docker Compose project and no compose backend is wired (case toolathlon-arxiv-latex-analysis-notion-word); a case scored without one would read as the model failing the task
failure	toolathlon	nql	seed=1	toolathlon-canvas-assignment-deadline-word-gcal	cannot acquire env: Toolathlon runs each task against a Docker Compose project and no compose backend is wired (case toolathlon-canvas-assignment-deadline-word-gcal); a case scored without one would read as the model failing the task
failure	toolathlon	nql	seed=1	toolathlon-canvas-assignment-word-notion	cannot acquire env: Toolathlon runs each task against a Docker Compose project and no compose backend is wired (case toolathlon-canvas-assignment-word-notion); a case scored without one would read as the model failing the task
failure	toolathlon	nql	seed=1	…and 37 more
failure	toolathlon	nql	seed=2	toolathlon-arxiv-latex-analysis-notion-word	cannot acquire env: Toolathlon runs each task against a Docker Compose project and no compose backend is wired (case toolathlon-arxiv-latex-analysis-notion-word); a case scored without one would read as the model failing the task
failure	toolathlon	nql	seed=2	toolathlon-canvas-assignment-deadline-word-gcal	cannot acquire env: Toolathlon runs each task against a Docker Compose project and no compose backend is wired (case toolathlon-canvas-assignment-deadline-word-gcal); a case scored without one would read as the model failing the task
failure	toolathlon	nql	seed=2	toolathlon-canvas-assignment-word-notion	cannot acquire env: Toolathlon runs each task against a Docker Compose project and no compose backend is wired (case toolathlon-canvas-assignment-word-notion); a case scored without one would read as the model failing the task
failure	toolathlon	nql	seed=2	…and 37 more

errored	1440
