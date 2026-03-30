# BatMan Demo: One-Click Data Pipeline Synthesis with Conversational Editing

---

This repository is the official implementation of "BatMan Demo: One-Click Data Pipeline Synthesis with Conversational Editing"


Automated Data Preparation (ADP) aims to transform diverse relational tables into a specified target table, serving as a fundamental prerequisite for downstream applications such as business intelligence analysis and machine learning. However, existing ADP tools heavily rely on external knowledge (e.g., explicit input-output examples or transformation patterns). This poses a major hurdle for non-experts, since they lack the knowledge of data transformations. To address this limitation, we present BatMan, an end-to-end data pipeline synthesizer featuring one-click generation and conversational editing. BatMan autonomously navigates the huge transformation space without external supervisory signals, leveraging an LLM-driven Monte Carlo tree search bounded by a dedicated action sandbox. To democratize data preparation for non-expert users, BatMan abstracts opaque transformation code into a transparent visual interface, empowering users to iteratively refine the generated pipeline via natural-language feedback. In this demonstration, the VLDB audience explores a real-world ADP scenario to experience how BatMan seamlessly generates, visually inspects, and conversationally refines complex data pipelines.

---

## Requirements

```bash
cd src/backend
pip install -r requirements.txt

cd ../frontend
npm install
```

---

## Quickstart

### 1) Start Backend

Linux/macOS:

```bash
bash scripts/start-backend.sh \
	--api-base-url "https://openrouter.ai/api/v1" \
	--api-key "your-key" \
	--generation-model "openai/gpt-5.4" \
	--explanation-model "openai/gpt-5.4" \
	--timeout-seconds 120 \
	--session-ttl-seconds 7200 \
	--host "127.0.0.1" \
	--port 8001
```

Windows PowerShell:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/start-backend.ps1 `
	-ApiBaseUrl "https://openrouter.ai/api/v1" `
	-ApiKey "your-key" `
	-GenerationModel "openai/gpt-5.4" `
	-ExplanationModel "openai/gpt-5.4" `
	-TimeoutSeconds 120 `
	-SessionTtlSeconds 7200 `
	-Host "127.0.0.1" `
	-Port 8001
```

### 2) Start Frontend

Linux/macOS:

```bash
bash scripts/start-frontend.sh \
	--api-base-url "http://127.0.0.1:8001" \
	--host "127.0.0.1" \
	--port 3000
```

Windows PowerShell:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/start-frontend.ps1 `
	-ApiBaseUrl "http://127.0.0.1:8001" `
	-Host "127.0.0.1" `
	-Port 3000
```