from transformers import pipeline, AutoTokenizer, AutoModelForSeq2SeqLM

# --- Load a small open-source model ---
# (Use a big model later after successful testing)
model_name = "google/flan-t5-small"

tokenizer = AutoTokenizer.from_pretrained(model_name)
model = AutoModelForSeq2SeqLM.from_pretrained(model_name)

pipe = pipeline("text2text-generation", model=model, tokenizer=tokenizer)

# --- Read your lineage text file ---
with open("lineage.txt", "r", encoding="utf-8") as f:
    lineage_text = f.read()

# --- Prepare smart prompt ---
prompt = f"""
You are a data lineage expert.

Given the following column-level lineage for curated tables:
(Note: Source table names are missing. Assume if not mentioned, the column comes from the immediately previous layer.)

Lineage:
{lineage_text}

Instructions:
1. Summarize the lineage in a clean tabular format.
2. Assume missing source tables logically.
3. Identify derived columns and possible transformations.
4. Highlight dropped columns if visible.
5. Create a simple mermaid flow diagram if possible.

Be precise, technical, and clean in your output.
"""

# --- Send to Gen AI model ---
output = pipe(prompt, max_length=4096)[0]['generated_text']

# --- Save output ---
with open("genai_response.txt", "w", encoding="utf-8") as f:
    f.write(output)

print("GenAI Response saved to 'genai_response.txt'")
