#!/usr/bin/env python
# coding: utf-8

# In[24]:


import streamlit as st
import requests
import PyPDF2
import psycopg2
from influxdb_client import InfluxDBClient
import re

query_patterns = {
    "list_buckets": r"\b(list|show)\s+(all\s+)?buckets?\b",
    "list_measurements": r"\b(list|show)\s+(all\s+)?measurements?\b",
    "show_fields": r"\b(show|list)\s+(all\s+)?fields?\s+(in|from)\s+([a-z0-9_]+)\b",
    "field_data": r"\b(get|show|latest|average|min|max|count|sum|first)\s+(?:field\s+)?([a-zA-Z0-9_]+).*?(?:from|in).*?(?:measurement\s+)?([a-z0-9_]+).*?(?:bucket\s+)?([a-zA-Z0-9_]+)(?:.*?(?:last|past)?\s+(\d+)\s*(seconds|minutes|hours|days|weeks|d|h|m|s))?"
}


if "model_ready" not in st.session_state:
    st.session_state.model_ready = False

# --- PostgreSQL Chat History ---
def insert_chat(prompt, response):
    try:
        conn = psycopg2.connect(
            host="localhost",
            database="SLM",
            user="postgres",
            password="Vallika@2003"
        )
        cursor = conn.cursor()
        cursor.execute("INSERT INTO chat_history (prompt, response) VALUES (%s, %s)", (prompt, response))
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        st.error(f"DB Error: {e}")

def fetch_recent_history(n=10):
    try:
        conn = psycopg2.connect(
            host="localhost",
            database="SLM",
            user="postgres",
            password="Vallika@2003"
        )
        cursor = conn.cursor()
        cursor.execute("SELECT prompt, response FROM chat_history ORDER BY timestamp DESC LIMIT %s", (n,))
        records = cursor.fetchall()
        cursor.close()
        conn.close()
        return records
    except Exception as e:
        st.error(f"DB Error while fetching history: {e}")
        return []

# --- InfluxDB Setup ---
INFLUX_URL = "http://localhost:8086"
INFLUX_TOKEN = "awGCB8S8I79Frtj_sptg3u1ZAsKIufnlIN4s7vm5trw-LLq1M7dDWvy3VtdQOBu3Nlznq7oJkSvkCRYBvM78ig=="
INFLUX_ORG = "IIIOT"
INFLUX_BUCKETS = ["A", "B"]

# --- Field Keywords ---
field_keywords = {
    "temperature_2m": ["temp", "temperature", "temperature_2m"],
    "relative_humidity_2m": ["humidity", "rh", "relative_humidity_2m"],
    "pressure_msl": ["pressure", "pres", "pressure_msl"],
    "windspeed_10m": ["wind", "wind_spd", "windspeed_10m"],
    "cloudcover": ["cloud", "clouds", "cloudcover"]
}

# --- Parse User Query ---
def parse_nlp_intent(user_text):
    user_text = user_text.lower()
    for intent, pattern in query_patterns.items():
        match = re.search(pattern, user_text)
        if match:
            if intent == "show_fields":
                return {
                    "query_type": "show_fields",
                    "bucket": match.group(4)
                }
            elif intent == "field_data":
                groups = match.groups()
                duration_value = groups[5] if len(groups) > 5 and groups[5] else "30"
                duration_unit = groups[6] if len(groups) > 6 and groups[6] else "days"
                if duration_unit in ["h", "hours", "m", "minutes", "s", "seconds"]:
                    duration_value = "30"
                    duration_unit = "d"
                return {
                    "query_type": "field_data",
                    "function": match.group(1),
                    "field": match.group(2),
                    "measurement": match.group(3),
                    "bucket": match.group(4).upper() if match.group(4).upper() in INFLUX_BUCKETS else INFLUX_BUCKETS[0],
                    "duration_value": duration_value,
                    "duration_unit": duration_unit
                }

            elif intent in ["list_buckets", "list_measurements"]:
                return {"query_type": intent}
    return {"query_type": "unknown"}


def generate_flux_query(intent):
    qtype = intent.get("query_type")
    if qtype == "show_fields":
        return f'import "influxdata/influxdb/schema"\nschema.fieldKeys(bucket: "{intent["bucket"]}") |> limit(n: 100)'
    elif qtype == "field_data":
        bucket = intent.get("bucket")
        measurement = intent.get("measurement")
        field = intent.get("field")
        duration_value = 30
        duration_unit = 'd'
        func = intent.get("function", "latest").lower()
        base_query = f'''
            from(bucket: "{bucket}")
            |> range(start: -{duration_value}{duration_unit})
            |> filter(fn: (r) => r["_measurement"] == "{measurement}")
            |> filter(fn: (r) => r["_field"] == "{field}")
            '''
        if func in ["average", "mean"]:
            return base_query + '|> aggregateWindow(every: 1h, fn: mean, createEmpty: false)\n|> yield(name: "mean")'
        elif func == "min":
            return base_query + '|> aggregateWindow(every: 1h, fn: min, createEmpty: false)\n|> yield(name: "min")'
        elif func == "max":
            return base_query + '|> aggregateWindow(every: 1h, fn: max, createEmpty: false)\n|> yield(name: "max")'
        elif func == "count":
            return base_query + '|> aggregateWindow(every: 1h, fn: count, createEmpty: false)\n|> yield(name: "count")'
        elif func == "sum":
            return base_query + '|> aggregateWindow(every: 1h, fn: sum, createEmpty: false)\n|> yield(name: "sum")'
        elif func == "first":
            return base_query + '|> first()'
        else:  # latest, get, show
            return base_query + '|> limit(n: 50)\n|> sort(columns: ["_time"], desc: true)'
        
    elif qtype == "list_buckets":
        return 'buckets() |> keep(columns: ["name"])'
    elif qtype == "list_measurements":
        return 'import "influxdata/influxdb/schema"\nschema.measurements(bucket: "A") |> limit(n: 100)'  # Default bucket
    else:
        return None


# --- InfluxDB Query ---
def run_flux_query(query):
    try:
        client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
        query_api = client.query_api()
        result = query_api.query(org=INFLUX_ORG, query=query)

        structured_data = []
        raw_output = []

        for table in result:
            for record in table.records:
                time = record.get_time()
                field = record.get_field()
                value = record.get_value()
                raw_output.append(f"{time} — {field} = {value}")
                structured_data.append((time, field, value))

        return "\n".join(raw_output), structured_data

    except Exception as e:
        return f"❌ Error executing Flux query: {e}", []

def summarize_sensor_data(structured_data):
    if not structured_data:
        return "⚠️ No data returned from sensor query."

    summary = {}
    for time, field, value in structured_data:
        if isinstance(value, (int, float)):  # Only process numeric values
            if field not in summary:
                summary[field] = []
            summary[field].append(value)

    lines = []
    for field, values in summary.items():
        avg = sum(values) / len(values)
        min_val = min(values)
        max_val = max(values)
        count = len(values)
        lines.append(
            f"- **{field}**: {count} records | avg: `{avg:.2f}` | min: `{min_val:.2f}` | max: `{max_val:.2f}`"
        )

    return "\n".join(lines)


# --- Streamlit UI ---
st.title("💬 Chat with OpenHermes + Sensor AI")

uploaded_pdf = st.file_uploader("📄 Upload a PDF", type=["pdf"])
pdf_text = ""
if uploaded_pdf:
    reader = PyPDF2.PdfReader(uploaded_pdf)
    for page in reader.pages:
        text = page.extract_text()
        if text:
            pdf_text += text.strip() + "\n"

user_input = st.text_area("💬 Ask your question:", height=100)

if st.button("Send"):
    if not user_input.strip():
        st.warning("⚠️ Please enter a question.")
    else:
        final_prompt = "You are a highly accurate assistant. ONLY use the provided sensor summary to answer the question. Do NOT assume anything beyond the sensor data.If sensor data is present, answer based only on it. If no data is found, say so.\n"

        if pdf_text:
            final_prompt += f"\n📄 Document:\n{pdf_text[:1500]}..."

        # --- Parse user query via NLP
        parsed_intent = parse_nlp_intent(user_input)
        st.write("🧠 Parsed Intent:", parsed_intent)
        flux_query = generate_flux_query(parsed_intent)
        st.write("🧠 Parsed NLP Intent:", parsed_intent)
        if not flux_query:
            st.error("❌ Could not parse your request into a valid query.")
        else:
            st.subheader("📡 Raw Sensor Output")
            st.code(flux_query, language="flux")
            # ✅ FIX: Fetch query results
            raw_result, structured_data = run_flux_query(flux_query)
            summary_result = summarize_sensor_data(structured_data)

            st.text_area("Result", raw_result, height=200)
            st.subheader("📈 Sensor Summary")
            st.markdown(summary_result)

            # Add chat history
            history = fetch_recent_history()
            if history:
                final_prompt += "\n\n🧠 Past Q&A:\n"
                final_prompt += "\n".join([f"Q: {p}\nA: {r}" for p, r in history])

            final_prompt += f"\n\n📡 Sensor Summary:\n{summary_result}"
            final_prompt += f"\n\n❓ Question:\n{user_input}"
            
            # Store for model
            st.session_state.model_ready = True
            st.session_state.last_prompt = final_prompt
            st.session_state.last_result = raw_result
            st.session_state.last_user_input = user_input
            
            # --- Send to Ollama
            try:
                response = requests.post(
                    "http://localhost:11434/api/generate",
                    json={"model": "openhermes", "prompt": final_prompt, "stream": False}
                )
                if response.ok:
                    result_json = response.json()
                    answer = result_json.get("response", "No response from model.")
                    st.text_area("🧠 Model Response:", value=answer, height=200)
                    insert_chat(user_input, answer)
                else:
                    st.error(f"❌ Ollama Error: {response.status_code}")
            except Exception as e:
                st.error(f"❌ Ollama call failed: {e}")


# In[ ]:




