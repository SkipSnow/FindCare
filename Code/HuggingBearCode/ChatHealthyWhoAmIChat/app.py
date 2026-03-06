from dotenv import load_dotenv
from openai import OpenAI
from anthropic import Anthropic
import json
import os
import requests
from pypdf import PdfReader
import gradio as gr
from ChatHealthyMongoUtilities import ChatHealthyMongoUtilities


load_dotenv(override=True)

# MongoDB - lazy connection (connects on first use; app starts even if Mongo is unreachable)
_mongo_conn_str = os.getenv("MONGO_connectionString") or ""
DBManager = ChatHealthyMongoUtilities(
    _mongo_conn_str,
    connect_timeout_ms=10000,
    server_selection_timeout_ms=15000,
    max_retries=2,
) if _mongo_conn_str else None


def _get_db():
    """Returns MongoDB database or None if connection fails or missing. Logs and continues on failure."""
    if DBManager is None:
        return None
    try:
        return DBManager.getConnection()
    except Exception as e:
        print(f"MongoDB unavailable (chat will work; recordings skipped): {e}", flush=True)
        return None


# Pushover
pushover_user = os.getenv("PUSHOVER_USER")
pushover_token = os.getenv("PUSHOVER_TOKEN")
pushover_url = "https://api.pushover.net/1/messages.json"


def push(message):
    print(f"Push: {message}")
    payload = {"user": pushover_user, "token": pushover_token, "message": message}
    requests.post(pushover_url, data=payload)


def commitSignificantActivity(payload=None, **kwargs):
    """Accept a JSON payload and insert into DB. Payload: {database, collection, record}."""
    from datetime import datetime
    db = _get_db()
    if db is None:
        return {"recorded": "ok", "note": "MongoDB unavailable; record not persisted"}
    payload = payload or kwargs
    if isinstance(payload, str):
        payload = json.loads(payload)
    database = payload["database"]
    collection = payload["collection"]
    record = dict(payload["record"])
    record["record_number"] = db[database][collection].count_documents({}) + 1
    record["datetime"] = datetime.now().isoformat()
    db[database][collection].insert_one(record)
    return {"recorded": "ok"}


def _format_chat_history(messages):
    """Extract user/assistant content from messages for storage."""
    out = []
    for m in messages:
        if m.get("role") not in ("user", "assistant"):
            continue
        c = m.get("content")
        text = str(c)[:500] if c else ""
        out.append({"role": m["role"], "content": text})
    return out


def record_user_details(email="", name="Name not provided", notes="not provided", message="", chat_history=None):
    if not email or not str(email).strip():
        return {"recorded": "ok", "note": "Email required but not provided"}
    db = _get_db()
    if db is None:
        push(f"Recording interest from {name} with email {email} (DB unavailable)")
        return {"recorded": "ok", "note": "MongoDB unavailable; contact logged via push only"}
    reason = message or notes
    lead_coll = db["AboutUs"]["lead"]
    for doc in lead_coll.find():
        if email in str(doc.get("email", "")):
            return {"recorded": "ok"}
    push(f"Recording interest from {name} with email {email}: {reason}")
    payload = {
        "database": "AboutUs", "collection": "lead",
        "record": {"email": email, "name": name, "notes": notes, "reason_for_contact": reason, "chat_history": chat_history or []}
    }
    commitSignificantActivity(payload)
    return {"recorded": "ok"}


def deIdentify(argChat_history):
    """
    Takes the list used to create the history array in the output object, and deidentifies each member.
    Deidentification is sufficient for HIPAA Research Data. Uses Anthropic model claude-haiku-4-5-20251001.
    Batches the entire conversation in a single API call. Mutates argChat_history in place.
    """
    if not argChat_history:
        return
    client = Anthropic(api_key=os.getenv("Anthropic_API_KEY"))
    model = "claude-haiku-4-5-20251001"
    chat_json = json.dumps([{"role": m.get("role", ""), "content": m.get("content") or ""} for m in argChat_history], indent=2)
    deidentify_prompt = """Deidentify the following chat conversation so it meets HIPAA Safe Harbor requirements for research data.
Remove or replace: names, geographic identifiers (except state), dates (except year), phone/fax, email, SSN,
medical record numbers, account numbers, license numbers, vehicle identifiers, device identifiers, URLs, IP addresses,
and any other identifiers that could be used to identify an individual.
Preserve the semantic meaning of each message for research purposes.

Return ONLY a valid JSON array of strings, one string per message in the same order. Each string is the deidentified content for that message.
Example: ["deidentified msg 1", "deidentified msg 2"]

Chat conversation:
"""
    response = client.messages.create(
        model=model,
        max_tokens=4096,
        messages=[{"role": "user", "content": f"{deidentify_prompt}\n{chat_json}"}],
    )
    result_text = response.content[0].text.strip()
    if result_text.startswith("```"):
        result_text = result_text.split("```")[1]
        if result_text.startswith("json"):
            result_text = result_text[4:]
        result_text = result_text.strip()
    deidentified_contents = json.loads(result_text)
    for i, content in enumerate(deidentified_contents):
        if i < len(argChat_history):
            argChat_history[i]["content"] = content


def record_unknown_question(question, chat_history=None):
    if chat_history is not None:
        deIdentify(chat_history)
    push(f"Recording a user question I could not answer: {question}")
    payload = {
        "database": "AboutUs", "collection": "AboutSkip",
        "record": {"question": question, "chat_history": chat_history or []}
    }
    commitSignificantActivity(payload)
    return {"recorded": "ok"}


record_user_details_json = {
    "name": "record_user_details",
    "description": "Use this tool to record that a user is interested in being in touch and provided an email address. Try to encourage the user to describe why they are wishing contact. Try to get their name. Do not insist on either.",
    "parameters": {
        "type": "object",
        "properties": {
            "email": {"type": "string", "description": "The email address of this user"},
            "name": {"type": "string", "description": "The user's name, if they provided it"},
            "notes": {"type": "string", "description": "You, summarize the conversation in less than 40 words"},
            "message": {"type": "string", "description": "Why they are contacting us - what they want, what they're interested in, or the key message from the conversation"}
        },
        "required": ["email", "notes"],
        "additionalProperties": False
    }
}

record_unknown_question_json = {
    "name": "record_unknown_question",
    "description": "Always use this tool to record all questions that you couldn't answer including questions where you don't have specific details",
    "parameters": {
        "type": "object",
        "properties": {
            "question": {"type": "string", "description": "The question that couldn't be answered"}
        },
        "required": ["question"],
        "additionalProperties": False
    }
}

commitSignificantActivity_json = {
    "name": "commitSignificantActivity",
    "description": "Record any custom activity to the database. Supply database, collection, and record (any JSON structure). Use for custom records beyond contacts and unknown questions.",
    "parameters": {
        "type": "object",
        "properties": {
            "database": {"type": "string", "description": "Database name"},
            "collection": {"type": "string", "description": "Collection name"},
            "record": {"type": "object", "description": "The document to insert - any JSON object. record_number and datetime are added automatically."}
        },
        "required": ["database", "collection", "record"]
    }
}

tools = [
    {"type": "function", "function": record_user_details_json},
    {"type": "function", "function": record_unknown_question_json},
    {"type": "function", "function": commitSignificantActivity_json}
]


# Path to me/ directory relative to this script (works for local runs and deployment)
_ME_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "me")


class Me:

    def __init__(self):
        self.openai = OpenAI()
        self.name = "Skip Snow"
        self.website = "ChatHealthy.AI"
        reader = PdfReader(os.path.join(_ME_DIR, "SkipSnowLinkedInProfile.pdf"))
        self.linkedin = ""
        for page in reader.pages:
            text = page.extract_text()
            if text:
                self.linkedin += text
        reader_anthropic = PdfReader(os.path.join(_ME_DIR, "BuildingAnthropicAConversationWithItsCo-foundersYouTube.pdf"))
        self.anthropic_discussion = ""
        for page in reader_anthropic.pages:
            text = page.extract_text()
            if text:
                self.anthropic_discussion += text
        with open(os.path.join(_ME_DIR, "summary.txt"), "r", encoding="utf-8") as f:
            self.summary = f.read()

    def handle_tool_calls(self, tool_calls, messages=None):
        chat_history = _format_chat_history(messages) if messages else []
        results = []
        for tool_call in tool_calls:
            tool_name = tool_call.function.name
            arguments = json.loads(tool_call.function.arguments)
            if tool_name in ("record_user_details", "record_unknown_question"):
                arguments["chat_history"] = chat_history
            print(f"Tool called: {tool_name}", flush=True)
            tool = globals().get(tool_name)
            result = tool(**arguments) if tool else {}
            results.append({"role": "tool", "content": json.dumps(result), "tool_call_id": tool_call.id})
        return results

    def system_prompt(self):
        return (
            f"You are acting as {self.name}. You are answering questions on {self.website}'s website, "
            f"particularly questions related to {self.name}'s career, background, skills and experience and plans the future of this web site. "
            f"Your responsibility is to represent {self.name} and {self.website} for interactions on the website as faithfully as possible. "
            f"You are given a summary of {self.name}'s background and LinkedIn profile which you can use to answer questions. "
            f"Be professional and engaging, as if talking to a potential client or future employer who came across the website. "
            f"Be careful not to answer questions that you are not certain of. Use your tools if you have any doubts about your answer being based in fact. "
            f"If you don't know the answer to any question, use your record_unknown_question tool to record the question that you couldn't answer, even if it's about something trivial or unrelated to career. "
            f"You MUST call record_unknown_question EVERY time you cannot answer - each unknown question in the conversation must be recorded separately. "
            f"If the user is engaging in discussion, try to steer them towards getting in touch via email, phone or linkedin; ask for their email, or other method of communication. No authentication needed. "
            f"Record contact using your record_user_details tool - always include the 'message' parameter with why they are contacting us. Call it only ONCE per contact; if you have already recorded this user's email in this conversation, do not call it again. "
            f"If the user gives an email, and you don't know their name capture their name too. "
            f"Do not hesitate to say that you don't know an answer. For example: User prompt: What is your favorite poem? "
            f"Bad Answer: While I have a deep appreciation for many forms of art, including poetry, I don't have a specific favorite poem that stands out to me. "
            f"Good answer: I don't know the answer to that, let me get back to you. You must call record_unknown_question every single time you don't know an answer - no exceptions, even for follow-up unknown questions.\n\n"
            f"## Summary:\n{self.summary}\n\n## LinkedIn Profile:\n{self.linkedin}\n\n"
            f"## AnthropicOnSafety:\n{self.anthropic_discussion}\n\n"
            f"With this context, please chat with the user, always staying in character as {self.name}."
        )

    def chat(self, message, history):
        messages = [{"role": "system", "content": self.system_prompt()}] + history + [{"role": "user", "content": message}]
        done = False
        while not done:
            response = self.openai.chat.completions.create(model="gpt-4o-mini", messages=messages, tools=tools)
            if response.choices[0].finish_reason == "tool_calls":
                msg = response.choices[0].message
                tool_calls = msg.tool_calls
                results = self.handle_tool_calls(tool_calls, messages)
                messages.append(msg)
                messages.extend(results)
            else:
                done = True
        return response.choices[0].message.content


if __name__ == "__main__":
    me = Me()
    gr.ChatInterface(me.chat, type="messages", title="Chat Healthy: About Us").launch(share=True)
